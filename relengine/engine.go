package relengine

import (
	"context"
	"fmt"
	"sync"
)

// Engine 根据已绑定的逻辑计划构造物理执行器。它不包含任何 SQL 解析器，
// 调用方直接组合逻辑计划；这保证测试可以构造任意多棵计划树，而不是一条写死查询。
type Engine struct {
	opts ExecOptions

	mu       sync.Mutex
	lastPool *MemManager
}

func NewEngine(opts ExecOptions) *Engine {
	return &Engine{opts: opts.withDefaults()}
}

// Execute 绑定计划并执行，返回全部结果行。
func (en *Engine) Execute(ctx context.Context, plan LogicalPlan) ([]Tuple, *Schema, error) {
	schema, err := plan.Bind()
	if err != nil {
		return nil, nil, err
	}
	opts := en.opts
	if opts.Pool == nil {
		// 每个查询独立的额度池：同一查询内的多个连接共享这一份额度，
		// 不同查询的取消/失败互不影响。
		opts.Pool = NewMemManager(opts.MemRows)
	}
	en.mu.Lock()
	en.lastPool = opts.Pool
	en.mu.Unlock()
	exec, err := en.build(plan, opts)
	if err != nil {
		return nil, nil, err
	}
	rows, err := Drain(ctx, exec)
	if err != nil {
		return nil, nil, err
	}
	return rows, schema, nil
}

// LastMemStats 返回最近一次 Execute 使用的额度池快照，用于报告实际占用。
// 若 ExecOptions.Pool 由调用方提供，也可直接对该池调用 Stats()。
func (en *Engine) LastMemStats() (MemStats, bool) {
	en.mu.Lock()
	defer en.mu.Unlock()
	if en.lastPool == nil {
		return MemStats{}, false
	}
	return en.lastPool.Stats(), true
}

func (en *Engine) build(plan LogicalPlan, opts ExecOptions) (Executor, error) {
	switch p := plan.(type) {
	case *Scan:
		return &scanExec{tbl: p.Table, sz: opts.BatchRows}, nil
	case *Filter:
		child, err := en.build(p.Child, opts)
		if err != nil {
			return nil, err
		}
		return &filterExec{child: child, cond: p.Cond, sz: opts.BatchRows}, nil
	case *Project:
		child, err := en.build(p.Child, opts)
		if err != nil {
			return nil, err
		}
		return &projectExec{child: child, items: p.Items}, nil
	case *Join:
		left, err := en.build(p.L, opts)
		if err != nil {
			return nil, err
		}
		right, err := en.build(p.R, opts)
		if err != nil {
			return nil, err
		}
		return BuildJoin(left, right, p.On, p.Type,
			len(p.L.Schema().Cols), len(p.R.Schema().Cols), opts), nil
	case *Aggregate:
		child, err := en.build(p.Child, opts)
		if err != nil {
			return nil, err
		}
		return BuildAggregate(child, p, opts, opts.SpillAggregate)
	default:
		return nil, fmt.Errorf("未知逻辑计划节点 %T", plan)
	}
}
