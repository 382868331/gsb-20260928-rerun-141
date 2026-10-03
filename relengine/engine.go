package relengine

import (
	"context"
	"fmt"
)

// Engine 根据已绑定的逻辑计划构造物理执行器。它不包含任何 SQL 解析器，
// 调用方直接组合逻辑计划；这保证测试可以构造任意多棵计划树，而不是一条写死查询。
type Engine struct {
	opts ExecOptions
	pool *MemPool
}

func NewEngine(opts ExecOptions) *Engine {
	return &Engine{opts: opts.withDefaults()}
}

// Pool 返回最近一次 Execute 使用的内存池（用于核对实际占用）。
func (en *Engine) Pool() *MemPool { return en.pool }

// Execute 绑定计划并执行，返回全部结果行。
func (en *Engine) Execute(ctx context.Context, plan LogicalPlan) ([]Tuple, *Schema, error) {
	opts := en.opts
	if opts.Pool == nil {
		opts.Pool = NewMemPool(opts.MemRows)
	}
	en.pool = opts.Pool
	schema, err := plan.Bind()
	if err != nil {
		return nil, nil, err
	}
	exec, err := en.buildWith(plan, opts)
	if err != nil {
		return nil, nil, err
	}
	rows, err := Drain(ctx, exec)
	if err != nil {
		return nil, nil, err
	}
	return rows, schema, nil
}

func (en *Engine) build(plan LogicalPlan) (Executor, error) {
	return en.buildWith(plan, en.opts)
}

func (en *Engine) buildWith(plan LogicalPlan, opts ExecOptions) (Executor, error) {
	switch p := plan.(type) {
	case *Scan:
		return &scanExec{tbl: p.Table, sz: opts.BatchRows}, nil
	case *Filter:
		child, err := en.buildWith(p.Child, opts)
		if err != nil {
			return nil, err
		}
		return &filterExec{child: child, cond: p.Cond, sz: opts.BatchRows}, nil
	case *Project:
		child, err := en.buildWith(p.Child, opts)
		if err != nil {
			return nil, err
		}
		return &projectExec{child: child, items: p.Items}, nil
	case *Join:
		left, err := en.buildWith(p.L, opts)
		if err != nil {
			return nil, err
		}
		right, err := en.buildWith(p.R, opts)
		if err != nil {
			return nil, err
		}
		return BuildJoin(left, right, p.On, p.Type,
			len(p.L.Schema().Cols), len(p.R.Schema().Cols), opts), nil
	case *Aggregate:
		child, err := en.buildWith(p.Child, opts)
		if err != nil {
			return nil, err
		}
		return BuildAggregate(child, p, opts, opts.SpillAggregate)
	default:
		return nil, fmt.Errorf("未知逻辑计划节点 %T", plan)
	}
}
