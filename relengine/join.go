package relengine

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// 连接放大语义：匹配 k 行输出 k 行；重复行做笛卡尔放大；ON 仅 TRUE 匹配；
// LEFT 未匹配右侧补 NULL，RIGHT 未匹配左侧补 NULL。
func eqJoinKeys(on Expr, ln int) (leftEq, rightEq []int) {
	var walk func(Expr)
	walk = func(e Expr) {
		be, ok := e.(*BinaryExpr)
		if !ok {
			return
		}
		if be.Op == OpAnd {
			walk(be.L)
			walk(be.R)
			return
		}
		if be.Op != OpEq {
			return
		}
		lc, lok := be.L.(*ColumnRef)
		rc, rok := be.R.(*ColumnRef)
		if !lok || !rok {
			return
		}
		if lc.idx < ln && rc.idx >= ln {
			leftEq = append(leftEq, lc.idx)
			rightEq = append(rightEq, rc.idx-ln)
		} else if rc.idx < ln && lc.idx >= ln {
			leftEq = append(leftEq, rc.idx)
			rightEq = append(rightEq, lc.idx-ln)
		}
	}
	walk(on)
	return leftEq, rightEq
}

type joinExec struct {
	l, r     Executor
	on       Expr
	jt       JoinType
	ln, rn   int
	opts     ExecOptions
	prepared bool
	inner    Executor
}

func BuildJoin(l, r Executor, on Expr, jt JoinType, ln, rn int, opts ExecOptions) Executor {
	return &joinExec{l: l, r: r, on: on, jt: jt, ln: ln, rn: rn, opts: opts}
}

func (e *joinExec) Close() error {
	if e.inner != nil {
		_ = e.inner.Close()
	}
	if err := e.l.Close(); err != nil {
		return err
	}
	return e.r.Close()
}

func (e *joinExec) prepare(ctx context.Context) error {
	if e.opts.MemRows < 1 || e.opts.BatchRows < 1 {
		return fmt.Errorf("%w: MemRows=%d BatchRows=%d",
			ErrInsufficientBudget, e.opts.MemRows, e.opts.BatchRows)
	}
	leftEq, rightEq := eqJoinKeys(e.on, e.ln)
	if e.jt == LeftJoin {
		b, err := collectLimited(ctx, e.r, e.opts.MemRows, e.opts.Pool, "join-build(right)")
		if err != nil {
			return err
		}
		if b.spilled {
			e.inner = newHashJoinSpill(e.l, e.r, b.pending, e.on, e.jt, e.ln, e.rn,
				leftEq, rightEq, true, e.opts)
			return nil
		}
		e.inner = newMemHashJoin(e.l, b.rows, e.on, e.jt, e.ln, e.rn,
			leftEq, rightEq, true, e.opts.BatchRows, b.grants)
		return nil
	}
	b, err := collectLimited(ctx, e.l, e.opts.MemRows, e.opts.Pool, "join-build(left)")
	if err != nil {
		return err
	}
	if b.spilled {
		e.inner = newHashJoinSpill(e.r, e.l, b.pending, e.on, e.jt, e.ln, e.rn,
			rightEq, leftEq, false, e.opts)
		return nil
	}
	e.inner = newMemHashJoin(e.r, b.rows, e.on, e.jt, e.ln, e.rn,
		rightEq, leftEq, false, e.opts.BatchRows, b.grants)
	return nil
}

func (e *joinExec) Next(ctx context.Context) (*Batch, error) {
	if !e.prepared {
		if err := e.prepare(ctx); err != nil {
			if e.inner != nil {
				_ = e.inner.Close()
			}
			return nil, err
		}
		e.prepared = true
	}
	return e.inner.Next(ctx)
}

type collected struct {
	rows    []Tuple
	pending []Tuple
	spilled bool
	grants  []*Grant
}

// collectLimited 收集构建侧行，最多在内存保留 limit 行；pool 非空时
// 每批都向共享池记账。额度不足（被同查询的其他阶段占用）或超过 limit 时
// 转入落盘路径：已占额度立即释放，已收集行交给调用方写入临时分片。
func collectLimited(ctx context.Context, src Executor, limit int, pool *MemManager, owner string) (*collected, error) {
	c := &collected{}
	release := func() {
		releaseGrants(c.grants)
		c.grants = nil
	}
	for {
		if err := ctx.Err(); err != nil {
			release()
			return nil, err
		}
		b, err := src.Next(ctx)
		if errors.Is(err, io.EOF) {
			return c, nil
		}
		if err != nil {
			release()
			return nil, err
		}
		if pool != nil && len(b.Rows) > 0 {
			g := pool.TryAcquire(len(b.Rows), owner)
			if g == nil {
				// 共享额度被其他阶段占用：转入临时存储而不是重复计算额度。
				pool.NoteSpill(owner, len(c.rows)+len(b.Rows),
					"共享内存额度不足，构建侧转入临时存储")
				release()
				c.spilled = true
				c.pending = append(c.rows, b.Rows...)
				c.rows = nil
				return c, nil
			}
			c.grants = append(c.grants, g)
		}
		c.rows = append(c.rows, b.Rows...)
		if len(c.rows) > limit {
			if pool != nil {
				pool.NoteSpill(owner, len(c.rows),
					"构建侧超过单算子内存上限 MemRows，转入临时存储")
			}
			release()
			c.spilled = true
			c.pending = c.rows
			c.rows = nil
			return c, nil
		}
	}
}

// releaseGrants 释放一组额度（取消、局部失败与正常 Close 共用）。
func releaseGrants(grants []*Grant) {
	for _, g := range grants {
		g.Release()
	}
}

type memHashJoin struct {
	probe        Executor
	ht           map[string][]Tuple
	allBuild     []Tuple
	grants       []*Grant
	on           Expr
	jt           JoinType
	ln, rn       int
	probeEq      []int
	buildEq      []int
	probeLeft    bool
	sz           int
	probeBatch   *Batch
	probePos     int
	cands        []Tuple
	candPos      int
	currentProbe Tuple
	matched      int
}

func newMemHashJoin(probe Executor, build []Tuple, on Expr, jt JoinType, ln, rn int,
	probeEq, buildEq []int, probeLeft bool, sz int, grants []*Grant) *memHashJoin {
	m := &memHashJoin{probe: probe, on: on, jt: jt, ln: ln, rn: rn,
		probeEq: probeEq, buildEq: buildEq, probeLeft: probeLeft, sz: sz, grants: grants}
	if len(buildEq) == 0 {
		m.allBuild = build
		return m
	}
	m.ht = make(map[string][]Tuple)
	for _, row := range build {
		k := groupKey(row, buildEq)
		m.ht[k] = append(m.ht[k], row)
	}
	return m
}

func (m *memHashJoin) Close() error {
	releaseGrants(m.grants)
	m.grants = nil
	return m.probe.Close()
}

func (m *memHashJoin) combine(probe, build Tuple) Tuple {
	if m.probeLeft {
		return append(append(Tuple{}, probe...), build...)
	}
	return append(append(Tuple{}, build...), probe...)
}

func (m *memHashJoin) unmatched(probe Tuple) Tuple {
	if m.probeLeft {
		return append(append(Tuple{}, probe...), nullRow(m.rn)...)
	}
	return append(append(Tuple{}, nullRow(m.ln)...), probe...)
}

func (m *memHashJoin) candidates(probe Tuple) []Tuple {
	if m.allBuild != nil {
		return m.allBuild
	}
	return m.ht[groupKey(probe, m.probeEq)]
}

func (m *memHashJoin) nextProbeRow(ctx context.Context) error {
	for m.probeBatch == nil || m.probePos >= len(m.probeBatch.Rows) {
		b, err := m.probe.Next(ctx)
		if err != nil {
			return err
		}
		m.probeBatch = b
		m.probePos = 0
	}
	m.currentProbe = m.probeBatch.Rows[m.probePos]
	m.cands = m.candidates(m.currentProbe)
	m.candPos = 0
	m.matched = 0
	return nil
}

func (m *memHashJoin) finishProbeRow() {
	m.probePos++
	m.currentProbe = nil
}

func (m *memHashJoin) Next(ctx context.Context) (*Batch, error) {
	out := &Batch{}
	for len(out.Rows) < m.sz {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if m.currentProbe == nil {
			if err := m.nextProbeRow(ctx); err != nil {
				if errors.Is(err, io.EOF) && len(out.Rows) > 0 {
					return out, nil
				}
				return nil, err
			}
		}
		for m.candPos < len(m.cands) && len(out.Rows) < m.sz {
			build := m.cands[m.candPos]
			m.candPos++
			combined := m.combine(m.currentProbe, build)
			v, err := m.on.Eval(combined)
			if err != nil {
				return nil, err
			}
			if v.IsTruthy() {
				m.matched++
				out.Rows = append(out.Rows, combined)
			}
		}
		if m.candPos >= len(m.cands) {
			if m.matched == 0 && (m.jt == LeftJoin || m.jt == RightJoin) {
				out.Rows = append(out.Rows, m.unmatched(m.currentProbe))
			}
			m.finishProbeRow()
		}
	}
	return out, nil
}
