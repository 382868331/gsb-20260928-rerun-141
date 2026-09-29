package relengine

import (
	"context"
	"errors"
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
	leftEq, rightEq := eqJoinKeys(e.on, e.ln)
	if e.jt == LeftJoin {
		b, err := collectLimited(ctx, e.r, e.opts.MemRows)
		if err != nil {
			return err
		}
		if b.spilled {
			e.inner = newHashJoinSpill(e.l, e.r, b.pending, e.on, e.jt, e.ln, e.rn,
				leftEq, rightEq, true, e.opts)
			return nil
		}
		e.inner = newMemHashJoin(e.l, b.rows, e.on, e.jt, e.ln, e.rn,
			leftEq, rightEq, true, e.opts.BatchRows)
		return nil
	}
	b, err := collectLimited(ctx, e.l, e.opts.MemRows)
	if err != nil {
		return err
	}
	if b.spilled {
		e.inner = newHashJoinSpill(e.r, e.l, b.pending, e.on, e.jt, e.ln, e.rn,
			rightEq, leftEq, false, e.opts)
		return nil
	}
	e.inner = newMemHashJoin(e.r, b.rows, e.on, e.jt, e.ln, e.rn,
		rightEq, leftEq, false, e.opts.BatchRows)
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
}

func collectLimited(ctx context.Context, src Executor, limit int) (*collected, error) {
	c := &collected{}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b, err := src.Next(ctx)
		if errors.Is(err, io.EOF) {
			return c, nil
		}
		if err != nil {
			return nil, err
		}
		c.rows = append(c.rows, b.Rows...)
		if len(c.rows) > limit {
			c.spilled = true
			c.pending = c.rows
			c.rows = nil
			return c, nil
		}
	}
}

type memHashJoin struct {
	probe        Executor
	ht           map[string][]Tuple
	allBuild     []Tuple
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
	probeEq, buildEq []int, probeLeft bool, sz int) *memHashJoin {
	m := &memHashJoin{probe: probe, on: on, jt: jt, ln: ln, rn: rn,
		probeEq: probeEq, buildEq: buildEq, probeLeft: probeLeft, sz: sz}
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

func (m *memHashJoin) Close() error { return m.probe.Close() }

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
