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
	grant    int // 已向 Pool 申请、尚未归还的行额度
}

func BuildJoin(l, r Executor, on Expr, jt JoinType, ln, rn int, opts ExecOptions) Executor {
	return &joinExec{l: l, r: r, on: on, jt: jt, ln: ln, rn: rn, opts: opts}
}

func (e *joinExec) Close() error {
	if e.opts.Pool != nil && e.grant > 0 {
		e.opts.Pool.Release(e.grant)
		e.grant = 0
	}
	if e.inner != nil {
		_ = e.inner.Close()
	}
	if err := e.l.Close(); err != nil {
		return err
	}
	return e.r.Close()
}

// acquireRow 为构建侧常驻的一行申请额度。逐行申请使得每一行常驻
// 都恰好对应 1 行授予额度，不多占、不漏计；无池时只受 MemRows 上限约束。
func (e *joinExec) acquireRow() bool {
	if e.opts.Pool == nil {
		return true
	}
	if e.opts.Pool.Acquire(1) == 0 {
		return false
	}
	e.grant++
	return true
}

// MemUsage 报告当前在内存中保留的行数（构建侧物化 + 回读块）。
func (e *joinExec) MemUsage() int {
	switch in := e.inner.(type) {
	case *memHashJoin:
		return in.MemUsage()
	case *hashJoinSpill:
		return in.MemUsage()
	}
	return 0
}

func (e *joinExec) prepare(ctx context.Context) error {
	if e.opts.MemRows < 1 || e.opts.BatchRows < 1 {
		return fmt.Errorf("%w: MemRows=%d BatchRows=%d",
			ErrInsufficientBudget, e.opts.MemRows, e.opts.BatchRows)
	}
	leftEq, rightEq := eqJoinKeys(e.on, e.ln)
	// 构建侧超出授予额度而落盘时，回读阶段的探测块是唯一常驻内存的
	// 连接状态，必须全部来自已授予额度：让出多余保留，只留 1 行
	// 探测块额度供其他阶段使用；授予 0 行时不允许靠一行未计额缓冲
	// 硬撑，而是记录原因并返回可解释的预算不足。
	spill := func(probe, build Executor, pending []Tuple, probeEq, buildEq []int, probeLeft bool) error {
		memRows := e.opts.MemRows // 无池：MemRows 本身就是预算
		if e.opts.Pool != nil {
			if e.grant < 1 {
				e.opts.Pool.NoteSpill(
					"连接构建侧需落盘，但共享池额度已耗尽（授予 0 行），无法保留探测块")
				return fmt.Errorf("%w: 共享池额度已耗尽，连接回读需要至少 1 行探测块",
					ErrInsufficientBudget)
			}
			held := e.grant
			e.opts.Pool.Release(held - 1)
			e.grant = 1
			memRows = 1 // 1 行探测块，已计入额度
			e.opts.Pool.NoteSpill(
				"连接构建侧超过授予额度 %d 行（池剩余不足），转入临时存储；回读探测块 %d 行已计入额度",
				held, memRows)
		}
		e.inner = newHashJoinSpill(probe, build, pending, e.on, e.jt, e.ln, e.rn,
			probeEq, buildEq, probeLeft, e.opts, memRows)
		return nil
	}
	mem := func(probe Executor, rows []Tuple, probeEq, buildEq []int, probeLeft bool) {
		// 逐行申请保证 grant == len(rows)，无需事后找平。
		e.inner = newMemHashJoin(probe, rows, e.on, e.jt, e.ln, e.rn,
			probeEq, buildEq, probeLeft, e.opts.BatchRows)
	}
	if e.jt == LeftJoin {
		b, err := e.collectBuild(ctx, e.r)
		if err != nil {
			return err
		}
		if b.spilled {
			return spill(e.l, e.r, b.pending, leftEq, rightEq, true)
		}
		mem(e.l, b.rows, leftEq, rightEq, true)
		return nil
	}
	b, err := e.collectBuild(ctx, e.l)
	if err != nil {
		return err
	}
	if b.spilled {
		return spill(e.r, e.l, b.pending, rightEq, leftEq, false)
	}
	mem(e.r, b.rows, rightEq, leftEq, false)
	return nil
}

func (e *joinExec) Next(ctx context.Context) (*Batch, error) {
	if !e.prepared {
		if err := e.prepare(ctx); err != nil {
			if e.opts.Pool != nil && e.grant > 0 {
				e.opts.Pool.Release(e.grant)
				e.grant = 0
			}
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

// collectBuild 逐行收集构建侧：每一行常驻都先取得 Pool 额度（且不超过
// MemRows 上限）。额度耗尽即转入落盘模式：已收集行与后续行全部交给
// 落盘路径（pending + 继续读 src），不会靠未计额的行维持推进。
func (e *joinExec) collectBuild(ctx context.Context, src Executor) (*collected, error) {
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
		for i, row := range b.Rows {
			if len(c.rows) < e.opts.MemRows && e.acquireRow() {
				c.rows = append(c.rows, row)
				continue
			}
			// 额度耗尽：已收集行与本批剩余行作为落盘首批，
			// 后续输入由落盘路径流式读取，不在内存中堆积。
			c.spilled = true
			c.pending = append(c.pending, c.rows...)
			c.pending = append(c.pending, b.Rows[i:]...)
			c.rows = nil
			return c, nil
		}
	}
}

type memHashJoin struct {
	probe        Executor
	ht           map[string][]Tuple
	allBuild     []Tuple
	buildRows    int
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
		probeEq: probeEq, buildEq: buildEq, probeLeft: probeLeft, sz: sz,
		buildRows: len(build)}
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

// MemUsage 报告当前内存占用：构建侧物化行 + 当前探测批次。
func (m *memHashJoin) MemUsage() int {
	n := m.buildRows
	if m.probeBatch != nil {
		n += len(m.probeBatch.Rows)
	}
	return n
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
