package relengine

import (
	"context"
	"errors"
	"io"
)

// aggExec 执行分组聚合。每个分组维护一组 Accumulator，逐行 Update，
// 因此连接放大产生的每一行都会被统计（不做任何去重）。
//
// 第一版内存策略：输入先经分批算子流入内存哈希表；当逻辑计划标记需要溢出时，
// 使用 aggSpillExec 在读取前建立分片，把行按分组键哈希落盘，再逐片聚合。
type aggExec struct {
	child Executor
	aggs  []*AggExpr
	gbIdx []int
	sz    int

	prepared bool
	out      []Tuple
	pos      int
	memLimit int
	spill    bool
	tempDir  string
}

// BuildAggregate 构造分组聚合执行器。
func BuildAggregate(child Executor, agg *Aggregate, opts ExecOptions, spill bool) (Executor, error) {
	e := &aggExec{
		child:    child,
		aggs:     agg.Aggs,
		sz:       opts.BatchRows,
		memLimit: opts.MemRows,
		spill:    spill,
		tempDir:  opts.TempDir,
	}
	for _, g := range agg.GroupBy {
		cr, ok := g.(*ColumnRef)
		if !ok {
			return nil, errors.New("第一版 GROUP BY 仅支持列引用")
		}
		e.gbIdx = append(e.gbIdx, cr.idx)
	}
	if spill {
		return &aggSpillExec{inner: e}, nil
	}
	return e, nil
}

func (e *aggExec) Close() error { return e.child.Close() }

type groupState struct {
	key  Tuple
	accs []Accumulator
}

func (e *aggExec) newAccs() ([]Accumulator, error) {
	accs := make([]Accumulator, len(e.aggs))
	for i, a := range e.aggs {
		acc, err := a.NewAccumulator()
		if err != nil {
			return nil, err
		}
		accs[i] = acc
	}
	return accs, nil
}

func (e *aggExec) groupKeyValues(row Tuple) Tuple {
	if len(e.gbIdx) == 0 {
		return nil
	}
	k := make(Tuple, len(e.gbIdx))
	for i, idx := range e.gbIdx {
		k[i] = row[idx]
	}
	return k
}

func (e *aggExec) prepare(ctx context.Context) error {
	states := map[string]*groupState{}
	order := make([]string, 0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := e.child.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		for _, row := range b.Rows {
			kRaw := e.groupKeyValues(row)
			kStr := groupKey(row, e.gbIdx)
			st, ok := states[kStr]
			if !ok {
				accs, err := e.newAccs()
				if err != nil {
					return err
				}
				st = &groupState{key: kRaw, accs: accs}
				states[kStr] = st
				order = append(order, kStr)
			}
			for _, acc := range st.accs {
				if err := acc.Update(row); err != nil {
					return err
				}
			}
		}
	}
	return e.emit(states, order)
}

func (e *aggExec) emit(states map[string]*groupState, order []string) error {
	if len(order) == 0 {
		if len(e.gbIdx) == 0 {
			accs, err := e.newAccs()
			if err != nil {
				return err
			}
			row, err := e.rowFromAccs(nil, accs)
			if err != nil {
				return err
			}
			e.out = []Tuple{row}
		}
		return nil
	}
	e.out = make([]Tuple, 0, len(order))
	for _, k := range order {
		st := states[k]
		row, err := e.rowFromAccs(st.key, st.accs)
		if err != nil {
			return err
		}
		e.out = append(e.out, row)
	}
	return nil
}

func (e *aggExec) rowFromAccs(key Tuple, accs []Accumulator) (Tuple, error) {
	row := make(Tuple, 0, len(key)+len(accs))
	row = append(row, key...)
	for _, acc := range accs {
		row = append(row, acc.Result())
	}
	return row, nil
}

func (e *aggExec) Next(ctx context.Context) (*Batch, error) {
	if !e.prepared {
		if err := e.prepare(ctx); err != nil {
			return nil, err
		}
		e.prepared = true
	}
	if e.pos >= len(e.out) {
		return nil, io.EOF
	}
	end := e.pos + e.sz
	if end > len(e.out) {
		end = len(e.out)
	}
	b := &Batch{Rows: e.out[e.pos:end]}
	e.pos = end
	return b, nil
}

// aggSpillExec 先消费上游并按键分片落盘，再逐片读回，用 aggExec 的内存逻辑聚合。
type aggSpillExec struct {
	inner *aggExec

	prepared bool
	spill    *spiller
	parts    int
	cur      int
	curExec  *aggExec
	curSrc   *sliceSource
	out      []Tuple
	pos      int
}

func (e *aggSpillExec) Close() error {
	childErr := e.inner.child.Close()
	if e.spill != nil {
		if err := e.spill.cleanup(); err != nil {
			return err
		}
	}
	return childErr
}

func (e *aggSpillExec) writeAll(ctx context.Context) error {
	parts := 4
	if e.inner.memLimit > 0 {
		parts = 8
	}
	if len(e.inner.gbIdx) == 0 {
		parts = 1
	}
	e.parts = parts
	s, err := newSpiller(e.inner.tempDir, parts)
	if err != nil {
		return err
	}
	e.spill = s
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := e.inner.child.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		bufs := make([][]Tuple, parts)
		for _, row := range b.Rows {
			p := 0
			if len(e.inner.gbIdx) > 0 {
				p = int(keyHash(row, e.inner.gbIdx) % uint64(parts))
			}
			bufs[p] = append(bufs[p], row)
		}
		for p, rows := range bufs {
			if len(rows) > 0 {
				if err := s.write(p, rows); err != nil {
					return err
				}
			}
		}
	}
}

func (e *aggSpillExec) prepare(ctx context.Context) error {
	if err := e.writeAll(ctx); err != nil {
		return err
	}
	for p := 0; p < e.parts; p++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		r, err := e.spill.reader(p)
		if err != nil {
			return err
		}
		var rows []Tuple
		for {
			got, err := r.next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			rows = append(rows, got...)
		}
		partAgg := &aggExec{
			aggs:     e.inner.aggs,
			gbIdx:    e.inner.gbIdx,
			sz:       e.inner.sz,
			memLimit: e.inner.memLimit,
			tempDir:  e.inner.tempDir,
			child:    &sliceSource{rows: rows, sz: e.inner.sz},
		}
		if err := partAgg.prepare(ctx); err != nil {
			return err
		}
		e.out = append(e.out, partAgg.out...)
	}
	return nil
}

func (e *aggSpillExec) Next(ctx context.Context) (*Batch, error) {
	if !e.prepared {
		if err := e.prepare(ctx); err != nil {
			_ = e.Close()
			return nil, err
		}
		e.prepared = true
	}
	if e.pos >= len(e.out) {
		return nil, io.EOF
	}
	sz := e.inner.sz
	end := e.pos + sz
	if end > len(e.out) {
		end = len(e.out)
	}
	b := &Batch{Rows: e.out[e.pos:end]}
	e.pos = end
	return b, nil
}
