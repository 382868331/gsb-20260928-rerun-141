package relengine

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// nestedLoopJoin 是 Hash Join 的等价物理实现：不建哈希表，逐对检查 ON。
// 与 Hash Join 一样遵守内存约定：右侧物化行数超过 MemRows（或共享池
// 额度不足）时，右侧整体写入临时分片，之后每个左侧行都流式重扫该分片，
// 内存只保留一个左侧批次和一个右侧落盘块，不会在首批结果后偷偷保留
// 整份右侧。输出顺序（左行为主、右行为次的嵌套序）与重复行数与
// 纯内存路径完全一致。
type nestedLoopJoin struct {
	l, r   Executor
	on     Expr
	jt     JoinType
	ln, rn int
	opts   ExecOptions

	checked   bool
	loaded    bool
	rightRows []Tuple
	grants    []*Grant

	// 右侧落盘后的重扫状态。
	spill      *spiller
	rightR     *partitionReader
	rightChunk []Tuple
	rightEOF   bool

	leftBatch *Batch
	leftPos   int
	left      Tuple
	rightPos  int
	rightDone bool
	matched   int
}

func BuildNestedLoopJoin(l, r Executor, on Expr, jt JoinType, ln, rn int, opts ExecOptions) Executor {
	return &nestedLoopJoin{l: l, r: r, on: on, jt: jt, ln: ln, rn: rn, opts: opts}
}

func (e *nestedLoopJoin) Close() error {
	releaseGrants(e.grants)
	e.grants = nil
	if e.spill != nil {
		_ = e.spill.cleanup()
		e.spill = nil
	}
	err1 := e.l.Close()
	err2 := e.r.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// spillRight 把已物化的右侧行和 src 中剩余行全部写入单分片临时文件，
// 并释放已占额度。之后右侧只从磁盘流式重扫。
func (e *nestedLoopJoin) spillRight(ctx context.Context, first []Tuple) error {
	s, err := newSpiller(e.opts.TempDir, 1)
	if err != nil {
		return err
	}
	e.spill = s
	releaseGrants(e.grants)
	e.grants = nil
	if len(e.rightRows) > 0 {
		if err := s.write(0, e.rightRows); err != nil {
			return err
		}
		e.rightRows = nil
	}
	if len(first) > 0 {
		if err := s.write(0, first); err != nil {
			return err
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := e.r.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := s.write(0, b.Rows); err != nil {
			return err
		}
	}
}

func (e *nestedLoopJoin) loadRight(ctx context.Context) error {
	limit := e.opts.MemRows
	pool := e.opts.Pool
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := e.r.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if pool != nil && len(b.Rows) > 0 {
			g := pool.TryAcquire(len(b.Rows), "nlj-build(right)")
			if g == nil {
				pool.NoteSpill("nlj-build(right)", len(e.rightRows)+len(b.Rows),
					"共享内存额度不足，右侧转入临时存储")
				return e.spillRight(ctx, b.Rows)
			}
			e.grants = append(e.grants, g)
		}
		e.rightRows = append(e.rightRows, b.Rows...)
		if len(e.rightRows) > limit {
			if pool != nil {
				pool.NoteSpill("nlj-build(right)", len(e.rightRows),
					"右侧超过单算子内存上限 MemRows，转入临时存储")
			}
			return e.spillRight(ctx, nil)
		}
	}
}

// resetRightScan 为新的左侧行重置右侧扫描：内存路径重置下标，
// 落盘路径重新打开分片读句柄从头流式读取。
func (e *nestedLoopJoin) resetRightScan() error {
	e.rightPos = 0
	e.matched = 0
	e.rightDone = false
	if e.spill == nil {
		return nil
	}
	r, err := e.spill.reader(0)
	if err != nil {
		return err
	}
	e.rightR = r
	e.rightChunk = nil
	e.rightEOF = false
	return nil
}

// nextRightRow 取下一条右侧行；内存路径取自切片，落盘路径按落盘块流式读取。
func (e *nestedLoopJoin) nextRightRow() (Tuple, bool, error) {
	if e.spill == nil {
		if e.rightPos >= len(e.rightRows) {
			e.rightDone = true
			return nil, false, nil
		}
		row := e.rightRows[e.rightPos]
		e.rightPos++
		return row, true, nil
	}
	for e.rightPos >= len(e.rightChunk) {
		if e.rightEOF {
			e.rightDone = true
			return nil, false, nil
		}
		chunk, err := e.rightR.next()
		if errors.Is(err, io.EOF) {
			e.rightEOF = true
			e.rightDone = true
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		e.rightChunk = chunk
		e.rightPos = 0
	}
	row := e.rightChunk[e.rightPos]
	e.rightPos++
	return row, true, nil
}

func (e *nestedLoopJoin) Next(ctx context.Context) (*Batch, error) {
	if !e.checked {
		if e.opts.MemRows < 1 || e.opts.BatchRows < 1 {
			return nil, fmt.Errorf("%w: MemRows=%d BatchRows=%d",
				ErrInsufficientBudget, e.opts.MemRows, e.opts.BatchRows)
		}
		e.checked = true
	}
	if !e.loaded {
		if err := e.loadRight(ctx); err != nil {
			return nil, err
		}
		e.loaded = true
	}
	sz := e.opts.BatchRows
	out := &Batch{}
	for len(out.Rows) < sz {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if e.left == nil {
			b, err := e.l.Next(ctx)
			if errors.Is(err, io.EOF) {
				if len(out.Rows) > 0 {
					return out, nil
				}
				return nil, io.EOF
			}
			if err != nil {
				return nil, err
			}
			e.leftBatch = b
			e.leftPos = 0
			e.left = b.Rows[0]
			if err := e.resetRightScan(); err != nil {
				return nil, err
			}
		}
		left := e.left
		for len(out.Rows) < sz && !e.rightDone {
			right, ok, err := e.nextRightRow()
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			combined := append(append(Tuple{}, left...), right...)
			v, err := e.on.Eval(combined)
			if err != nil {
				return nil, err
			}
			if v.IsTruthy() {
				e.matched++
				out.Rows = append(out.Rows, combined)
			}
		}
		if e.rightDone {
			// 右侧已扫完一轮：外连接补 NULL，然后推进到下一左行。
			if e.matched == 0 && e.jt == LeftJoin {
				out.Rows = append(out.Rows, append(append(Tuple{}, left...), nullRow(e.rn)...))
			}
			e.leftPos++
			if e.leftBatch != nil && e.leftPos < len(e.leftBatch.Rows) {
				e.left = e.leftBatch.Rows[e.leftPos]
			} else {
				e.left = nil
			}
			if e.left != nil {
				if err := e.resetRightScan(); err != nil {
					return nil, err
				}
			}
		}
	}
	return out, nil
}
