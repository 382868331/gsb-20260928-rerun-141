package relengine

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// nestedLoopJoin 是 Hash Join 的等价物理实现：不建哈希表，逐对检查 ON。
// 右侧在内存中最多物化 Pool/MemRows 授予的行数，超出部分单片落盘；
// 左侧每来一行都完整扫描一次“内存部分 + 落盘部分”，结果与全内存一致。
type nestedLoopJoin struct {
	l, r   Executor
	on     Expr
	jt     JoinType
	ln, rn int
	sz     int
	opts   ExecOptions

	loaded    bool
	grant     int
	rightRows []Tuple
	spill     *spiller
	spillR    *partitionReader
	spillOpen bool
	leftBatch *Batch
	leftPos   int
	left      Tuple
	rightPos  int
	diskDone  bool
	matched   int
}

func BuildNestedLoopJoin(l, r Executor, on Expr, jt JoinType, ln, rn int, opts ExecOptions) Executor {
	return &nestedLoopJoin{l: l, r: r, on: on, jt: jt, ln: ln, rn: rn, sz: opts.BatchRows, opts: opts}
}

func (e *nestedLoopJoin) Close() error {
	if e.opts.Pool != nil && e.grant > 0 {
		e.opts.Pool.Release(e.grant)
		e.grant = 0
	}
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

// MemUsage 报告当前内存占用：右侧驻留行（<= limit）加当前左侧批次。
func (e *nestedLoopJoin) MemUsage() int {
	n := len(e.rightRows)
	if e.leftBatch != nil {
		n += len(e.leftBatch.Rows)
	}
	return n
}

func (e *nestedLoopJoin) loadRight(ctx context.Context) error {
	if e.opts.MemRows < 1 || e.opts.BatchRows < 1 {
		return fmt.Errorf("%w: MemRows=%d BatchRows=%d",
			ErrInsufficientBudget, e.opts.MemRows, e.opts.BatchRows)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := e.r.Next(ctx)
		if errors.Is(err, io.EOF) {
			e.loaded = true
			return nil
		}
		if err != nil {
			return err
		}
		for _, row := range b.Rows {
			// 逐行申请额度：每一行右侧驻留都恰好对应 1 行授予额度；
			// 额度耗尽的行直接落盘，不靠未计额缓冲硬撑。
			if len(e.rightRows) < e.opts.MemRows && e.acquireRow() {
				e.rightRows = append(e.rightRows, row)
				continue
			}
			if err := e.spillRow(row); err != nil {
				return err
			}
		}
	}
}

// acquireRow 为右侧驻留的一行申请额度；无池时只受 MemRows 上限约束。
func (e *nestedLoopJoin) acquireRow() bool {
	if e.opts.Pool == nil {
		return true
	}
	if e.opts.Pool.Acquire(1) == 0 {
		return false
	}
	e.grant++
	return true
}

// spillRow 把放不进内存预算的右侧行写入单片临时文件。
func (e *nestedLoopJoin) spillRow(row Tuple) error {
	if e.spill == nil {
		s, err := newSpiller(e.opts.TempDir, 1)
		if err != nil {
			return err
		}
		e.spill = s
		if e.opts.Pool != nil {
			e.opts.Pool.NoteSpill(
				"嵌套循环连接右侧驻留 %d 行后池额度耗尽，剩余行转入临时存储", len(e.rightRows))
		}
	}
	return e.spill.write(0, []Tuple{row})
}

// nextDiskRow 从落盘部分读出下一行；ok=false 表示落盘部分已扫完。
// 落盘块固定为单行（见 spillRow），解码出的行立即参与比较、不留存，
// 因此落盘侧没有任何常驻内存缓冲需要计入额度。
func (e *nestedLoopJoin) nextDiskRow() (Tuple, bool, error) {
	if e.spill == nil {
		return nil, false, nil
	}
	if !e.spillOpen {
		r, err := e.spill.reader(0)
		if err != nil {
			return nil, false, err
		}
		e.spillR = r
		e.spillOpen = true
	}
	for {
		chunk, err := e.spillR.next()
		if errors.Is(err, io.EOF) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		if len(chunk) > 0 {
			return chunk[0], true, nil
		}
	}
}

func (e *nestedLoopJoin) Next(ctx context.Context) (*Batch, error) {
	if !e.loaded {
		if err := e.loadRight(ctx); err != nil {
			return nil, err
		}
	}
	out := &Batch{}
	for len(out.Rows) < e.sz {
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
			e.rightPos = 0
			e.diskDone = false
			e.spillOpen = false
			e.matched = 0
		}
		left := e.left
		for e.rightPos < len(e.rightRows) && len(out.Rows) < e.sz {
			right := e.rightRows[e.rightPos]
			e.rightPos++
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
		for e.rightPos >= len(e.rightRows) && !e.diskDone && len(out.Rows) < e.sz {
			var right Tuple
			var ok bool
			var err error
			right, ok, err = e.nextDiskRow()
			if err != nil {
				return nil, err
			}
			if !ok {
				e.diskDone = true
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
		if e.rightPos >= len(e.rightRows) && e.diskDone {
			if e.matched == 0 && e.jt == LeftJoin {
				out.Rows = append(out.Rows, append(append(Tuple{}, left...), nullRow(e.rn)...))
			}
			e.leftPos++
			if e.leftBatch != nil && e.leftPos < len(e.leftBatch.Rows) {
				e.left = e.leftBatch.Rows[e.leftPos]
				e.rightPos = 0
				e.diskDone = false
				e.spillOpen = false
				e.matched = 0
			} else {
				e.left = nil
			}
		}
	}
	return out, nil
}
