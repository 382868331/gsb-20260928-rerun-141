package relengine

import (
	"context"
	"errors"
	"io"
)

// nestedLoopJoin 是 Hash Join 的等价物理实现：不建哈希表，逐对检查 ON。
// 右侧小表先物化，左侧每来一行都完整扫描一次右侧切片。
type nestedLoopJoin struct {
	l, r   Executor
	on     Expr
	jt     JoinType
	ln, rn int
	sz     int

	loaded    bool
	rightRows []Tuple
	leftBatch *Batch
	leftPos   int
	left      Tuple
	rightPos  int
	matched   int
}

func BuildNestedLoopJoin(l, r Executor, on Expr, jt JoinType, ln, rn int, opts ExecOptions) Executor {
	return &nestedLoopJoin{l: l, r: r, on: on, jt: jt, ln: ln, rn: rn, sz: opts.BatchRows}
}

func (e *nestedLoopJoin) Close() error {
	err1 := e.l.Close()
	err2 := e.r.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (e *nestedLoopJoin) loadRight(ctx context.Context) error {
	for {
		b, err := e.r.Next(ctx)
		if errors.Is(err, io.EOF) {
			e.loaded = true
			return nil
		}
		if err != nil {
			return err
		}
		e.rightRows = append(e.rightRows, b.Rows...)
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
		if e.rightPos >= len(e.rightRows) {
			if e.matched == 0 && e.jt == LeftJoin {
				out.Rows = append(out.Rows, append(append(Tuple{}, left...), nullRow(e.rn)...))
			}
			e.leftPos++
			if e.leftBatch != nil && e.leftPos < len(e.leftBatch.Rows) {
				e.left = e.leftBatch.Rows[e.leftPos]
			} else {
				e.left = nil
			}
			e.rightPos = 0
			e.matched = 0
		}
	}
	return out, nil
}
