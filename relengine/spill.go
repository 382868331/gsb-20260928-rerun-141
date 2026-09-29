package relengine

import (
	"context"
	"errors"
	"io"
)

// hashJoinSpill 在构建侧超过内存上限时使用。两侧按各自连接键的同一哈希分片；
// 无等值键时单片落盘。Close 删除整个临时目录，取消或出错都不留中间结果。
type hashJoinSpill struct {
	probe, build Executor
	pendingBuild []Tuple
	on           Expr
	jt           JoinType
	ln, rn       int
	probeEq      []int
	buildEq      []int
	probeLeft    bool
	opts         ExecOptions

	spill     *spiller
	partCount int
	curPart   int
	curJoin   Executor
}

func newHashJoinSpill(probe, build Executor, pending []Tuple, on Expr, jt JoinType,
	ln, rn int, probeEq, buildEq []int, probeLeft bool, opts ExecOptions) *hashJoinSpill {
	parts := 4
	if len(probeEq) == 0 {
		parts = 1
	}
	return &hashJoinSpill{probe: probe, build: build, pendingBuild: pending, on: on,
		jt: jt, ln: ln, rn: rn, probeEq: probeEq, buildEq: buildEq,
		probeLeft: probeLeft, opts: opts, partCount: parts}
}

func (e *hashJoinSpill) partBuild(p int) int { return p }
func (e *hashJoinSpill) partProbe(p int) int { return e.partCount + p }

func (e *hashJoinSpill) Close() error {
	if e.curJoin != nil {
		_ = e.curJoin.Close()
	}
	return e.spill.cleanup()
}

func (e *hashJoinSpill) writeSide(ctx context.Context, src Executor, rows []Tuple,
	keyIdx []int, partOffset func(int) int) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var batch []Tuple
		if rows != nil {
			batch = rows
			rows = nil
		} else {
			b, err := src.Next(ctx)
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			batch = b.Rows
		}
		bufs := make([][]Tuple, e.partCount)
		for _, row := range batch {
			p := 0
			if len(keyIdx) > 0 {
				p = int(keyHash(row, keyIdx) % uint64(e.partCount))
			}
			bufs[p] = append(bufs[p], row)
		}
		for p, rs := range bufs {
			if len(rs) > 0 {
				if err := e.spill.write(partOffset(p), rs); err != nil {
					return err
				}
			}
		}
	}
}

func (e *hashJoinSpill) prepare(ctx context.Context) error {
	s, err := newSpiller(e.opts.TempDir, e.partCount*2)
	if err != nil {
		return err
	}
	e.spill = s
	if err := e.writeSide(ctx, e.build, e.pendingBuild, e.buildEq, e.partBuild); err != nil {
		_ = s.cleanup()
		return err
	}
	if err := e.writeSide(ctx, e.probe, nil, e.probeEq, e.partProbe); err != nil {
		_ = s.cleanup()
		return err
	}
	return nil
}

type sliceSource struct {
	rows []Tuple
	pos  int
	sz   int
}

func (s *sliceSource) Next(ctx context.Context) (*Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.pos >= len(s.rows) {
		return nil, io.EOF
	}
	end := s.pos + s.sz
	if end > len(s.rows) {
		end = len(s.rows)
	}
	b := &Batch{Rows: s.rows[s.pos:end]}
	s.pos = end
	return b, nil
}

func (s *sliceSource) Close() error { return nil }

func (e *hashJoinSpill) readPart(part int) (probe, build []Tuple, err error) {
	br, err := e.spill.reader(e.partBuild(part))
	if err != nil {
		return nil, nil, err
	}
	for {
		got, err := br.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		build = append(build, got...)
	}
	pr, err := e.spill.reader(e.partProbe(part))
	if err != nil {
		return nil, nil, err
	}
	for {
		got, err := pr.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		probe = append(probe, got...)
	}
	return probe, build, nil
}

func (e *hashJoinSpill) Next(ctx context.Context) (*Batch, error) {
	if e.spill == nil {
		if err := e.prepare(ctx); err != nil {
			return nil, err
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if e.curJoin == nil {
			if e.curPart >= e.partCount {
				return nil, io.EOF
			}
			probe, build, err := e.readPart(e.curPart)
			if err != nil {
				return nil, err
			}
			e.curPart++
			src := &sliceSource{rows: probe, sz: e.opts.BatchRows}
			e.curJoin = newMemHashJoin(src, build, e.on, e.jt, e.ln, e.rn,
				e.probeEq, e.buildEq, e.probeLeft, e.opts.BatchRows)
		}
		b, err := e.curJoin.Next(ctx)
		if errors.Is(err, io.EOF) {
			_ = e.curJoin.Close()
			e.curJoin = nil
			continue
		}
		if err != nil {
			return nil, err
		}
		return b, nil
	}
}
