package relengine

import (
	"context"
	"errors"
	"fmt"
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
	memRows      int // 探测侧驻留内存的行数上限（来自 Pool 授予额度）

	spill     *spiller
	partCount int
	curPart   int

	// 回读状态：探测侧按 MemRows 分块驻留内存，构建侧按落盘批次流式重扫，
	// 因此热键（单侧分片极大）也不会突破内存预算。
	openPart  int
	partOpen  bool
	probeR    *partitionReader
	probeBuf  []Tuple
	probeEOF  bool
	probeBlk  []Tuple
	matched   []bool
	buildR    *partitionReader
	buildChk  []Tuple
	buildPos  int
	buildEOF  bool
	unmatNext int
	blkOpen   bool
}

func newHashJoinSpill(probe, build Executor, pending []Tuple, on Expr, jt JoinType,
	ln, rn int, probeEq, buildEq []int, probeLeft bool, opts ExecOptions, memRows int) *hashJoinSpill {
	parts := 4
	if len(probeEq) == 0 {
		parts = 1
	}
	if memRows < 1 {
		memRows = 1
	}
	return &hashJoinSpill{probe: probe, build: build, pendingBuild: pending, on: on,
		jt: jt, ln: ln, rn: rn, probeEq: probeEq, buildEq: buildEq,
		probeLeft: probeLeft, opts: opts, partCount: parts, memRows: memRows}
}

// MemUsage 报告当前内存占用：驻留的探测块（<= memRows）加缓冲中的行。
func (e *hashJoinSpill) MemUsage() int {
	return len(e.probeBlk) + len(e.probeBuf) + len(e.buildChk)
}

func (e *hashJoinSpill) partBuild(p int) int { return p }
func (e *hashJoinSpill) partProbe(p int) int { return e.partCount + p }

func (e *hashJoinSpill) Close() error {
	if e.spill == nil {
		return nil
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
	if e.opts.BatchRows < 1 {
		return fmt.Errorf("%w: MemRows=%d BatchRows=%d",
			ErrInsufficientBudget, e.opts.MemRows, e.opts.BatchRows)
	}
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

func (e *hashJoinSpill) combine(probe, build Tuple) Tuple {
	if e.probeLeft {
		return append(append(Tuple{}, probe...), build...)
	}
	return append(append(Tuple{}, build...), probe...)
}

func (e *hashJoinSpill) unmatched(probe Tuple) Tuple {
	if e.probeLeft {
		return append(append(Tuple{}, probe...), nullRow(e.rn)...)
	}
	return append(append(Tuple{}, nullRow(e.ln)...), probe...)
}

// nextBlock 载入下一块探测侧行（至多 MemRows 行），必要时前进到下一个分片。
// 返回 false 表示所有分片都已耗尽。
func (e *hashJoinSpill) nextBlock(ctx context.Context) (bool, error) {
	limit := e.memRows
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if !e.partOpen {
			if e.curPart >= e.partCount {
				return false, nil
			}
			pr, err := e.spill.reader(e.partProbe(e.curPart))
			if err != nil {
				return false, err
			}
			e.probeR = pr
			e.probeBuf = e.probeBuf[:0]
			e.probeEOF = false
			e.openPart = e.curPart
			e.curPart++
			e.partOpen = true
		}
		for len(e.probeBuf) < limit && !e.probeEOF {
			chunk, err := e.probeR.next()
			if errors.Is(err, io.EOF) {
				e.probeEOF = true
				break
			}
			if err != nil {
				return false, err
			}
			e.probeBuf = append(e.probeBuf, chunk...)
		}
		if len(e.probeBuf) == 0 {
			// 当前分片探测侧为空，推进到下一分片。
			e.partOpen = false
			e.probeR = nil
			continue
		}
		n := limit
		if n > len(e.probeBuf) {
			n = len(e.probeBuf)
		}
		blk := make([]Tuple, n)
		copy(blk, e.probeBuf[:n])
		e.probeBuf = append(e.probeBuf[:0], e.probeBuf[n:]...)
		e.probeBlk = blk
		e.matched = make([]bool, n)
		br, err := e.spill.reader(e.partBuild(e.openPart))
		if err != nil {
			return false, err
		}
		e.buildR = br
		e.buildChk = nil
		e.buildPos = 0
		e.buildEOF = false
		e.unmatNext = 0
		e.blkOpen = true
		return true, nil
	}
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

// Next 逐批产出连接结果。探测侧每次只驻留一块（<= MemRows 行），
// 构建侧分片按落盘批次流式扫描；块内每一行都与整片构建侧比较，
// 因此热键的多对多放大结果完整，且内存不随分片大小增长。
func (e *hashJoinSpill) Next(ctx context.Context) (*Batch, error) {
	if e.spill == nil {
		if err := e.prepare(ctx); err != nil {
			return nil, err
		}
	}
	sz := e.opts.BatchRows
	out := &Batch{}
	for len(out.Rows) < sz {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !e.blkOpen {
			ok, err := e.nextBlock(ctx)
			if err != nil {
				return nil, err
			}
			if !ok {
				if len(out.Rows) > 0 {
					return out, nil
				}
				return nil, io.EOF
			}
		}
		if !e.buildEOF {
			if e.buildPos >= len(e.buildChk) {
				chunk, err := e.buildR.next()
				if errors.Is(err, io.EOF) {
					e.buildEOF = true
					e.buildChk = nil
					continue
				}
				if err != nil {
					return nil, err
				}
				e.buildChk = chunk
				e.buildPos = 0
				if len(chunk) == 0 {
					continue
				}
			}
			build := e.buildChk[e.buildPos]
			e.buildPos++
			for i, probe := range e.probeBlk {
				combined := e.combine(probe, build)
				v, err := e.on.Eval(combined)
				if err != nil {
					return nil, err
				}
				if v.IsTruthy() {
					e.matched[i] = true
					out.Rows = append(out.Rows, combined)
					if len(out.Rows) >= sz {
						return out, nil
					}
				}
			}
			continue
		}
		// 构建侧整片扫完：外连接为未匹配的探测行补 NULL，然后换下一块。
		for e.unmatNext < len(e.probeBlk) && len(out.Rows) < sz {
			i := e.unmatNext
			e.unmatNext++
			if !e.matched[i] && (e.jt == LeftJoin || e.jt == RightJoin) {
				out.Rows = append(out.Rows, e.unmatched(e.probeBlk[i]))
			}
		}
		if e.unmatNext >= len(e.probeBlk) {
			e.blkOpen = false
		}
	}
	return out, nil
}
