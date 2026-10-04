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

	// 回读状态：探测侧按 memRows（= Pool 授予额度）分块驻留内存，是唯一
	// 常驻的连接状态；构建侧分片按落盘块（<= BatchRows 行）流式重扫，
	// 扫描缓冲与管道批次一样受 BatchRows 约束。因此热键（单侧分片极大）
	// 也不会突破内存预算，且每一行驻留都能对应到某一条额度规则。
	openPart  int
	partOpen  bool
	probeR    *partitionReader
	probeBlk  []Tuple
	matched   []bool
	probePos  int
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

// MemUsage 报告当前内存占用：驻留的探测块（<= memRows，已计入 Pool 额度）
// 加构建侧流式扫描缓冲（<= BatchRows）。
func (e *hashJoinSpill) MemUsage() int {
	return len(e.probeBlk) + len(e.buildChk)
}

func (e *hashJoinSpill) partBuild(p int) int { return p }
func (e *hashJoinSpill) partProbe(p int) int { return e.partCount + p }

func (e *hashJoinSpill) Close() error {
	if e.spill == nil {
		return nil
	}
	return e.spill.cleanup()
}

// writeSide 把一侧输入按连接键哈希写入分片。chunkRows 限制单个落盘块的
// 行数，从而限制回读时单个解码块在内存中的行数：探测侧块就是回读时的
// 驻留块（<= memRows，计入 Pool 额度），构建侧块是流式扫描缓冲
// （<= BatchRows，与管道批次同一约束）。
func (e *hashJoinSpill) writeSide(ctx context.Context, src Executor, rows []Tuple,
	keyIdx []int, partOffset func(int) int, chunkRows int) error {
	if chunkRows < 1 {
		chunkRows = 1
	}
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
			for len(rs) > 0 {
				n := chunkRows
				if n > len(rs) {
					n = len(rs)
				}
				if err := e.spill.write(partOffset(p), rs[:n]); err != nil {
					return err
				}
				rs = rs[n:]
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
	if err := e.writeSide(ctx, e.build, e.pendingBuild, e.buildEq, e.partBuild, e.opts.BatchRows); err != nil {
		_ = s.cleanup()
		return err
	}
	if err := e.writeSide(ctx, e.probe, nil, e.probeEq, e.partProbe, e.memRows); err != nil {
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

// nextBlock 载入下一块探测侧行（一个落盘块，至多 memRows 行，全部来自
// Pool 授予额度），必要时前进到下一个分片。返回 false 表示所有分片都已耗尽。
func (e *hashJoinSpill) nextBlock(ctx context.Context) (bool, error) {
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
			e.openPart = e.curPart
			e.curPart++
			e.partOpen = true
		}
		chunk, err := e.probeR.next()
		if errors.Is(err, io.EOF) {
			// 当前分片探测侧已耗尽，推进到下一分片。
			e.partOpen = false
			e.probeR = nil
			continue
		}
		if err != nil {
			return false, err
		}
		if len(chunk) == 0 {
			continue
		}
		e.probeBlk = chunk
		e.matched = make([]bool, len(chunk))
		e.probePos = 0
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
			// 当前构建行要与块内每一行探测行比较；批满返回时
			// probePos 记录进度，buildPos 暂不前进，下一批继续，
			// 保证跨批边界不漏匹配。
			build := e.buildChk[e.buildPos]
			for e.probePos < len(e.probeBlk) && len(out.Rows) < sz {
				i := e.probePos
				e.probePos++
				probe := e.probeBlk[i]
				combined := e.combine(probe, build)
				v, err := e.on.Eval(combined)
				if err != nil {
					return nil, err
				}
				if v.IsTruthy() {
					e.matched[i] = true
					out.Rows = append(out.Rows, combined)
				}
			}
			if e.probePos >= len(e.probeBlk) {
				e.buildPos++
				e.probePos = 0
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
