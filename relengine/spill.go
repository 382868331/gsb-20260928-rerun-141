package relengine

import (
	"context"
	"errors"
	"fmt"
	"io"
)

const (
	spillBuildPart = 0
	spillProbePart = 1
)

// boundedJoinExec 在构建侧能放入内存时使用哈希表；超预算时把两侧流式写入临时文件，
// 再以 MemRows 大小的构建块反复扫描探测侧。输出逐批产生，不缓存多对多放大结果。
type boundedJoinExec struct {
	left, right Executor
	on          Expr
	jt          JoinType
	ln, rn      int
	leftEq      []int
	rightEq     []int
	opts        ExecOptions

	prepared    bool
	closed      bool
	spilled     bool
	finished    bool
	spill       *spiller
	inner       Executor
	buildReader *partitionReader
	buildRows   []Tuple
	lastBlock   bool

	unmatched      bool
	probeReader    *partitionReader
	probeRows      []Tuple
	probePos       int
	currentProbe   Tuple
	unmatchedBuild []Tuple
	unmatchedPos   int
	currentMatched bool
}

func newBoundedJoin(l, r Executor, on Expr, jt JoinType, ln, rn int,
	leftEq, rightEq []int, opts ExecOptions) *boundedJoinExec {
	return &boundedJoinExec{left: l, right: r, on: on, jt: jt, ln: ln, rn: rn,
		leftEq: leftEq, rightEq: rightEq, opts: opts.withDefaults()}
}

func (e *boundedJoinExec) orientation() (probe, build Executor, probeEq, buildEq []int, probeLeft bool) {
	if e.jt == RightJoin {
		return e.right, e.left, e.rightEq, e.leftEq, false
	}
	return e.left, e.right, e.leftEq, e.rightEq, true
}

func (e *boundedJoinExec) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true

	var firstErr error
	if e.inner != nil {
		if err := e.inner.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		e.inner = nil
	}
	if err := e.left.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := e.right.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if e.spill != nil {
		if err := e.spill.cleanup(); err != nil && firstErr == nil {
			firstErr = err
		}
		e.spill = nil
	}
	return firstErr
}

func (e *boundedJoinExec) fail(err error) error {
	_ = e.Close()
	return err
}

func writeInBatches(s *spiller, part int, rows []Tuple, size int) error {
	for len(rows) > 0 {
		n := len(rows)
		if n > size {
			n = size
		}
		if err := s.write(part, rows[:n]); err != nil {
			return err
		}
		rows = rows[n:]
	}
	return nil
}

func drainToSpill(ctx context.Context, src Executor, s *spiller, part, batchRows int, batches ...[]Tuple) error {
	for _, rows := range batches {
		if err := writeInBatches(s, part, rows, batchRows); err != nil {
			return err
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := src.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := writeInBatches(s, part, b.Rows, batchRows); err != nil {
			return err
		}
	}
}

func (e *boundedJoinExec) prepare(ctx context.Context) error {
	probe, build, probeEq, buildEq, probeLeft := e.orientation()
	limit := e.opts.MemRows
	batchRows := e.opts.BatchRows

	var inMemory []Tuple
	for len(inMemory) < limit {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := build.Next(ctx)
		if errors.Is(err, io.EOF) {
			e.inner = newMemHashJoin(probe, inMemory, e.on, e.jt, e.ln, e.rn,
				probeEq, buildEq, probeLeft, batchRows)
			return nil
		}
		if err != nil {
			return err
		}
		n := limit - len(inMemory)
		if n > len(b.Rows) {
			n = len(b.Rows)
		}
		inMemory = append(inMemory, b.Rows[:n]...)
		if n < len(b.Rows) {
			return e.spillBuild(ctx, probe, build, probeEq, buildEq, probeLeft,
				inMemory, b.Rows[n:])
		}
	}

	b, err := build.Next(ctx)
	if errors.Is(err, io.EOF) {
		e.inner = newMemHashJoin(probe, inMemory, e.on, e.jt, e.ln, e.rn,
			probeEq, buildEq, probeLeft, batchRows)
		return nil
	}
	if err != nil {
		return err
	}
	return e.spillBuild(ctx, probe, build, probeEq, buildEq, probeLeft, inMemory, b.Rows)
}

func (e *boundedJoinExec) spillBuild(ctx context.Context, probe, build Executor,
	probeEq, buildEq []int, probeLeft bool, firstBuild, nextBuild []Tuple) error {
	s, err := newSpiller(e.opts.TempDir, 2)
	if err != nil {
		return fmt.Errorf("创建连接临时文件失败: %w", err)
	}
	e.spill = s
	e.spilled = true
	if err := drainToSpill(ctx, build, s, spillBuildPart, e.opts.MemRows, firstBuild, nextBuild); err != nil {
		return fmt.Errorf("写入连接构建侧临时文件失败: %w", err)
	}
	firstBuild = nil
	nextBuild = nil
	if err := drainToSpill(ctx, probe, s, spillProbePart, e.opts.BatchRows); err != nil {
		return fmt.Errorf("写入连接探测侧临时文件失败: %w", err)
	}
	r, err := s.reader(spillBuildPart)
	if err != nil {
		return fmt.Errorf("打开连接构建侧临时文件失败: %w", err)
	}
	e.buildReader = r
	e.buildRows = nil
	e.lastBlock = false
	return e.nextBuildBlock(ctx)
}

type fileBatchSource struct {
	r       *partitionReader
	sz      int
	pending []Tuple
}

func (e *boundedJoinExec) closePartitionReader(r *partitionReader) error {
	if r == nil || r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	return err
}

func (s *fileBatchSource) Next(ctx context.Context) (*Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(s.pending) > 0 {
		n := len(s.pending)
		if n > s.sz {
			n = s.sz
		}
		b := &Batch{Rows: s.pending[:n]}
		s.pending = s.pending[n:]
		return b, nil
	}
	rows, err := s.r.next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("读取连接临时文件失败: %w", err)
	}
	if len(rows) > s.sz {
		b := &Batch{Rows: rows[:s.sz]}
		s.pending = rows[s.sz:]
		return b, nil
	}
	s.pending = nil
	return &Batch{Rows: rows}, nil
}

func (s *fileBatchSource) Close() error {
	if s.r == nil || s.r.f == nil {
		return nil
	}
	err := s.r.f.Close()
	s.r.f = nil
	s.pending = nil
	return err
}

func (e *boundedJoinExec) nextBuildBlock(ctx context.Context) error {
	if e.inner != nil {
		if err := e.inner.Close(); err != nil {
			return err
		}
		e.inner = nil
	}

	e.buildRows = e.buildRows[:0]
	for len(e.buildRows) < e.opts.MemRows {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := e.buildReader.next()
		if errors.Is(err, io.EOF) {
			e.lastBlock = true
			break
		}
		if err != nil {
			return fmt.Errorf("读取连接构建侧临时文件失败: %w", err)
		}
		e.buildRows = append(e.buildRows, rows...)
		if len(e.buildRows) >= e.opts.MemRows {
			e.buildRows = e.buildRows[:e.opts.MemRows]
			break
		}
	}

	probeReader, err := e.spill.reader(spillProbePart)
	if err != nil {
		return fmt.Errorf("打开连接探测侧临时文件失败: %w", err)
	}
	_, _, probeEq, buildEq, probeLeft := e.orientation()
	e.inner = newMemHashJoin(&fileBatchSource{r: probeReader, sz: e.opts.BatchRows},
		e.buildRows, e.on, InnerJoin, e.ln, e.rn, probeEq, buildEq, probeLeft, e.opts.BatchRows)
	return nil
}

func (e *boundedJoinExec) startUnmatched(ctx context.Context) error {
	if e.inner != nil {
		if err := e.inner.Close(); err != nil {
			return err
		}
		e.inner = nil
	}
	probeReader, err := e.spill.reader(spillProbePart)
	if err != nil {
		return fmt.Errorf("打开连接探测侧临时文件失败: %w", err)
	}
	buildReader, err := e.spill.reader(spillBuildPart)
	if err != nil {
		return fmt.Errorf("打开连接构建侧临时文件失败: %w", err)
	}
	e.probeReader = probeReader
	e.buildReader = buildReader
	e.probeRows = nil
	e.probePos = 0
	e.currentProbe = nil
	e.unmatchedBuild = nil
	e.unmatchedPos = 0
	e.currentMatched = false
	e.unmatched = true
	return e.nextUnmatchedProbe(ctx)
}

func (e *boundedJoinExec) nextUnmatchedProbe(ctx context.Context) error {
	for e.probePos >= len(e.probeRows) {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := e.probeReader.next()
		if errors.Is(err, io.EOF) {
			return io.EOF
		}
		if err != nil {
			return fmt.Errorf("读取连接探测侧临时文件失败: %w", err)
		}
		e.probeRows = rows
		e.probePos = 0
	}
	e.currentProbe = e.probeRows[e.probePos]
	e.probePos++
	if err := e.closePartitionReader(e.buildReader); err != nil {
		return err
	}
	r, err := e.spill.reader(spillBuildPart)
	if err != nil {
		return fmt.Errorf("打开连接构建侧临时文件失败: %w", err)
	}
	e.buildReader = r
	e.unmatchedBuild = nil
	e.unmatchedPos = 0
	e.currentMatched = false
	return nil
}

func (e *boundedJoinExec) nextUnmatchedBuild(ctx context.Context) error {
	for e.unmatchedPos >= len(e.unmatchedBuild) {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := e.buildReader.next()
		if errors.Is(err, io.EOF) {
			return io.EOF
		}
		if err != nil {
			return fmt.Errorf("读取连接构建侧临时文件失败: %w", err)
		}
		n := e.opts.MemRows
		if n > len(rows) {
			n = len(rows)
		}
		e.unmatchedBuild = rows[:n]
		e.unmatchedPos = 0
	}
	return nil
}

func (e *boundedJoinExec) buildUnmatchedRow(probe Tuple) Tuple {
	if e.jt == RightJoin {
		return append(append(Tuple{}, nullRow(e.ln)...), probe...)
	}
	return append(append(Tuple{}, probe...), nullRow(e.rn)...)
}

func (e *boundedJoinExec) nextUnmatched(ctx context.Context) (*Batch, error) {
	out := &Batch{}
	for len(out.Rows) < e.opts.BatchRows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if e.currentProbe == nil {
			if err := e.nextUnmatchedProbe(ctx); errors.Is(err, io.EOF) {
				return nil, io.EOF
			} else if err != nil {
				return nil, err
			}
		}

		if !e.currentMatched {
			if err := e.nextUnmatchedBuild(ctx); errors.Is(err, io.EOF) {
				out.Rows = append(out.Rows, e.buildUnmatchedRow(e.currentProbe))
				e.currentProbe = nil
				continue
			} else if err != nil {
				return nil, err
			}
			build := e.unmatchedBuild[e.unmatchedPos]
			e.unmatchedPos++
			combined := build
			if e.jt == RightJoin {
				combined = append(append(Tuple{}, build...), e.currentProbe...)
			} else {
				combined = append(append(Tuple{}, e.currentProbe...), build...)
			}
			v, err := e.on.Eval(combined)
			if err != nil {
				return nil, err
			}
			if v.IsTruthy() {
				e.currentMatched = true
				e.currentProbe = nil
			}
		} else {
			e.currentProbe = nil
		}
	}
	return out, nil
}

func (e *boundedJoinExec) Next(ctx context.Context) (*Batch, error) {
	if e.closed {
		return nil, fmt.Errorf("连接执行器已经结束")
	}
	if !e.prepared {
		if err := e.prepare(ctx); err != nil {
			return nil, e.fail(err)
		}
		e.prepared = true
	}
	if e.finished {
		return nil, io.EOF
	}
	if e.unmatched {
		b, err := e.nextUnmatched(ctx)
		if errors.Is(err, io.EOF) {
			e.finished = true
			_ = e.Close()
			return nil, io.EOF
		}
		if err != nil {
			return nil, e.fail(err)
		}
		return b, nil
	}

	b, err := e.inner.Next(ctx)
	if err == nil {
		return b, nil
	}
	if !errors.Is(err, io.EOF) {
		return nil, e.fail(err)
	}
	if e.spilled && e.lastBlock && (e.jt == LeftJoin || e.jt == RightJoin) && !e.unmatched {
		if err := e.startUnmatched(ctx); err != nil {
			return nil, e.fail(err)
		}
		return e.Next(ctx)
	}
	if !e.spilled || e.lastBlock {
		e.finished = true
		_ = e.Close()
		return nil, io.EOF
	}
	if err := e.nextBuildBlock(ctx); err != nil {
		return nil, e.fail(err)
	}
	return e.Next(ctx)
}
