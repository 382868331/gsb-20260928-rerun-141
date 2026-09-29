package engine

import (
	"context"
	"os"
)

type blockNestedLoopJoin struct {
	left       Source
	right      Source
	joinType   JoinType
	leftKeys   []Expr
	rightKeys  []Expr
	predicate  Expr
	rightWidth int
	opts       Options
	prepared   bool
	leftFile   string
	rightFiles []string
	leftCount  int
	matched    []uint64
	chunkIndex int
	rights     []Row
	rightIndex int
	leftIndex  int
	leftRows   []Row
	currLeft   Row
	padding    bool
	done       bool
}

func (j *blockNestedLoopJoin) prepare(ctx context.Context) error {
	leftWriter, err := newChunkWriter(j.opts.TempDir, "join-left")
	if err != nil {
		return err
	}
	rightWriter, err := newChunkWriter(j.opts.TempDir, "join-right")
	if err != nil {
		leftWriter.remove()
		return err
	}
	rightCount := 0
	if err := materialize(ctx, j.left, j.opts.BatchSize, func(rows []Row) error {
		j.leftCount += len(rows)
		return leftWriter.write(rows)
	}); err != nil {
		leftWriter.remove()
		rightWriter.remove()
		return err
	}
	if err := materialize(ctx, j.right, j.opts.BatchSize, func(rows []Row) error {
		for _, row := range rows {
			if rightCount > 0 && rightCount%j.opts.MaxInMemoryRows == 0 {
				if err := rightWriter.close(); err != nil {
					return err
				}
				j.rightFiles = append(j.rightFiles, rightWriter.file.Name())
				rightWriter, err = newChunkWriter(j.opts.TempDir, "join-right")
				if err != nil {
					return err
				}
			}
			if err := rightWriter.write([]Row{row}); err != nil {
				return err
			}
			rightCount++
		}
		return nil
	}); err != nil {
		leftWriter.remove()
		rightWriter.remove()
		return err
	}
	if rightCount == 0 {
		j.rightFiles = append(j.rightFiles, "")
	} else {
		j.rightFiles = append(j.rightFiles, rightWriter.file.Name())
	}
	if err := leftWriter.close(); err != nil {
		leftWriter.remove()
		rightWriter.remove()
		return err
	}
	if err := rightWriter.close(); err != nil {
		rightWriter.remove()
		return err
	}
	j.leftFile = leftWriter.file.Name()
	if j.joinType == JoinLeft {
		j.matched = make([]uint64, (j.leftCount+63)/64)
	}
	return nil
}

func (j *blockNestedLoopJoin) Next(ctx context.Context) (Row, error) {
	if !j.prepared {
		if err := j.prepare(ctx); err != nil {
			return nil, err
		}
		j.prepared = true
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if j.currLeft != nil {
			for j.rightIndex < len(j.rights) {
				right := j.rights[j.rightIndex]
				j.rightIndex++
				out := append(append(Row(nil), j.currLeft...), right...)
				ok, err := j.matches(out)
				if err != nil {
					return nil, err
				}
				if ok {
					j.markMatched(j.leftIndex - 1)
					return out, nil
				}
			}
			j.currLeft = nil
		}
		left, err := j.nextLeft()
		_ = left
		if err == ErrEnd {
			if err := j.nextChunk(ctx); err != nil {
				if err != ErrEnd {
					return nil, err
				}
				return j.nextUnmatched(ctx)
			}
			continue
		}
		if err != nil {
			return nil, err
		}
	}
}

func (j *blockNestedLoopJoin) nextChunk(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	for _, row := range j.leftRows {
		_ = row
	}
	j.leftRows = nil
	if err := drainSpill(j.leftFile, j.opts.BatchSize, func(rows []Row) error {
		j.leftRows = append(j.leftRows, rows...)
		return nil
	}); err != nil {
		return err
	}
	for {
		if j.chunkIndex >= len(j.rightFiles) {
			return ErrEnd
		}
		name := j.rightFiles[j.chunkIndex]
		j.chunkIndex++
		j.rights = nil
		if name != "" {
			if err := drainSpill(name, j.opts.BatchSize, func(rows []Row) error {
				j.rights = append(j.rights, rows...)
				return nil
			}); err != nil {
				return err
			}
		}
		j.leftIndex = 0
		j.rightIndex = 0
		if len(j.rights) > 0 || name == "" {
			return nil
		}
	}
}

func (j *blockNestedLoopJoin) nextLeft() (Row, error) {
	if j.leftIndex >= len(j.leftRows) {
		return nil, ErrEnd
	}
	j.currLeft = j.leftRows[j.leftIndex]
	j.leftIndex++
	j.rightIndex = 0
	return j.currLeft, nil
}

func (j *blockNestedLoopJoin) nextUnmatched(ctx context.Context) (Row, error) {
	if j.done || j.joinType != JoinLeft {
		return nil, ErrEnd
	}
	j.done = true
	if !j.padding {
		j.padding = true
		j.leftIndex = 0
	}
	if j.leftCount == 0 && len(j.rightFiles) == 0 {
		return nil, ErrEnd
	}
	if len(j.leftRows) == 0 {
		if err := drainSpill(j.leftFile, j.opts.BatchSize, func(rows []Row) error {
			j.leftRows = append(j.leftRows, rows...)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	for j.leftIndex < len(j.leftRows) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		index := j.leftIndex
		left := j.leftRows[index]
		j.leftIndex++
		if j.matched[index/64]&(1<<uint(index%64)) == 0 {
			return append(append(Row(nil), left...), make(Row, j.rightWidth)...), nil
		}
	}
	return nil, ErrEnd
}

func (j *blockNestedLoopJoin) markMatched(index int) {
	if j.matched != nil {
		j.matched[index/64] |= 1 << uint(index%64)
	}
}

func (j *blockNestedLoopJoin) matches(row Row) (bool, error) {
	if len(j.leftKeys) > 0 {
		split := len(row) - j.rightWidth
		leftValues, err := evalKeys(j.leftKeys, row[:split])
		if err != nil {
			return false, err
		}
		rightValues, err := evalKeys(j.rightKeys, row[split:])
		if err != nil {
			return false, err
		}
		for i := range leftValues {
			if leftValues[i] == nil || rightValues[i] == nil {
				return false, nil
			}
			if c, ok := compareComparable(leftValues[i], rightValues[i]); !ok || c != 0 {
				return false, nil
			}
		}
	}
	return joinPredicateOk(j.predicate, row)
}

func (j *blockNestedLoopJoin) Close() error {
	err1 := j.left.Close()
	err2 := j.right.Close()
	if j.leftFile != "" {
		_ = os.Remove(j.leftFile)
	}
	for _, name := range j.rightFiles {
		if name != "" {
			_ = os.Remove(name)
		}
	}
	if err1 != nil {
		return err1
	}
	return err2
}
