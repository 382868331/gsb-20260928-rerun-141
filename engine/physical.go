package engine

import (
	"context"
	"fmt"
)

type Source interface {
	Next(context.Context) (Row, error)
	Close() error
}

type Options struct {
	BatchSize       int
	MaxInMemoryRows int
	TempDir         string
}

func (o Options) normalize() Options {
	if o.BatchSize <= 0 {
		o.BatchSize = 1024
	}
	if o.MaxInMemoryRows <= 0 {
		o.MaxInMemoryRows = 100000
	}
	if o.TempDir == "" {
		o.TempDir = "."
	}
	return o
}

type errEnd struct{}

func (errEnd) Error() string { return "end of source" }

var ErrEnd = errEnd{}

type scanSource struct {
	table Table
	index int
}

func (s *scanSource) Next(ctx context.Context) (Row, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if s.index >= len(s.table.Rows) {
		return nil, ErrEnd
	}
	row := s.table.Rows[s.index]
	s.index++
	copyRow := append(Row(nil), row...)
	return copyRow, nil
}
func (s *scanSource) Close() error { return nil }

type filterSource struct {
	input     Source
	predicate Expr
}

func (s *filterSource) Next(ctx context.Context) (Row, error) {
	for {
		row, err := s.input.Next(ctx)
		if err != nil {
			return nil, err
		}
		value, err := s.predicate.Eval(row)
		if err != nil {
			return nil, err
		}
		if value != nil && value.(bool) {
			return row, nil
		}
	}
}
func (s *filterSource) Close() error { return s.input.Close() }

type projectSource struct {
	input Source
	items []ProjectItem
}

func (s *projectSource) Next(ctx context.Context) (Row, error) {
	row, err := s.input.Next(ctx)
	if err != nil {
		return nil, err
	}
	out := make(Row, len(s.items))
	for i, item := range s.items {
		value, err := item.Expr.Eval(row)
		if err != nil {
			return nil, err
		}
		out[i] = value
	}
	return out, nil
}
func (s *projectSource) Close() error { return s.input.Close() }

type memoryHashJoin struct {
	left, right    Source
	joinType       JoinType
	leftKeys       []Expr
	rightKeys      []Expr
	predicate      Expr
	rightWidth     int
	loaded         bool
	table          map[uint64][]Row
	currentMatches []Row
	currentLeft    Row
	currentRight   []Row
	matchIndex     int
	currentEmitted bool
}

func (j *memoryHashJoin) load(ctx context.Context) error {
	j.table = make(map[uint64][]Row)
	for {
		row, err := j.right.Next(ctx)
		if err == ErrEnd {
			return nil
		}
		if err != nil {
			return err
		}
		keys, err := evalKeys(j.rightKeys, row)
		if err != nil {
			return err
		}
		hash := hashRow(keys)
		j.table[hash] = append(j.table[hash], row)
	}
}

func (j *memoryHashJoin) Next(ctx context.Context) (Row, error) {
	if !j.loaded {
		if err := j.load(ctx); err != nil {
			return nil, err
		}
		j.loaded = true
	}
	for {
		for len(j.currentRight) > 0 {
			right := j.currentRight[0]
			j.currentRight = j.currentRight[1:]
			row := append(append(Row(nil), j.currentLeft...), right...)
			match, err := j.keysMatch(row)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
			ok, err := joinPredicateOk(j.predicate, row)
			if err != nil {
				return nil, err
			}
			if ok {
				j.currentEmitted = true
				return row, nil
			}
		}
		if j.joinType == JoinLeft && !j.currentEmitted && j.currentLeft != nil {
			out := append(append(Row(nil), j.currentLeft...), make(Row, j.rightWidth)...)
			j.currentEmitted = true
			j.currentLeft = nil
			return out, nil
		}
		left, err := j.left.Next(ctx)
		if err != nil {
			return nil, err
		}
		keys, err := evalKeys(j.leftKeys, left)
		if err != nil {
			return nil, err
		}
		j.currentLeft = left
		j.currentRight = append([]Row(nil), j.table[hashRow(keys)]...)
		j.currentEmitted = false
	}
}

func (j *memoryHashJoin) keysMatch(row Row) (bool, error) {
	if len(j.leftKeys) == 0 {
		return true, nil
	}
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
	return true, nil
}
func (j *memoryHashJoin) Close() error {
	err1 := j.left.Close()
	err2 := j.right.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func evalKeys(exprs []Expr, row Row) ([]any, error) {
	keys := make([]any, len(exprs))
	for i, expr := range exprs {
		v, err := expr.Eval(row)
		if err != nil {
			return nil, err
		}
		keys[i] = v
	}
	return keys, nil
}

func joinPredicateOk(predicate Expr, row Row) (bool, error) {
	if predicate == nil {
		return true, nil
	}
	v, err := predicate.Eval(row)
	if err != nil {
		return false, err
	}
	return v != nil && v.(bool), nil
}

func materialize(ctx context.Context, source Source, batchSize int, fn func([]Row) error) error {
	batch := make([]Row, 0, batchSize)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		row, err := source.Next(ctx)
		if err == ErrEnd {
			break
		}
		if err != nil {
			return err
		}
		batch = append(batch, row)
		if len(batch) == batchSize {
			if err := fn(batch); err != nil {
				return err
			}
			batch = make([]Row, 0, batchSize)
		}
	}
	if len(batch) > 0 {
		return fn(batch)
	}
	return nil
}

func runPlan(ctx context.Context, plan Source) ([]Row, error) {
	defer plan.Close()
	var rows []Row
	for {
		row, err := plan.Next(ctx)
		if err == ErrEnd {
			return rows, nil
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
}

func unsupported(node Node) error {
	return fmt.Errorf("unsupported logical node %T", node)
}
