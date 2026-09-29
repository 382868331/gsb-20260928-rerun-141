package engine

import (
	"context"
	"hash/fnv"
	"os"
	"sort"
)

type aggregateState struct {
	Key Row
	Acc []accumulator
}

type aggregateSource struct {
	input     Source
	node      *AggregateNode
	opts      Options
	groups    []aggregateState
	index     int
	done      bool
	spillFile []string
}

func (s *aggregateSource) Next(ctx context.Context) (Row, error) {
	if !s.done {
		if err := s.execute(ctx); err != nil {
			return nil, err
		}
		s.done = true
	}
	if s.index >= len(s.groups) {
		return nil, ErrEnd
	}
	state := s.groups[s.index]
	s.index++
	out := append(Row(nil), state.Key...)
	for i, fn := range s.node.Functions {
		out = append(out, state.Acc[i].result(fn))
	}
	return out, nil
}

func (s *aggregateSource) execute(ctx context.Context) error {
	states := make(map[string]aggregateState)
	raw, err := newChunkWriter(s.opts.TempDir, "aggregate-raw")
	if err != nil {
		return err
	}
	s.spillFile = append(s.spillFile, raw.file.Name())
	overflow := false
	err = materialize(ctx, s.input, s.opts.BatchSize, func(rows []Row) error {
		if err := raw.write(rows); err != nil {
			return err
		}
		for _, row := range rows {
			key, err := evalKeys(s.node.GroupKeys, row)
			if err != nil {
				return err
			}
			token := rowToken(key)
			state, ok := states[token]
			if !ok {
				state = aggregateState{Key: key, Acc: make([]accumulator, len(s.node.Functions))}
			}
			for i, fn := range s.node.Functions {
				if err := state.Acc[i].add(fn, row); err != nil {
					return err
				}
			}
			states[token] = state
			if len(states) > s.opts.MaxInMemoryRows {
				overflow = true
			}
		}
		return nil
	})
	closeErr := raw.close()
	if err != nil || closeErr != nil {
		raw.remove()
		if err != nil {
			return err
		}
		return closeErr
	}
	if !overflow {
		_ = os.Remove(raw.file.Name())
		s.spillFile = nil
		s.groups = mapStates(states)
		return nil
	}
	parts, err := repartitionRows(ctx, &rowFileSource{file: raw.file.Name()}, s.opts, 0, s.node.GroupKeys, &s.spillFile)
	_ = os.Remove(raw.file.Name())
	if err != nil {
		return err
	}
	all := make([]aggregateState, 0)
	for _, file := range parts {
		partStates, err := aggregatePartition(ctx, file, s.node, s.opts, 1, &s.spillFile)
		_ = os.Remove(file)
		if err != nil {
			return err
		}
		all = append(all, partStates...)
	}
	s.groups = all
	return nil
}

func aggregatePartition(ctx context.Context, file string, node *AggregateNode, opts Options, salt uint64, cleanup *[]string) ([]aggregateState, error) {
	states := make(map[string]aggregateState)
	overflow := false
	err := drainSpill(file, opts.BatchSize, func(rows []Row) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		for _, row := range rows {
			key, err := evalKeys(node.GroupKeys, row)
			if err != nil {
				return err
			}
			token := rowToken(key)
			state, ok := states[token]
			if !ok {
				state = aggregateState{Key: key, Acc: make([]accumulator, len(node.Functions))}
			}
			for i, fn := range node.Functions {
				if err := state.Acc[i].add(fn, row); err != nil {
					return err
				}
			}
			states[token] = state
			if len(states) > opts.MaxInMemoryRows {
				overflow = true
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !overflow {
		return mapStates(states), nil
	}
	if salt > 6 {
		return nil, ErrMemoryLimit
	}
	parts, err := repartitionRows(ctx, &rowFileSource{file: file}, opts, salt, node.GroupKeys, cleanup)
	if err != nil {
		return nil, err
	}
	result := make([]aggregateState, 0)
	for _, part := range parts {
		partStates, err := aggregatePartition(ctx, part, node, opts, salt+1, cleanup)
		_ = os.Remove(part)
		if err != nil {
			return nil, err
		}
		result = append(result, partStates...)
	}
	return result, nil
}

func repartitionRows(ctx context.Context, source Source, opts Options, salt uint64, keys []Expr, cleanup *[]string) ([]string, error) {
	defer source.Close()
	writers := make([]*chunkWriter, 16)
	err := materialize(ctx, source, opts.BatchSize, func(rows []Row) error {
		for _, row := range rows {
			keyValues, err := evalKeys(keys, row)
			if err != nil {
				return err
			}
			hash := partitionHash(keyValues, salt) % uint64(len(writers))
			if writers[hash] == nil {
				writer, createErr := newChunkWriter(opts.TempDir, "aggregate")
				if createErr != nil {
					return createErr
				}
				writers[hash] = writer
				*cleanup = append(*cleanup, writer.file.Name())
			}
			if err := writers[hash].write([]Row{row}); err != nil {
				return err
			}
		}
		return nil
	})
	files := make([]string, 0, len(writers))
	for _, writer := range writers {
		if writer == nil {
			continue
		}
		if closeErr := writer.close(); closeErr != nil {
			return nil, closeErr
		}
		files = append(files, writer.file.Name())
	}
	return files, err
}

type rowFileSource struct {
	file   string
	reader *chunkReader
}

func (s *rowFileSource) Next(context.Context) (Row, error) {
	if s.reader == nil {
		reader, err := openChunkReader(s.file)
		if err != nil {
			return nil, err
		}
		s.reader = reader
	}
	row, err := s.reader.next()
	if err != nil {
		s.reader.close()
		s.reader = nil
		return nil, err
	}
	return row, nil
}
func (s *rowFileSource) Close() error {
	if s.reader == nil {
		return nil
	}
	err := s.reader.close()
	s.reader = nil
	return err
}

func (s *aggregateSource) Close() error {
	err := s.input.Close()
	for _, file := range s.spillFile {
		_ = os.Remove(file)
	}
	return err
}

func mapStates(states map[string]aggregateState) []aggregateState {
	result := make([]aggregateState, 0, len(states))
	for _, state := range states {
		result = append(result, state)
	}
	sortStates(result)
	return result
}

func partitionHash(row Row, salt uint64) uint64 {
	h := fnv.New64a()
	var saltBytes [8]byte
	for i := range saltBytes {
		saltBytes[i] = byte(salt >> (i * 8))
	}
	h.Write(saltBytes[:])
	h.Write([]byte(rowToken(row)))
	return h.Sum64()
}

func rowToken(row Row) string {
	token := make([]byte, 0)
	for _, value := range row {
		token = append(token, valueSortKey(value)...)
		token = append(token, 0)
	}
	return string(token)
}

func sortStates(states []aggregateState) {
	sort.SliceStable(states, func(i, j int) bool { return rowToken(states[i].Key) < rowToken(states[j].Key) })
}
