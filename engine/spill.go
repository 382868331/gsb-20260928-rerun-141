package engine

import (
	"bufio"
	"encoding/gob"
	"io"
	"os"
)

type chunkWriter struct {
	file *os.File
	bw   *bufio.Writer
	enc  *gob.Encoder
}

func newChunkWriter(dir, prefix string) (*chunkWriter, error) {
	file, err := os.CreateTemp(dir, prefix+"-*.rows")
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriter(file)
	return &chunkWriter{file: file, bw: bw, enc: gob.NewEncoder(bw)}, nil
}

func (w *chunkWriter) write(rows []Row) error {
	for _, row := range rows {
		if err := w.enc.Encode(row); err != nil {
			return err
		}
	}
	return nil
}

func (w *chunkWriter) close() error {
	if err := w.bw.Flush(); err != nil {
		_ = w.file.Close()
		_ = os.Remove(w.file.Name())
		return err
	}
	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		_ = os.Remove(w.file.Name())
		return err
	}
	err := w.file.Close()
	if err != nil {
		_ = os.Remove(w.file.Name())
		return err
	}
	return nil
}

func (w *chunkWriter) remove() {
	_ = os.Remove(w.file.Name())
}

type chunkReader struct {
	file *os.File
	dec  *gob.Decoder
}

func openChunkReader(name string) (*chunkReader, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return &chunkReader{file: file, dec: gob.NewDecoder(bufio.NewReader(file))}, nil
}

func (r *chunkReader) next() (Row, error) {
	var row Row
	if err := r.dec.Decode(&row); err != nil {
		if err == io.EOF {
			return nil, ErrEnd
		}
		return nil, err
	}
	return row, nil
}

func (r *chunkReader) close() error { return r.file.Close() }

func drainSpill(name string, batchSize int, fn func([]Row) error) (err error) {
	reader, err := openChunkReader(name)
	if err != nil {
		return err
	}
	defer func() { err = reader.close() }()
	batch := make([]Row, 0, batchSize)
	for {
		row, decodeErr := reader.next()
		if decodeErr == ErrEnd {
			break
		}
		if decodeErr != nil {
			return decodeErr
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
