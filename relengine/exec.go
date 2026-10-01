package relengine

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
)

// Batch 是执行器之间传递的数据单位。逐批拉取使得内存占用可由 BatchRows 控制：
// 上游每次只产生一批，下游处理完才要下一批；需要整侧输入的算子（Join/聚合）
// 在放不下时把数据按哈希切成磁盘分片，再逐分片读回处理。
type Batch struct {
	Rows []Tuple
}

// ExecOptions 控制资源使用与临时文件位置。
type ExecOptions struct {
	BatchRows int    // 每批行数上限（>0）
	MemRows   int    // 单个“需要全量输入”的算子允许在内存保留的行数上限（>0）
	TempDir   string // 落盘目录；为空时用 os.TempDir
	// SpillAggregate 强制分组聚合走“分片落盘 -> 逐片聚合”的受限内存路径。
	// Join 会根据右/左构建侧实际行数自动选择是否落盘。
	SpillAggregate bool
}

func (o ExecOptions) withDefaults() ExecOptions {
	if o.BatchRows <= 0 {
		o.BatchRows = 1024
	}
	if o.MemRows <= 0 {
		o.MemRows = 4096
	}
	if o.TempDir == "" {
		o.TempDir = os.TempDir()
	}
	return o
}

// Executor 是物理算子接口。Next 返回下一批；无数据时返回 io.EOF。
// Close 必须释放所有中间结果（包括磁盘分片文件），无论是否出错或被取消。
type Executor interface {
	Next(ctx context.Context) (*Batch, error)
	Close() error
}

// Drain 把执行器结果全部收集到内存（测试与小结果集使用）。
func Drain(ctx context.Context, e Executor) ([]Tuple, error) {
	defer e.Close()
	var out []Tuple
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b, err := e.Next(ctx)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		for _, r := range b.Rows {
			out = append(out, r.Clone())
		}
	}
}

// ---- 基表扫描：按 BatchRows 切批 ----

type scanExec struct {
	tbl *Table
	pos int
	sz  int
}

func (e *scanExec) Next(ctx context.Context) (*Batch, error) {
	if e.pos >= len(e.tbl.rows) {
		return nil, io.EOF
	}
	end := e.pos + e.sz
	if end > len(e.tbl.rows) {
		end = len(e.tbl.rows)
	}
	b := &Batch{Rows: make([]Tuple, end-e.pos)}
	for i := e.pos; i < end; i++ {
		b.Rows[i-e.pos] = e.tbl.rows[i].Clone()
	}
	e.pos = end
	return b, nil
}

func (e *scanExec) Close() error { return nil }

type sliceSource struct {
	rows []Tuple
	pos  int
	sz   int
}

func (s *sliceSource) Next(context.Context) (*Batch, error) {
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

// ---- 过滤：逐批逐行，NULL/FALSE 都被丢弃 ----

type filterExec struct {
	child Executor
	cond  Expr
	sz    int
}

func (e *filterExec) Next(ctx context.Context) (*Batch, error) {
	for {
		b, err := e.child.Next(ctx)
		if err != nil {
			return nil, err
		}
		out := &Batch{Rows: make([]Tuple, 0, len(b.Rows))}
		for _, row := range b.Rows {
			v, err := e.cond.Eval(row)
			if err != nil {
				return nil, err
			}
			if v.IsTruthy() { // 只有 TRUE 通过；UNKNOWN/FALSE 不通过
				out.Rows = append(out.Rows, row)
			}
		}
		if len(out.Rows) > 0 {
			return out, nil
		}
	}
}

func (e *filterExec) Close() error { return e.child.Close() }

// ---- 投影 ----

type projectExec struct {
	child Executor
	items []ProjItem
}

func (e *projectExec) Next(ctx context.Context) (*Batch, error) {
	b, err := e.child.Next(ctx)
	if err != nil {
		return nil, err
	}
	out := &Batch{Rows: make([]Tuple, len(b.Rows))}
	for i, row := range b.Rows {
		nr := make(Tuple, len(e.items))
		for j, it := range e.items {
			v, err := it.Expr.Eval(row)
			if err != nil {
				return nil, err
			}
			nr[j] = v
		}
		out.Rows[i] = nr
	}
	return out, nil
}

func (e *projectExec) Close() error { return e.child.Close() }

// ---- 磁盘分片：值的 gob 编解码 ----

func init() {
	// gob 需要注册所有可能出现在 Value 具体字段里的具体类型；
	// Value 本身是结构体，字段都是基础类型，无需额外注册。
}

// spillPartition 是一个磁盘分片：打开的文件 + gob 编码器。
type spillPartition struct {
	f    *os.File
	enc  *gob.Encoder
	path string
	rows int64
}

// spiller 管理一次溢出产生的全部分片，负责统一清理。
type spiller struct {
	parts []*spillPartition
	dir   string
}

func newSpiller(tempDir string, n int) (*spiller, error) {
	dir, err := os.MkdirTemp(tempDir, "relengine-spill-*")
	if err != nil {
		return nil, fmt.Errorf("创建临时目录失败: %w", err)
	}
	s := &spiller{dir: dir}
	for i := 0; i < n; i++ {
		path := filepath.Join(dir, fmt.Sprintf("part-%d.gob", i))
		f, err := os.Create(path)
		if err != nil {
			_ = s.cleanup()
			return nil, err
		}
		s.parts = append(s.parts, &spillPartition{f: f, enc: gob.NewEncoder(f), path: path})
	}
	return s, nil
}

// gobTuple 是 Tuple 的线格式：gob 对具名类型切片需要显式包装。
type gobBatch struct {
	Rows []Tuple
}

func (s *spiller) write(part int, rows []Tuple) error {
	p := s.parts[part]
	if err := p.enc.Encode(gobBatch{Rows: rows}); err != nil {
		return err
	}
	p.rows += int64(len(rows))
	return nil
}

func (s *spiller) cleanup() error {
	var firstErr error
	for _, p := range s.parts {
		if p.f != nil {
			if err := p.f.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	if err := os.RemoveAll(s.dir); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// reader 顺序读回某个分片的批次。
type partitionReader struct {
	f   *os.File
	dec *gob.Decoder
}

func (s *spiller) reader(part int) (*partitionReader, error) {
	// 先关闭写句柄确保数据落盘，再以只读方式重新打开。
	p := s.parts[part]
	f, err := os.Open(p.path)
	if err != nil {
		return nil, err
	}
	p.f = f // 统一在 cleanup 中关闭
	return &partitionReader{f: f, dec: gob.NewDecoder(f)}, nil
}

func (r *partitionReader) next() ([]Tuple, error) {
	var gb gobBatch
	if err := r.dec.Decode(&gb); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("临时文件已损坏或被截断: %w", err)
		}
		return nil, err
	}
	return gb.Rows, nil
}

// keyHash 对若干列组成的键做稳定 FNV 风格哈希（NULL 与各类型都参与）。
func keyHash(row Tuple, idx []int) uint64 {
	const (
		offset64 = 1469598103934665603
		prime    = 1099511628211
	)
	h := uint64(offset64)
	mix := func(b byte) {
		h ^= uint64(b)
		h *= prime
	}
	for _, i := range idx {
		v := row[i]
		if v.Null {
			mix(0)
			continue
		}
		mix(byte(v.Typ))
		switch v.Typ {
		case TypeInt:
			x := v.I
			for k := 0; k < 8; k++ {
				mix(byte(x))
				x >>= 8
			}
		case TypeFloat:
			x := int64(math.Float64bits(v.F))
			for k := 0; k < 8; k++ {
				mix(byte(x))
				x >>= 8
			}
		case TypeString:
			for k := 0; k < len(v.S); k++ {
				mix(v.S[k])
			}
		case TypeBool:
			if v.B {
				mix(1)
			} else {
				mix(2)
			}
		}
	}
	return h
}

// groupKey 是 map 用的可比较键：把键列序列化成字符串。
// NULL 用前缀标记，与任何具体值（包括字符串 "\x00..."）都不会碰撞。
func groupKey(row Tuple, idx []int) string {
	b := make([]byte, 0, len(idx)*8)
	for _, i := range idx {
		v := row[i]
		if v.Null {
			b = append(b, 0, 0) // NULL 标记
			continue
		}
		b = append(b, 1, byte(v.Typ))
		switch v.Typ {
		case TypeInt:
			b = appendVarint(b, v.I)
		case TypeFloat:
			b = appendVarint(b, int64(math.Float64bits(v.F)))
		case TypeString:
			b = append(b, []byte(v.S)...)
		case TypeBool:
			if v.B {
				b = append(b, 1)
			} else {
				b = append(b, 0)
			}
		}
		b = append(b, 0) // 分隔符，避免相邻字段粘连
	}
	return string(b)
}

func appendVarint(b []byte, x int64) []byte {
	u := uint64(x)
	for u >= 0x80 {
		b = append(b, byte(u)|0x80)
		u >>= 7
	}
	return append(b, byte(u))
}

// nullRow 构造指定宽度的全 NULL 行（外连接补位）。
func nullRow(n int) Tuple {
	t := make(Tuple, n)
	for i := range t {
		t[i] = NullValue()
	}
	return t
}

var _ = sort.Slice // sort 预留给后续 merge 计划，占位避免未使用导入
var _ = io.EOF
