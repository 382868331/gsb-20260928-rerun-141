// Package relengine 是一个不依赖任何现成数据库的本地关系查询引擎。
//
// 数据模型约定（第一版即明确，后续表达式沿用同一套规则）：
//
//   - 行（Tuple）是有序的值数组，附带 Schema 描述每列名称与类型。
//   - 重复行是有意义的：关系在这里是“多重集合（bag）”，不是数学集合。
//     两次插入完全相同的行会得到两条记录，COUNT(*) 与连接放大都按行计数。
//   - NULL 表示“未知/缺失”，与任何值（包括另一个 NULL）的普通比较结果都不是
//     TRUE 而是 UNKNOWN。筛选只保留结果为 TRUE 的行，因此 NULL 不会被过滤成
//     匹配，也不会被当成等于某个具体值。NULL 与 NULL 也不相等。
//   - 聚合里 COUNT(*) 统计行数（含含 NULL 的行）；COUNT(col) 只统计该列非 NULL 的行；
//     SUM/AVG/MIN/MAX 忽略 NULL 输入；没有任何非 NULL 输入时结果为 NULL。
package relengine

import (
	"fmt"
	"strings"
)

// DataType 是列的逻辑类型。第一版支持整数、浮点、字符串、布尔。
// NULL 值本身可以出现在任意可空列中，不单独占一个类型。
type DataType int

const (
	TypeNull DataType = iota // 仅用于未类型化的字面 NULL；列类型不会是它
	TypeInt
	TypeFloat
	TypeString
	TypeBool
)

func (t DataType) String() string {
	switch t {
	case TypeInt:
		return "INT"
	case TypeFloat:
		return "FLOAT"
	case TypeString:
		return "STRING"
	case TypeBool:
		return "BOOL"
	default:
		return "NULL"
	}
}

// Value 是表达式计算结果。Null 为 true 时其余字段无意义。
type Value struct {
	Null bool
	Typ  DataType
	I    int64
	F    float64
	S    string
	B    bool
}

func NullValue() Value  { return Value{Null: true, Typ: TypeNull} }
func Int(v int64) Value { return Value{Typ: TypeInt, I: v} }
func Float(v float64) Value {
	if v != v { // NaN 统一归一成 NULL，避免排序/分组出现不可预测键
		return NullValue()
	}
	return Value{Typ: TypeFloat, F: v}
}
func Str(v string) Value { return Value{Typ: TypeString, S: v} }
func Bool(v bool) Value  { return Value{Typ: TypeBool, B: v} }

// IsTruthy 把布尔值映射到筛选语义：只有 TRUE 通过；FALSE 与 NULL(UNKNOWN) 都不通过。
func (v Value) IsTruthy() bool { return !v.Null && v.Typ == TypeBool && v.B }

func (v Value) String() string {
	if v.Null {
		return "NULL"
	}
	switch v.Typ {
	case TypeInt:
		return fmt.Sprintf("%d", v.I)
	case TypeFloat:
		return fmt.Sprintf("%g", v.F)
	case TypeString:
		return v.S
	case TypeBool:
		if v.B {
			return "true"
		}
		return "false"
	default:
		return "?"
	}
}

// Column 是 Schema 中的一列。
// Nullable 决定该列是否允许 NULL；中间结果（如外连接补位列）恒为可空。
type Column struct {
	Name     string
	Type     DataType
	Nullable bool
}

// Schema 是有序列定义。列通过下标访问，名称只用于解析与展示。
type Schema struct {
	Cols       []Column
	idx        map[string]int
	qualifiers [][]string
	qualified  map[string]int
}

func NewSchema(cols []Column) *Schema {
	s := &Schema{Cols: cols, idx: map[string]int{}, qualifiers: make([][]string, len(cols)), qualified: map[string]int{}}
	for i, c := range cols {
		if _, dup := s.idx[c.Name]; dup {
			// 名称冲突时保留第一次出现；限定名在计划层处理。
			continue
		}
		s.idx[c.Name] = i
	}
	return s
}

// AddQualified 登记 table.column 形式的限定列名。
func (s *Schema) AddQualified(qname string, i int) {
	if s.qualified == nil {
		s.qualified = map[string]int{}
	}
	if _, exists := s.qualified[qname]; !exists {
		s.qualified[qname] = i
	}
}

func (s *Schema) addQualifier(i int, tableName string) {
	if s.qualifiers == nil {
		s.qualifiers = make([][]string, len(s.Cols))
	}
	for _, q := range s.qualifiers[i] {
		if q == tableName {
			return
		}
	}
	s.qualifiers[i] = append(s.qualifiers[i], tableName)
}

func (s *Schema) Index(name string) (int, bool) {
	if i, ok := s.qualified[name]; ok {
		return i, ok
	}
	i, ok := s.idx[name]
	return i, ok
}

func (s *Schema) TypeAt(i int) DataType { return s.Cols[i].Type }

// AppendSchema 拼接两个 Schema（连接后左右列依次排列）。
func AppendSchema(a, b *Schema) *Schema {
	cols := make([]Column, 0, len(a.Cols)+len(b.Cols))
	cols = append(cols, a.Cols...)
	cols = append(cols, b.Cols...)
	s := NewSchema(cols)
	s.qualifiers = make([][]string, len(cols))
	for i := range a.Cols {
		s.qualifiers[i] = append(s.qualifiers[i], a.qualifiers[i]...)
		for _, q := range a.qualifiers[i] {
			s.AddQualified(q+"."+a.Cols[i].Name, i)
		}
	}
	for j := range b.Cols {
		i := len(a.Cols) + j
		s.qualifiers[i] = append(s.qualifiers[i], b.qualifiers[j]...)
		for _, q := range b.qualifiers[j] {
			s.AddQualified(q+"."+b.Cols[j].Name, i)
		}
	}
	return s
}

func (s *Schema) String() string {
	parts := make([]string, len(s.Cols))
	for i, c := range s.Cols {
		n := "NOT NULL"
		if c.Nullable {
			n = "NULL"
		}
		parts[i] = fmt.Sprintf("%s %s %s", c.Name, c.Type, n)
	}
	return strings.Join(parts, ", ")
}

// Tuple 是一行。值的顺序与所属 Schema 对应。
type Tuple []Value

// Clone 复制一行，避免分批缓冲与调用方共享底层数组。
func (t Tuple) Clone() Tuple {
	out := make(Tuple, len(t))
	copy(out, t)
	return out
}

// Table 是一张基表：Schema + 已插入的行（重复行全部保留）。
type Table struct {
	Name   string
	Schema *Schema
	rows   []Tuple
}

func NewTable(name string, schema *Schema) *Table {
	return &Table{Name: name, Schema: schema}
}

// Insert 插入一行并做最基本的类型与可空性检查。重复行会原样保留。
func (t *Table) Insert(row Tuple) error {
	if len(row) != len(t.Schema.Cols) {
		return fmt.Errorf("table %s: 期望 %d 列，得到 %d 列", t.Name, len(t.Schema.Cols), len(row))
	}
	for i, v := range row {
		col := t.Schema.Cols[i]
		if v.Null {
			if !col.Nullable {
				return fmt.Errorf("table %s: 列 %s 不允许 NULL", t.Name, col.Name)
			}
			continue
		}
		if v.Typ != col.Type {
			return fmt.Errorf("table %s: 列 %s 期望 %s，得到 %s", t.Name, col.Name, col.Type, v.Typ)
		}
	}
	cp := make(Tuple, len(row))
	copy(cp, row)
	t.rows = append(t.rows, cp)
	return nil
}

func (t *Table) Rows() []Tuple { return t.rows }

// Catalog 是引擎当前会话可见的基表集合。
type Catalog struct {
	tables map[string]*Table
}

func NewCatalog() *Catalog { return &Catalog{tables: map[string]*Table{}} }

func (c *Catalog) Create(t *Table) error {
	if _, exists := c.tables[t.Name]; exists {
		return fmt.Errorf("表 %s 已存在", t.Name)
	}
	c.tables[t.Name] = t
	return nil
}

func (c *Catalog) MustCreate(t *Table) {
	if err := c.Create(t); err != nil {
		panic(err)
	}
}

func (c *Catalog) Table(name string) (*Table, bool) {
	t, ok := c.tables[name]
	return t, ok
}
