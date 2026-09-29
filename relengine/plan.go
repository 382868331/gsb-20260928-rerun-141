package relengine

import (
	"fmt"
	"strings"
)

// LogicalPlan 是逻辑查询树。每种节点声明自己的输出 Schema，
// Bind 沿树自底向上完成列解析与类型检查。优化器只变换这棵树，不碰执行器。
type LogicalPlan interface {
	// Schema 返回该节点输出行的结构（Bind 后列下标才有意义）。
	Schema() *Schema
	// Bind 做类型检查并返回自身输出 Schema。
	Bind() (*Schema, error)
	String() string
}

// ---- 基表扫描 ----

type Scan struct {
	Table  *Table
	Alias  string // 为空时用表名；连接两侧同名表时必须给不同别名
	schema *Schema
}

func NewScan(t *Table, alias string) *Scan {
	if alias == "" {
		alias = t.Name
	}
	cols := make([]Column, len(t.Schema.Cols))
	for i, c := range t.Schema.Cols {
		name := c.Name
		cols[i] = Column{Name: name, Type: c.Type, Nullable: c.Nullable}
	}
	schema := NewSchema(cols)
	for i, c := range t.Schema.Cols {
		schema.addQualifier(i, alias)
		if alias != t.Name {
			schema.addQualifier(i, t.Name)
		}
		schema.AddQualified(alias+"."+c.Name, i)
		if alias != t.Name {
			schema.AddQualified(t.Name+"."+c.Name, i)
		}
	}
	return &Scan{Table: t, Alias: alias, schema: schema}
}

func (s *Scan) Schema() *Schema        { return s.schema }
func (s *Scan) Bind() (*Schema, error) { return s.schema, nil }
func (s *Scan) String() string         { return fmt.Sprintf("Scan(%s AS %s)", s.Table.Name, s.Alias) }

// ---- 过滤 ----

type Filter struct {
	Child LogicalPlan
	Cond  Expr
}

func NewFilter(child LogicalPlan, cond Expr) *Filter {
	return &Filter{Child: child, Cond: cond}
}

func (f *Filter) Schema() *Schema { return f.Child.Schema() }

func (f *Filter) Bind() (*Schema, error) {
	in, err := f.Child.Bind()
	if err != nil {
		return nil, err
	}
	t, err := f.Cond.Check(in)
	if err != nil {
		return nil, fmt.Errorf("Filter 类型检查失败: %w", err)
	}
	if t != TypeBool && t != TypeNull {
		return nil, fmt.Errorf("Filter 条件必须是布尔表达式，得到 %s", t)
	}
	return in, nil
}

func (f *Filter) String() string {
	return fmt.Sprintf("Filter(%s)", f.Cond)
}

// ---- 投影 ----

// ProjItem 是一个输出列。Name 为空时用表达式文本。
type ProjItem struct {
	Expr Expr
	Name string
	ty   DataType
}

func Item(expr Expr, name string) ProjItem { return ProjItem{Expr: expr, Name: name} }

type Project struct {
	Child  LogicalPlan
	Items  []ProjItem
	schema *Schema
}

func NewProject(child LogicalPlan, items ...ProjItem) *Project {
	return &Project{Child: child, Items: items}
}

func (p *Project) Schema() *Schema { return p.schema }

func (p *Project) Bind() (*Schema, error) {
	in, err := p.Child.Bind()
	if err != nil {
		return nil, err
	}
	cols := make([]Column, len(p.Items))
	for i, it := range p.Items {
		t, err := it.Expr.Check(in)
		if err != nil {
			return nil, fmt.Errorf("Project 列 %d 类型检查失败: %w", i, err)
		}
		p.Items[i].ty = t
		name := it.Name
		if name == "" {
			name = it.Expr.String()
		}
		cols[i] = Column{Name: name, Type: t, Nullable: true} // 投影表达式结果保守视为可空
	}
	p.schema = NewSchema(cols)
	return p.schema, nil
}

func (p *Project) String() string {
	names := make([]string, len(p.Items))
	for i, it := range p.Items {
		n := it.Name
		if n == "" {
			n = it.Expr.String()
		}
		names[i] = n
	}
	return "Project(" + strings.Join(names, ", ") + ")"
}

// ---- 连接 ----

type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
)

func (j JoinType) String() string {
	switch j {
	case LeftJoin:
		return "LEFT"
	case RightJoin:
		return "RIGHT"
	default:
		return "INNER"
	}
}

type Join struct {
	L, R   LogicalPlan
	On     Expr
	Type   JoinType
	schema *Schema
}

func NewJoin(l, r LogicalPlan, on Expr, jt JoinType) *Join {
	return &Join{L: l, R: r, On: on, Type: jt}
}

func (j *Join) Schema() *Schema { return j.schema }

func (j *Join) Bind() (*Schema, error) {
	ls, err := j.L.Bind()
	if err != nil {
		return nil, err
	}
	rs, err := j.R.Bind()
	if err != nil {
		return nil, err
	}
	combined := AppendSchema(ls, rs)
	t, err := j.On.Check(combined)
	if err != nil {
		return nil, fmt.Errorf("Join ON 类型检查失败: %w", err)
	}
	if t != TypeBool && t != TypeNull {
		return nil, fmt.Errorf("Join ON 必须是布尔表达式，得到 %s", t)
	}
	// 外连接中来自“可选一侧”的列在输出里变成可空。
	lcols := cloneCols(ls.Cols)
	rcols := cloneCols(rs.Cols)
	if j.Type == LeftJoin {
		for i := range rcols {
			rcols[i].Nullable = true
		}
	}
	if j.Type == RightJoin {
		for i := range lcols {
			lcols[i].Nullable = true
		}
	}
	out := make([]Column, 0, len(lcols)+len(rcols))
	out = append(out, lcols...)
	out = append(out, rcols...)
	j.schema = NewSchema(out)
	j.schema.qualifiers = make([][]string, len(out))
	for i := range ls.Cols {
		j.schema.qualifiers[i] = append(j.schema.qualifiers[i], ls.qualifiers[i]...)
		for _, q := range ls.qualifiers[i] {
			j.schema.AddQualified(q+"."+ls.Cols[i].Name, i)
		}
	}
	for i := range rs.Cols {
		at := len(ls.Cols) + i
		j.schema.qualifiers[at] = append(j.schema.qualifiers[at], rs.qualifiers[i]...)
		for _, q := range rs.qualifiers[i] {
			j.schema.AddQualified(q+"."+rs.Cols[i].Name, at)
		}
	}
	return j.schema, nil
}

func cloneCols(cs []Column) []Column {
	out := make([]Column, len(cs))
	copy(out, cs)
	return out
}

func (j *Join) String() string { return fmt.Sprintf("Join[%s](%s)", j.Type, j.On) }

// ---- 分组聚合 ----

type Aggregate struct {
	Child   LogicalPlan
	GroupBy []Expr // 可以为空，表示全局聚合（无 GROUP BY 也输出一行）
	Aggs    []*AggExpr
	schema  *Schema
	gbTypes []DataType
	agTypes []DataType
}

func NewAggregate(child LogicalPlan, groupBy []Expr, aggs []*AggExpr) *Aggregate {
	return &Aggregate{Child: child, GroupBy: groupBy, Aggs: aggs}
}

func (a *Aggregate) Schema() *Schema { return a.schema }

func (a *Aggregate) Bind() (*Schema, error) {
	in, err := a.Child.Bind()
	if err != nil {
		return nil, err
	}
	a.gbTypes, err = CheckExprs(in, a.GroupBy)
	if err != nil {
		return nil, fmt.Errorf("GROUP BY 类型检查失败: %w", err)
	}
	a.agTypes = make([]DataType, len(a.Aggs))
	for i, ag := range a.Aggs {
		t, err := ag.Check(in)
		if err != nil {
			return nil, fmt.Errorf("聚合 %s 类型检查失败: %w", ag, err)
		}
		a.agTypes[i] = t
	}
	cols := make([]Column, 0, len(a.GroupBy)+len(a.Aggs))
	for i, e := range a.GroupBy {
		cols = append(cols, Column{Name: e.String(), Type: a.gbTypes[i], Nullable: true})
	}
	for i, ag := range a.Aggs {
		cols = append(cols, Column{Name: ag.String(), Type: a.agTypes[i], Nullable: true})
	}
	a.schema = NewSchema(cols)
	return a.schema, nil
}

func (a *Aggregate) String() string {
	parts := make([]string, 0, len(a.Aggs))
	for _, ag := range a.Aggs {
		parts = append(parts, ag.String())
	}
	gb := ""
	if len(a.GroupBy) > 0 {
		gbs := make([]string, len(a.GroupBy))
		for i, e := range a.GroupBy {
			gbs[i] = e.String()
		}
		gb = " BY " + strings.Join(gbs, ", ")
	}
	return fmt.Sprintf("Aggregate%s(%s)", gb, strings.Join(parts, ", "))
}

// printPlan 以缩进形式打印计划树。
func printPlan(p LogicalPlan, depth int) string {
	indent := strings.Repeat("  ", depth)
	s := indent + p.String() + "  ->  [" + p.Schema().String() + "]\n"
	switch v := p.(type) {
	case *Filter:
		s += printPlan(v.Child, depth+1)
	case *Project:
		s += printPlan(v.Child, depth+1)
	case *Join:
		s += printPlan(v.L, depth+1)
		s += printPlan(v.R, depth+1)
	case *Aggregate:
		s += printPlan(v.Child, depth+1)
	}
	return s
}

// Explain 返回绑定后的可读计划。
func Explain(p LogicalPlan) (string, error) {
	if _, err := p.Bind(); err != nil {
		return "", err
	}
	return printPlan(p, 0), nil
}
