package engine

import (
	"fmt"
	"strings"
)

type Node interface {
	Schema() Schema
	Check() error
	String() string
}

type ScanNode struct {
	table  Table
	schema Schema
}

func NewScan(table Table) *ScanNode {
	return &ScanNode{table: table, schema: table.Columns}
}
func (n *ScanNode) Check() error   { return n.table.validate() }
func (n *ScanNode) Schema() Schema { return n.schema }
func (n *ScanNode) String() string { return "Scan(" + n.table.Name + ")" }

type FilterNode struct {
	Input     Node
	Predicate Expr
}

func (n *FilterNode) Schema() Schema { return n.Input.Schema() }
func (n *FilterNode) Check() error {
	if _, err := n.Predicate.TypeCheck(n.Input.Schema()); err != nil {
		return err
	}
	return n.Input.Check()
}
func (n *FilterNode) String() string { return "Filter(" + n.Predicate.String() + ")" }

type ProjectItem struct {
	Expr Expr
	Name string
}

type ProjectNode struct {
	Input  Node
	Items  []ProjectItem
	schema Schema
}

func NewProject(input Node, items []ProjectItem) *ProjectNode {
	schema := make(Schema, len(items))
	for i, item := range items {
		schema[i] = Column{Name: item.Name}
	}
	return &ProjectNode{Input: input, Items: items, schema: schema}
}

func (n *ProjectNode) Check() error {
	if err := n.Input.Check(); err != nil {
		return err
	}
	for i := range n.Items {
		t, err := n.Items[i].Expr.TypeCheck(n.Input.Schema())
		if err != nil {
			return err
		}
		n.schema[i].Type = t
	}
	return nil
}
func (n *ProjectNode) Schema() Schema { return n.schema }
func (n *ProjectNode) String() string {
	names := make([]string, len(n.Items))
	for i, item := range n.Items {
		names[i] = item.Name
	}
	return "Project(" + strings.Join(names, ", ") + ")"
}

type JoinType string

const (
	JoinInner JoinType = "INNER"
	JoinLeft  JoinType = "LEFT"
)

type JoinNode struct {
	Left      Node
	Right     Node
	Type      JoinType
	LeftKeys  []Expr
	RightKeys []Expr
	Predicate Expr
	schema    Schema
}

func NewJoin(left, right Node, joinType JoinType, leftKeys, rightKeys []Expr, predicate Expr) *JoinNode {
	return &JoinNode{Left: left, Right: right, Type: joinType, LeftKeys: leftKeys, RightKeys: rightKeys, Predicate: predicate, schema: concatSchema(left.Schema(), right.Schema())}
}

func (n *JoinNode) Check() error {
	if err := n.Left.Check(); err != nil {
		return err
	}
	if err := n.Right.Check(); err != nil {
		return err
	}
	if len(n.LeftKeys) != len(n.RightKeys) {
		return fmt.Errorf("join key counts differ")
	}
	for i := range n.LeftKeys {
		lt, err := n.LeftKeys[i].TypeCheck(n.Left.Schema())
		if err != nil {
			return err
		}
		rt, err := n.RightKeys[i].TypeCheck(n.Right.Schema())
		if err != nil {
			return err
		}
		if (lt == TypeString || rt == TypeString || lt == TypeBool || rt == TypeBool) && lt != rt {
			return fmt.Errorf("join key %d types differ: %s and %s", i, lt, rt)
		}
	}
	if n.Predicate != nil {
		if _, err := n.Predicate.TypeCheck(n.schema); err != nil {
			return err
		}
	}
	return nil
}
func (n *JoinNode) Schema() Schema { return n.schema }
func (n *JoinNode) String() string { return string(n.Type) + " Join" }

type AggregateItem struct {
	Function string
	Arg      Expr
	Name     string
}

type AggregateNode struct {
	Input     Node
	GroupKeys []Expr
	Functions []AggregateItem
	keyNames  []string
	schema    Schema
}

func NewAggregate(input Node, groupKeys []ProjectItem, functions []AggregateItem) *AggregateNode {
	n := &AggregateNode{Input: input, Functions: functions}
	n.keyNames = make([]string, len(groupKeys))
	n.GroupKeys = make([]Expr, len(groupKeys))
	for i, item := range groupKeys {
		n.GroupKeys[i] = item.Expr
		n.keyNames[i] = item.Name
	}
	return n
}

func (n *AggregateNode) Check() error {
	if err := n.Input.Check(); err != nil {
		return err
	}
	n.schema = make(Schema, 0, len(n.GroupKeys)+len(n.Functions))
	inputSchema := n.Input.Schema()
	for i, key := range n.GroupKeys {
		t, err := key.TypeCheck(inputSchema)
		if err != nil {
			return err
		}
		n.schema = append(n.schema, Column{Name: n.keyNames[i], Type: t})
	}
	for _, fn := range n.Functions {
		t, err := checkAggregate(fn, inputSchema)
		if err != nil {
			return err
		}
		n.schema = append(n.schema, Column{Name: fn.Name, Type: t})
	}
	return nil
}
func (n *AggregateNode) Schema() Schema { return n.schema }
func (n *AggregateNode) String() string { return "Aggregate" }
