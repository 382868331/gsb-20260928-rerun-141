package engine

import (
	"context"
	"fmt"
)

type Strategy string

const (
	StrategyHash  Strategy = "hash"
	StrategyBlock Strategy = "block-nested-loop"
)

type QueryResult struct {
	Schema Schema
	Rows   []Row
}

func Execute(ctx context.Context, db Database, node Node, opts Options, strategy Strategy) (QueryResult, error) {
	if err := node.Check(); err != nil {
		return QueryResult{}, err
	}
	opts = opts.normalize()
	source, err := build(node, db, opts, strategy)
	if err != nil {
		return QueryResult{}, err
	}
	rows, err := runPlan(ctx, source)
	if err != nil {
		return QueryResult{}, err
	}
	return QueryResult{Schema: node.Schema(), Rows: rows}, nil
}

func build(node Node, db Database, opts Options, strategy Strategy) (Source, error) {
	switch node := node.(type) {
	case *ScanNode:
		table, err := db.Table(node.table.Name)
		if err != nil {
			return nil, err
		}
		return &scanSource{table: table}, nil
	case *FilterNode:
		input, err := build(node.Input, db, opts, strategy)
		if err != nil {
			return nil, err
		}
		return &filterSource{input: input, predicate: node.Predicate}, nil
	case *ProjectNode:
		input, err := build(node.Input, db, opts, strategy)
		if err != nil {
			return nil, err
		}
		return &projectSource{input: input, items: node.Items}, nil
	case *AggregateNode:
		input, err := build(node.Input, db, opts, strategy)
		if err != nil {
			return nil, err
		}
		return &aggregateSource{input: input, node: node, opts: opts}, nil
	case *JoinNode:
		left, err := build(node.Left, db, opts, strategy)
		if err != nil {
			return nil, err
		}
		right, err := build(node.Right, db, opts, strategy)
		if err != nil {
			return nil, err
		}
		if strategy == StrategyBlock {
			return &blockNestedLoopJoin{
				left: left, right: right, joinType: node.Type,
				leftKeys: node.LeftKeys, rightKeys: node.RightKeys,
				predicate: node.Predicate, rightWidth: len(node.Right.Schema()), opts: opts,
			}, nil
		}
		return &memoryHashJoin{
			left: left, right: right, joinType: node.Type,
			leftKeys: node.LeftKeys, rightKeys: node.RightKeys,
			predicate: node.Predicate, rightWidth: len(node.Right.Schema()),
		}, nil
	default:
		return nil, unsupported(node)
	}
}

func Col(name string) *ColumnRef  { return &ColumnRef{Name: name} }
func Int(value int64) Literal     { return NewLiteral(value) }
func Float(value float64) Literal { return NewLiteral(value) }
func Text(value string) Literal   { return NewLiteral(value) }
func Bool(value bool) Literal     { return NewLiteral(value) }

func Binary(op BinaryOp, left, right Expr) *BinaryExpr {
	return &BinaryExpr{Op: op, Left: left, Right: right}
}

func Not(expr Expr) *UnaryExpr    { return &UnaryExpr{Op: OpNot, Expr: expr} }
func IsNull(expr Expr) *UnaryExpr { return &UnaryExpr{Op: OpIsNull, Expr: expr} }

func Describe(node Node) string {
	switch node := node.(type) {
	case *ScanNode:
		return node.String()
	case *FilterNode:
		return node.String() + " -> " + Describe(node.Input)
	case *ProjectNode:
		return node.String() + " -> " + Describe(node.Input)
	case *AggregateNode:
		return node.String() + " -> " + Describe(node.Input)
	case *JoinNode:
		return fmt.Sprintf("%s(%s, %s)", node.String(), Describe(node.Left), Describe(node.Right))
	default:
		return node.String()
	}
}
