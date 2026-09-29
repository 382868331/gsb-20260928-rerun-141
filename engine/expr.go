package engine

import (
	"fmt"
	"hash/fnv"
	"math"
)

type Expr interface {
	TypeCheck(Schema) (Type, error)
	Eval(Row) (any, error)
	String() string
}

type Literal struct {
	Value any
	typ   Type
}

func NewLiteral(value any) Literal {
	switch value.(type) {
	case nil:
		return Literal{Value: nil, typ: TypeNull}
	case int64:
		return Literal{Value: value, typ: TypeInt}
	case float64:
		return Literal{Value: value, typ: TypeFloat}
	case string:
		return Literal{Value: value, typ: TypeString}
	case bool:
		return Literal{Value: value, typ: TypeBool}
	default:
		panic(fmt.Sprintf("unsupported literal type %T", value))
	}
}

func (e Literal) TypeCheck(Schema) (Type, error) { return e.typ, nil }
func (e Literal) Eval(Row) (any, error)          { return e.Value, nil }
func (e Literal) String() string                 { return fmt.Sprint(e.Value) }

type ColumnRef struct {
	Name string
	typ  Type
	idx  int
}

func (e *ColumnRef) TypeCheck(schema Schema) (Type, error) {
	idx, ok := schema.Index(e.Name)
	if !ok {
		return TypeNull, fmt.Errorf("unknown column %q", e.Name)
	}
	e.idx = idx
	e.typ = schema[idx].Type
	return e.typ, nil
}

func (e *ColumnRef) Eval(row Row) (any, error) { return row[e.idx], nil }
func (e *ColumnRef) String() string            { return e.Name }

type BinaryOp string

const (
	OpAdd BinaryOp = "+"
	OpSub BinaryOp = "-"
	OpMul BinaryOp = "*"
	OpDiv BinaryOp = "/"
	OpEq  BinaryOp = "="
	OpNeq BinaryOp = "<>"
	OpLt  BinaryOp = "<"
	OpLte BinaryOp = "<="
	OpGt  BinaryOp = ">"
	OpGte BinaryOp = ">="
	OpAnd BinaryOp = "AND"
	OpOr  BinaryOp = "OR"
)

type BinaryExpr struct {
	Op          BinaryOp
	Left, Right Expr
	typ         Type
}

func (e *BinaryExpr) TypeCheck(schema Schema) (Type, error) {
	lt, err := e.Left.TypeCheck(schema)
	if err != nil {
		return TypeNull, err
	}
	rt, err := e.Right.TypeCheck(schema)
	if err != nil {
		return TypeNull, err
	}
	if e.Op == OpAnd || e.Op == OpOr {
		if lt != TypeBool && lt != TypeNull || rt != TypeBool && rt != TypeNull {
			return TypeNull, fmt.Errorf("%s requires boolean operands, got %s and %s", e.Op, lt, rt)
		}
		e.typ = TypeBool
		return TypeBool, nil
	}
	if isComparison(e.Op) {
		if comparableTypes(lt, rt) != nil {
			return TypeNull, comparableTypes(lt, rt)
		}
		e.typ = TypeBool
		return TypeBool, nil
	}
	if !numeric(lt) || !numeric(rt) {
		return TypeNull, fmt.Errorf("%s requires numeric operands, got %s and %s", e.Op, lt, rt)
	}
	if lt == TypeFloat || rt == TypeFloat {
		e.typ = TypeFloat
	} else {
		e.typ = TypeInt
	}
	return e.typ, nil
}

func (e *BinaryExpr) Eval(row Row) (any, error) {
	lv, err := e.Left.Eval(row)
	if err != nil {
		return nil, err
	}
	rv, err := e.Right.Eval(row)
	if err != nil {
		return nil, err
	}
	if e.Op == OpAnd {
		return threeValueAnd(lv, rv), nil
	}
	if e.Op == OpOr {
		return threeValueOr(lv, rv), nil
	}
	if lv == nil || rv == nil {
		return nil, nil
	}
	if isComparison(e.Op) {
		c, ok := compareComparable(lv, rv)
		if !ok {
			return nil, fmt.Errorf("cannot compare %T and %T", lv, rv)
		}
		switch e.Op {
		case OpEq:
			return c == 0, nil
		case OpNeq:
			return c != 0, nil
		case OpLt:
			return c < 0, nil
		case OpLte:
			return c <= 0, nil
		case OpGt:
			return c > 0, nil
		case OpGte:
			return c >= 0, nil
		}
	}
	l, lok := toFloat(lv)
	r, rok := toFloat(rv)
	if !lok || !rok {
		return nil, fmt.Errorf("arithmetic requires numeric values")
	}
	var result float64
	switch e.Op {
	case OpAdd:
		result = l + r
	case OpSub:
		result = l - r
	case OpMul:
		result = l * r
	case OpDiv:
		if r == 0 {
			return nil, nil
		}
		result = l / r
	}
	if e.typ == TypeInt && e.Op != OpDiv {
		return int64(result), nil
	}
	return result, nil
}

func (e *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", e.Left.String(), e.Op, e.Right.String())
}

type UnaryOp string

const (
	OpNot    UnaryOp = "NOT"
	OpIsNull UnaryOp = "IS NULL"
)

type UnaryExpr struct {
	Op   UnaryOp
	Expr Expr
}

func (e *UnaryExpr) TypeCheck(schema Schema) (Type, error) {
	t, err := e.Expr.TypeCheck(schema)
	if err != nil {
		return TypeNull, err
	}
	if e.Op == OpNot && t != TypeBool && t != TypeNull {
		return TypeNull, fmt.Errorf("NOT requires boolean, got %s", t)
	}
	return TypeBool, nil
}

func (e *UnaryExpr) Eval(row Row) (any, error) {
	v, err := e.Expr.Eval(row)
	if err != nil {
		return nil, err
	}
	if e.Op == OpIsNull {
		return v == nil, nil
	}
	if v == nil {
		return nil, nil
	}
	b, ok := v.(bool)
	if !ok {
		return nil, fmt.Errorf("NOT requires boolean value")
	}
	return !b, nil
}

func (e *UnaryExpr) String() string { return fmt.Sprintf("%s(%s)", e.Op, e.Expr.String()) }

func numeric(t Type) bool { return t == TypeInt || t == TypeFloat || t == TypeNull }

func comparableTypes(left, right Type) error {
	if left == TypeNull || right == TypeNull {
		return nil
	}
	if numeric(left) && numeric(right) {
		return nil
	}
	if left == right && (left == TypeString || left == TypeBool) {
		return nil
	}
	return fmt.Errorf("incompatible comparable types %s and %s", left, right)
}
func isComparison(op BinaryOp) bool {
	switch op {
	case OpEq, OpNeq, OpLt, OpLte, OpGt, OpGte:
		return true
	}
	return false
}

func toFloat(value any) (float64, bool) {
	switch value := value.(type) {
	case int64:
		return float64(value), true
	case float64:
		return value, true
	default:
		return 0, false
	}
}

func compareNumeric(left, right any) (int, bool) {
	l, lok := toFloat(left)
	r, rok := toFloat(right)
	if !lok || !rok {
		return 0, false
	}
	if l < r {
		return -1, true
	}
	if l > r {
		return 1, true
	}
	return 0, true
}

func threeValueAnd(left, right any) any {
	if left == false || right == false {
		return false
	}
	if left == nil || right == nil {
		return nil
	}
	return true
}

func threeValueOr(left, right any) any {
	if left == true || right == true {
		return true
	}
	if left == nil || right == nil {
		return nil
	}
	return false
}

func hashRow(parts []any) uint64 {
	h := fnv.New64a()
	for _, part := range parts {
		if part == nil {
			h.Write([]byte{0})
		} else {
			h.Write([]byte(valueSortKey(part)))
		}
	}
	return h.Sum64()
}

func floatMaybeInt(value float64) any {
	if math.Trunc(value) == value && !math.IsInf(value, 0) {
		return int64(value)
	}
	return value
}
