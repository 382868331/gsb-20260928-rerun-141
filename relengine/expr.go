package relengine

import (
	"fmt"
	"math"
)

// Expr 是表达式的抽象语法树。新增表达式只需实现该接口：
// Check 在绑定阶段做类型检查/推导，Eval 在执行阶段对一行求值。
// 所有表达式（包括以后新增的）都走同一个 Check 入口，因此类型规则集中在一处。
type Expr interface {
	// Check 在给定输入 Schema 下解析列引用并推导出结果类型。
	Check(in *Schema) (DataType, error)
	// Eval 对一行求值。返回 NULL 用 Value.Null 表达。
	Eval(row Tuple) (Value, error)
	// String 用于计划展示。
	String() string
}

// ---- 列引用 ----

type ColumnRef struct {
	Name string
	idx  int
	typ  DataType
}

func Col(name string) *ColumnRef { return &ColumnRef{Name: name, idx: -1} }

func (c *ColumnRef) Check(in *Schema) (DataType, error) {
	i, ok := in.Index(c.Name)
	if !ok {
		return TypeNull, fmt.Errorf("未知列: %s", c.Name)
	}
	c.idx = i
	c.typ = in.Cols[i].Type
	return c.typ, nil
}

func (c *ColumnRef) Eval(row Tuple) (Value, error) {
	if c.idx < 0 {
		return Value{}, fmt.Errorf("列 %s 尚未绑定", c.Name)
	}
	return row[c.idx], nil
}

func (c *ColumnRef) String() string { return c.Name }

// Index 返回绑定后的列下标（执行器构造投影时使用）。
func (c *ColumnRef) Index() int { return c.idx }

// ---- 字面量 ----

type Literal struct {
	Val Value
}

func Lit(v Value) *Literal { return &Literal{Val: v} }

func (l *Literal) Check(*Schema) (DataType, error) { return l.Val.Typ, nil }
func (l *Literal) Eval(Tuple) (Value, error)       { return l.Val, nil }
func (l *Literal) String() string                  { return l.Val.String() }

// ---- 二元运算 ----

// BinOp 是二元运算符标识。后续新增运算符只需在 switch 中补充类型规则与求值。
type BinOp int

const (
	OpAdd BinOp = iota
	OpSub
	OpMul
	OpDiv
	OpEq
	OpNeq
	OpLt
	OpLte
	OpGt
	OpGte
	OpAnd
	OpOr
)

func (op BinOp) String() string {
	switch op {
	case OpAdd:
		return "+"
	case OpSub:
		return "-"
	case OpMul:
		return "*"
	case OpDiv:
		return "/"
	case OpEq:
		return "="
	case OpNeq:
		return "<>"
	case OpLt:
		return "<"
	case OpLte:
		return "<="
	case OpGt:
		return ">"
	case OpGte:
		return ">="
	case OpAnd:
		return "AND"
	case OpOr:
		return "OR"
	}
	return "?"
}

type BinaryExpr struct {
	Op       BinOp
	L, R     Expr
	resultTy DataType
}

func Bin(op BinOp, l, r Expr) *BinaryExpr { return &BinaryExpr{Op: op, L: l, R: r} }

func isArith(op BinOp) bool {
	switch op {
	case OpAdd, OpSub, OpMul, OpDiv:
		return true
	}
	return false
}

func isCompare(op BinOp) bool {
	switch op {
	case OpEq, OpNeq, OpLt, OpLte, OpGt, OpGte:
		return true
	}
	return false
}

// numericResult 统一数值类型提升：INT 与 FLOAT 混合时结果为 FLOAT。
func numericResult(a, b DataType) (DataType, bool) {
	if a != TypeInt && a != TypeFloat {
		return TypeNull, false
	}
	if b != TypeInt && b != TypeFloat {
		return TypeNull, false
	}
	if a == TypeFloat || b == TypeFloat {
		return TypeFloat, true
	}
	return TypeInt, true
}

func comparable(a, b DataType) bool {
	if a == b {
		return true
	}
	// 数值之间可以比较；字符串、布尔只与同类型比较。
	if (a == TypeInt || a == TypeFloat) && (b == TypeInt || b == TypeFloat) {
		return true
	}
	return false
}

func (e *BinaryExpr) Check(in *Schema) (DataType, error) {
	lt, err := e.L.Check(in)
	if err != nil {
		return TypeNull, err
	}
	rt, err := e.R.Check(in)
	if err != nil {
		return TypeNull, err
	}
	// 字面 NULL 参与运算时类型取另一侧；两侧都是 NULL 时按 NULL 处理（仍合法）。
	if lt == TypeNull || rt == TypeNull {
		e.resultTy = TypeNull
		if isCompare(e.Op) || e.Op == OpAnd || e.Op == OpOr {
			return TypeBool, nil
		}
		if lt != TypeNull {
			e.resultTy = lt
		}
		return e.resultTy, nil
	}
	switch {
	case isArith(e.Op):
		rt2, ok := numericResult(lt, rt)
		if !ok {
			return TypeNull, fmt.Errorf("运算符 %s 要求两侧为数值，得到 %s 与 %s", e.Op, lt, rt)
		}
		e.resultTy = rt2
		return rt2, nil
	case isCompare(e.Op):
		if !comparable(lt, rt) {
			return TypeNull, fmt.Errorf("运算符 %s 无法比较 %s 与 %s", e.Op, lt, rt)
		}
		e.resultTy = TypeBool
		return TypeBool, nil
	case e.Op == OpAnd || e.Op == OpOr:
		if lt != TypeBool || rt != TypeBool {
			return TypeNull, fmt.Errorf("运算符 %s 要求两侧为布尔，得到 %s 与 %s", e.Op, lt, rt)
		}
		e.resultTy = TypeBool
		return TypeBool, nil
	}
	return TypeNull, fmt.Errorf("未知运算符 %d", e.Op)
}

func (e *BinaryExpr) Eval(row Tuple) (Value, error) {
	lv, err := e.L.Eval(row)
	if err != nil {
		return Value{}, err
	}
	rv, err := e.R.Eval(row)
	if err != nil {
		return Value{}, err
	}
	if isArith(e.Op) {
		return evalArith(e.Op, lv, rv)
	}
	if isCompare(e.Op) {
		return evalCompare(e.Op, lv, rv), nil
	}
	return evalLogic(e.Op, lv, rv), nil
}

func (e *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", e.L, e.Op, e.R)
}

func asFloat(v Value) float64 {
	if v.Typ == TypeInt {
		return float64(v.I)
	}
	return v.F
}

// evalArith: 任一输入为 NULL，结果为 NULL。整数除零与浮点除零都产生 NULL（不 panic）。
func evalArith(op BinOp, a, b Value) (Value, error) {
	if a.Null || b.Null {
		return NullValue(), nil
	}
	floatMode := a.Typ == TypeFloat || b.Typ == TypeFloat
	if floatMode {
		x, y := asFloat(a), asFloat(b)
		if op == OpDiv && y == 0 {
			return NullValue(), nil
		}
		var r float64
		switch op {
		case OpAdd:
			r = x + y
		case OpSub:
			r = x - y
		case OpMul:
			r = x * y
		case OpDiv:
			r = x / y
		}
		if math.IsInf(r, 0) {
			return NullValue(), nil
		}
		return Float(r), nil
	}
	x, y := a.I, b.I
	if op == OpDiv {
		if y == 0 {
			return NullValue(), nil
		}
		// 整数除法向零取整，与 Go 一致，语义在文档中固定下来。
		return Int(x / y), nil
	}
	var r int64
	switch op {
	case OpAdd:
		r = x + y
	case OpSub:
		r = x - y
	case OpMul:
		r = x * y
	}
	return Int(r), nil
}

// compareScalar 返回 -1/0/1；调用方已排除 NULL，并保证类型可比较。
func compareScalar(a, b Value) int {
	if a.Typ == TypeString {
		switch {
		case a.S < b.S:
			return -1
		case a.S > b.S:
			return 1
		default:
			return 0
		}
	}
	if a.Typ == TypeBool {
		ai, bi := 0, 0
		if a.B {
			ai = 1
		}
		if b.B {
			bi = 1
		}
		return ai - bi
	}
	x, y := asFloat(a), asFloat(b)
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	default:
		return 0
	}
}

// evalCompare 实现三值逻辑：任一为 NULL => UNKNOWN(NULL)；NULL 绝不等于 NULL。
func evalCompare(op BinOp, a, b Value) Value {
	if a.Null || b.Null {
		return NullValue()
	}
	c := compareScalar(a, b)
	var ok bool
	switch op {
	case OpEq:
		ok = c == 0
	case OpNeq:
		ok = c != 0
	case OpLt:
		ok = c < 0
	case OpLte:
		ok = c <= 0
	case OpGt:
		ok = c > 0
	case OpGte:
		ok = c >= 0
	}
	return Bool(ok)
}

// evalLogic 实现 SQL 风格三值逻辑：
//
//	AND: FALSE 优先（一侧 FALSE 即 FALSE，否则含 NULL 为 NULL）
//	OR : TRUE  优先（一侧 TRUE 即 TRUE，否则含 NULL 为 NULL）
func evalLogic(op BinOp, a, b Value) Value {
	ab, bb := triBool(a), triBool(b)
	if op == OpAnd {
		if ab == triFalse || bb == triFalse {
			return Bool(false)
		}
		if ab == triNull || bb == triNull {
			return NullValue()
		}
		return Bool(true)
	}
	if ab == triTrue || bb == triTrue {
		return Bool(true)
	}
	if ab == triNull || bb == triNull {
		return NullValue()
	}
	return Bool(false)
}

type tri int

const (
	triFalse tri = iota
	triNull
	triTrue
)

func triBool(v Value) tri {
	if v.Null || v.Typ != TypeBool {
		return triNull
	}
	if v.B {
		return triTrue
	}
	return triFalse
}

// ---- 一元（目前只提供 NOT；同样走 Check）----

type NotExpr struct{ X Expr }

func Not(x Expr) *NotExpr { return &NotExpr{X: x} }

func (e *NotExpr) Check(in *Schema) (DataType, error) {
	t, err := e.X.Check(in)
	if err != nil {
		return TypeNull, err
	}
	if t != TypeBool && t != TypeNull {
		return TypeNull, fmt.Errorf("NOT 要求布尔表达式，得到 %s", t)
	}
	return TypeBool, nil
}

func (e *NotExpr) Eval(row Tuple) (Value, error) {
	v, err := e.X.Eval(row)
	if err != nil {
		return Value{}, err
	}
	if v.Null {
		return NullValue(), nil
	}
	return Bool(!v.B), nil
}

func (e *NotExpr) String() string { return fmt.Sprintf("(NOT %s)", e.X) }

// CheckExprs 是供计划节点使用的统一检查入口：新增表达式类型后无需改动调用方。
func CheckExprs(in *Schema, exprs []Expr) ([]DataType, error) {
	out := make([]DataType, len(exprs))
	for i, ex := range exprs {
		t, err := ex.Check(in)
		if err != nil {
			return nil, fmt.Errorf("表达式 %s 类型检查失败: %w", ex, err)
		}
		out[i] = t
	}
	return out, nil
}
