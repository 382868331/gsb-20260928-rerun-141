package relengine

import "fmt"

// AggKind 标识聚合函数。新增聚合函数时在这里登记，并在 accumulator 中实现，
// 分组节点无需改动 —— 聚合与标量表达式一样通过统一接口与类型检查接入。
type AggKind int

const (
	AggCountStar AggKind = iota // COUNT(*)：统计行数，含 NULL 列
	AggCount                    // COUNT(x)：统计 x 非 NULL 的行数
	AggSum
	AggAvg
	AggMin
	AggMax
)

func (k AggKind) String() string {
	switch k {
	case AggCountStar:
		return "COUNT(*)"
	case AggCount:
		return "COUNT"
	case AggSum:
		return "SUM"
	case AggAvg:
		return "AVG"
	case AggMin:
		return "MIN"
	case AggMax:
		return "MAX"
	}
	return "?"
}

// AggExpr 是聚合调用。它不是“逐行标量表达式”：Check 仍在输入 Schema 上
// 推导参数类型并确定结果类型，真正的逐组累积由 Accumulator 完成。
type AggExpr struct {
	Kind AggKind
	Arg  Expr // AggCountStar 时为 nil
	argT DataType
}

func CountStar() *AggExpr   { return &AggExpr{Kind: AggCountStar} }
func Count(x Expr) *AggExpr { return &AggExpr{Kind: AggCount, Arg: x} }
func Sum(x Expr) *AggExpr   { return &AggExpr{Kind: AggSum, Arg: x} }
func Avg(x Expr) *AggExpr   { return &AggExpr{Kind: AggAvg, Arg: x} }
func Min(x Expr) *AggExpr   { return &AggExpr{Kind: AggMin, Arg: x} }
func Max(x Expr) *AggExpr   { return &AggExpr{Kind: AggMax, Arg: x} }

func (a *AggExpr) Check(in *Schema) (DataType, error) {
	switch a.Kind {
	case AggCountStar, AggCount:
		if a.Arg != nil {
			t, err := a.Arg.Check(in)
			if err != nil {
				return TypeNull, err
			}
			a.argT = t
		}
		return TypeInt, nil
	case AggSum:
		t, err := a.requireNumeric(in)
		if err != nil {
			return TypeNull, err
		}
		a.argT = t
		return t, nil // SUM(INT)=INT, SUM(FLOAT)=FLOAT
	case AggAvg:
		t, err := a.requireNumeric(in)
		if err != nil {
			return TypeNull, err
		}
		a.argT = t
		return TypeFloat, nil // AVG 恒为浮点（整数均值可能带小数）
	case AggMin, AggMax:
		if a.Arg == nil {
			return TypeNull, fmt.Errorf("%s 需要一个参数", a.Kind)
		}
		t, err := a.Arg.Check(in)
		if err != nil {
			return TypeNull, err
		}
		if t == TypeNull {
			return TypeNull, fmt.Errorf("%s 参数类型未知", a.Kind)
		}
		a.argT = t
		return t, nil
	}
	return TypeNull, fmt.Errorf("未知聚合 %d", a.Kind)
}

func (a *AggExpr) requireNumeric(in *Schema) (DataType, error) {
	if a.Arg == nil {
		return TypeNull, fmt.Errorf("%s 需要一个参数", a.Kind)
	}
	t, err := a.Arg.Check(in)
	if err != nil {
		return TypeNull, err
	}
	if t != TypeInt && t != TypeFloat && t != TypeNull {
		return TypeNull, fmt.Errorf("%s 参数必须是数值，得到 %s", a.Kind, t)
	}
	return t, nil
}

func (a *AggExpr) String() string {
	if a.Arg == nil {
		return a.Kind.String()
	}
	return fmt.Sprintf("%s(%s)", a.Kind, a.Arg)
}

// Accumulator 是单个聚合在单个分组上的增量状态。
// 分批处理时每个分片各自维护 Accumulator，最后再 Merge，保证连接放大的每一行
// 都进入统计：一行连接出 N 条，就会被 Update N 次。
type Accumulator interface {
	Update(row Tuple) error
	// Merge 合并另一个分片的同构状态（用于分批/落盘后的归并）。
	Merge(other Accumulator) error
	Result() Value
}

func (a *AggExpr) NewAccumulator() (Accumulator, error) {
	switch a.Kind {
	case AggCountStar:
		return &countStarAcc{}, nil
	case AggCount:
		return &countAcc{arg: a.Arg}, nil
	case AggSum:
		return &sumAcc{arg: a.Arg, float: a.argT == TypeFloat}, nil
	case AggAvg:
		return &avgAcc{arg: a.Arg}, nil
	case AggMin:
		return &minMaxAcc{arg: a.Arg, max: false}, nil
	case AggMax:
		return &minMaxAcc{arg: a.Arg, max: true}, nil
	}
	return nil, fmt.Errorf("未知聚合 %d", a.Kind)
}

type countStarAcc struct{ n int64 }

func (a *countStarAcc) Update(Tuple) error { a.n++; return nil }
func (a *countStarAcc) Merge(o Accumulator) error {
	x, ok := o.(*countStarAcc)
	if !ok {
		return fmt.Errorf("COUNT(*) 状态类型不匹配")
	}
	a.n += x.n
	return nil
}
func (a *countStarAcc) Result() Value { return Int(a.n) }

type countAcc struct {
	arg Expr
	n   int64
}

func (a *countAcc) Update(row Tuple) error {
	v, err := a.arg.Eval(row)
	if err != nil {
		return err
	}
	if !v.Null {
		a.n++
	}
	return nil
}
func (a *countAcc) Merge(o Accumulator) error {
	x, ok := o.(*countAcc)
	if !ok {
		return fmt.Errorf("COUNT 状态类型不匹配")
	}
	a.n += x.n
	return nil
}
func (a *countAcc) Result() Value { return Int(a.n) }

type sumAcc struct {
	arg        Expr
	float      bool
	i          int64
	f          float64
	hasNonNull bool
}

func (a *sumAcc) Update(row Tuple) error {
	v, err := a.arg.Eval(row)
	if err != nil {
		return err
	}
	if v.Null {
		return nil
	}
	a.hasNonNull = true
	if a.float {
		a.f += asFloat(v)
	} else {
		a.i += v.I
	}
	return nil
}
func (a *sumAcc) Merge(o Accumulator) error {
	x, ok := o.(*sumAcc)
	if !ok || x.float != a.float {
		return fmt.Errorf("SUM 状态类型不匹配")
	}
	if x.hasNonNull {
		a.hasNonNull = true
		if a.float {
			a.f += x.f
		} else {
			a.i += x.i
		}
	}
	return nil
}
func (a *sumAcc) Result() Value {
	if !a.hasNonNull {
		return NullValue()
	}
	if a.float {
		return Float(a.f)
	}
	return Int(a.i)
}

type avgAcc struct {
	arg   Expr
	sum   float64
	count int64
}

func (a *avgAcc) Update(row Tuple) error {
	v, err := a.arg.Eval(row)
	if err != nil {
		return err
	}
	if v.Null {
		return nil
	}
	a.sum += asFloat(v)
	a.count++
	return nil
}
func (a *avgAcc) Merge(o Accumulator) error {
	x, ok := o.(*avgAcc)
	if !ok {
		return fmt.Errorf("AVG 状态类型不匹配")
	}
	a.sum += x.sum
	a.count += x.count
	return nil
}
func (a *avgAcc) Result() Value {
	if a.count == 0 {
		return NullValue()
	}
	return Float(a.sum / float64(a.count))
}

type minMaxAcc struct {
	arg      Expr
	max      bool
	has      bool
	sentinel Value
}

func (a *minMaxAcc) Update(row Tuple) error {
	v, err := a.arg.Eval(row)
	if err != nil {
		return err
	}
	if v.Null {
		return nil
	}
	if !a.has {
		a.has = true
		a.sentinel = v
		return nil
	}
	c := compareScalar(v, a.sentinel)
	if (a.max && c > 0) || (!a.max && c < 0) {
		a.sentinel = v
	}
	return nil
}
func (a *minMaxAcc) Merge(o Accumulator) error {
	x, ok := o.(*minMaxAcc)
	if !ok || x.max != a.max {
		return fmt.Errorf("MIN/MAX 状态类型不匹配")
	}
	if x.has {
		if !a.has {
			a.has = true
			a.sentinel = x.sentinel
		} else {
			c := compareScalar(x.sentinel, a.sentinel)
			if (a.max && c > 0) || (!a.max && c < 0) {
				a.sentinel = x.sentinel
			}
		}
	}
	return nil
}
func (a *minMaxAcc) Result() Value {
	if !a.has {
		return NullValue()
	}
	return a.sentinel
}
