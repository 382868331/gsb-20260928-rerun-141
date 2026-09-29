package engine

import "fmt"

func checkAggregate(item AggregateItem, schema Schema) (Type, error) {
	switch item.Function {
	case "COUNT":
		if item.Arg != nil {
			if _, err := item.Arg.TypeCheck(schema); err != nil {
				return TypeNull, err
			}
		}
		return TypeInt, nil
	case "SUM", "AVG":
		if item.Arg == nil {
			return TypeNull, fmt.Errorf("%s requires an argument", item.Function)
		}
		t, err := item.Arg.TypeCheck(schema)
		if err != nil {
			return TypeNull, err
		}
		if !numeric(t) {
			return TypeNull, fmt.Errorf("%s requires numeric argument", item.Function)
		}
		if item.Function == "SUM" {
			return t, nil
		}
		return TypeFloat, nil
	case "MIN", "MAX":
		if item.Arg == nil {
			return TypeNull, fmt.Errorf("%s requires an argument", item.Function)
		}
		return item.Arg.TypeCheck(schema)
	default:
		return TypeNull, fmt.Errorf("unknown aggregate %q", item.Function)
	}
}

type accumulator struct {
	Count    int64
	IntSum   int64
	FloatSum float64
	Value    any
	Has      bool
	IsFloat  bool
}

func (a *accumulator) add(item AggregateItem, row Row) error {
	if item.Function == "COUNT" {
		if item.Arg == nil {
			a.Count++
			return nil
		}
		v, err := item.Arg.Eval(row)
		if err != nil {
			return err
		}
		if v != nil {
			a.Count++
		}
		return nil
	}
	v, err := item.Arg.Eval(row)
	if err != nil {
		return err
	}
	if v == nil {
		return nil
	}
	a.Count++
	switch item.Function {
	case "SUM":
		switch v := v.(type) {
		case int64:
			a.IntSum += v
		case float64:
			a.IsFloat = true
			a.FloatSum += v
		}
	case "AVG":
		switch v := v.(type) {
		case int64:
			a.FloatSum += float64(v)
		case float64:
			a.FloatSum += v
		}
	case "MIN", "MAX":
		if !a.Has {
			a.Value = v
			a.Has = true
		} else if c, ok := compareComparable(a.Value, v); ok {
			if item.Function == "MIN" && c > 0 || item.Function == "MAX" && c < 0 {
				a.Value = v
			}
		}
	}
	return nil
}

func (a *accumulator) result(item AggregateItem) any {
	switch item.Function {
	case "COUNT":
		return a.Count
	case "SUM":
		if a.Count == 0 {
			return nil
		}
		if a.IsFloat {
			return a.FloatSum + float64(a.IntSum)
		}
		return a.IntSum
	case "AVG":
		if a.Count == 0 {
			return nil
		}
		return a.FloatSum / float64(a.Count)
	case "MIN", "MAX":
		if !a.Has {
			return nil
		}
		return a.Value
	}
	return nil
}

func (a *accumulator) merge(other *accumulator, item AggregateItem) {
	a.Count += other.Count
	a.IntSum += other.IntSum
	a.FloatSum += other.FloatSum
	a.IsFloat = a.IsFloat || other.IsFloat
	if other.Has {
		if !a.Has {
			a.Value = other.Value
			a.Has = true
		} else if c, ok := compareComparable(a.Value, other.Value); ok {
			if item.Function == "MIN" && c > 0 || item.Function == "MAX" && c < 0 {
				a.Value = other.Value
			}
		}
	}
}

func compareComparable(left, right any) (int, bool) {
	switch left := left.(type) {
	case int64:
		return compareNumeric(left, right)
	case float64:
		return compareNumeric(left, right)
	case string:
		r, ok := right.(string)
		if !ok || left < r {
			return -1, ok
		} else if left > r {
			return 1, true
		}
		return 0, true
	case bool:
		r, ok := right.(bool)
		if !ok {
			return 0, false
		}
		if left == r {
			return 0, true
		}
		if !left && r {
			return -1, true
		}
		return 1, true
	}
	return 0, false
}
