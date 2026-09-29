package relengine

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func ordersSchema() *Schema {
	return NewSchema([]Column{
		{Name: "o_sku", Type: TypeString, Nullable: true},
		{Name: "o_qty", Type: TypeInt, Nullable: true},
	})
}

func productsSchema() *Schema {
	return NewSchema([]Column{{Name: "p_sku", Type: TypeString, Nullable: false}})
}

func salesPlan(orders, products *Table) LogicalPlan {
	on := Bin(OpEq, Col("o_sku"), Col("p_sku"))
	join := NewJoin(NewScan(orders, "o"), NewScan(products, "p"), on, InnerJoin)
	agg := NewAggregate(join,
		[]Expr{Col("p_sku")},
		[]*AggExpr{CountStar(), Count(Col("o_qty")), Sum(Col("o_qty"))},
	)
	return NewProject(agg,
		Item(Col("p_sku"), "sku"),
		Item(Col("COUNT(*)"), "rows"),
		Item(Col("COUNT(o_qty)"), "non_null_qty"),
		Item(Col("SUM(o_qty)"), "total_qty"),
	)
}

func buildPhysical(t *testing.T, plan LogicalPlan, opts ExecOptions, nl bool) (Executor, *Schema) {
	t.Helper()
	schema, err := plan.Bind()
	if err != nil {
		t.Fatal(err)
	}
	var build func(LogicalPlan) (Executor, error)
	build = func(p LogicalPlan) (Executor, error) {
		switch q := p.(type) {
		case *Scan:
			return &scanExec{tbl: q.Table, sz: opts.BatchRows}, nil
		case *Filter:
			c, err := build(q.Child)
			return &filterExec{child: c, cond: q.Cond, sz: opts.BatchRows}, err
		case *Project:
			c, err := build(q.Child)
			return &projectExec{child: c, items: q.Items}, err
		case *Join:
			l, err := build(q.L)
			if err != nil {
				return nil, err
			}
			r, err := build(q.R)
			if err != nil {
				return nil, err
			}
			ln := len(q.L.Schema().Cols)
			rn := len(q.R.Schema().Cols)
			if nl {
				return BuildNestedLoopJoin(l, r, q.On, q.Type, ln, rn, opts), nil
			}
			return BuildJoin(l, r, q.On, q.Type, ln, rn, opts), nil
		case *Aggregate:
			c, err := build(q.Child)
			if err != nil {
				return nil, err
			}
			return BuildAggregate(c, q, opts, opts.SpillAggregate)
		}
		panic("unreachable")
	}
	e, err := build(plan)
	if err != nil {
		t.Fatal(err)
	}
	return e, schema
}

func canonical(rows []Tuple) []string {
	out := make([]string, len(rows))
	for i, row := range rows {
		parts := make([]string, len(row))
		for j, v := range row {
			parts[j] = v.String()
		}
		out[i] = joinLines(parts)
	}
	sort.Strings(out)
	return out
}

func joinLines(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	s := parts[0]
	for _, p := range parts[1:] {
		s += "|" + p
	}
	return s
}

func TestHashAndNestedLoopPlansAgreeOnExhaustiveSmallTables(t *testing.T) {
	skuDomain := []Value{NullValue(), Str("A"), Str("B")}
	qtyDomain := []Value{NullValue(), Int(1), Int(2)}
	productDomain := []Value{Str("A"), Str("B")}

	cases := 0
	for _, o1sku := range skuDomain {
		for _, o1qty := range qtyDomain {
			for _, o2sku := range skuDomain {
				for _, o2qty := range qtyDomain {
					for _, p1 := range productDomain {
						for _, p2 := range productDomain {
							orders := NewTable("orders", ordersSchema())
							mustInsert(t, orders, Tuple{o1sku, o1qty})
							mustInsert(t, orders, Tuple{o2sku, o2qty})
							products := NewTable("products", productsSchema())
							mustInsert(t, products, Tuple{p1})
							mustInsert(t, products, Tuple{p2})

							plan1 := salesPlan(orders, products)
							plan2 := salesPlan(orders, products)
							opts := ExecOptions{BatchRows: 2, MemRows: 1, TempDir: t.TempDir()}

							e1, _ := buildPhysical(t, plan1, opts, false)
							hashRows, err := Drain(context.Background(), e1)
							if err != nil {
								t.Fatal(err)
							}
							e2, _ := buildPhysical(t, plan2, opts, true)
							nlRows, err := Drain(context.Background(), e2)
							if err != nil {
								t.Fatal(err)
							}
							got1, got2 := canonical(hashRows), canonical(nlRows)
							if len(got1) != len(got2) {
								t.Fatalf("group count mismatch\nhash=%v\nnl=%v", got1, got2)
							}
							for i := range got1 {
								if got1[i] != got2[i] {
									t.Fatalf("mismatch\nhash=%v\nnl=%v", got1, got2)
								}
							}
							cases++
						}
					}

				}
			}
		}
	}
	if cases != 3*3*3*3*2*2 {
		t.Fatalf("穷举用例数异常: %d", cases)
	}
}

func mustInsert(t *testing.T, tbl *Table, row Tuple) {
	t.Helper()
	if err := tbl.Insert(row); err != nil {
		t.Fatal(err)
	}
}

func TestJoinDuplicationAndNulls(t *testing.T) {
	orders := NewTable("orders", ordersSchema())
	mustInsert(t, orders, Tuple{Str("A"), Int(2)})
	mustInsert(t, orders, Tuple{Str("A"), Int(3)})
	mustInsert(t, orders, Tuple{NullValue(), Int(5)})
	products := NewTable("products", productsSchema())
	mustInsert(t, products, Tuple{Str("A")})
	mustInsert(t, products, Tuple{Str("A")})

	on := Bin(OpEq, Col("o_sku"), Col("p_sku"))
	plan := NewAggregate(NewJoin(NewScan(orders, "o"), NewScan(products, "p"), on, InnerJoin),
		nil, []*AggExpr{CountStar(), Count(Col("o_qty")), Sum(Col("o_qty"))})
	en := NewEngine(ExecOptions{BatchRows: 1, MemRows: 1, TempDir: t.TempDir()})
	rows, _, err := en.Execute(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	got := canonical(rows)
	want := []string{"4|4|10"} // 2 个 A 订单 * 2 条重复产品 = 4；NULL SKU 不匹配
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestSpillAggregateAndCancellationCleanTempFiles(t *testing.T) {
	dir := t.TempDir()
	orders := NewTable("orders", ordersSchema())
	products := NewTable("products", productsSchema())
	for i := 0; i < 20; i++ {
		sku := "A"
		if i%2 == 0 {
			sku = "B"
		}
		mustInsert(t, orders, Tuple{Str(sku), Int(int64(i + 1))})
		mustInsert(t, products, Tuple{Str(sku)})
	}
	on := Bin(OpEq, Col("o_sku"), Col("p_sku"))
	plan := NewAggregate(NewJoin(NewScan(orders, "o"), NewScan(products, "p"), on, InnerJoin),
		[]Expr{Col("p_sku")}, []*AggExpr{CountStar(), Sum(Col("o_qty"))})
	en := NewEngine(ExecOptions{BatchRows: 3, MemRows: 2, TempDir: dir, SpillAggregate: true})
	rows, _, err := en.Execute(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(canonical(rows)) != 2 {
		t.Fatalf("期望两个分组，得到 %v", canonical(rows))
	}
	entries, _ := os.ReadDir(dir)
	for _, en := range entries {
		if filepath.Base(en.Name()) != "" && len(filepath.Base(en.Name())) >= 14 &&
			filepath.Base(en.Name())[:14] == "relengine-" {
			t.Fatalf("临时结果未清理: %s", en.Name())
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = NewEngine(ExecOptions{BatchRows: 1, MemRows: 1, TempDir: dir, SpillAggregate: true}).Execute(ctx, salesPlan(orders, products))
	if err == nil {
		t.Fatal("取消的查询应当返回错误")
	}
}
