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

func skewJoinPlan(orders, products *Table, jt JoinType) LogicalPlan {
	on := Bin(OpEq, Col("o_sku"), Col("p_sku"))
	return NewJoin(NewScan(orders, "o"), NewScan(products, "p"), on, jt)
}

func skewAggPlan(orders, products *Table, jt JoinType) LogicalPlan {
	return NewAggregate(skewJoinPlan(orders, products, jt), nil,
		[]*AggExpr{CountStar(), Count(Col("o_qty")), Sum(Col("o_qty"))})
}

func skewTables(t *testing.T) (*Table, *Table) {
	t.Helper()
	orders := NewTable("orders", ordersSchema())
	for i := 1; i <= 40; i++ {
		mustInsert(t, orders, Tuple{Str("A"), Int(int64(i))})
	}
	mustInsert(t, orders, Tuple{Str("B"), Int(41)})
	mustInsert(t, orders, Tuple{NullValue(), NullValue()})

	products := NewTable("products", productsSchema())
	for i := 0; i < 8; i++ {
		mustInsert(t, products, Tuple{Str("A")})
	}
	mustInsert(t, products, Tuple{Str("B")})
	mustInsert(t, products, Tuple{Str("C")})
	return orders, products
}

func TestSkewedJoinPlansAgreeAndKeepAggregates(t *testing.T) {
	orders, products := skewTables(t)
	wantRows := map[JoinType]int{InnerJoin: 321, LeftJoin: 322, RightJoin: 322}
	wantAgg := map[JoinType]string{
		InnerJoin: "321|321|6601",
		LeftJoin:  "322|321|6601",
		RightJoin: "322|321|6601",
	}
	for _, jt := range []JoinType{InnerJoin, LeftJoin, RightJoin} {
		opts := ExecOptions{BatchRows: 1, MemRows: 1, TempDir: t.TempDir()}
		hashExec, _ := buildPhysical(t, skewJoinPlan(orders, products, jt), opts, false)
		first, err := hashExec.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(first.Rows) != 1 {
			t.Fatalf("%s 首批应只返回一行，得到 %d 行", jt, len(first.Rows))
		}
		hashRows := append([]Tuple{}, first.Rows...)
		rest, err := Drain(context.Background(), hashExec)
		if err != nil {
			t.Fatal(err)
		}
		hashRows = append(hashRows, rest...)
		nlExec, _ := buildPhysical(t, skewJoinPlan(orders, products, jt), opts, true)
		firstNL, err := nlExec.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(firstNL.Rows) != 1 {
			t.Fatalf("%s NL 首批应只返回一行，得到 %d 行", jt, len(firstNL.Rows))
		}
		nlRows := append([]Tuple{}, firstNL.Rows...)
		rest, err = Drain(context.Background(), nlExec)
		if err != nil {
			t.Fatal(err)
		}
		nlRows = append(nlRows, rest...)
		if len(hashRows) != wantRows[jt] || len(nlRows) != wantRows[jt] {
			t.Fatalf("%s 行数错误: hash=%d nl=%d want=%d", jt, len(hashRows), len(nlRows), wantRows[jt])
		}
		if got := canonical(hashRows); !equalStrings(got, canonical(nlRows)) {
			t.Fatalf("%s Hash 与 NL 结果不一致\nhash=%v\nnl=%v", jt, got, canonical(nlRows))
		}

		hashAgg, _ := buildPhysical(t, skewAggPlan(orders, products, jt), opts, false)
		hashAggRows, err := Drain(context.Background(), hashAgg)
		if err != nil {
			t.Fatal(err)
		}
		nlAgg, _ := buildPhysical(t, skewAggPlan(orders, products, jt), opts, true)
		nlAggRows, err := Drain(context.Background(), nlAgg)
		if err != nil {
			t.Fatal(err)
		}
		if got := canonical(hashAggRows); len(got) != 1 || got[0] != wantAgg[jt] {
			t.Fatalf("%s hash 聚合错误: got=%v want=%s", jt, got, wantAgg[jt])
		}
		if got := canonical(nlAggRows); len(got) != 1 || got[0] != wantAgg[jt] {
			t.Fatalf("%s NL 聚合错误: got=%v want=%s", jt, got, wantAgg[jt])
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestJoinCancellationAndCorruptSpillCleanup(t *testing.T) {
	orders, products := skewTables(t)
	dir := t.TempDir()
	opts := ExecOptions{BatchRows: 1, MemRows: 1, TempDir: dir}
	executor, _ := buildPhysical(t, skewJoinPlan(orders, products, InnerJoin), opts, false)
	if _, err := executor.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := executor.Next(ctx); err == nil {
		t.Fatal("取消后继续读取连接应返回错误")
	}
	if err := executor.Close(); err != nil {
		t.Fatal(err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("取消后临时目录未清理: %v", entries)
	}

	dir = t.TempDir()
	opts.TempDir = dir
	corrupt, _ := buildPhysical(t, skewJoinPlan(orders, products, InnerJoin), opts, false)
	if _, err := corrupt.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "relengine-spill-*", "part-1.gob"))
	if len(matches) != 1 {
		t.Fatalf("未找到探测侧临时文件: %v", matches)
	}
	if err := os.WriteFile(matches[0], []byte("broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	var readErr error
	for {
		_, readErr = corrupt.Next(context.Background())
		if readErr != nil {
			break
		}
	}
	if readErr == nil {
		t.Fatal("损坏临时文件应明确返回错误")
	}
	if err := corrupt.Close(); err != nil {
		t.Fatal(err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("损坏文件错误后临时目录未清理: %v", entries)
	}
}

func TestJoinSpillCreationFailureDoesNotFallbackToUnlimitedMemory(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	orders, products := skewTables(t)
	opts := ExecOptions{BatchRows: 1, MemRows: 1, TempDir: blocker}
	executor, _ := buildPhysical(t, skewJoinPlan(orders, products, InnerJoin), opts, false)
	defer executor.Close()
	if _, err := executor.Next(context.Background()); err == nil {
		t.Fatal("临时文件预算路径创建失败时应结束查询")
	}
}
