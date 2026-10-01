package relengine

import (
	"context"
	"errors"
	"fmt"
	"io"
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

// 高倾斜 + 热键：左 40 行同键、右 60 行同键（多对多放大 2400 行），
// 再混入 NULL 与不匹配键。MemRows=1 强制落盘，Hash 与 Nested Loop
// 两种计划的返回行与聚合值必须完全一致。
func TestSpillJoinHotKeyMatchesNestedLoop(t *testing.T) {
	mkTables := func() (*Table, *Table) {
		orders := NewTable("orders", ordersSchema())
		products := NewTable("products", productsSchema())
		for i := 0; i < 40; i++ {
			mustInsert(t, orders, Tuple{Str("H"), Int(int64(i))})
		}
		mustInsert(t, orders, Tuple{NullValue(), Int(7)})
		mustInsert(t, orders, Tuple{Str("Z"), Int(9)})
		for i := 0; i < 60; i++ {
			mustInsert(t, products, Tuple{Str("H")})
		}
		mustInsert(t, products, Tuple{Str("Y")})
		return orders, products
	}

	opts := ExecOptions{BatchRows: 1, MemRows: 1, TempDir: t.TempDir()}
	var got [2][]Tuple
	for i, nl := range []bool{false, true} {
		orders, products := mkTables()
		plan := salesPlan(orders, products)
		e, _ := buildPhysical(t, plan, opts, nl)
		rows, err := Drain(context.Background(), e)
		if err != nil {
			t.Fatalf("nl=%v: %v", nl, err)
		}
		got[i] = rows
	}
	c1, c2 := canonical(got[0]), canonical(got[1])
	if len(c1) != len(c2) {
		t.Fatalf("分组数不一致\nhash=%v\nnl=%v", c1, c2)
	}
	for i := range c1 {
		if c1[i] != c2[i] {
			t.Fatalf("结果不一致\nhash=%v\nnl=%v", c1, c2)
		}
	}
	// 热键组：COUNT(*)=2400，COUNT(o_qty)=2400，SUM(o_qty)=60*(0+...+39)=46800。
	// INNER JOIN 下未匹配的 Y/Z/NULL 不产生分组。
	want := []string{"H|2400|2400|46800"}
	if len(c1) != 1 || c1[0] != want[0] {
		t.Fatalf("聚合值不符: got %v want %v", c1, want)
	}
}

// LEFT JOIN 落盘路径：未匹配左侧补 NULL，且每行只补一次。
func TestSpillLeftJoinUnmatchedOnce(t *testing.T) {
	orders := NewTable("orders", ordersSchema())
	for i := 0; i < 10; i++ {
		mustInsert(t, orders, Tuple{Str("A"), Int(1)})
	}
	for i := 0; i < 5; i++ {
		mustInsert(t, orders, Tuple{Str("B"), Int(2)})
	}
	products := NewTable("products", productsSchema())
	for i := 0; i < 3; i++ {
		mustInsert(t, products, Tuple{Str("A")})
	}
	on := Bin(OpEq, Col("o_sku"), Col("p_sku"))
	plan := NewJoin(NewScan(orders, "o"), NewScan(products, "p"), on, LeftJoin)
	en := NewEngine(ExecOptions{BatchRows: 1, MemRows: 1, TempDir: t.TempDir()})
	rows, _, err := en.Execute(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	// 10*3 匹配 + 5 行未匹配补 NULL = 35。
	if len(rows) != 35 {
		t.Fatalf("期望 35 行，得到 %d: %v", len(rows), canonical(rows))
	}
	nulls := 0
	for _, row := range rows {
		if row[2].Null {
			nulls++
		}
	}
	if nulls != 5 {
		t.Fatalf("期望 5 行补 NULL，得到 %d", nulls)
	}
}

func spillDirs(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, en := range entries {
		out = append(out, en.Name())
	}
	return out
}

// 损坏的临时文件必须导致明确错误，且 Close 后临时目录被清理。
func TestSpillJoinCorruptTempFileFailsAndCleansUp(t *testing.T) {
	dir := t.TempDir()
	orders := NewTable("orders", ordersSchema())
	products := NewTable("products", productsSchema())
	for i := 0; i < 20; i++ {
		mustInsert(t, orders, Tuple{Str(fmt.Sprintf("k%d", i%3)), Int(int64(i))})
		mustInsert(t, products, Tuple{Str(fmt.Sprintf("k%d", i%3))})
	}
	on := Bin(OpEq, Col("o_sku"), Col("p_sku"))
	plan := NewJoin(NewScan(orders, "o"), NewScan(products, "p"), on, InnerJoin)
	e, _ := buildPhysical(t, plan, ExecOptions{BatchRows: 1, MemRows: 1, TempDir: dir}, false)

	ctx := context.Background()
	if _, err := e.Next(ctx); err != nil {
		t.Fatalf("首批应成功: %v", err)
	}
	names := spillDirs(t, dir)
	if len(names) == 0 {
		t.Fatal("落盘后应存在临时目录")
	}
	// 破坏所有分片文件。
	for _, n := range names {
		files, _ := os.ReadDir(filepath.Join(dir, n))
		for _, f := range files {
			p := filepath.Join(dir, n, f.Name())
			if err := os.WriteFile(p, []byte("corrupted-gob-data"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	var runErr error
	for runErr == nil {
		_, runErr = e.Next(ctx)
	}
	if errors.Is(runErr, io.EOF) {
		t.Fatal("损坏的临时文件不应被当作正常结束")
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close 应清理成功: %v", err)
	}
	if left := spillDirs(t, dir); len(left) != 0 {
		t.Fatalf("临时目录未清理: %v", left)
	}
}

// 取消发生在落盘回读中途：明确报错并清理临时文件。
func TestSpillJoinCancelDuringReadback(t *testing.T) {
	dir := t.TempDir()
	orders := NewTable("orders", ordersSchema())
	products := NewTable("products", productsSchema())
	for i := 0; i < 30; i++ {
		mustInsert(t, orders, Tuple{Str("A"), Int(1)})
		mustInsert(t, products, Tuple{Str("A")})
	}
	on := Bin(OpEq, Col("o_sku"), Col("p_sku"))
	plan := NewJoin(NewScan(orders, "o"), NewScan(products, "p"), on, InnerJoin)
	e, _ := buildPhysical(t, plan, ExecOptions{BatchRows: 1, MemRows: 1, TempDir: dir}, false)

	ctx, cancel := context.WithCancel(context.Background())
	if _, err := e.Next(ctx); err != nil {
		t.Fatalf("首批应成功: %v", err)
	}
	cancel()
	if _, err := e.Next(ctx); err == nil {
		t.Fatal("取消后继续拉取应返回错误")
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close 应清理成功: %v", err)
	}
	if left := spillDirs(t, dir); len(left) != 0 {
		t.Fatalf("临时目录未清理: %v", left)
	}
}

// 预算不足必须明确报错，不能悄悄退回无限内存。
func TestJoinInsufficientBudgetFails(t *testing.T) {
	orders := NewTable("orders", ordersSchema())
	mustInsert(t, orders, Tuple{Str("A"), Int(1)})
	products := NewTable("products", productsSchema())
	mustInsert(t, products, Tuple{Str("A")})
	on := Bin(OpEq, Col("o_sku"), Col("p_sku"))
	// 绕过 NewEngine 的默认值，直接构造零预算。
	e, _ := buildPhysical(t, NewJoin(NewScan(orders, "o"), NewScan(products, "p"), on, InnerJoin),
		ExecOptions{BatchRows: 0, MemRows: 0, TempDir: t.TempDir()}, false)
	_, err := e.Next(context.Background())
	if !errors.Is(err, ErrInsufficientBudget) {
		t.Fatalf("期望 ErrInsufficientBudget，得到 %v", err)
	}
	_ = e.Close()
}
