package relengine

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"
)

// 顺序敏感的逐行比较（值层面）。
func equalRows(a, b []Tuple) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !reflect.DeepEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func mkSkewed(t *testing.T, leftRows, rightRows int) (*Table, *Table) {
	t.Helper()
	orders := NewTable("orders", ordersSchema())
	products := NewTable("products", productsSchema())
	for i := 0; i < leftRows; i++ {
		sku := "H"
		if i%7 == 3 {
			sku = "C"
		}
		mustInsert(t, orders, Tuple{Str(sku), Int(int64(i))})
	}
	mustInsert(t, orders, Tuple{NullValue(), Int(-1)})
	for i := 0; i < rightRows; i++ {
		sku := "H"
		if i%5 == 4 {
			sku = "C"
		}
		mustInsert(t, products, Tuple{Str(sku)})
	}
	mustInsert(t, products, Tuple{Str("Z")})
	return orders, products
}

// 回归：Nested Loop Join 曾在首批结果后把整份右侧（如 100 行）留在内存，
// 绕过 MemRows=1 的约定。现在右侧超限必须转入临时存储并如实记账。
func TestNestedLoopJoinRespectsMemRows(t *testing.T) {
	orders, products := mkSkewed(t, 30, 100)
	on := Bin(OpEq, Col("o_sku"), Col("p_sku"))
	mkPlan := func() LogicalPlan {
		return NewJoin(NewScan(orders, "o"), NewScan(products, "p"), on, InnerJoin)
	}

	// 参照：大预算纯内存 NLJ 的完整有序输出。
	refOpts := ExecOptions{BatchRows: 4, MemRows: 1 << 20, TempDir: t.TempDir()}
	eRef, _ := buildPhysical(t, mkPlan(), refOpts, true)
	refRows, err := Drain(context.Background(), eRef)
	if err != nil {
		t.Fatal(err)
	}

	for _, batch := range []int{1, 2, 3} {
		pool := NewMemManager(1) // 只能容纳一行
		opts := ExecOptions{BatchRows: batch, MemRows: 1, TempDir: t.TempDir(), Pool: pool}
		e, _ := buildPhysical(t, mkPlan(), opts, true)
		rows, err := Drain(context.Background(), e)
		if err != nil {
			t.Fatalf("BatchRows=%d: %v", batch, err)
		}
		// 资源转移（落盘）不得改变结果顺序约定与重复行。
		if !equalRows(rows, refRows) {
			t.Fatalf("BatchRows=%d: 落盘 NLJ 的有序输出与内存路径不一致", batch)
		}
		st := pool.Stats()
		if st.HighWater > st.Capacity {
			t.Fatalf("BatchRows=%d: 峰值占用 %d 行超过额度 %d 行", batch, st.HighWater, st.Capacity)
		}
		if st.InUse != 0 {
			t.Fatalf("BatchRows=%d: 结束后仍有 %d 行额度未释放", batch, st.InUse)
		}
		if len(st.Spills) == 0 {
			t.Fatalf("BatchRows=%d: 右侧 100 行超限应记录落盘事件", batch)
		}
	}
}

// 同一查询中的两个连接共享同一份额度：内层连接持有构建侧期间，
// 外层连接必须转入临时存储，而不是各自把整份 MemRows 重复计算。
func TestSharedBudgetAcrossJoinsInOneQuery(t *testing.T) {
	mk := func() (*Table, *Table, *Table) {
		a := NewTable("a", NewSchema([]Column{
			{Name: "a_k", Type: TypeString}, {Name: "a_v", Type: TypeInt}}))
		b := NewTable("b", NewSchema([]Column{
			{Name: "b_k", Type: TypeString}, {Name: "b_v", Type: TypeInt}}))
		c := NewTable("c", NewSchema([]Column{
			{Name: "c_k", Type: TypeString}, {Name: "c_v", Type: TypeInt}}))
		for i := 0; i < 6; i++ {
			k := Str("k" + string(rune('0'+i%3)))
			mustInsert(t, a, Tuple{k, Int(int64(i))})
			mustInsert(t, b, Tuple{k, Int(int64(i * 10))})
			mustInsert(t, c, Tuple{k, Int(int64(i * 100))})
		}
		return a, b, c
	}
	mkPlan := func() LogicalPlan {
		a, b, c := mk()
		j1 := NewJoin(NewScan(a, "a"), NewScan(b, "b"),
			Bin(OpEq, Col("a_k"), Col("b_k")), InnerJoin)
		return NewJoin(j1, NewScan(c, "c"),
			Bin(OpEq, Col("b_k"), Col("c_k")), InnerJoin)
	}

	ref := NewEngine(ExecOptions{BatchRows: 4, MemRows: 1 << 20, TempDir: t.TempDir()})
	wantRows, _, err := ref.Execute(context.Background(), mkPlan())
	if err != nil {
		t.Fatal(err)
	}

	en := NewEngine(ExecOptions{BatchRows: 2, MemRows: 4, TempDir: t.TempDir()})
	gotRows, _, err := en.Execute(context.Background(), mkPlan())
	if err != nil {
		t.Fatal(err)
	}
	got, want := canonical(gotRows), canonical(wantRows)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("共享预算下结果不一致 got=%v want=%v", got, want)
	}
	st, ok := en.LastMemStats()
	if !ok {
		t.Fatal("应能报告最近一次执行的额度快照")
	}
	if st.HighWater > st.Capacity {
		t.Fatalf("峰值占用 %d 行超过共享额度 %d 行（重复计算？）", st.HighWater, st.Capacity)
	}
	if st.InUse != 0 {
		t.Fatalf("查询结束后仍有 %d 行额度未释放", st.InUse)
	}
	if len(st.Spills) == 0 {
		t.Fatal("额度被内层连接占用时，外层应记录转入临时存储的事件")
	}
	t.Logf("共享额度 %d 行，峰值 %d 行，落盘事件: %+v", st.Capacity, st.HighWater, st.Spills)
}

// 取消与局部失败必须明确释放所属资源，且不影响同时使用同一池的独立查询。
func TestCancelReleasesSharedBudget(t *testing.T) {
	pool := NewMemManager(8)
	mkPlan := func() LogicalPlan {
		orders, products := mkSkewed(t, 20, 20)
		return NewJoin(NewScan(orders, "o"), NewScan(products, "p"),
			Bin(OpEq, Col("o_sku"), Col("p_sku")), InnerJoin)
	}
	opts := func() ExecOptions {
		return ExecOptions{BatchRows: 2, MemRows: 8, TempDir: t.TempDir(), Pool: pool}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := NewEngine(opts()).Execute(ctx, mkPlan()); err == nil {
		t.Fatal("已取消的查询应返回错误")
	}
	if st := pool.Stats(); st.InUse != 0 {
		t.Fatalf("取消后仍有 %d 行额度未释放", st.InUse)
	}

	// 局部失败：执行中途取消。
	e, _ := buildPhysical(t, mkPlan(), opts(), false)
	ctx2, cancel2 := context.WithCancel(context.Background())
	if _, err := e.Next(ctx2); err != nil {
		t.Fatalf("首批应成功: %v", err)
	}
	cancel2()
	for err := error(nil); err == nil; {
		_, err = e.Next(ctx2)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if st := pool.Stats(); st.InUse != 0 {
		t.Fatalf("局部失败后仍有 %d 行额度未释放", st.InUse)
	}

	// 同一池上的独立查询不受影响。
	if _, _, err := NewEngine(opts()).Execute(context.Background(), mkPlan()); err != nil {
		t.Fatalf("独立查询应正常完成: %v", err)
	}
	if st := pool.Stats(); st.InUse != 0 {
		t.Fatalf("独立查询结束后仍有 %d 行额度未释放", st.InUse)
	}
}

// 额度被占用时 Acquire 等待并记录原因；释放后等待者继续；取消等待返回错误。
func TestMemManagerWaitReportsReason(t *testing.T) {
	pool := NewMemManager(4)
	holder, err := pool.Acquire(context.Background(), 3, "holder", "初始占用")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		g, err := pool.Acquire(context.Background(), 3, "waiter", "构建侧需要 3 行额度")
		if err == nil {
			g.Release()
		}
		done <- err
	}()
	// 等待者应当阻塞并记录原因。
	time.Sleep(50 * time.Millisecond)
	st := pool.Stats()
	if len(st.Waits) != 1 || st.Waits[0].Owner != "waiter" ||
		st.Waits[0].Reason != "构建侧需要 3 行额度" {
		t.Fatalf("等待事件及原因记录不符: %+v", st.Waits)
	}
	select {
	case <-done:
		t.Fatal("额度不足时不应提前获得")
	default:
	}
	holder.Release()
	if err := <-done; err != nil {
		t.Fatalf("释放后等待者应成功: %v", err)
	}

	// 等待中取消：明确报错，不影响池。
	ctx, cancel := context.WithCancel(context.Background())
	holder2, _ := pool.Acquire(context.Background(), 4, "holder2", "占满")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if _, err := pool.Acquire(ctx, 2, "waiter2", "将被取消"); !errors.Is(err, context.Canceled) {
		t.Fatalf("等待中取消应返回 Canceled，得到 %v", err)
	}
	wg.Wait()
	holder2.Release()
	if st := pool.Stats(); st.InUse != 0 {
		t.Fatalf("全部释放后仍有 %d 行占用", st.InUse)
	}
}

// 交叉连接（无等值键，单片落盘）与偏斜键在不同批次/预算下的完整性：
// 两种物理方式、多种 BatchRows/MemRows 组合的结果都必须与参照一致，
// 且总占用不超过共享额度。
func TestCrossJoinAndSkewUnderTightBudget(t *testing.T) {
	on := Bin(OpEq, Col("o_sku"), Col("p_sku"))
	cross := Lit(Bool(true))

	for _, tc := range []struct {
		name string
		on   Expr
	}{
		{"skewed-equi", on},
		{"cross", cross},
	} {
		for _, batch := range []int{1, 2, 3} {
			for _, mem := range []int{1, 2, 3} {
				orders, products := mkSkewed(t, 12, 9)
				mkPlan := func() LogicalPlan {
					return NewJoin(NewScan(orders, "o"), NewScan(products, "p"), tc.on, InnerJoin)
				}
				refOpts := ExecOptions{BatchRows: batch, MemRows: 1 << 20, TempDir: t.TempDir()}
				eRef, _ := buildPhysical(t, mkPlan(), refOpts, false)
				want, err := Drain(context.Background(), eRef)
				if err != nil {
					t.Fatal(err)
				}
				for _, nl := range []bool{false, true} {
					pool := NewMemManager(mem)
					opts := ExecOptions{BatchRows: batch, MemRows: mem, TempDir: t.TempDir(), Pool: pool}
					e, _ := buildPhysical(t, mkPlan(), opts, nl)
					got, err := Drain(context.Background(), e)
					if err != nil {
						t.Fatalf("%s batch=%d mem=%d nl=%v: %v", tc.name, batch, mem, nl, err)
					}
					if !reflect.DeepEqual(canonical(got), canonical(want)) {
						t.Fatalf("%s batch=%d mem=%d nl=%v: 结果行数/重复行不符", tc.name, batch, mem, nl)
					}
					st := pool.Stats()
					if st.HighWater > st.Capacity {
						t.Fatalf("%s batch=%d mem=%d nl=%v: 峰值 %d 超过额度 %d",
							tc.name, batch, mem, nl, st.HighWater, st.Capacity)
					}
					if st.InUse != 0 {
						t.Fatalf("%s batch=%d mem=%d nl=%v: 结束后仍有 %d 行未释放",
							tc.name, batch, mem, nl, st.InUse)
					}
				}
			}
		}
	}
}

// 落盘路径的 EOF 语义：结果完整读完后返回 io.EOF，且额度已随 Close 释放。
func TestSpillJoinEOFAndGrantRelease(t *testing.T) {
	orders, products := mkSkewed(t, 10, 10)
	pool := NewMemManager(2)
	opts := ExecOptions{BatchRows: 3, MemRows: 2, TempDir: t.TempDir(), Pool: pool}
	plan := NewJoin(NewScan(orders, "o"), NewScan(products, "p"),
		Bin(OpEq, Col("o_sku"), Col("p_sku")), InnerJoin)
	e, _ := buildPhysical(t, plan, opts, false)
	ctx := context.Background()
	total := 0
	for {
		b, err := e.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		total += len(b.Rows)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if total == 0 {
		t.Fatal("应有匹配行")
	}
	if st := pool.Stats(); st.InUse != 0 {
		t.Fatalf("Close 后仍有 %d 行额度未释放", st.InUse)
	}
}
