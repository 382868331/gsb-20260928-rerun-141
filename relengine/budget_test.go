package relengine

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

type usageReporter interface{ MemUsage() int }

func kvSchema() *Schema {
	return NewSchema([]Column{
		{Name: "k", Type: TypeString, Nullable: true},
		{Name: "v", Type: TypeInt, Nullable: true},
	})
}

// 回归：MemRows=1 时 Nested Loop 不得在首批结果后仍把整侧（100 行）留在内存。
// 每种执行方式都必须遵守同一预算并如实报告占用。
func TestNestedLoopHonorsMemRowsAfterFirstBatch(t *testing.T) {
	dir := t.TempDir()
	left := NewTable("l", kvSchema())
	right := NewTable("r", kvSchema())
	for i := 0; i < 3; i++ {
		mustInsert(t, left, Tuple{Str("A"), Int(int64(i))})
	}
	for i := 0; i < 100; i++ {
		mustInsert(t, right, Tuple{Str("A"), Int(int64(i))})
	}
	on := Bin(OpEq, Col("l.k"), Col("r.k"))
	opts := ExecOptions{BatchRows: 1, MemRows: 1, TempDir: dir}

	e, _ := buildPhysical(t, NewJoin(NewScan(left, "l"), NewScan(right, "r"), on, InnerJoin), opts, true)
	ur, ok := e.(usageReporter)
	if !ok {
		t.Fatal("nestedLoopJoin 应实现 MemUsage 报告实际占用")
	}
	ctx := context.Background()
	first, err := e.Next(ctx)
	if err != nil {
		t.Fatalf("首批应成功: %v", err)
	}
	if got := ur.MemUsage(); got > 2 { // 1 行右侧驻留 + 1 行左侧批次
		t.Fatalf("首批后内存保留 %d 行，超出 MemRows=1 的约定", got)
	}
	nlRows := append([]Tuple{}, first.Rows...)
	for {
		b, err := e.Next(ctx)
		if err != nil {
			break
		}
		nlRows = append(nlRows, b.Rows...)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if left0 := spillDirs(t, dir); len(left0) != 0 {
		t.Fatalf("临时目录未清理: %v", left0)
	}
	// 与 Hash 计划（同预算，走落盘路径）结果一致：3*100=300 行，重复行保留。
	e2, _ := buildPhysical(t, NewJoin(NewScan(left, "l"), NewScan(right, "r"), on, InnerJoin), opts, false)
	hashRows, err := Drain(ctx, e2)
	if err != nil {
		t.Fatal(err)
	}
	if len(nlRows) != 300 || len(hashRows) != 300 {
		t.Fatalf("期望 300 行，nl=%d hash=%d", len(nlRows), len(hashRows))
	}
	g1, g2 := canonical(nlRows), canonical(hashRows)
	for i := range g1 {
		if g1[i] != g2[i] {
			t.Fatalf("结果不一致\nnl=%v\nhash=%v", g1, g2)
		}
	}
}

// 同一查询中的两个连接共享一份内存池：一个阶段占用额度时，
// 另一个阶段转入临时存储，总占用不超过池上限，取消/结束明确归还。
func TestSharedPoolAcrossJoins(t *testing.T) {
	mk := func() (*Table, *Table, *Table) {
		a := NewTable("a", kvSchema())
		b := NewTable("b", kvSchema())
		c := NewTable("c", kvSchema())
		for i := 0; i < 3; i++ {
			mustInsert(t, a, Tuple{Str("x"), Int(int64(i))})
			mustInsert(t, b, Tuple{Str("x"), Int(int64(i + 10))})
			mustInsert(t, c, Tuple{Str("x"), Int(int64(i + 20))})
		}
		return a, b, c
	}
	planOf := func() LogicalPlan {
		a, b, c := mk()
		inner := NewJoin(NewScan(a, "a"), NewScan(b, "b"),
			Bin(OpEq, Col("a.k"), Col("b.k")), InnerJoin)
		return NewJoin(inner, NewScan(c, "c"),
			Bin(OpEq, Col("a.k"), Col("c.k")), InnerJoin)
	}
	// 参照：充足内存、独占预算。
	ref, _, err := NewEngine(ExecOptions{BatchRows: 2, MemRows: 4096, TempDir: t.TempDir()}).
		Execute(context.Background(), planOf())
	if err != nil {
		t.Fatal(err)
	}
	if len(ref) != 27 { // 3*3*3
		t.Fatalf("参照结果应为 27 行，得到 %d", len(ref))
	}
	// 共享池：总额度 4 行，两个连接阶段不能各自重复计算整份预算。
	pool := NewMemPool(4)
	en := NewEngine(ExecOptions{BatchRows: 2, MemRows: 4, TempDir: t.TempDir(), Pool: pool})
	got, _, err := en.Execute(context.Background(), planOf())
	if err != nil {
		t.Fatal(err)
	}
	g1, g2 := canonical(got), canonical(ref)
	if len(g1) != len(g2) {
		t.Fatalf("共享池结果行数不符: got %d want %d", len(g1), len(g2))
	}
	for i := range g1 {
		if g1[i] != g2[i] {
			t.Fatalf("共享池结果不一致\ngot=%v\nwant=%v", g1, g2)
		}
	}
	st := pool.Stats()
	if st.Peak > st.Total {
		t.Fatalf("峰值占用 %d 超过池上限 %d", st.Peak, st.Total)
	}
	if st.Spills == 0 || len(st.Events) == 0 {
		t.Fatalf("额度不足时应转入临时存储并说明原因: %+v", st)
	}
	if st.InUse != 0 {
		t.Fatalf("查询结束后额度应全部归还，仍占用 %d 行", st.InUse)
	}
	t.Logf("等待/落盘原因: %v", st.Events)
}

// 取消与局部失败明确释放所属资源，不影响同时进行的独立查询。
func TestPoolReleasedOnCancelAndIndependentQueries(t *testing.T) {
	dir1 := t.TempDir()
	orders := NewTable("orders", ordersSchema())
	products := NewTable("products", productsSchema())
	for i := 0; i < 30; i++ {
		mustInsert(t, orders, Tuple{Str("A"), Int(1)})
		mustInsert(t, products, Tuple{Str("A")})
	}
	on := Bin(OpEq, Col("o_sku"), Col("p_sku"))
	pool1 := NewMemPool(2)
	e1, _ := buildPhysical(t, NewJoin(NewScan(orders, "o"), NewScan(products, "p"), on, InnerJoin),
		ExecOptions{BatchRows: 1, MemRows: 2, TempDir: dir1, Pool: pool1}, false)

	ctx, cancel := context.WithCancel(context.Background())
	if _, err := e1.Next(ctx); err != nil {
		t.Fatalf("首批应成功: %v", err)
	}
	// 独立查询同时进行：自己的池、自己的临时目录，不受 q1 占用/取消影响。
	var wg sync.WaitGroup
	defer wg.Wait()
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// 每个独立查询使用自己的计划树（Bind 会就地绑定列下标）。
			on := Bin(OpEq, Col("o_sku"), Col("p_sku"))
			en := NewEngine(ExecOptions{BatchRows: 2, MemRows: 2, TempDir: t.TempDir()})
			rows, _, err := en.Execute(context.Background(),
				NewJoin(NewScan(orders, "o"), NewScan(products, "p"), on, InnerJoin))
			if err != nil {
				t.Errorf("独立查询应成功: %v", err)
			}
			if len(rows) != 900 {
				t.Errorf("独立查询结果应为 900 行，得到 %d", len(rows))
			}
		}()
	}
	cancel()
	if _, err := e1.Next(ctx); err == nil {
		t.Fatal("取消后继续拉取应返回错误")
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("Close 应清理成功: %v", err)
	}
	if got := pool1.Stats().InUse; got != 0 {
		t.Fatalf("取消并关闭后额度应全部归还，仍占用 %d 行", got)
	}
	if left := spillDirs(t, dir1); len(left) != 0 {
		t.Fatalf("临时目录未清理: %v", left)
	}
}

// 偏斜键 + 交叉连接 + 不同结果批次：总占用受限、输出完整、
// 同一物理路径的顺序约定不随批次变化，重复行不因资源转移而丢失。
func TestSkewAndCrossJoinUnderBudget(t *testing.T) {
	mkSkew := func() (*Table, *Table) {
		l := NewTable("l", kvSchema())
		r := NewTable("r", kvSchema())
		for i := 0; i < 50; i++ {
			mustInsert(t, l, Tuple{Str("H"), Int(int64(i % 3))}) // 热键 + 重复行
		}
		mustInsert(t, l, Tuple{Str("Z"), Int(9)})
		for i := 0; i < 30; i++ {
			mustInsert(t, r, Tuple{Str("H"), Int(int64(i % 3))})
		}
		return l, r
	}
	on := Bin(OpEq, Col("l.k"), Col("r.k"))

	// 不同 BatchRows 下落盘路径的输出行序列必须完全一致（顺序约定稳定）。
	var prev []string
	for _, batch := range []int{1, 3, 7} {
		l, r := mkSkew()
		en := NewEngine(ExecOptions{BatchRows: batch, MemRows: 2, TempDir: t.TempDir()})
		rows, _, err := en.Execute(context.Background(),
			NewJoin(NewScan(l, "l"), NewScan(r, "r"), on, InnerJoin))
		if err != nil {
			t.Fatalf("BatchRows=%d: %v", batch, err)
		}
		if len(rows) != 1500 { // 50*30，Z 不匹配
			t.Fatalf("BatchRows=%d: 期望 1500 行，得到 %d", batch, len(rows))
		}
		var ord []string
		for _, row := range rows {
			var parts []string
			for _, v := range row {
				parts = append(parts, v.String())
			}
			ord = append(ord, joinLines(parts))
		}
		if prev != nil {
			for i := range ord {
				if ord[i] != prev[i] {
					t.Fatalf("结果顺序随 BatchRows 改变（%d 行处）", i)
				}
			}
		}
		prev = ord
	}

	// 交叉连接（无等值键，单片落盘）：4x5=20 行，重复行保留；
	// Hash 落盘、Nested Loop 落盘与全内存参照的多重集合一致。
	mkCross := func() (*Table, *Table) {
		l := NewTable("l", kvSchema())
		r := NewTable("r", kvSchema())
		for i := 0; i < 4; i++ {
			mustInsert(t, l, Tuple{Str("a"), Int(1)})
		}
		for i := 0; i < 5; i++ {
			mustInsert(t, r, Tuple{Str("b"), Int(2)})
		}
		return l, r
	}
	crossPlan := func() LogicalPlan {
		l, r := mkCross()
		return NewJoin(NewScan(l, "l"), NewScan(r, "r"), Lit(Bool(true)), InnerJoin)
	}
	ref, _, err := NewEngine(ExecOptions{BatchRows: 4, MemRows: 100, TempDir: t.TempDir()}).
		Execute(context.Background(), crossPlan())
	if err != nil {
		t.Fatal(err)
	}
	if len(ref) != 20 {
		t.Fatalf("交叉连接参照应为 20 行，得到 %d", len(ref))
	}
	pool := NewMemPool(1)
	spilled, _, err := NewEngine(ExecOptions{BatchRows: 3, MemRows: 1, TempDir: t.TempDir(), Pool: pool}).
		Execute(context.Background(), crossPlan())
	if err != nil {
		t.Fatal(err)
	}
	g1, g2 := canonical(spilled), canonical(ref)
	for i := range g1 {
		if g1[i] != g2[i] {
			t.Fatalf("交叉连接落盘结果不一致\ngot=%v\nwant=%v", g1, g2)
		}
	}
	if pool.Stats().Spills == 0 {
		t.Fatal("MemRows=1 的交叉连接应落盘")
	}
	l, r := mkCross()
	nl, _ := buildPhysical(t, NewJoin(NewScan(l, "l"), NewScan(r, "r"), Lit(Bool(true)), InnerJoin),
		ExecOptions{BatchRows: 3, MemRows: 1, TempDir: t.TempDir()}, true)
	nlRows, err := Drain(context.Background(), nl)
	if err != nil {
		t.Fatal(err)
	}
	if got := canonical(nlRows); len(got) != len(g2) {
		t.Fatalf("Nested Loop 交叉连接行数不符: %d", len(got))
	} else {
		for i := range got {
			if got[i] != g2[i] {
				t.Fatalf("Nested Loop 交叉连接结果不一致\ngot=%v\nwant=%v", got, g2)
			}
		}
	}
}

// 极小总额度（3 行）下两个连接阶段共享一份预算：逐批核对每个阶段的
// 常驻行都计入额度、授予总额与池内记账一致，结束后全部归还。
func TestTinyPoolMultiJoinLedger(t *testing.T) {
	mk := func() (*Table, *Table, *Table) {
		a := NewTable("a", kvSchema())
		b := NewTable("b", kvSchema())
		c := NewTable("c", kvSchema())
		for i := 0; i < 2; i++ {
			mustInsert(t, a, Tuple{Str("x"), Int(int64(i))})
			mustInsert(t, b, Tuple{Str("x"), Int(int64(i + 10))})
		}
		for i := 0; i < 3; i++ {
			mustInsert(t, c, Tuple{Str("x"), Int(int64(i + 20))})
		}
		return a, b, c
	}
	planOf := func() LogicalPlan {
		a, b, c := mk()
		inner := NewJoin(NewScan(a, "a"), NewScan(b, "b"),
			Bin(OpEq, Col("a.k"), Col("b.k")), InnerJoin)
		return NewJoin(inner, NewScan(c, "c"),
			Bin(OpEq, Col("a.k"), Col("c.k")), InnerJoin)
	}
	ref, _, err := NewEngine(ExecOptions{BatchRows: 2, MemRows: 4096, TempDir: t.TempDir()}).
		Execute(context.Background(), planOf())
	if err != nil {
		t.Fatal(err)
	}
	if len(ref) != 12 { // 2*2*3
		t.Fatalf("参照结果应为 12 行，得到 %d", len(ref))
	}

	pool := NewMemPool(3)
	a, b, c := mk()
	innerPlan := NewJoin(NewScan(a, "a"), NewScan(b, "b"),
		Bin(OpEq, Col("a.k"), Col("b.k")), InnerJoin)
	plan := NewJoin(innerPlan, NewScan(c, "c"),
		Bin(OpEq, Col("a.k"), Col("c.k")), InnerJoin)
	e, _ := buildPhysical(t, plan, ExecOptions{BatchRows: 2, MemRows: 3, TempDir: t.TempDir(), Pool: pool}, false)
	outer := e.(*joinExec)

	ctx := context.Background()
	var got []Tuple
	for {
		batch, err := e.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, batch.Rows...)
		// 每个阶段常驻行都必须落在自己的授予额度内。
		inner := outer.l.(*joinExec)
		switch in := inner.inner.(type) {
		case *memHashJoin:
			if in.buildRows != inner.grant {
				t.Fatalf("内层连接常驻 %d 行，授予 %d 行", in.buildRows, inner.grant)
			}
		}
		if sp, ok := outer.inner.(*hashJoinSpill); ok {
			if len(sp.probeBlk) > outer.grant {
				t.Fatalf("外层探测块 %d 行超出授予 %d 行", len(sp.probeBlk), outer.grant)
			}
		}
		// 池内记账必须等于各阶段授予之和（占用与记账一致）。
		st := pool.Stats()
		if want := inner.grant + outer.grant; st.InUse != want {
			t.Fatalf("池记账 %d 行，各阶段授予合计 %d 行", st.InUse, want)
		}
		if st.Peak > st.Total {
			t.Fatalf("峰值 %d 超过池上限 %d", st.Peak, st.Total)
		}
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	g1, g2 := canonical(got), canonical(ref)
	if len(g1) != len(g2) {
		t.Fatalf("极小池结果行数不符: got %d want %d", len(g1), len(g2))
	}
	for i := range g1 {
		if g1[i] != g2[i] {
			t.Fatalf("极小池结果不一致\ngot=%v\nwant=%v", g1, g2)
		}
	}
	st := pool.Stats()
	if st.InUse != 0 {
		t.Fatalf("结束后额度应全部归还，仍占用 %d 行", st.InUse)
	}
	if st.Peak != st.Total {
		t.Fatalf("极小池应被用满到上限 %d，峰值 %d", st.Total, st.Peak)
	}
	if st.Spills == 0 || len(st.Events) == 0 {
		t.Fatalf("额度不足应落盘并记录原因: %+v", st)
	}
	t.Logf("占用/等待/释放事件: %v", st.Events)
}

// 多层连接耗尽共享额度：外层已占满池时，内层不能再靠一行未计额
// 探测缓冲硬撑，必须返回可解释的预算不足；所属查询释放额度后，
// 独立查询不受影响。
func TestSharedPoolExhaustionFailsExplainably(t *testing.T) {
	dir := t.TempDir()
	mk := func() (*Table, *Table, *Table) {
		a := NewTable("a", kvSchema())
		b := NewTable("b", kvSchema())
		c := NewTable("c", kvSchema())
		for i := 0; i < 3; i++ {
			mustInsert(t, a, Tuple{Str("x"), Int(int64(i))})
			mustInsert(t, b, Tuple{Str("x"), Int(int64(i + 10))})
			mustInsert(t, c, Tuple{Str("x"), Int(int64(i + 20))})
		}
		return a, b, c
	}
	a, b, c := mk()
	innerPlan := NewJoin(NewScan(a, "a"), NewScan(b, "b"),
		Bin(OpEq, Col("a.k"), Col("b.k")), InnerJoin)
	plan := NewJoin(innerPlan, NewScan(c, "c"),
		Bin(OpEq, Col("a.k"), Col("c.k")), InnerJoin)
	// 内层先占满 3 行额度，外层连 1 行探测块都申请不到。
	pool := NewMemPool(3)
	_, _, err := NewEngine(ExecOptions{BatchRows: 2, MemRows: 3, TempDir: dir, Pool: pool}).
		Execute(context.Background(), plan)
	if !errors.Is(err, ErrInsufficientBudget) {
		t.Fatalf("额度耗尽应返回 ErrInsufficientBudget，得到 %v", err)
	}
	st := pool.Stats()
	if st.InUse != 0 {
		t.Fatalf("失败后额度应全部归还，仍占用 %d 行", st.InUse)
	}
	explained := false
	for _, ev := range st.Events {
		if strings.Contains(ev, "耗尽") {
			explained = true
		}
	}
	if !explained {
		t.Fatalf("事件应解释额度耗尽原因: %v", st.Events)
	}
	if left := spillDirs(t, dir); len(left) != 0 {
		t.Fatalf("临时目录未清理: %v", left)
	}
	// 独立查询用自己的池，不受失败查询影响。
	a2, b2, c2 := mk()
	inner2 := NewJoin(NewScan(a2, "a"), NewScan(b2, "b"),
		Bin(OpEq, Col("a.k"), Col("b.k")), InnerJoin)
	plan2 := NewJoin(inner2, NewScan(c2, "c"),
		Bin(OpEq, Col("a.k"), Col("c.k")), InnerJoin)
	rows, _, err := NewEngine(ExecOptions{BatchRows: 2, MemRows: 4096, TempDir: t.TempDir()}).
		Execute(context.Background(), plan2)
	if err != nil {
		t.Fatalf("独立查询应成功: %v", err)
	}
	if len(rows) != 27 {
		t.Fatalf("独立查询应为 27 行，得到 %d", len(rows))
	}
}

// 展示“占用-等待-释放”的对应关系：额度被占满时 AcquireWait 明确等待，
// 持有者释放后等待者获得额度；取消等待则返回 ctx 错误。
func TestPoolAcquireWaitBlocksUntilRelease(t *testing.T) {
	pool := NewMemPool(2)
	if got := pool.Acquire(2); got != 2 {
		t.Fatalf("空池应授予 2 行，得到 %d", got)
	}
	ctx := context.Background()
	type result struct {
		grant int
		err   error
	}
	done := make(chan result, 1)
	go func() {
		g, err := pool.AcquireWait(ctx, 1)
		done <- result{g, err}
	}()
	// 等待期间不应获得额度。
	select {
	case r := <-done:
		t.Fatalf("额度未释放前不应返回: %+v", r)
	case <-time.After(50 * time.Millisecond):
	}
	st := pool.Stats()
	if st.InUse != 2 {
		t.Fatalf("等待期间占用应为 2 行，得到 %d", st.InUse)
	}
	pool.Release(2)
	r := <-done
	if r.err != nil || r.grant != 1 {
		t.Fatalf("释放后应获得 1 行额度，得到 %+v", r)
	}
	pool.Release(r.grant)
	st = pool.Stats()
	if st.InUse != 0 {
		t.Fatalf("全部释放后占用应为 0，得到 %d", st.InUse)
	}
	var waited, granted bool
	for _, ev := range st.Events {
		if strings.Contains(ev, "等待") {
			waited = true
		}
		if strings.Contains(ev, "等待结束") {
			granted = true
		}
	}
	if !waited || !granted {
		t.Fatalf("事件应记录等待与获得: %v", st.Events)
	}
	// 取消中的等待返回 ctx 错误而不是死等。
	cctx, cancel := context.WithCancel(context.Background())
	pool.Acquire(2)
	cancel()
	if _, err := pool.AcquireWait(cctx, 1); err == nil {
		t.Fatal("ctx 取消后等待应返回错误")
	}
	pool.Release(2)
}
