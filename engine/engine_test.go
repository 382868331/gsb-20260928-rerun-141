package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testDatabase() Database {
	customers := Table{
		Name: "customers",
		Columns: Schema{
			{Name: "cid", Type: TypeInt},
			{Name: "region", Type: TypeString},
			{Name: "credit", Type: TypeInt},
		},
		Rows: []Row{
			{int64(1), "east", int64(10)},
			{int64(2), "west", nil},
			{int64(3), "east", int64(20)},
			{int64(4), "west", int64(5)},
		},
	}
	orders := Table{
		Name: "orders",
		Columns: Schema{
			{Name: "oid", Type: TypeInt},
			{Name: "cid", Type: TypeInt},
			{Name: "amount", Type: TypeInt},
		},
		Rows: []Row{
			{int64(10), int64(1), int64(4)},
			{int64(11), int64(1), int64(6)},
			{int64(12), int64(2), nil},
			{int64(13), int64(2), int64(3)},
			{int64(14), int64(3), int64(20)},
			{int64(15), nil, int64(9)},
			{int64(15), nil, int64(9)},
		},
	}
	return Database{"customers": customers, "orders": orders}
}

func TestFilterProjectTypeCheckAndNulls(t *testing.T) {
	scan := NewScan(mustTable(t, testDatabase(), "orders"))
	filter := &FilterNode{
		Input:     scan,
		Predicate: Binary(OpGte, Col("amount"), Int(4)),
	}
	project := NewProject(filter, []ProjectItem{
		{Expr: Col("oid"), Name: "order_id"},
		{Expr: Binary(OpMul, Col("amount"), Int(2)), Name: "double_amount"},
		{Expr: IsNull(Col("amount")), Name: "missing"},
	})
	result, err := Execute(context.Background(), testDatabase(), project, Options{}, StrategyHash)
	if err != nil {
		t.Fatal(err)
	}
	sortRows(result.Rows)
	want := []Row{
		{int64(10), int64(8), false},
		{int64(11), int64(12), false},
		{int64(14), int64(40), false},
		{int64(15), int64(18), false},
		{int64(15), int64(18), false},
	}
	assertRows(t, want, result.Rows)
}

func TestEquivalentPlansJoinAggregate(t *testing.T) {
	queries := buildJoinAggregateQueries(t)
	for _, query := range queries {
		hashResult, err := Execute(context.Background(), testDatabase(), query, Options{BatchSize: 2, MaxInMemoryRows: 1, TempDir: t.TempDir()}, StrategyHash)
		if err != nil {
			t.Fatalf("hash plan %s: %v", query.String(), err)
		}
		blockResult, err := Execute(context.Background(), testDatabase(), query, Options{BatchSize: 2, MaxInMemoryRows: 1, TempDir: t.TempDir()}, StrategyBlock)
		if err != nil {
			t.Fatalf("block plan %s: %v", query.String(), err)
		}
		sortRows(hashResult.Rows)
		sortRows(blockResult.Rows)
		assertRows(t, hashResult.Rows, blockResult.Rows)
	}
}

func TestJoinFanoutAggregateCountsAndNulls(t *testing.T) {
	result := executeOne(t, buildExpectedJoinAggregate(t), StrategyBlock)
	sortRows(result.Rows)
	want := []Row{
		{"east", int64(3), int64(30), 40.0 / 3.0, int64(3)},
		{"west", int64(1), int64(3), nil, int64(1)},
	}
	assertRows(t, want, result.Rows)
}

func TestLeftJoinPreservesUnmatchedAndNullKeys(t *testing.T) {
	joined := buildLeftJoin(t)
	result := executeOne(t, &FilterNode{Input: joined, Predicate: Col("unmatched")}, StrategyBlock)
	sortRows(result.Rows)
	want := []Row{
		{int64(4), int64(5), nil, nil, true},
	}
	assertRows(t, want, result.Rows)
}

func TestTypeCheckRejectsUnknownColumn(t *testing.T) {
	scan := NewScan(mustTable(t, testDatabase(), "orders"))
	filter := &FilterNode{Input: scan, Predicate: Col("missing")}
	_, err := Execute(context.Background(), testDatabase(), filter, Options{}, StrategyHash)
	if err == nil || !strings.Contains(err.Error(), "unknown column") {
		t.Fatalf("expected unknown column error, got %v", err)
	}
}

func TestCancelCleansIntermediateFiles(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Execute(ctx, testDatabase(), buildExpectedJoinAggregate(t), Options{BatchSize: 1, MaxInMemoryRows: 1, TempDir: dir}, StrategyBlock)
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		t.Errorf("intermediate file was not removed: %s", entry.Name())
	}
}

func buildJoinAggregateQueries(t *testing.T) []Node {
	regions := []string{"east", "west"}
	thresholds := []int64{0, 3, 10}
	queries := make([]Node, 0, len(regions)*len(thresholds))
	for _, region := range regions {
		for _, threshold := range thresholds {
			scan := buildJoinBase(t, region, threshold)
			agg := NewAggregate(scan,
				[]ProjectItem{{Expr: Col("region"), Name: "region"}},
				[]AggregateItem{
					{Function: "COUNT", Name: "rows"},
					{Function: "SUM", Arg: Col("amount"), Name: "total"},
					{Function: "AVG", Arg: Col("credit"), Name: "avg_credit"},
					{Function: "COUNT", Arg: Col("amount"), Name: "non_null_amounts"},
				},
			)
			queries = append(queries, agg)
		}
	}
	return queries
}

func buildExpectedJoinAggregate(t *testing.T) Node {
	scan := buildJoinBase(t, "", 0)
	return NewAggregate(scan,
		[]ProjectItem{{Expr: Col("region"), Name: "region"}},
		[]AggregateItem{
			{Function: "COUNT", Name: "rows"},
			{Function: "SUM", Arg: Col("amount"), Name: "total"},
			{Function: "AVG", Arg: Col("credit"), Name: "avg_credit"},
			{Function: "COUNT", Arg: Col("amount"), Name: "non_null_amounts"},
		},
	)
}

func buildJoinBase(t *testing.T, region string, threshold int64) Node {
	t.Helper()
	db := testDatabase()
	join := NewJoin(
		NewScan(mustTable(t, db, "customers")),
		NewScan(mustTable(t, db, "orders")),
		JoinInner,
		[]Expr{Col("cid")},
		[]Expr{Col("cid")},
		nil,
	)
	var predicate Expr = Binary(OpGte, Col("amount"), Int(threshold))
	if region != "" {
		predicate = Binary(OpAnd, predicate, Binary(OpEq, Col("region"), Text(region)))
	}
	return &FilterNode{Input: join, Predicate: predicate}
}

func buildLeftJoin(t *testing.T) Node {
	db := testDatabase()
	join := NewJoin(
		NewScan(mustTable(t, db, "customers")),
		NewScan(mustTable(t, db, "orders")),
		JoinLeft,
		[]Expr{Col("cid")},
		[]Expr{Col("cid")},
		nil,
	)
	return NewProject(join, []ProjectItem{
		{Expr: Col("cid"), Name: "customer_id"},
		{Expr: Col("credit"), Name: "credit"},
		{Expr: Col("oid"), Name: "order_id"},
		{Expr: Col("amount"), Name: "amount"},
		{Expr: IsNull(Col("oid")), Name: "unmatched"},
	})
}

func executeOne(t *testing.T, node Node, strategy Strategy) QueryResult {
	t.Helper()
	result, err := Execute(context.Background(), testDatabase(), node, Options{BatchSize: 2, MaxInMemoryRows: 1, TempDir: t.TempDir()}, strategy)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustTable(t *testing.T, db Database, name string) Table {
	t.Helper()
	table, err := db.Table(name)
	if err != nil {
		t.Fatal(err)
	}
	return table
}

func assertRows(t *testing.T, want, got []Row) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("row count mismatch\nwant %#v\ngot  %#v", want, got)
	}
	for i := range want {
		if rowToken(want[i]) != rowToken(got[i]) {
			t.Fatalf("row %d mismatch\nwant %#v\ngot  %#v", i, want, got)
		}
	}
}

func TestNoSpillFilesRemainAfterSuccess(t *testing.T) {
	dir := t.TempDir()
	_, err := Execute(context.Background(), testDatabase(), buildExpectedJoinAggregate(t), Options{BatchSize: 1, MaxInMemoryRows: 1, TempDir: dir}, StrategyBlock)
	if err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".rows" {
			t.Fatalf("temporary row file remains: %s", entry.Name())
		}
	}
}
