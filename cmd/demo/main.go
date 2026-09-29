package main

import (
	"context"
	"fmt"
	"os"

	"localrel/engine"
)

func main() {
	db := database()
	thresholds := []int64{0, 5, 10}
	for _, threshold := range thresholds {
		plan := query(threshold)
		hash, err := engine.Execute(context.Background(), db, plan, engine.Options{BatchSize: 2, MaxInMemoryRows: 100}, engine.StrategyHash)
		must(err)
		block, err := engine.Execute(context.Background(), db, plan, engine.Options{BatchSize: 2, MaxInMemoryRows: 1, TempDir: os.TempDir()}, engine.StrategyBlock)
		must(err)
		engine.SortRows(hash.Rows)
		engine.SortRows(block.Rows)
		fmt.Printf("query threshold=%d plan=%s\n", threshold, engine.Describe(plan))
		for _, row := range hash.Rows {
			fmt.Printf("  %v\n", row)
		}
		if fmt.Sprint(hash.Rows) != fmt.Sprint(block.Rows) {
			panic("equivalent plans returned different rows")
		}
		fmt.Println("  hash plan and block-nested-loop plan match")
	}
}

func query(threshold int64) engine.Node {
	db := database()
	customers := engine.NewScan(db["customers"])
	orders := engine.NewScan(db["orders"])
	joined := engine.NewJoin(customers, orders, engine.JoinInner,
		[]engine.Expr{engine.Col("cid")}, []engine.Expr{engine.Col("cid")}, nil)
	filtered := &engine.FilterNode{
		Input:     joined,
		Predicate: engine.Binary(engine.OpGte, engine.Col("amount"), engine.Int(threshold)),
	}
	return engine.NewAggregate(filtered,
		[]engine.ProjectItem{{Expr: engine.Col("region"), Name: "region"}},
		[]engine.AggregateItem{
			{Function: "COUNT", Name: "joined_rows"},
			{Function: "SUM", Arg: engine.Col("amount"), Name: "amount_sum"},
			{Function: "AVG", Arg: engine.Col("credit"), Name: "avg_credit_after_join_fanout"},
		},
	)
}

func database() engine.Database {
	return engine.Database{
		"customers": engine.Table{
			Name: "customers",
			Columns: engine.Schema{
				{Name: "cid", Type: engine.TypeInt},
				{Name: "region", Type: engine.TypeString},
				{Name: "credit", Type: engine.TypeInt},
			},
			Rows: []engine.Row{
				{int64(1), "east", int64(10)},
				{int64(2), "west", nil},
				{int64(3), "east", int64(20)},
				{int64(4), "west", int64(5)},
			},
		},
		"orders": engine.Table{
			Name: "orders",
			Columns: engine.Schema{
				{Name: "oid", Type: engine.TypeInt},
				{Name: "cid", Type: engine.TypeInt},
				{Name: "amount", Type: engine.TypeInt},
			},
			Rows: []engine.Row{
				{int64(10), int64(1), int64(4)},
				{int64(11), int64(1), int64(6)},
				{int64(12), int64(2), nil},
				{int64(13), int64(2), int64(3)},
				{int64(14), int64(3), int64(20)},
				{int64(15), nil, int64(9)},
				{int64(15), nil, int64(9)},
			},
		},
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
