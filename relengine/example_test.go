package relengine_test

import (
	"context"
	"fmt"

	"relengine"
)

func Example() {
	orders := relengine.NewTable("orders", relengine.NewSchema([]relengine.Column{
		{Name: "sku", Type: relengine.TypeString, Nullable: true},
		{Name: "qty", Type: relengine.TypeInt, Nullable: true},
	}))
	_ = orders.Insert(relengine.Tuple{relengine.Str("A"), relengine.Int(2)})
	_ = orders.Insert(relengine.Tuple{relengine.Str("A"), relengine.Int(3)})
	_ = orders.Insert(relengine.Tuple{relengine.NullValue(), relengine.Int(9)})

	products := relengine.NewTable("products", relengine.NewSchema([]relengine.Column{
		{Name: "sku", Type: relengine.TypeString, Nullable: false},
	}))
	_ = products.Insert(relengine.Tuple{relengine.Str("A")})
	_ = products.Insert(relengine.Tuple{relengine.Str("A")}) // 重复产品行：连接放大

	on := relengine.Bin(relengine.OpEq, relengine.Col("o.sku"), relengine.Col("p.sku"))
	join := relengine.NewJoin(relengine.NewScan(orders, "o"), relengine.NewScan(products, "p"), on, relengine.InnerJoin)
	query := relengine.NewProject(
		relengine.NewAggregate(join,
			[]relengine.Expr{relengine.Col("p.sku")},
			[]*relengine.AggExpr{relengine.CountStar(), relengine.Sum(relengine.Col("qty"))}),
		relengine.Item(relengine.Col("p.sku"), "sku"),
		relengine.Item(relengine.Col("COUNT(*)"), "cnt"),
		relengine.Item(relengine.Col("SUM(qty)"), "qty"),
	)

	// 两个表都有同名列；第一版 Scan 保留原名，因此为了示例清晰直接用宽表下标表达式。
	rows, schema, err := relengine.NewEngine(relengine.ExecOptions{BatchRows: 2, MemRows: 8}).Execute(context.Background(), query)
	_, _ = schema, err
	for _, row := range rows {
		fmt.Println(row)
	}
	// Output:
	// [A 4 10]
}
