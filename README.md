# 本地关系查询引擎

这是一个不依赖现成数据库的 Go 关系查询原型。表、逻辑查询计划、表达式类型检查和物理执行都在本仓库实现。

## 已支持

- 表数据：`Table` 包含显式 `Schema` 与多行 `Row`。
- 逻辑算子：扫描、筛选、投影、内连接、左外连接、分组聚合。
- 表达式：字面量、列引用、算术、比较、`AND`、`OR`、`NOT`、`IS NULL`。新增表达式仍实现同一套 `Expr.TypeCheck` 与 `Expr.Eval` 接口。
- 聚合：`COUNT(*)`、`COUNT(expr)`、`SUM`、`AVG`、`MIN`、`MAX`。
- 两种物理计划：内存哈希连接与分批块嵌套循环连接；同一逻辑计划可分别执行并对照结果。

## 语义约定

- 重复行不去重。两条完全相同的行表示两条独立事实，`COUNT(*)` 会分别计数。
- `NULL` 表示未知或缺失，不等同于空字符串、0 或 false。
- 比较和算术遇到 `NULL` 输出 `NULL`；筛选只保留结果明确为 `true` 的行。
- 连接键中的 `NULL` 不与任何值匹配，包括另一个 `NULL`。
- 左外连接在右侧没有匹配时保留左侧行，并用 `NULL` 补齐右侧列。
- 连接可能放大行数。例如一个左行匹配两个右行就产生两条输出；之后的 `COUNT`、`SUM`、`AVG` 都基于放大后的行计算，不会先按左表压缩。
- `COUNT(*)` 统计所有行；`COUNT(expr)` 跳过表达式结果为 `NULL` 的行；`SUM` 与 `AVG` 跳过 `NULL` 输入。没有非空输入时结果为 `NULL`。

## 分批与取消

- `Options.BatchSize` 控制扫描、落盘和回读的批大小。
- `Options.MaxInMemoryRows` 限制块嵌套连接的右侧块大小以及分组阶段保留的组数。
- 超限时中间数据写入 `Options.TempDir` 的临时文件；执行结束后删除。
- `context.Context` 取消会让迭代器返回 `context.Canceled`，算子的 `Close` 会关闭输入并清理临时文件。
- 分组超限时对原始行做哈希分区，再递归处理分区；键值带 salt，降低重复碰撞后仍超限的概率。

## 验证

测试使用小表覆盖重复行、空值、一对多连接、左外补空、动态筛选阈值和分组统计：

```bash
go test ./...
go run ./cmd/demo
```

`TestEquivalentPlansJoinAggregate` 动态生成 2 个区域 × 3 个阈值共 6 条查询，对每条查询分别执行哈希计划和块嵌套计划，排序后逐行对照。`MaxInMemoryRows=1` 且 `BatchSize=2`，因此块连接与分组路径会实际使用分批和临时文件，而不是只验证一条写死的内存查询。

## 代码结构

- `engine/types.go`：类型、表、行和排序。
- `engine/expr.go`：表达式接口、类型检查、三值逻辑和求值。
- `engine/logical.go`：逻辑查询计划。
- `engine/aggregate.go`：聚合类型检查与状态。
- `engine/physical.go`：迭代器接口、扫描、筛选、投影、内存哈希连接。
- `engine/bnl_join.go`：分批块嵌套连接和左外连接。
- `engine/aggregate_source.go`：内存/落盘分区分组聚合。
- `engine/planner.go`：逻辑计划到物理执行器的构建。
