# 本地关系查询引擎（第一版）

纯 Go 实现，不调用 SQLite、DuckDB 等现成数据库执行核心查询。当前没有 SQL 解析器：
调用方直接构造逻辑计划（`Scan -> Filter/Join -> Aggregate -> Project`），引擎做统一
类型检查后生成物理执行器。后续增加表达式时实现同一个 `Expr.Check` / `Expr.Eval`
接口即可，筛选、连接、投影和聚合都会复用同一套类型系统。

## 数据语义

- **重复行有意义**：数据模型是 bag 而不是 set。插入两条相同记录就保留两份，不隐式去重。
- **连接会放大行数**：左行匹配 `k` 条右行输出 `k` 行；左有 `m` 份、右有 `n` 份时输出
  `m*n` 份。上层 `COUNT(*)`、`SUM(x)` 等聚合逐行更新，因此不会少算。
- **NULL 是未知值**：普通比较遵循三值逻辑，结果为 `TRUE/FALSE/UNKNOWN`。
  `NULL = NULL` 的结果是 UNKNOWN，不是 TRUE；筛选和连接条件只接受 TRUE。
- **AND/OR**：AND 是 FALSE 优先，OR 是 TRUE 优先，其余含 NULL 的组合返回 UNKNOWN。
- **聚合 NULL**：`COUNT(*)` 计所有行；`COUNT(x)` 跳过 x 为 NULL 的行；
  `SUM/AVG/MIN/MAX` 忽略 NULL；全局聚合在空输入上也输出一行。
- **外连接补位**：LEFT JOIN 未匹配时右侧补全 NULL；RIGHT JOIN 未匹配时左侧补全 NULL。

## 计划与表达式

- 逻辑计划：`Scan`、`Filter`、`Project`、`Join`（INNER/LEFT/RIGHT）、`Aggregate`。
- 表达式：列引用（含 `alias.column` 限定名）、字面量、算术、比较、AND/OR/NOT。
- 聚合：`COUNT(*)`、`COUNT(x)`、`SUM`、`AVG`、`MIN`、`MAX`，均实现可合并状态。
- `Bind()` 自底向上完成列绑定、类型推导和参数校验；类型不匹配会直接报错。

## 执行、内存与取消

- 执行器接口为分批拉取：`Next(ctx) -> Batch`，`Close()` 释放资源。
- `ExecOptions.BatchRows` 控制每批大小；`MemRows` 控制连接构建块的行内存阈值。
- Hash Join 超阈值时，两侧流式写入 gob 临时文件，再按 `MemRows` 读入构建块；
  探测侧每块从头流式扫描，输出仍严格按 `BatchRows` 拉取。
- 块嵌套循环连接使用同一预算；热键产生完整多对多结果，但不会为慢消费者积压输出。
- 外连接落盘时先完成全部构建块的匹配，再用受限扫描补齐确实未匹配的探测行，避免重复补 NULL。
- 分组聚合可通过 `SpillAggregate: true` 强制走“分片落盘 -> 逐片聚合”路径。
- 临时目录由 `spiller` 统一管理，正常结束、错误和取消路径都调用 `Close()` 删除。
- 所有算子检查 `context.Context`；取消后返回错误，并清理已经产生的中间文件。

## 验证

`go test ./...` 包含：

- 小表穷举：订单 SKU/数量取 `{NULL,A,B} x {NULL,1,2}`，产品重复取 `A/B`，
  对全部 324 种组合分别执行 Hash Join 计划与 Nested Loop Join 计划。
- 两个等价物理计划按规范化多重集合比较，验证重复行、NULL 和连接放大完全一致。
- 独立用例验证 2 个 A 订单连接 2 条重复产品得到 4 行，数量总和为 10。
- 分批大小和内存阈值均设为 1~3 行，强制覆盖落盘路径。
- 聚合分片落盘后的临时文件清理，以及已取消 context 的错误返回。
