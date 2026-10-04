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
- `ExecOptions.BatchRows` 控制每批大小；`MemRows` 控制 Hash Join 构建侧的内存阈值。
- **共享内存额度**：`ExecOptions.Pool`（`MemManager`，以“行”记账）非空时，同一查询
  中的多个连接算子从同一份额度记账；为空时 `Engine` 为每次 `Execute` 创建容量为
  `MemRows` 的独立池，不同查询互不影响。某个阶段暂时多占时，其他阶段
  `TryAcquire` 失败即转入临时存储（不会各自把整份额度重复计算），或调用
  `Acquire` 阻塞等待；等待与落盘都会记录带原因的 `WaitEvent`/`SpillEvent`，
  通过 `MemManager.Stats()` 或 `Engine.LastMemStats()` 报告实际占用
  （当前值、峰值、等待与落盘原因）。取消或局部失败时 `Close()` 幂等释放所属额度，
  不影响同时使用同一池的独立查询。
- Hash Join 超阈值时，左右两侧按等值连接键哈希写到同一组 gob 临时分片，再逐片读回
  处理；没有等值键时退化为单片落盘。回读阶段同样遵守预算：探测侧每次只在内存
  保留一块（不超过 MemRows 行），构建侧分片按落盘批次流式重扫，热键的多对多
  放大结果完整产生，但内存不随分片大小增长。
- Nested Loop Join 同样遵守 `MemRows`：右侧物化超限（或共享额度不足）时整体写入
  单分片临时文件，之后每个左行流式重扫右侧，不会在首批结果后把整份右侧留在内存；
  输出顺序（左主右次的嵌套序）与重复行数与纯内存路径完全一致。
- 预算不足（MemRows/BatchRows < 1）时连接算子返回 ErrInsufficientBudget，
  不会悄悄退回无限内存；取消或临时文件损坏都会明确报错并清理临时目录。
- 分组聚合可通过 `SpillAggregate: true` 强制走“分片落盘 -> 逐片聚合”路径。
- 临时目录由 `spiller` 统一管理，正常结束、错误和取消路径都调用 `Close()` 删除。
- 所有算子检查 `context.Context`；取消后返回错误，并清理已经产生的中间文件。

## 结果顺序约定

- 数据模型是 bag：比较结果以规范化多重集合为准（测试中的 `canonical`）。
- Nested Loop Join 额外保证确定性的左主右次嵌套序，落盘与否不改变该顺序。
- Hash Join 落盘后按分片顺序产出，不保证与内存路径相同的行序，但多重集合相同。

## 验证

`go test ./...` 包含：

- 小表穷举：订单 SKU/数量取 `{NULL,A,B} x {NULL,1,2}`，产品重复取 `A/B`，
  对全部 324 种组合分别执行 Hash Join 计划与 Nested Loop Join 计划。
- 两个等价物理计划按规范化多重集合比较，验证重复行、NULL 和连接放大完全一致。
- 独立用例验证 2 个 A 订单连接 2 条重复产品得到 4 行，数量总和为 10。
- 分批大小和内存阈值均设为 1~3 行，强制覆盖落盘路径。
- 共享额度：一个查询中两个连接共享一份额度时外层转入临时存储且结果不变；
  取消/局部失败后额度全部释放，同池独立查询不受影响；等待方记录等待原因。
- 偏斜键与交叉连接在 BatchRows/MemRows 1~3 全组合下两种物理方式结果一致，
  且峰值占用不超过共享额度；Nested Loop 落盘前后有序输出逐行相同。
- 聚合分片落盘后的临时文件清理，以及已取消 context 的错误返回。
