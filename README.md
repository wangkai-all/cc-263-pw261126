# SandboxLab — 分析沙箱的克隆、刷新、销毁与源记录删除

数据团队从上游快照建立多个分析沙箱。每个沙箱可以独立改表，也能刷新上游。
本仓库从零实现了一个可运行的版本：**真实 SQLite 文件 + 内容寻址共享块 +
本地覆盖层 + 操作网页**，并用虚构数据验证分支隔离、刷新冲突、有界历史和
删除清理。

无第三方依赖：Python 3.11 标准库 + 原生 JS。没有 CDN、没有构建步骤、没有遥测。

```bash
python3 -m sandboxlab            # → http://127.0.0.1:8080/  (数据在 ./var)
python3 -m unittest discover -s tests -v
python3 scripts/acceptance.py    # 端到端走查，打印每一步的通过/失败
```

## 1. 方案选择：共享块 + 覆盖层（不是全量复制）

| 维度 | 全量复制 | 本项目：共享块 + 覆盖层 |
| --- | --- | --- |
| 空间 | N 个沙箱 = N 份数据 | 每个上游版本一个内容寻址 gzip 块；沙箱只存引用 + 改动行 |
| 克隆 | 复制全部行 | 写一条引用 + 一条 history，O(1) 行数据 |
| 隔离 | 天然隔离 | 基底固定（pinned），改动只进本地覆盖层 |
| 刷新 | 整表覆盖会静默丢本地改动 | 差量检测 → 冲突逐条决策 → 才推进基底 |
| 历史 | 空间换无限回看 | 有限窗口（默认 25 个版本），更早的明确拒绝 |
| 删除 | 在 N 个文件 / 副本 / 导出里逐个找 | 重写一个块 + 一次性清理所有沙箱覆盖层/缓存/导出 |

**明确不承诺的事情**：不能同时保证“无限回看”和“彻底删除”。代码里删除优先级最高，
历史保留窗口有硬上限（`HISTORY_HARD_CAP`），超出窗口的回看会返回
`history_pruned`，而不是返回一个错误的历史状态。

### 文件布局

```
<data>/
  data.db                     # 上游表 + 目录（沙箱、块索引、墓碑、导出、审计）
  segments/<sha256>.json.gz   # 不可变、内容寻址的共享块
  sandboxes/<id>.sqlite3      # 真实沙箱数据库：引用 + 本地覆盖层 + 历史
  exports/<id>.csv            # 真实导出文件
```

沙箱库里有 `base_ref`（指向块）、`base_history`（每次刷新 pin 的块）、
`local_rows` / `local_tombstones` / `local_meta`（覆盖层）、`row_history`
（有界撤销日志）、`conflicts`、`pending_refresh`、`query_cache`、`erased_rows`
（墓碑镜像）。读取 = 共享块 + 覆盖层合并。

## 2. 关键语义

### 克隆
- 新沙箱引用当前共享块，不复制行；`sandboxes/<id>.sqlite3` 是真实数据库文件，
  但其中没有上游行负载（验收脚本会验证空沙箱的覆盖层为 0 字节）。
- 同一块的多个沙箱共享一个物理文件；`base_shared_refs` 在 UI 上明确显示这一点。

### 刷新
- 沙箱默认固定在克隆时的块。上游变更后，沙箱看不到，直到刷新。
- `GET .../refresh/preview` 先展示差异与冲突，不落任何状态。
- 若“本地改过”的行上游也变了 → 刷新**停下**（HTTP 409 `refresh_conflicts`），
  候选块暂存在 `pending_refresh`，基底不推进；必须逐条
  `keep`（保留本地）/ `take`（采用上游）后才能 `refresh/apply`。
  本地改动绝不会被静默覆盖。
- 本地删除 vs 上游删除不算冲突；本地删除 vs 上游更新算冲突。

### 历史
- 每个沙箱有逻辑时钟 `seq`，每次用户可见变更 +1（克隆/刷新也占一个 seq）。
- `GET .../rows?as_of=N` 用“压缩基线 + 保留窗口内的撤销日志”重建当时的覆盖层，
  并按 `seq` 选择当时 pin 的基底块。
- 窗口是硬的：超过保留量后旧日志折叠进 `history_baseline`，更早的 `as_of`
  返回 409 `history_pruned`，不会假装能回看。

### 源记录删除
一条记录的删除会按以下顺序完成，全部幂等：

1. **先落墓碑**（`erased_rows`），这是唯一权威；此后所有读路径都过滤它，
   所有写路径（写入/恢复/克隆/刷新）都拒绝它。
2. 重写所有包含它的共享块，旧文件物理删除；旧块 id 通过 `replaced_by`
   解析到新块，因此**无需逐个改写沙箱文件**就能让所有引用者看到删除。
3. 清理每个沙箱的：本地覆盖行、墓碑里保存的旧值、历史日志里的内容
   （置空并标记 `redacted`/`erased`，保留“曾编辑过”的审计事实）、
   **折叠后的历史基线**（`history_baseline` 里聚合的状态对象也要重写，
   否则被压缩的旧值仍可读）、未决冲突里的载荷、读缓存；
   随后 `VACUUM` + WAL 截断（`secure_delete` 只能清空空闲单元，
   旧 WAL 页与空闲页需要重写 + 截断才真正消失）。
4. **操作日志脱敏**：`operations.detail` 里的变更快照可能带姓名/金额；
   保留“哪一行变过”的审计事实，把命中该行的条目替换为
   `{"row_id": ..., "redacted": true}`，并对 `data.db` 同样做 `VACUUM` + WAL 截断。
5. 吊销并删除所有包含该行的导出文件。
6. 写审计报告：块重写、沙箱清理、操作日志脱敏、导出吊销、缓存刷新、
   无法就地清理的沙箱（例如文件已不在）。

`GET /api/erasures/<row_id>` 可复核是否仍有残留（覆盖层、历史日志、
压缩基线、冲突载荷、读缓存、仍有效的导出、共享块文件逐项检查，
全部为空才算 `clean`）；`POST /api/recover` 补齐任何中途崩溃的清理。
**恢复只做删除方向的动作，不能复活数据**：墓碑不可移除，重放清理也不会把内容写回。

### 销毁
- 吊销导出 → 清空覆盖层 → 标记 destroyed（事务提交）→ 删除三个 SQLite 文件。
- 崩溃在 unlink 之前也无妨：`recover` 会清理 destroyed 沙箱遗留的文件。
- 共享块不受影响；其它沙箱照常读取（测试覆盖）。

### 失败与并发
- 所有写操作走 `BEGIN IMMEDIATE`；删除额外持有进程内 `_erasure_lock`。
- 删除提交后、清理开始前的读会被过滤；清理期间的本地写入在提交后会重新检查墓碑，
  发现已删除则回收刚写入的载荷并返回 410（跨进程竞态的兜底）。
- 读缓存带 `erasure_epoch` 与行 id 双重校验，删除后不会命中旧结果。
- 注入式 `failpoint(name)` 用于测试：墓碑后崩溃、冲突暂存后崩溃、swap 后崩溃、
  unlink 前崩溃，均有对应测试。

## 3. 网页

`/` 提供完整管理界面：

- 左侧：克隆新沙箱、沙箱列表（改动数/删除数/磁盘/冲突/待决）、上游概览与
  “模拟上游变更”。
- 主区：基底块与共享引用计数、本地改动/删除、沙箱磁盘、冲突、回看窗口；
  数据表格支持搜索/区域过滤/排序/编辑/删除/恢复/显示已删行；回看滑块；
  刷新预览、刷新（冲突时引导逐条决策）；列改名/隐藏（仅本沙箱）；导出 CSV；
  导出列表（显示吊销状态）；基底历史；本沙箱操作日志；销毁。
- 顶栏“按记录执行删除…”：输入行号 → 预览影响范围（哪些沙箱、多少导出）→
  填写删除依据 → 输入 `DELETE` 确认 → 完成后展示逐项清理报告，
  并再次调用 `GET /api/erasures/<row>` 做一次**独立残留复核**
  （覆盖层、历史、压缩基线、冲突载荷、读缓存、共享块、导出逐项检查，
  而不是只相信刚才写下的报告）。`acct-004` 带有合规保留义务，
  未勾选确认时会被拒绝。
- “查看取舍说明”展示第 1 节的对比。

界面是原生 JS，直接调用同一套 HTTP API；所有错误以
`{"error": {"code", "message", "details"}}` 返回，UI 对
`refresh_conflicts`、`row_erased`、`export_revoked`、`history_pruned`
等分别处理。

## 4. HTTP API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/state` | 上游、沙箱列表、最近操作 |
| POST | `/api/sandboxes` | 克隆 `{label}` |
| GET | `/api/sandboxes/<id>` | 详情（基底、覆盖层计数、冲突、磁盘、导出） |
| GET | `/api/sandboxes/<id>/rows` | 读视图：`query,region,sort,direction,include_deleted,as_of,nocache` |
| PATCH/DELETE | `/api/sandboxes/<id>/rows/<row>` | 本地写 / 本地删 |
| POST | `/api/sandboxes/<id>/rows/<row>/restore` | 恢复本地删除 |
| PATCH | `/api/sandboxes/<id>/columns` | 列别名/可见性（仅本沙箱） |
| GET | `/api/sandboxes/<id>/refresh/preview` | 差异 + 冲突预览 |
| POST | `/api/sandboxes/<id>/refresh` | 刷新；冲突时 409 |
| GET/POST | `/api/sandboxes/<id>/conflicts[/<row>]` | 冲突列表 / 决策 `{resolution: keep\|take}` |
| POST | `/api/sandboxes/<id>/refresh/apply` | 推进基底（必须无未决冲突） |
| POST | `/api/sandboxes/<id>/exports` | 导出当前视图 |
| GET | `/api/exports/<id>/download` | 下载（吊销后 410） |
| DELETE | `/api/sandboxes/<id>` | 销毁 `{reason}` |
| POST | `/api/upstream/simulate` | 模拟上游变更 `{ops, seed}` |
| GET | `/api/erasure/preview?row_id=` | 删除影响范围 |
| POST | `/api/upstream/erase` | 执行删除 `{row_id, reason, acknowledge_hold}` |
| GET | `/api/erasures/<row>` | 删除审计与残留复核 |
| POST | `/api/recover` | 补齐未完成的删除/销毁清理 |

## 5. 虚构数据

48 条虚构客户记录，域名全部使用保留 TLD `example`，不会路由到任何真实地址。
`scripts/acceptance.py` 会用确定性的方式走完克隆、隔离、上游变更、刷新冲突、
历史、导出、删除、销毁与恢复。

## 6. 已知边界

- 单进程文件存储：SQLite 文件级锁 + 进程内锁，没有分布式协调。
- 大表扫描时每次读都会解压共享块；这是“共享块”方案换取删除正确性的代价。
- `as_of` 的分辨率是操作序号，不是墙钟时间；界面按序号展示以避免歧义。
- 导出是 CSV 快照；吊销会删除文件并返回 410，但已经被外部复制走的内容
  无法追回——这是任何导出机制的物理边界，审计报告会如实列出“曾经暴露”的沙箱。
