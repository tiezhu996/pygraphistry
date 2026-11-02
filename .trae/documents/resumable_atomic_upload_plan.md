# 可恢复 + 原子版本切换的图数据上传 实施计划

> 实施备注（2026-11-02，已完成）：
> 1. chunk 原语最终落在 `ArrowFileUploader.repost_arrow()`（复用 file_id 重发 + 404 重建协调）；PENDING/FILE_CREATED 状态转换由 session 自身持有，因为无状态原语无法表达"create 已确认、post 结果未知"的语义。
> 2. 内容指纹改用 Arrow IPC stream 序列化后取 sha256：零拷贝行切片共享父 buffer，直接哈希 buffer 会让所有切片得到同一摘要。
> 3. 异常/会话类型从 `graphistry` 顶层包导出（`graphistry.ResumableUploadError` 等）。
> 验证：新增 16 个测试全绿；相关 8 个回归文件共 202 passed；ruff、pyright ratchet、注释密度、类型卫生四个守卫均通过。


## 一、仓库调研结论

### 当前上传链路（`as_files=True`，即 `.upload()` 走的路径）

1. [PlotterBase.plot](file:///Users/pi/标注任务/swe/swe-source/sources/5908-pygraphistry/graphistry/PlotterBase.py#L2498-L2562) → `_plot_dispatch()` → `_make_arrow_dataset()` 构造一个全新的 `ArrowUploader`（不带 `dataset_id`）。
2. [ArrowUploader.post](file:///Users/pi/标注任务/swe/swe-source/sources/5908-pygraphistry/graphistry/arrow_uploader.py#L639-L701)：
   - `POST /api/v2/files/` 创建 edge 文件，再 `POST /api/v2/upload/files/{file_id}?erase=true` **整表一次性**上传（[ArrowFileUploader](file:///Users/pi/标注任务/swe/swe-source/sources/5908-pygraphistry/graphistry/ArrowFileUploader.py#L58-L169)）；
   - nodes 同理；
   - 最后 `POST /api/v2/upload/datasets/` 创建新 dataset，payload 中 `edge_files`/`node_files` 是**列表**（服务端天然支持多文件聚合）；
   - 成功后 plot 尾部才把新 `dataset_id`/file_id/url 绑定到返回的新 `g` 上。
3. 可用但未被利用的端点：`GET /api/v2/upload/datasets/{dataset_id}`（[pygraphistry.py:1999](file:///Users/pi/标注任务/swe/swe-source/sources/5908-pygraphistry/graphistry/pygraphistry.py#L1999-L2009)），可用于提交后核验。

### 问题对应关系

| 用户诉求 | 现状 |
|---|---|
| 断网后从头再传 | 单请求整表上传（`arrow_to_buffer` 一次性序列化 + 单次 POST），无分块、无进度记录 |
| 重试可能换掉在用数据集 | 重试重新 create files；`ArrowFileUploader._CACHE` 仅进程内按整表 hash 去重，失败中途 uploader 已被部分改写；无显式"提交点"概念 |
| 同批数据重试不出副本 | 无 chunk 级幂等：create 成功但响应丢失时再试会再造 staging 文件 |
| 全部校验通过才切版本 | 校验只有每文件响应里的 `is_valid`；dataset 创建与"全部内容校验"之间无显式闸门 |
| 取消/失败后旧图可打开 | plot 中途抛错时旧 `g` 本身不被修改（链式不可变），但没有任何机制保证/验证这一点，也无法续传 |

### 约束（已与用户确认）

- **纯客户端实现**：服务端只支持「创建文件 / 整表 Arrow 上传 / 创建 dataset / 查 dataset」，不做服务端改造。
- **opt-in**：新增参数启用，默认行为不变。
- **进程内内存账本**：不做磁盘持久化，续传通过失败异常携带的 session 对象在同进程内完成。

## 二、设计：分块暂存 + 校验闸门 + 单次提交（事务式上传）

核心思路：一次上传 = 一个内存事务会话 `ResumableUploadSession`。旧 dataset 全程只读；所有新数据先进入**与旧版本完全隔离的 staging 文件**；全部 chunk 确认且校验通过后，**唯一一次** `create_dataset` 作为提交点；提交成功后才绑定新 id。

### 分块（确定性、可重放）

- 按**行**切分 `pa.Table`：按 `nbytes/num_rows` 估算每行字节数，使每块接近目标大小（默认 64MB，`chunk_size_mb` 可调）；零行/小表退化为 1 块，与现有请求形态完全一致。
- 切片用 `table.slice(offset, length)`，offset/length 列表完整覆盖且不重叠；同一张表每次切出的块序列、每块内容 hash 完全一致。
- chunk 内容指纹：schema + 全部 column buffer 的 sha256 hex（沿用 `_hash_full_table` 的零拷贝遍历，但输出完整 256-bit hex，避免截断碰撞）。
- edges / nodes 各自独立分块（nodes 可缺省）。

### 内存账本与状态机

每个 chunk：`PENDING → FILE_CREATED(file_id) → CONFIRMED(file_id, resp)`；
会话整体：`STAGING → STAGED → COMMITTING → COMMITTED`，另有 `ABORTED`。

发送单个 chunk 的幂等策略：

1. `CONFIRMED`：跳过（**不重复 create、不重复 post**）。
2. `FILE_CREATED`（create 已发、post 结果未知）：**复用原 file_id** 重发 `post_arrow(chunk, file_id, erase=true)`——成功则 CONFIRMED；404（create 实际未生效）才新建文件；连接错误则保持 `FILE_CREATED`，下次继续。→ 常见重试路径不产生任何新文件。
3. `PENDING`：create→post；若 create 发生连接错误，状态保持 PENDING。极端歧义窗口（服务端已建文件但响应丢失）至多留下一个**不被任何 dataset 引用的孤儿 staging 文件**，不产生重复 dataset，且不影响旧图。

### 校验闸门（提交前）

- 每个 chunk 的 post 响应 `is_valid == true`（沿用现有 `post_arrow` 抛错语义），并复核：
  - 所有 chunk 行数之和 == 原表 `num_rows`（edges、nodes 分别核对）；
  - 每块 schema 与原表一致（切片天然保证，加断言）；
  - CONFIRMED 的 file_id 无重复、数量与 chunk 数一致。
- 任一不满足：抛 `ResumableUploadError`，**不进入提交**。

### 提交（唯一切换点）与不确定态保护

- 置 `COMMITTING` → 调用一次 `create_dataset`（按 chunk 顺序传 `edge_files`/`node_files` 全量列表）→ 成功置 `COMMITTED` 并记录新 `dataset_id`。
- 提交后做一次 `GET /api/v2/upload/datasets/{id}` 核验（非 2xx 只告警不回滚——dataset 服务端已创建）。
- 若 `create_dataset` 发生连接错误（不知道服务端是否建成）：进入**不确定提交态**，普通 `resume()` 拒绝再次提交（防止造出第二个 dataset，正是"重试多出副本"的最坏情况）；需显式调用 `session.resume(retry_commit=True)` 才会重新提交。
- 仅在 `COMMITTED` 后执行 plot 收尾（share link、生成 url、绑定新 g）；收尾结果缓存，重复 `resume()` 直接返回、零额外请求。

### 取消与旧版本安全

- `session.cancel()`：置 `ABORTED`，绝不调用 create_dataset；`resume()` 抛 `UploadCancelledError`。
- 旧版本安全不变量：
  - 会话只操作新建的 staging file_id，**绝不读取/覆写** uploader 上既有的 dataset_id / 旧 file_id；
  - 上传期间 `self`（旧 g）不做任何写操作；只有成功返回的新 `g` 携带新 id；
  - 失败/取消后旧 g 的 `_dataset_id`/`_url` 原样可用。

### 对外接口

- `PlotterBase.upload(...)` / `plot(...)` 新增（穿过 `Plottable` Protocol 签名）：
  - `resumable: bool = False`（要求 `as_files=True`，否则 `ValueError`；`.upload()` 固定为 True 路径所以无影响）；
  - `chunk_size_mb: int = 64`。
- 失败时抛 `ResumableUploadError`（`graphistry/upload_session.py`），带 `.session`；用户代码模式：
  - `session.resume()` 从最后确认块继续，成功返回最终 Plottable；
  - `session.cancel()` 放弃；
  - `UncertainCommitError` 为其子类，`resume(retry_commit=True)` 显式重提。
- 默认 `resumable=False` 时代码路径与现在**完全一致**（单一 chunk 也走旧路径，避免任何行为漂移）。

## 三、改动文件

- **新增** [graphistry/upload_session.py](file:///Users/pi/标注任务/swe/swe-source/sources/5908-pygraphistry/graphistry/upload_session.py)：
  - `chunk_table(table, target_bytes)` 及内容指纹函数；
  - `_ChunkState`/`_SessionState` 枚举、内存 journal（`threading.RLock` 保护）；
  - `ResumableUploadSession`（stage/validate/commit/cancel/resume，完整类型标注）；
  - `ResumableUploadError`、`UncertainCommitError`、`UploadCancelledError`。
- [graphistry/ArrowFileUploader.py](file:///Users/pi/标注任务/swe/swe-source/sources/5908-pygraphistry/graphistry/ArrowFileUploader.py)：新增 chunk 级原语 `create_and_post_chunk(chunk, file_id=None, expected_file_id=...)`（封装"复用 file_id 重发 / 404 重建"协调逻辑）；不动现有 `create_and_post_file` 与 `_CACHE`。
- [graphistry/arrow_uploader.py](file:///Users/pi/标注任务/swe/swe-source/sources/5908-pygraphistry/graphistry/arrow_uploader.py)：新增 `make_resumable_session(chunk_size_mb)`（不改 `post()`）；提交回调内部复用现有 `create_dataset()`。
- [Graphistry/PlotterBase.py](file:///Users/pi/标注任务/swe/swe-source/sources/5908-pygraphistry/graphistry/PlotterBase.py)：
  - `plot()`/`upload()` 增加 `resumable`、`chunk_size_mb` 参数与 docstring；
  - 抽取现有上传后收尾逻辑（share link→info→viz_url→render 分支）为私有 `_finalize_upload(uploader, ...)`，旧路径与事务路径共用；
  - 事务路径：构造 session → `session.run(finalize=...)`，失败原样抛出错异常，不触碰旧绑定。
- [graphistry/Plottable.py](file:///Users/pi/标注任务/swe/swe-source/sources/5908-pygraphistry/graphistry/Plottable.py)：Protocol 的 `upload`/`plot` 签名同步新增参数。
- **新增** `graphistry/tests/test_resumable_upload.py`（mock 风格，对齐 [test_ArrowFileUploader.py](file:///Users/pi/标注任务/swe/swe-source/sources/5908-pygraphistry/graphistry/tests/test_ArrowFileUploader.py)）。

## 四、实施步骤（依赖顺序）

1. `upload_session.py`：分块 + 指纹 + 状态机 + 会话类（先不接 plot）。
2. `ArrowFileUploader` 增加 chunk 原语（含 404 重建协调）。
3. `ArrowUploader.make_resumable_session()` 接线（edges 必需、nodes 可选，提交回调复用 create_dataset）。
4. `PlotterBase`：抽 `_finalize_upload`，plot/upload 增加参数并接入会话；`Plottable` Protocol 同步。
5. 测试（先写/随实现并行）。
6. 跑 lint/typecheck/相关 pytest 并修正。

## 五、依赖与注意事项

- 新代码全部要求完整类型标注：仓库 CI 有 pyright/type-hygiene 闸门（`bin/ci_pyright_guard.py`、ruff）。
- 不向 `POST /api/v2/files/` 发送服务端契约外的 JSON 字段（DRF 序列化器行为未知，避免 400 风险）；幂等完全靠客户端 file_id 复用。
- `Plottable` 的 `_edges_file_id`/`_nodes_file_id` 是单值：多 chunk 时提交后给 uploader 保留有序列表（内部属性），单值字段取首个 chunk id，渲染只依赖 `dataset_id`。
- 网络错误判定：`requests.exceptions.ConnectionError`/`Timeout` 视为可恢复；HTTP 4xx（除 chunk 重发时的 404）、parse 校验失败视为不可恢复错误，直接抛出不误重试。
- 与现有 `memoize`/`_CACHE` 不叠加：resumable 路径自带 chunk 去重，绕过整表缓存。

## 六、验证

- 新增单测覆盖：
  1. 分块确定性与覆盖完整性（偏移不重不漏、重组一致；小表/零行退化为 1 块）；
  2. 全成功：N+M 次 create/post、create_dataset 仅 1 次且文件 id 列表有序；新 g 拿新 id、**旧 g 保持旧 id/url**；
  3. 中途断网：首次在 chunk2 抛 `ResumableUploadError`、无 create_dataset；`resume()` 不重发 chunk1、chunk2 复用原 file_id（create 次数不增加）；最终只建 1 个 dataset；
  4. 成功后重复 `resume()`：无任何额外 HTTP 调用，返回缓存结果；
  5. 校验失败（行数不符 / is_valid=false）：禁止提交；
  6. `cancel()` 后 resume 抛 `UploadCancelledError`，无 dataset、旧图可用；
  7. 提交歧义：create_dataset 连接错误 → `UncertainCommitError`；普通 resume 拒绝、不产生第二个 dataset；显式 `retry_commit=True` 才重提；
  8. `resumable=True` + `as_files=False` → `ValueError`。
- 回归：`graphistry/tests/test_ArrowFileUploader.py`、`test_arrow_uploader.py`、`test_dataset_id_invalidation.py`、`test_plotter.py` 全绿（默认路径零改动）。
- 质量门：`python -m ruff check graphistry/upload_session.py graphistry/ArrowFileUploader.py graphistry/arrow_uploader.py graphistry/PlotterBase.py graphistry/Plottable.py graphistry/tests/test_resumable_upload.py`；`python -m pyright`（或仓库 `bin/pyright.sh`）对新增/改动文件无新增错误。

## 七、风险

- **服务端多文件聚合语义**：`edge_files`/`node_files` 为列表强烈暗示多文件按序聚合，但仓库内无文档。缓解：单块（<64MB 的常见数据）请求形态与现状逐字节一致；多块顺序严格按行序；该能力 opt-in，可快速关闭。
- **create 响应丢失的孤儿 staging 文件**：纯客户端无法彻底消除；缓解为"复用 file_id 重发"把窗口压缩到仅 create 一步，且孤儿文件不被任何 dataset 引用、不影响旧图。
- **commit 歧义**：已用 COMMITTING 态 + 显式 `retry_commit` 防止重复 dataset，代价是该极端场景需用户显式确认。
- **仅进程内续传**（用户已确认）：内核重启后不能续传；会话丢失后 staging 文件成为孤儿，旧版本始终不受影响。
