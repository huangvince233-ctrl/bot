# 🤖 项目介绍与功能说明

> [!NOTE]
> 本文档是对该项目所有工作模式和核心功能的完整、详尽描述，适合作为系统百科全书供人工维护与 AI 助手深度引用。

---

## 📦 项目架构与 Dual Bot 隔离机制

**Telegram Video Copilot** 采用模块化解耦设计，于 V9.0 版本正式引入 **“底层资源共享，逻辑管辖隔离”** 的双机器人架构：

- **主 Bot (`tgporncopilot`)**: 锚定重度主题，负责特定频道及文件夹（如“整理、精品”）的搬运、备份与提取。
- **副 Bot (`my_porn_private_bot`)**: 锚定轻度主题，负责其余私密文件夹的日常维护。

### 项目物理目录树 (Panoramic View)

```text
f:\funny_project\tgporncopilot\
├── .env                              # 本地环境变量 (API_ID, BOT_TOKEN, 路径配置)
├── start_tgporncopilot.bat           # 主 Bot 启动入口
├── start_my_porn_private_bot.bat     # 副 Bot 启动入口
├── data/
│   ├── copilot.db                    # 核心 SQLite 数据库 (存放消息、索引、运行记录)
│   ├── archived/                     # 历史备份归档目录 (JSON/MD 原始快照)
│   ├── entities/                     # [隔離] 实体库核心规则 Data (JSON)
│   │   ├── tgporncopilot/
│   │   └── my_porn_private_bot/
│   ├── sessions/                     # 隔离存放各 Bot 的 Telethon Session 文件
│   └── logs/                         # 运行日志 (pipeline_server_{bot}.log, bot.log)
├── docs/
│   ├── entities/                     # [隔離] 预览与交互文档区 (MD/JSON samples)
│   │   ├── tgporncopilot/
│   │   │   ├── candidates/           # 候选词分卷池 (candidate_pool_part_*.md)
│   │   │   └── current_entities.md   # [LIVE] 全量词库可视化视图 (实时对账)
│   │   └── my_porn_private_bot/
│   └── tags/                         # P3 导出的分类预览 Markdown 快照
├── src/
│   ├── search_bot.py                 # Bot 交互总调度中心 (Dispatcher)
│   ├── db.py                         # 数据库驱动与 SQL 逻辑层
│   ├── search_mode/
│   │   ├── common/                   # 搜索底层逻辑
│   │   ├── program1_discovery/       # 流水线发现阶段逻辑 (P0/P1)
│   │   ├── program2_tagging/         # 自动化全量/增量打标核心 (P2)
│   │   └── program3_export/          # Markdown 聚合导出逻辑 (P3)
│   └── utils/config.py               # [CORE] 全局配置与多 Bot 路径动态映射中心
└── tools/sorter/                     # P1.5 Web 分拣工作台 (Flask 后端 + 前端)
```

---

## 🔄 四大核心工作模式

### 1. 同步模式 (`/sync`)

**目标**：将订阅频道的媒体内容实时或批量搬运至私密库群组。

#### UI 按钮与功能

- `1. 局部更新 (按分组)`：只同步某一个 Telegram 文件夹中的频道，**增量**执行。
- `2. 局部全时间轴 (按分组)`：只同步某一个文件夹，但对该文件夹内频道执行**全量重建式同步**。
- `3. 全局更新同步 (增量)`：对 `CONFIG['source_channels']` 中的全部频道执行增量同步。
- `4. 全局全时间轴同步`：对全局频道执行全量同步，风险最高，可能产生重复转发。
- `5. 高级回滚`：选择某个历史同步号，将数据库、日志和目标群物理转发一并回退。
- `6. 同步状态一览`：展示当前同步状态与历史运行情况。
- `7. 目标群聊管理`：维护 `target_groups` 表中的可选目标库，并切换当前活跃目标群。
- `切换为测试/正式模式`：切换当前对话的运行环境，决定写入测试序列还是正式序列。

#### 执行链路

1. `src/search_bot.py::render_main_sync_menu` 渲染同步菜单。
2. 用户选择模式后，`src/search_bot.py::execute_sync` 负责：
    - 锁住全局同步任务（`sync_job_lock`）防止并发；
    - 临时释放 `user_client`，避免子进程占用同一 Session；
    - 先运行 `src/sync_mode/update_docs.py --prepare` 预热元数据；
    - 再启动 `src/sync_mode/sync.py --mode ...` 作为真正的同步引擎；
    - 完成后再次执行 `src/sync_mode/update_docs.py` 刷新本地文档。

#### 增量同步原理（模式 1 / 3）

- 依赖数据库表：`sync_offsets`。
- `sync_offsets.last_msg_id` 记录每个源频道上次已同步到的消息 ID。
- `src/sync_mode/sync.py` 中调用 `db.get_last_offset(chat_id, is_test=IS_TEST)` 读取断点。
- 实际拉取时使用：
   - `client.iter_messages(entity, min_id=last_id, reverse=True)`
- 这意味着同步器只拿 **上次断点之后** 的消息，实现增量追更。
- 完成后调用 `db.update_offset(chat_id, max_msg_id, is_test=..., run_id=run_id)` 推进断点。

#### 全时间轴同步原理（模式 2 / 4）

- 若用户选择全量模式，`sync.py` 会先调用 `db.reset_channel_sync(chat_id, IS_TEST)` 重置该频道的同步基准线。
- 这样后续同步不会使用旧的 `sync_offsets`，而是从头重新扫描该频道。
- 因为目标库中旧转发消息可能仍存在，所以 `search_bot.py` 会先弹出 **高危操作警告**，提示：
   1. 目标群可能出现重复转发；
   2. 旧的本地日志编号体系会失效。

#### 相册 / 资源组机制

- 同步时会把连续共享同一 `grouped_id` 的媒体视为同一组。
- `flush_media_group(...)` 负责一次性处理整组媒体，并在目标群先发送一条 Header：
   - `📌 来源: **{source_name}** | 🔢 同步号: {run_label}`
   - `📦 第 N 组消息`
- 组内每条媒体都会写入 `messages`，并映射到原始消息 ID 与目标消息 ID。

#### 同步模式依赖的数据库表

- `sync_runs`：记录一次同步任务的元信息、编号、起止时间、统计值。
- `messages`：记录原始消息与目标群消息之间的映射关系，是回滚与模式 4 的核心桥梁。
- `sync_offsets`：记录每个频道的增量同步断点。
- `target_groups`：记录可选目标群和当前激活目标。
- `global_messages`：保存可检索、可打标的全局资源索引。
- `resource_counters`：资源编号计数器，保证每个频道的资源号连续。

#### 关键代码文件

- `src/search_bot.py`
- `src/sync_mode/sync.py`
- `src/sync_mode/update_docs.py`
- `src/db.py`

### 2. 备份模式 (`/backup`)

**目标**：拉取源频道所有历史记录至本地，建立全量 JSON/MD 离线仓库。

#### 备份的核心目标

- 产出 `data/archived/backups/**` 下的结构化 JSON 快照；
- 同步生成 `docs/archived/backups/**` 下的 Markdown 视图；
- 把每个频道的历史拉成“完整时间线快照”，供后续 P0/P1/P2/P3 使用。

#### 断点续传原理

- 依赖数据库表：`backup_offsets`。
- `backup_offsets.last_msg_id` 记录该频道上次备份成功推进到的最大消息号。
- 读取断点时，`src/backup_mode/backup.py::get_last_recorded_id(...)` 的优先级为：
   1. 先查 `db.get_backup_offset(chat_id, is_test=is_test)`；
   2. 若数据库没有，再去 `data/archived/backups/` 遍历旧历史文件做兼容扫描。
- 真正抓取消息时使用：
   - `client.iter_messages(entity, min_id=fetch_min_id, reverse=True)`
- 因此下次备份只取断点之后的新消息。

#### 断点“覆盖”与推进机制

- 备份过程中会维护 `max_seen_msg_id`，即使某些消息被分类成 `skip`、并未真正保存，也会记录扫描到的最大消息号。
- 这样当“本轮全是无效消息”时，仍可调用：
   - `db.update_backup_offset(chat_id, max_seen_msg_id, is_test=is_test)`
- 目的：避免下次再次重复扫描同一批已知无效消息。

#### `_PARTIAL` 中断机制

- 如果备份过程中收到停止信号，当前任务会进入 `is_partial = True`。
- 输出文件名会附加 `_PARTIAL` 标识。
- 后续扫描历史文件时，系统会**严格排除 `_PARTIAL` 文件**，防止把半成品当成正式快照。
- 同时，因为中断任务不会推进数据库断点，所以可在下次运行时安全重试。

#### “不是只存补丁，而是始终输出完整快照” 的原理

- 备份脚本会先载入历史快照：`historical_records`
- 再把本轮新拉取的 `records` 与历史合并：`merge_backup_records(records, historical_records)`
- 最终写出的新 `Bn` 文件是“完整时间线快照”，不是只包含这次新增的 patch。
- 这就是为什么旧记录会被“覆盖到新快照中”，而不是一直拆成散落的小补丁文件。

#### 进度预估机制

- 通过 `get_total_message_count(client, entity, min_id=...)` 判断待抓取量；
- 如果能精确测得增量，就做精确分母；
- 如果 Telegram 只返回头部范围，则根据 ID 差值做估算，文案里会使用 `~`。

#### 备份模式依赖的数据库表

- `backup_runs`：记录每次备份任务的总量、增量、新消息数、运行模式。
- `backup_offsets`：记录每个频道的备份断点。
- `channel_names`：记录频道改名前后的名字映射，避免目录改名造成历史断层。
- `resource_counters`：在备份阶段也会为首见消息预领取资源号，保证后续引用稳定。

#### 关键代码文件

- `src/backup_mode/backup.py`
- `src/db.py`
- `src/utils/config.py`

### 3. 海量发现流水线 (`/search` + Pipeline)

**目标**：利用 NLP 技术自动从海量备份中挖掘新实体，并提供 Web 交互界面供人工分拣。

#### 流水线生命周期：

1. **P0 (整合 / 导入)**
    - 作用：把备份中的频道 JSON 导入或投影到统一可处理的全局范围。
    - 关键脚本：`src/search_mode/program1_discovery/import_backups.py`
    - 场景：当备份目录新增了从未进入 DB 的频道时，P0 负责把它们纳入体系。

2. **P1 (发现 / 候选池生成)**
    - 关键脚本：`src/search_mode/program1_discovery/entity_extractor.py`
    - 作用：扫描 `data/archived/backups/` 中受管辖文件夹的历史文本，分词、计数、去重，并生成：
       - `candidate_pool_part_*.md`
       - `candidate_pool_part_*.json`
       - `candidate_samples.json`
       - `candidate_metadata.json`
    - 自动化去重依据：当前 Bot 对应的 `entities.json`
    - 新鲜度依据：`candidate_metadata.json` 中的 `latest_backup_id` 与 `max_mtime`

3. **P1.5 (Web Sorter 分拣台)**
    - 关键文件：
       - `tools/sorter/server.py`
       - `tools/sorter/index.html`
    - 功能：
       - 浏览候选池；
       - 查看候选词原文样本来源；
       - 打标签为 `Creator / Actor / Tag / Noise`；
       - 暂存与回滚。

4. **P2 (全量打标)**
    - 关键脚本：`src/search_mode/program2_tagging/global_tagger.py`
    - 作用：把 `entities.json` 翻译并写入 `global_messages` 中的结构化字段：
       - `creator`
       - `actor`
       - `keywords`

5. **P3 (导出展示)**
    - 关键脚本：`src/search_mode/program3_export/index_exporter.py`
    - 作用：把打标后的 `global_messages` 导出成：
       - `docs/tags/**.md`
       - `data/tags/**.json`

#### “工作模式 3” 依赖哪些数据库表？

模式 3 的不同阶段依赖不同数据层：

- **P0 / 导入层**：主要把备份材料导入全局域，依赖 `global_messages`。
- **P1 / 候选发现层**：主要依赖本地备份文件本身，不直接以 `copilot.db` 搜索为核心。
- **P2 / 打标层**：直接更新 `global_messages.creator / actor / keywords`。
- **P3 / 导出层**：从 `global_messages` 读取打标结果，并按频道聚合导出。
- **Sorter / 候选浏览**：依赖 `docs/entities/.../candidates/` 与 `data/entities/.../candidates/` 下的候选池文件。

#### 模式 3 的按钮 / 功能键

- `[🔄 更新词库]`：智能入口。若备份没变化则直接恢复现有 Sorter；若备份变了则重跑 P1。
- `分拣池`：浏览候选词主表。
- `类目管理`：浏览和维护当前实体树（Creators / Actors / Keywords / Noise）。
- `Tag Back`：把已分类项退回候选池。
- `打标并生成 tags (P2 → P3)`：正式提交后自动运行 P2 和 P3。

#### 关键代码文件

- `src/search_mode/program1_discovery/import_backups.py`
- `src/search_mode/program1_discovery/entity_extractor.py`
- `src/search_mode/program1_discovery/sync_entities.py`
- `src/search_mode/program2_tagging/global_tagger.py`
- `src/search_mode/program3_export/index_exporter.py`
- `tools/sorter/server.py`
- `tools/sorter/index.html`

### 4. 人工补全模式 (转发触发)

**目标**：对既有资源进行精细化、多维度的信息修正。

#### 入口按钮

- 主菜单中的 `📥 4. 手动补充信息`

#### 执行流程

1. 用户点击 `nav_mode_4_start`
2. `search_bot.py` 把当前会话状态改为：`awaiting_mode_4_forward`
3. 用户从目标私密群转发一条消息给 Bot
4. Bot 从该转发消息里提取：
    - `forwarded_chat_id`
    - `forwarded_msg_id`
5. 然后用 `messages` 表查询：
    - `SELECT original_chat_id, original_msg_id FROM messages WHERE forwarded_chat_id = ? AND forwarded_msg_id = ?`
6. 找到原始消息后，再从 `global_messages` 读取当前元数据字段，展示编辑面板。

#### 为什么模式 4 一定依赖数据库？

因为模式 4 的本质不是“编辑某条 Telegram 现有消息文本”，而是：

- 先通过 `messages` 表，把 **目标群中的转发消息** 反查回 **原始消息**；
- 再通过 `global_messages` 表，对原始消息的结构化元数据做更新。

也就是说：

- `messages` = 映射桥
- `global_messages` = 真正被修改的索引表

#### 可编辑字段

- `creator`
- `actor`
- `keywords`
- `supplement`

#### 更新原理

- 最终调用 `db.update_msg_entries(chat_id, msg_id, ...)`
- 直接更新 `global_messages` 表对应行
- 更新后会影响：
   - 搜索结果
   - tags 导出
   - 后续文档展示

#### 关键代码文件

- `src/search_bot.py`
- `src/db.py`

---

## ⌨️ 辅助指令集与功能键

### `/refresh`

- **用途**：刷新元数据归档、频道分组与封禁状态、关注列表文档。
- **典型场景**：
   - 在 Telegram 客户端中新增、删除、移动了频道文件夹；
   - 手动改动了本地归档目录；
   - 需要让 Bot 重新校准本地订阅视图。
- **代码入口**：`src/search_bot.py::trigger_metadata_refresh(...)`
- **主要机制**：调用 `src/sync_mode/update_docs.py`，重新扫描对话、文件夹、封禁状态，并更新本地文档与相关视图。

### `/stop`

- **用途**：紧急终止当前正在运行的任务（如备份或同步）。
- **代码入口**：`src/search_bot.py::stop_sync_job(...)`
- **机制**：
   - 向 `data/temp/stop_sync.flag` 或按 Bot 名区分的 `stop_sync_{bot}.flag` 写入停止标记；
   - 正在运行的 `sync.py` / `backup.py` 会周期性检查该标记；
   - 收到信号后在下一个安全边界停止，而不是粗暴中断进程。

### `/start`

- **用途**：显示主菜单。
- **代码入口**：`src/search_bot.py::render_main_menu(...)`
- **对应按钮**：
   - `同步管理`
   - `备份管理`
   - `搜索中心`
   - `手动补充信息`
   - `刷新元数据归档`

### `/close`

- **用途**：安全关闭当前 Bot 实例。
- **权限**：仅管理员可执行。
- **代码入口**：`src/search_bot.py::close_bot_command(...)`
- **机制**：
   - 先通知管理员实例即将下线；
   - 断开 `bot` 与 `user_client`；
   - 清理 sorter 子进程；
   - 最后退出进程。

### `/unlock`

- **用途**：强制释放同步锁。
- **典型场景**：上一次任务异常退出后，`sync_job_lock` 仍然被占用。

### `/ping`

- **用途**：确认 Bot 是否存活。

### `/target_groups`

- **用途**：管理目标群聊列表。
- **依赖表**：`target_groups`

### `[🔄 更新词库]` / Sorter 内流程按钮

- **逻辑**：这是流水线的“一键式”入口。它会自动判断当前环境，决定是启动扫描进度条（P1）还是直接推送已经生成的 Sorter 链接。

### 其它常见按钮与其代码入口

| 按钮 | 作用 | 代码入口 |
| :--- | :--- | :--- |
| `🗑️ 关闭菜单` | 删除当前 Inline 菜单消息 | `search_bot.py::delete_menu_callback` |
| `🛑 停止同步` | 给同步子进程发送停止信号 | `search_bot.py::stop_sync_callback` |
| `高级回滚` | 选择回滚目标版本 | `search_bot.py::show_rollback_list_callback` |
| `目标群聊管理` | 切换/添加/删除目标群 | `search_bot.py::render_target_groups_ui` |

---

## 🗃️ 数据库文件详解 (`data/copilot.db`)

`data/copilot.db` 是整个系统的中心状态库。它不是单纯的搜索索引，而是同时承载：

- 同步运行记录
- 备份运行记录
- 增量断点
- 原始消息与目标消息映射
- 全局可检索资源索引
- 频道名称映射
- 目标群配置

### 核心表一览

| 表名 | 作用 |
| :--- | :--- |
| `sync_runs` | 一次同步任务一行，记录同步号、时间、统计值、是否测试、目标群等 |
| `messages` | 同步映射表，记录原始消息与目标群消息的对应关系 |
| `sync_offsets` | 增量同步断点表 |
| `backup_runs` | 一次备份任务一行，记录是全量还是增量、覆盖频道数、新消息数 |
| `backup_offsets` | 备份断点表，记录上次备份到哪个 msg_id |
| `global_messages` | 全局索引主表，搜索、打标、模式 4 编辑都依赖它 |
| `target_groups` | 目标群注册与当前激活状态 |
| `channel_names` | 频道改名映射，保证历史目录与新名字不断裂 |
| `resource_counters` | 每频道资源编号计数器 |
| `entities` | 辅助实体审核/候选表 |

### 最重要的三张表

#### `messages`

是“同步流水映射表”。主要字段包括：

- `sync_run_id`
- `original_chat_id`
- `original_msg_id`
- `forwarded_chat_id`
- `forwarded_msg_id`
- `header_msg_id`

用于：

- 高级回滚
- 模式 4 转发反查
- 搜索结果跳转到已同步消息

#### `global_messages`

是“全局索引表”。主要字段包括：

- `chat_id`
- `msg_id`
- `text_content`
- `file_name`
- `creator`
- `actor`
- `keywords`
- `supplement`
- `search_tags`
- 各类 `res_*` 资源号字段

用于：

- 模式 3 的 P2/P3
- 搜索中心
- 模式 4 元数据编辑
- tags 文档导出

#### `sync_runs`

记录每次同步任务的运行边界与统计值。主要字段包括：

- `run_id`
- `is_test`
- `formal_number`
- `start_time`
- `end_time`
- `start_msg_id`
- `end_msg_id`
- `bot_name`
- `target_group_id`

用于：

- 生成同步号（如 `TEST-5` / `#17`）
- 生成同步报告
- 高级回滚时定位目标批次

---

## 🛠️ 后台核心组件表

| 脚本 / 文件           | 详细职责                                                                                |
| :-------------------- | :-------------------------------------------------------------------------------------- |
| `src/utils/config.py` | 负责多 Bot 路径映射，决定当前实例读取哪个 entities、candidates、tags、session 与 target 配置。 |
| `src/search_bot.py`   | 交互主控器。负责命令注册、菜单渲染、同步/备份/搜索/模式4 按钮调度、刷新与关闭逻辑。 |
| `src/db.py`           | 数据库核心层。负责建表、断点管理、搜索 SQL、运行记录、回滚、模式4 更新。 |
| `src/backup_mode/backup.py` | 历史备份引擎。负责断点续传、全量快照归并、进度估算、`_PARTIAL` 标记与 run 统计。 |
| `src/sync_mode/sync.py` | 同步引擎。负责目标群发送、RunLabel 生成、资源组 Header、增量推进、回滚擦除。 |
| `src/sync_mode/update_docs.py` | 本地文档与订阅元数据刷新器。供 `/refresh`、同步前预热、同步后归档使用。 |
| `src/search_mode/program1_discovery/import_backups.py` | P0 导入/整合入口，将备份纳入全局处理范围。 |
| `src/search_mode/program1_discovery/entity_extractor.py` | P1 候选发现引擎，生成候选池与 candidate samples。 |
| `src/search_mode/program1_discovery/sync_entities.py` | 将 MD 词库预览与 JSON 词库保持同步。 |
| `src/search_mode/program2_tagging/global_tagger.py`    | P2 打标器，把词库映射写回 `global_messages` 结构化字段。 |
| `src/search_mode/program3_export/index_exporter.py`   | P3 导出器，生成 `docs/tags/` 与 `data/tags/` 的频道聚合视图。 |
| `tools/sorter/server.py` | P1.5 Flask 后端。提供 candidates、samples、tags、entities API。 |
| `tools/sorter/index.html` | P1.5 Web 前端。提供候选池浏览、来源样本分页、类目管理与 tags 浏览。 |

---

## 🔐 安全与权限

- **管理员绑定**：系统仅响应 `.env` 中 `ADMIN_USER_ID` 配置的账号（支持逗号分隔的多个 ID）。
- **防止并发**：核心工作模式互斥，防止同一个 Bot 同时运行多个大规模 IO 任务（如备份与打标同时进行）。
