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

- **RunID 追溯**：每次作业生成唯一编号（如 `TEST-42`），用于后续在文件和数据库中回溯。
- **相册聚合 (Albums)**：基于 `grouped_id` 逻辑，确保相册内的多张图片/视频被视为一个整体进行搬运，避免信息割裂。
- **入库同步**：媒体消息实时写入 `global_messages` 表，等待后续 P2 流水线进行深度打标。

### 2. 备份模式 (`/backup`)

**目标**：拉取源频道所有历史记录至本地，建立全量 JSON/MD 离线仓库。

- **断点记录机制**：在 `backup_offsets` 中持久化记录每个频道的 `last_msg_id`。下次备份仅请求 `min_id=last_msg_id` 的增量部分。
- **双重进度预估**：
  - **Scan Mode**：首先进行快速头部探测，精准计算待备份条数。
  - **Estimate Mode**：若增量极大，使用 ID 差值估算分母（标记为 `~`）。
- **中断鲁棒性**：若备份中途退出，产生 `_PARTIAL` 状态，系统下次会自动重试该频道，不会漏掉任何数据。

### 3. 海量发现流水线 (`/search` + Pipeline)

**目标**：利用 NLP 技术自动从海量备份中挖掘新实体，并提供 Web 交互界面供人工分拣。

#### 流水线生命周期：

1. **P0 (整合)**：将跨频道的备份数据合并投影。
2. **P1 (发现)**：
   - **自动化去重**：自动对比对应 Bot 的 `entities.json`，只展示从未见过的新词。
   - **新鲜度校验 (V2.2)**：检查 `backup_id` 与物理文件的 `Max MTime`。只有物理环境未发生任何变动时，系统才会允许跳过 P0/P1 执行直接恢复。
3. **P1.5 (分拣 - Web Sorter)**：
   - **本地服务**：启动 Flask 服务并推送专属链接（支持多 IP/手机访问）。
   - **交互特性**：支持 Creator/Actor/Tag/Noise 的多维分类。
   - **暂存 (Staging)**：允许暂时保存进度而不影响主库。
   - **Tag Back (回滚)**：支持将已分类项退回候选池重来。
4. **P2/P3 (落库与展示)**：正式提交后，系统自动执行全量打标 (`global_tagger`) 并导出预览文档 (`index_exporter`)。

### 4. 人工补全模式 (转发触发)

**目标**：对既有资源进行精细化、多维度的信息修正。

- **触发机制**：在目标群中将任何消息转发给 Bot 即可唤起菜单。
- **身份识别**：支持识别“转发页眉 (Header)”特征，即使在开启了转发保护的频道中，也能通过指纹关联数据。
- **四维编辑 UI**：支持独立编辑 **创作者 (Creator)**、**女m (Actor)**、**关键词 (Keywords)**、**补充信息 (Supplement)**。所有编辑实时反映在搜索索引中。

---

## ⌨️ 辅助指令集与功能键

### `/refresh`

- **用途**：重新扫描本地归档目录。
- **场景**：当您手动移动了 `data/archived/backups/` 下的文件夹，或删除了某些本地快照时，运行此命令可以强制同步数据库中的偏移记录与物理路径现状。

### `/stop`

- **用途**：紧急终止当前正在运行的任务（如备份或同步）。
- **机制**：通过设置内存标志位安全退出循环，并自动生成当前进度的“断点标记”文件。

### `/start`

- **用途**：激活机器人交互菜单的核心入口。

### `[🔄 更新词库]` 按钮

- **逻辑**：这是流水线的“一键式”入口。它会自动判断当前环境，决定是启动扫描进度条（P1）还是直接推送已经生成的 Sorter 链接。

---

## 🛠️ 后台核心组件表

| 脚本 / 文件           | 详细职责                                                                                |
| :-------------------- | :-------------------------------------------------------------------------------------- |
| `src/utils/config.py` | 决定了系统是用 `tgporncopilot_entities.json` 还是 `my_porn_private_bot_entities.json`。 |
| `src/db.py`           | 维护 `messages` (流水表), `global_messages` (索引表) 和 `backup_offsets` (断点表)。     |
| `entity_extractor.py` | 核心分词挖掘引擎。支持 `--bot` 参数实现候选池彻底隔离。                                 |
| `global_tagger.py`    | 翻译器：将 `entities.json` 的字符串映射转化为数据库的结构化标签。                       |
| `index_exporter.py`   | 文档生成器：生成 `docs/tags/` 下的聚合视图，支持基于时间戳的逆序排序。                  |

---

## 🔐 安全与权限

- **管理员绑定**：系统仅响应 `.env` 中 `ADMIN_USER_ID` 配置的账号（支持逗号分隔的多个 ID）。
- **防止并发**：核心工作模式互斥，防止同一个 Bot 同时运行多个大规模 IO 任务（如备份与打标同时进行）。
