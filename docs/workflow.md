---
description: 机器人综合操作手册 (Bot Workflows)
---

# 🤖 综合操作手册 (Bot Workflows)

---

## 🏗️ 精细项目文件结构 (Detailed Project Structure)

```text
telegramporncopilot/
├── _agent/                  # AI 助手目录
│   └── workflows/           # 工作流与准则
│       ├── bot_workflows.md      # 🤖 [本文件] 综合操作手册
│       ├── agent_guidelines.md   # 📚 Agent 核心机制与开发准则
│       └── telegram_mechanics.md # 📱 Telegram 官方 API 底层机制
├── src/                     # 程序源码
│   ├── backup_mode/         # 💾 备份逻辑实现 (backup.py)
│   ├── sync_mode/           # 🔄 同步逻辑实现 (sync.py)
│   ├── search_mode/         # 🔍 检索逻辑实现 (search.py)
│   ├── utils/               # 🛠️ 通用工具
│   ├── db.py                # 🗄️ 数据库操作封装
│   └── search_bot.py        # 🤖 机器人入口与 UI 服务
├── data/                    # 动态数据 (数据库与快照)
├── docs/                    # 文档中心
├── .env                     # 环境变量
├── requirements.txt         # 依赖列表
└── start_bot.bat            # 快捷启动脚本
```

---

## 模式一：🔄 同步模式 (`/sync`)

**目标**：将订阅频道的新媒体资料完整地转发搬运到您的私有群组中。

- **快速启动**：向 Bot 发送 `/sync`。
- 🧪 **环境分流**: 支持「🧪 测试」与「🚀 正式」双环境。
  - **独立编号**: 两套环境拥有独立的运行编号体系。测试环境显示为 `TEST-1`, `TEST-2`...；正式环境显示为 `#1`, `#2`...。
  - **互不干扰**: 测试同步产生的偏移量与历史记录不会影响正式同步，反之亦然。
- ⚡ **速度**: 大约为 10~30 条消息/分钟。

---

## 模式二：💾 备份模式 (`/backup`)

**目标**：拉取源频道历史消息到本地 SQLite 数据库及 Markdown/JSON 快照。

- **快速启动**：向 Bot 发送 `/backup`。
- ⚡ **速度**: 极快，约 3,000 ~ 5,000 条消息/分钟。

---

## 模式三：🔍 检索分析模式 (`/search`)

**目标**：对本地已归档、已备份的数据进行全局模糊检索。

- **快速启动**：向 Bot 发送 `/search`。

---

## 🛡️ Agent 开发与维护守则 (Agent Guidelines)

> [!IMPORTANT]
> **1. 修改前必停机**：严禁在运行状态下修改核心代码。必须先执行 `python src/utils/send_offline.py` 并关闭进程。
> **2. 任务流规范**：
>
> - 复杂变更需提交 `implementation_plan.md`。
> - 任务完成后需生成 `walkthrough.md` 复盘。
>   **3. 语言规范**：所有分派给 AI 助手的 Artifacts 必须**全程使用中文**。
>   **4. 文档闭环**：修改本手册后，需同步更新 `docs/workflow.md`，确保内外文档一致。

---

## 📱 Telegram 官方底层机制 (Mechanics)

了解这些机制可以帮您更好地理解备份报告中的数据逻辑：

- **原始消息 (Raw Messages)**：Telegram 的最小单位。**一个 `msg_id` 只能带 1 个文件**。
- **相册 (Albums/Groups)**：由多个 `msg_id` 组成，通过 `grouped_id` 关联。
- **计数逻辑**：
  - **原始消息条数**：最精准的采集计数，也是**进度条**的分母来源。
  - **已合并消息**：阅读层面上的“帖子”数量，相册会被计为 1 条。
  - **资源总量**：实际下载的媒体文件总数。

---

## 📚 文档维护规则

> [!IMPORTANT]
> **自动维护原则**：在每一轮任务结束前完成修正，确保文档始终与最新代码同步。

> [!IMPORTANT]
> **语言规范**：AI 助手生成的所有 **Artifacts** 必须**全程使用中文**书写。
