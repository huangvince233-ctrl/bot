# 🤖 Agent 交互逻辑与核心守则

> [!IMPORTANT]
>
> ## 🛡️ 开发者与 Agent 核心守则 (Developer & Agent Mandatory Rules)
>
> 为确保开发的稳定性和逻辑生效的即时性，在修改任何核心代码（尤其是 `src/` 下的主程序）前，必须遵循以下准则：

### 1. 修改前必关机 (Shutdown Before Edit)

严禁在 Bot 运行状态下直接覆盖关键代码。Agent 修改代码前 **必须先停止** Bot 进程。

### 2. 关机规范与状态闭环

- **标准流程**：
  1. 运行 `python src/utils/send_offline.py` 发送下线通知。
  2. 执行 `taskkill` 或手动终止进程。
- **目的**：确保管理员在 Telegram 中收到通知，防止产生未定义的中间状态。

### 3. 变更后重启与核对

- 所有核心代码变更后，Agent 有义务重启 Bot。
- 协助核对新的 `RunID`。

---

## 🏗️ 协作工作流 (Collaboration Workflow)

### 诊断与调研

- 首先使用 `list_dir` 和 `grep_search` 了解代码上下文。
- 优先查阅 `docs/` 下的架构说明和官方机制文档。

### 方案评审

- 对于复杂逻辑变更，必须先在 `implementation_plan.md` 中提交方案。
- 只有在用户明确确认（或设置 `ShouldAutoProceed: true`）后方可实施。

### 验收与记录

- 任务完成后，更新 `walkthrough.md` 记录变更点。
- 定期维护 `README.md` 中的“自研机制”部分。
- **工作流同步更新**：修改 `.agent/workflows/sync.md` 后，必须顺手将其同步拷贝到 `docs/workflow.md`，确保用户与 Agent 看到的指引一致。
