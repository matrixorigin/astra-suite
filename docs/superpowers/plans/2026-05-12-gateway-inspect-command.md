# Gateway Inspect 命令实施计划

**目标：** 新增 `astra-gateway inspect` 子命令，让开发/运维人员通过二进制直接查看 gateway 当前状态数据（定时任务、skills、工作区、活跃任务等），无需连接数据库或查看日志。

**背景：** MCP 开发人员无法直接看到模型收到的动态数据（这些数据现在不再预填到 prompt 里，而是模型按需查询）。需要一个便捷的方式确认数据库里有什么。

**工期：** 0.5 天

---

## 功能设计

### 命令格式

```bash
# 查看全部数据
astra-gateway inspect

# 按 platform/chat_id 过滤
astra-gateway inspect --platform wecom --chat-id <id>

# 只看某个类别
astra-gateway inspect --only cron
astra-gateway inspect --only skills
astra-gateway inspect --only tasks
astra-gateway inspect --only prompt
```

### 输出内容

```
═══════════════════════════════════════════════════════
  astra-gateway inspect
═══════════════════════════════════════════════════════

── Cron Jobs ───────────────────────────────────────
  ✅ [wecom:chat_id] 64d74ad0 | 18 11 * * * | 每日GitHub活动总结
  ✅ [wecom:chat_id] 98aab59f | 55 10 * * * | 上周GitHub活动总结

── Skills ──────────────────────────────────────────
  [mcp:default] deploy-flow — Standard deploy
  [mcp:default] morning-report — 每日晨报流程

── Active Durable Tasks ────────────────────────────
  🔄 [wecom:chat_id] abc12345 | 数据迁移 | running | 45%

── Workspaces / Projects ───────────────────────────
  ~/github/astra-suite (main, 3 days ago)
  ~/work/frontend (develop, 1 hour ago)

── MCP Tools (17) ──────────────────────────────────
  gateway_cron_list             — List scheduled tasks
  gateway_cron_add              — Create recurring task
  ...

── System Prompt (Claude CLI) ──────────────────────
  ## Gateway
  Astra Gateway on **wecom**. User: ...
  (353 bytes)
```

### 实现要点

1. 读取 gateway.yaml 获取 storage 配置
2. 连接 DB（SQLite/MySQL），调用 GatewayStore trait 方法查询数据
3. 对于 cron/skills 的 "list all" 需求：当前 trait 的 `list_cron_jobs` 和 `list_skills` 都需要 platform + chat_id 参数，可能需要：
   - 方案 A：新增 `list_all_cron_jobs()` / `list_all_skills()` trait 方法
   - 方案 B：直接 SQL 查询绕过 trait（inspect 命令内部直接用 sqlx）
   - 方案 C：遍历已知的 platform/chat_id 组合

4. Durable tasks 通过 `DurableTaskStore::list(TaskFilter::default())` 可以获取全部
5. Projects 通过 `workspace::discover_all_projects()` 获取

---

## 文件变更

| 文件 | 变更 |
|------|------|
| `src/main.rs` | 新增 `Inspect` 子命令定义 + handler |
| `src/mcp/mod.rs` | 新增 `pub mod inspect;` |
| `src/mcp/inspect.rs` | inspect 逻辑实现（查询 + 格式化输出） |
| `src/store/mod.rs` | （可选）新增 `list_all_cron_jobs` / `list_all_skills` trait 方法 |
| `src/store/sqlite.rs` | （可选）实现上述方法 |
| `src/store/mysql.rs` | （可选）实现上述方法 |

---

## 待决策

1. **是否需要 `--only` 过滤？** 还是一次全部输出就行？
2. **"list all" 问题：** 不带 chat_id 过滤时怎么查全量数据？选方案 A/B/C？
3. **是否输出 JSON 格式？** 加 `--json` 标志方便脚本消费？
