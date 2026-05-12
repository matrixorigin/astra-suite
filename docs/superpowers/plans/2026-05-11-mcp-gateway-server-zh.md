# MCP Gateway Server 实施计划

> **给执行者：** 必须使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务执行本计划。步骤使用 checkbox (`- [x]`) 语法跟踪进度。

**目标：** 用 MCP stdio server 替代 `[[GATEWAY:...]]` 正则提取机制，使 CLI 与 gateway 的交互走结构化的 tool call。

**架构：** Gateway binary 新增 `mcp-serve` 子命令运行 MCP stdio 服务器。spawn Claude CLI 时生成临时 mcp-config JSON 指向 `astra-gateway mcp-serve`，通过 `--mcp-config` 传入。MCP handler 直接调用现有业务逻辑（cron、skills、tasks、workspace）。System prompt 精简到 ~300 bytes，不再预填动态数据或 action 格式文档。

**技术栈：** Rust、rmcp crate（MCP SDK）、tokio、serde_json、sqlx（现有）、clap（现有）

---

## 文件结构

| 文件 | 职责 |
|------|------|
| `crates/astra-gateway/src/mcp/mod.rs` | MCP 模块入口 |
| `crates/astra-gateway/src/mcp/server.rs` | MCP stdio 服务器入口 + 工具路由（`#[tool_router]` 宏） |
| `crates/astra-gateway/src/mcp/tools_cron.rs` | 定时任务工具逻辑（add/list/delete/remind） |
| `crates/astra-gateway/src/mcp/tools_skills.rs` | Skills 工具逻辑（list/read/add/delete） |
| `crates/astra-gateway/src/mcp/tools_tasks.rs` | 持久任务工具逻辑（list/status/create/complete/fail/cancel） |
| `crates/astra-gateway/src/mcp/tools_workspace.rs` | 工作区工具逻辑（list/switch/current） |
| `crates/astra-gateway/src/mcp/config.rs` | 为 CLI spawn 生成 mcp-config JSON |
| `crates/astra-gateway/src/main.rs` | 新增 `mcp-serve` 子命令 |
| `crates/astra-gateway/src/cli_pool.rs` | spawn 时传入 `--mcp-config` |
| `crates/astra-gateway/src/gateway_context.rs` | 新增 `to_slim_system_prompt()` |
| `crates/astra-gateway/src/runner.rs` | 生成 mcp config，传给 CLI spawn |
| `crates/astra-gateway/src/store/mod.rs` | GatewayStore trait 新增 skill 方法 |
| `crates/astra-gateway/src/store/sqlite.rs` | SQLite 实现 gw_skills CRUD |
| `crates/astra-gateway/src/store/mysql.rs` | MySQL 实现 gw_skills CRUD |
| `crates/astra-gateway/src/store/file.rs` | File 后端实现 skill CRUD |

---

### Task 1：添加 rmcp 依赖 + 创建 MCP 模块骨架

**文件：**
- 修改：`Cargo.toml`（workspace 依赖）
- 修改：`crates/astra-gateway/Cargo.toml`
- 新建：`crates/astra-gateway/src/mcp/mod.rs`
- 新建：`crates/astra-gateway/src/mcp/server.rs`
- 修改：`crates/astra-gateway/src/lib.rs`

- [x] **步骤 1：添加 rmcp 到 workspace 依赖**

根 `Cargo.toml` 中添加：
```toml
rmcp = { version = "1.6", features = ["server", "transport-io"] }
```

`crates/astra-gateway/Cargo.toml` 中添加：
```toml
rmcp = { workspace = true }
```

- [x] **步骤 2：创建 MCP 模块骨架**

`src/mcp/mod.rs`：
```rust
pub mod server;
pub mod config;
mod tools_cron;
mod tools_skills;
mod tools_tasks;
mod tools_workspace;
```

`src/mcp/server.rs`：通过 `#[tool_router(server_handler)]` 宏定义 `GatewayMcpServer` 结构体及所有 tool 方法。

- [x] **步骤 3：在 lib.rs 注册模块**

添加 `pub mod mcp;`

- [x] **步骤 4：验证编译**

运行：`cargo check -p astra-gateway`

---

### Task 2：添加 `mcp-serve` 子命令

**文件：**
- 修改：`crates/astra-gateway/src/main.rs`

- [x] **步骤 1：在 Command 枚举中添加 McpServe 变体**

```rust
#[command(name = "mcp-serve")]
McpServe {
    #[arg(long, env = "GATEWAY_DATABASE_URL")]
    database_url: Option<String>,
    #[arg(long, env = "GW_MCP_PLATFORM")]
    platform: Option<String>,
    #[arg(long, env = "GW_MCP_CHAT_ID")]
    chat_id: Option<String>,
    #[arg(long, env = "GW_MCP_USER_ID")]
    user_id: Option<String>,
    #[arg(long, env = "GW_MCP_PROJECT_DIRS")]
    project_dirs: Option<String>,
}
```

- [x] **步骤 2：在 main() 中处理子命令**

调用 `astra_gateway::mcp::server::run_stdio_server(...)`

- [x] **步骤 3：验证编译和 --help**

运行：`cargo build -p astra-gateway && ./target/debug/astra-gateway mcp-serve --help`

---

### Task 3：在存储层实现 gw_skills 表

**文件：**
- 修改：`crates/astra-gateway/src/store/mod.rs`
- 修改：`crates/astra-gateway/src/store/sqlite.rs`
- 修改：`crates/astra-gateway/src/store/mysql.rs`
- 修改：`crates/astra-gateway/src/store/file.rs`

- [x] **步骤 1：添加 SkillRecord 类型和 trait 方法**

```rust
pub struct SkillRecord {
    pub name: String,
    pub content: String,
    pub description: String,
    pub created_at: String,
}
```

GatewayStore trait 新增：
```rust
async fn list_skills(&self, platform: &str, chat_id: &str) -> Result<Vec<SkillRecord>, StoreError>;
async fn get_skill(&self, platform: &str, chat_id: &str, name: &str) -> Result<Option<SkillRecord>, StoreError>;
async fn upsert_skill(&self, platform: &str, chat_id: &str, name: &str, content: &str, description: &str) -> Result<(), StoreError>;
async fn delete_skill(&self, platform: &str, chat_id: &str, name: &str) -> Result<bool, StoreError>;
```

- [x] **步骤 2：SQLite 实现**

`ensure_schema()` 中新增建表：
```sql
CREATE TABLE IF NOT EXISTS gw_skills (
    platform TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    name TEXT NOT NULL,
    content TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
    PRIMARY KEY (platform, chat_id, name)
);
```

使用 `INSERT OR REPLACE` 实现 upsert。

- [x] **步骤 3：MySQL 实现**

相同 schema，使用 `INSERT ... ON DUPLICATE KEY UPDATE` 实现 upsert。

- [x] **步骤 4：File 后端实现**

存储为 `skills_{platform}_{chat_id}.json`，JSON 数组格式。

- [x] **步骤 5：验证编译**

---

### Task 4：实现 MCP 工具 — Skills

**文件：**
- 新建：`crates/astra-gateway/src/mcp/tools_skills.rs`
- 修改：`crates/astra-gateway/src/mcp/server.rs`

- [x] **步骤 1：实现 skills 工具**

工具：
- `gateway_skills_list` — 返回 `[{name, description}]`
- `gateway_skills_read` — 参数：`{name}`，返回 skill 全文
- `gateway_skills_add` — 参数：`{name, content, description}`，upsert
- `gateway_skills_delete` — 参数：`{name}`，删除

- [x] **步骤 2：注册到 server.rs 的 tool_router**

- [x] **步骤 3：验证编译**

---

### Task 5：实现 MCP 工具 — Cron

**文件：**
- 新建：`crates/astra-gateway/src/mcp/tools_cron.rs`
- 修改：`crates/astra-gateway/src/mcp/server.rs`

- [x] **步骤 1：实现 cron 工具**

工具：
- `gateway_cron_list` — 列出当前对话的定时任务
- `gateway_cron_add` — 参数：`{cron_expr, message}`，创建定时任务
- `gateway_cron_delete` — 参数：`{job_id}`，前缀匹配删除
- `gateway_remind_after` — 参数：`{minutes, message, exec: bool}`，一次性提醒

复用 `execute_gateway_actions_with_policy` 中的验证逻辑（is_valid_cron_expr、时间限制）。

- [x] **步骤 2：注册到 server.rs**

- [x] **步骤 3：验证编译**

---

### Task 6：实现 MCP 工具 — Tasks + Workspace

**文件：**
- 新建：`crates/astra-gateway/src/mcp/tools_tasks.rs`
- 新建：`crates/astra-gateway/src/mcp/tools_workspace.rs`
- 修改：`crates/astra-gateway/src/mcp/server.rs`

- [x] **步骤 1：实现持久任务工具**

工具：
- `gateway_tasks_list` — 列出活跃的持久任务
- `gateway_tasks_create` — 参数：`{name, description?}`
- `gateway_tasks_status` — 参数：`{task_id}`
- `gateway_tasks_complete` — 参数：`{task_id}`
- `gateway_tasks_fail` — 参数：`{task_id, error?}`
- `gateway_tasks_cancel` — 参数：`{task_id}`

- [x] **步骤 2：实现工作区工具**

工具：
- `gateway_workspace_current` — 返回当前工作目录
- `gateway_workspace_list` — 返回可用项目列表
- `gateway_workspace_switch` — 参数：`{path}`

- [x] **步骤 3：注册到 server.rs**

- [x] **步骤 4：验证编译**

---

### Task 7：MCP config 生成 + CLI spawn 集成

**文件：**
- 新建：`crates/astra-gateway/src/mcp/config.rs`
- 修改：`crates/astra-gateway/src/cli_pool.rs`
- 修改：`crates/astra-gateway/src/runner.rs`

- [x] **步骤 1：实现 config.rs — 生成临时 mcp-config JSON**

```rust
pub fn generate_mcp_config(
    database_url: Option<&str>,
    platform: &str,
    chat_id: &str,
    user_id: &str,
    project_dirs: &[String],
) -> Result<PathBuf, std::io::Error>
```

写入 `/tmp/gw-mcp-{hash}.json`，内容：
```json
{
  "mcpServers": {
    "gateway": {
      "command": "/path/to/astra-gateway",
      "args": ["mcp-serve"],
      "env": { "GATEWAY_DATABASE_URL": "...", "GW_MCP_PLATFORM": "...", ... }
    }
  }
}
```

- [x] **步骤 2：修改 cli_pool.rs — 传入 --mcp-config**

`build_persistent_command()` 和 `spawn()` 和 `begin_turn()` 都增加 `mcp_config: Option<&Path>` 参数。

- [x] **步骤 3：修改 runner.rs — 生成并传递 mcp config**

在调用 `begin_turn` 前为 Claude CLI 路径生成 mcp config 文件。

- [x] **步骤 4：验证编译**

---

### Task 8：精简 system prompt

**文件：**
- 修改：`crates/astra-gateway/src/gateway_context.rs`
- 修改：`crates/astra-gateway/src/runner.rs`

- [x] **步骤 1：新增 `to_slim_system_prompt()` 方法**

精简版 prompt（~350 bytes）：
```markdown
## Gateway

Astra Gateway on {{platform}}. User: {{user_display_name}} (`{{user_id}}`), CLI: `{{cli_name}}`

You have gateway MCP tools available for:
- Scheduling tasks and reminders (gateway_cron_*)
- Managing reusable skills (gateway_skills_*)
- Durable task tracking (gateway_tasks_*)
- Workspace management (gateway_workspace_*)

### User Commands (handled by gateway, not you)
/new /status /model /cli /ws /running /kill /cancel /manage /help

### Notes
- Mobile platform — keep responses concise. Respond in user's language.
- You CAN set reminders/schedules via gateway tools.
```

- [x] **步骤 2：runner.rs 中做分支**

```rust
let system_prompt = if CliProcessPool::supports_persistent(&cli_profile) {
    gw_context.to_slim_system_prompt()
} else {
    gw_context.to_system_prompt()  // 旧 CLI 走完整 prompt
};
```

- [x] **步骤 3：验证编译和测试**

注意：`gateway.md` 保持原样，继续服务于非 Claude CLI 路径。

---

### Task 9：非 Claude CLI 向后兼容

**文件：**
- 确认：`crates/astra-gateway/src/runner.rs`

- [x] **步骤 1：保留 `[[GATEWAY:...]]` 正则路径**

`execute_gateway_actions_with_policy` 函数不动——非 MCP CLI 仍需要它。

- [x] **步骤 2：条件 prompt：Claude 走精简，其他走完整**

已在 Task 8 步骤 2 中完成。

- [x] **步骤 3：验证两条路径都能编译**

---

### Task 10：构建和测试

- [x] **步骤 1：全量构建**

运行：`cargo build -p astra-gateway` — 通过

- [x] **步骤 2：运行所有现有测试**

运行：`cargo test --workspace` — 898 测试通过

- [x] **步骤 3：mcp-serve 子命令冒烟测试**

发送 `initialize` JSON-RPC → 收到正确响应

- [x] **步骤 4：测试 tools/list**

返回 17 个 gateway 工具，schema 正确。

- [x] **步骤 5：Claude CLI 端到端验证**

```bash
claude -p '列出所有 skills' --mcp-config /tmp/test-gw-mcp.json --dangerously-skip-permissions
```

模型正确调用 `gateway_skills_list`、`gateway_skills_add`、`gateway_remind_after`、`gateway_cron_list`。

---

## 验收标准

1. ✅ `cargo build -p astra-gateway` 无错误
2. ✅ `cargo test --workspace` — 898 测试通过
3. ✅ `astra-gateway mcp-serve` 响应 MCP JSON-RPC
4. ✅ `tools/list` 返回全部 17 个 gateway 工具
5. ✅ Claude CLI spawn 包含 `--mcp-config`
6. ✅ Claude CLI 的 system prompt < 500 bytes，不含动态数据
7. ✅ 非 Claude CLI 仍获得完整 prompt + `[[GATEWAY:...]]` 文档
8. ✅ `[[GATEWAY:...]]` 正则提取对非 MCP 后端仍然有效
9. ✅ `gw_skills` 表启动时创建，CRUD 正常
