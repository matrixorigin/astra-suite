# MCP Gateway Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `[[GATEWAY:...]]` regex-based action mechanism with a proper MCP stdio server embedded in the gateway binary, enabling structured tool-call interactions between CLI and gateway.

**Architecture:** Gateway binary gets a new subcommand `mcp-serve` that runs an MCP stdio server. When spawning Claude CLI, gateway generates a temp mcp-config JSON pointing to `astra-gateway mcp-serve` and passes it via `--mcp-config`. The MCP server handlers directly call existing business logic (cron, skills, tasks, workspace). System prompt is slimmed to ~300 bytes — no more dynamic data pre-filling or action format documentation.

**Tech Stack:** Rust, rmcp crate (MCP SDK), tokio, serde_json, sqlx (existing), clap (existing)

---

## File Structure

| File | Responsibility |
|------|---------------|
| `crates/astra-gateway/src/mcp/mod.rs` | MCP module root, re-exports |
| `crates/astra-gateway/src/mcp/server.rs` | MCP stdio server entry point, tool routing |
| `crates/astra-gateway/src/mcp/tools_cron.rs` | Cron tool handlers (add/list/delete/remind) |
| `crates/astra-gateway/src/mcp/tools_skills.rs` | Skills tool handlers (list/read/add/delete) |
| `crates/astra-gateway/src/mcp/tools_tasks.rs` | Durable task tool handlers (list/status/create/complete/fail/cancel) |
| `crates/astra-gateway/src/mcp/tools_workspace.rs` | Workspace tool handlers (list/switch/current) |
| `crates/astra-gateway/src/mcp/config.rs` | MCP config JSON generation for CLI spawn |
| `crates/astra-gateway/skills/gateway.md` | Slimmed system prompt template |
| `crates/astra-gateway/src/main.rs` | Add `mcp-serve` subcommand |
| `crates/astra-gateway/src/cli_pool.rs` | Add `--mcp-config` to spawn |
| `crates/astra-gateway/src/cli_bridge.rs` | Add `--mcp-config` to spawn |
| `crates/astra-gateway/src/gateway_context.rs` | Remove dynamic data injection |
| `crates/astra-gateway/src/runner.rs` | Generate mcp config, pass to CLI spawn |
| `crates/astra-gateway/src/store/mod.rs` | Add `gw_skills` table methods to trait |
| `crates/astra-gateway/src/store/sqlite.rs` | Implement `gw_skills` CRUD for SQLite |
| `crates/astra-gateway/src/store/mysql.rs` | Implement `gw_skills` CRUD for MySQL |

---

### Task 1: Add rmcp dependency and create MCP module skeleton

**Files:**
- Modify: `Cargo.toml` (workspace deps)
- Modify: `crates/astra-gateway/Cargo.toml`
- Create: `crates/astra-gateway/src/mcp/mod.rs`
- Create: `crates/astra-gateway/src/mcp/server.rs`
- Modify: `crates/astra-gateway/src/lib.rs`

- [ ] **Step 1: Add rmcp to workspace dependencies**

In root `Cargo.toml`, add:
```toml
rmcp = { version = "0.1", features = ["server", "transport-io"] }
```

In `crates/astra-gateway/Cargo.toml`, add:
```toml
rmcp = { workspace = true }
```

- [ ] **Step 2: Create MCP module skeleton**

Create `src/mcp/mod.rs`:
```rust
pub mod server;
pub mod config;
pub mod tools_cron;
pub mod tools_skills;
pub mod tools_tasks;
pub mod tools_workspace;
```

Create `src/mcp/server.rs` with a minimal MCP server that responds to `tools/list` with an empty list:
```rust
use rmcp::{Server, ServerHandler, model::*};

pub struct GatewayMcpServer {
    // Will hold DB connection info passed via env/args
}

impl ServerHandler for GatewayMcpServer {
    async fn list_tools(&self) -> Vec<Tool> { vec![] }
    async fn call_tool(&self, name: &str, args: serde_json::Value) -> ToolResult { ... }
}

pub async fn run_stdio_server() -> Result<(), Box<dyn std::error::Error>> {
    // Read config from env, connect to DB, run server on stdin/stdout
}
```

- [ ] **Step 3: Register module in lib.rs**

Add `pub mod mcp;` to `src/lib.rs`.

- [ ] **Step 4: Verify it compiles**

Run: `cargo check -p astra-gateway`

---

### Task 2: Add `mcp-serve` subcommand to binary

**Files:**
- Modify: `crates/astra-gateway/src/main.rs`

- [ ] **Step 1: Add McpServe variant to Command enum**

```rust
#[derive(Subcommand)]
enum Command {
    // ... existing variants ...
    /// Run as MCP stdio server (spawned by Claude CLI)
    #[command(name = "mcp-serve")]
    McpServe {
        /// Database URL for storage access
        #[arg(long, env = "GATEWAY_DATABASE_URL")]
        database_url: Option<String>,
        /// Platform identifier for scoping queries
        #[arg(long, env = "GW_MCP_PLATFORM")]
        platform: Option<String>,
        /// Chat ID for scoping queries
        #[arg(long, env = "GW_MCP_CHAT_ID")]
        chat_id: Option<String>,
    },
}
```

- [ ] **Step 2: Handle the subcommand in main()**

```rust
if let Some(Command::McpServe { database_url, platform, chat_id }) = cli.command {
    astra_gateway::mcp::server::run_stdio_server(database_url, platform, chat_id).await;
    return;
}
```

- [ ] **Step 3: Verify it compiles and runs with --help**

Run: `cargo build -p astra-gateway && ./target/debug/astra-gateway mcp-serve --help`

---

### Task 3: Implement gw_skills table in storage layer

**Files:**
- Modify: `crates/astra-gateway/src/store/mod.rs`
- Modify: `crates/astra-gateway/src/store/sqlite.rs`
- Modify: `crates/astra-gateway/src/store/mysql.rs`

- [ ] **Step 1: Add SkillRecord type and trait methods**

In `store/mod.rs`, add:
```rust
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SkillRecord {
    pub name: String,
    pub content: String,
    pub description: String,
    pub created_at: String,
}
```

Add to `GatewayStore` trait:
```rust
async fn list_skills(&self, platform: &str, chat_id: &str) -> Result<Vec<SkillRecord>, StoreError>;
async fn get_skill(&self, platform: &str, chat_id: &str, name: &str) -> Result<Option<SkillRecord>, StoreError>;
async fn upsert_skill(&self, platform: &str, chat_id: &str, name: &str, content: &str, description: &str) -> Result<(), StoreError>;
async fn delete_skill(&self, platform: &str, chat_id: &str, name: &str) -> Result<bool, StoreError>;
```

- [ ] **Step 2: Implement in SQLite backend**

Add table creation to `ensure_schema()`:
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

Implement all 4 methods.

- [ ] **Step 3: Implement in MySQL backend**

Same schema and logic, adapted for MySQL syntax.

- [ ] **Step 4: Verify it compiles**

Run: `cargo check -p astra-gateway`

---

### Task 4: Implement MCP tool handlers — Skills

**Files:**
- Create: `crates/astra-gateway/src/mcp/tools_skills.rs`
- Modify: `crates/astra-gateway/src/mcp/server.rs`

- [ ] **Step 1: Implement skills tools**

Tools:
- `gateway_skills_list` — returns `[{name, description}]`
- `gateway_skills_read` — params: `{name: string}`, returns skill content
- `gateway_skills_add` — params: `{name, content, description}`, upserts
- `gateway_skills_delete` — params: `{name}`, deletes

- [ ] **Step 2: Register in server.rs tool list and call_tool dispatch**

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p astra-gateway`

---

### Task 5: Implement MCP tool handlers — Cron

**Files:**
- Create: `crates/astra-gateway/src/mcp/tools_cron.rs`
- Modify: `crates/astra-gateway/src/mcp/server.rs`

- [ ] **Step 1: Implement cron tools**

Tools:
- `gateway_cron_list` — list scheduled tasks for this chat
- `gateway_cron_add` — params: `{cron_expr, message}`, creates cron job
- `gateway_cron_delete` — params: `{job_id}`, prefix-match delete
- `gateway_remind_after` — params: `{minutes, message, exec: bool}`, one-time reminder

Reuse validation logic from `execute_gateway_actions_with_policy` (is_valid_cron_expr, time limits).

- [ ] **Step 2: Register in server.rs**

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p astra-gateway`

---

### Task 6: Implement MCP tool handlers — Tasks & Workspace

**Files:**
- Create: `crates/astra-gateway/src/mcp/tools_tasks.rs`
- Create: `crates/astra-gateway/src/mcp/tools_workspace.rs`
- Modify: `crates/astra-gateway/src/mcp/server.rs`

- [ ] **Step 1: Implement durable task tools**

Tools:
- `gateway_tasks_list` — list active durable tasks
- `gateway_tasks_create` — params: `{name, description?}`
- `gateway_tasks_status` — params: `{task_id}`
- `gateway_tasks_complete` — params: `{task_id}`
- `gateway_tasks_fail` — params: `{task_id, error?}`
- `gateway_tasks_cancel` — params: `{task_id}`

- [ ] **Step 2: Implement workspace tools**

Tools:
- `gateway_workspace_current` — returns current workspace path
- `gateway_workspace_list` — returns available projects
- `gateway_workspace_switch` — params: `{path}`

- [ ] **Step 3: Register all in server.rs**

- [ ] **Step 4: Verify it compiles**

Run: `cargo check -p astra-gateway`

---

### Task 7: MCP config generation and CLI spawn integration

**Files:**
- Create: `crates/astra-gateway/src/mcp/config.rs`
- Modify: `crates/astra-gateway/src/cli_pool.rs`
- Modify: `crates/astra-gateway/src/cli_bridge.rs`
- Modify: `crates/astra-gateway/src/runner.rs`

- [ ] **Step 1: Implement config.rs — generate temp mcp-config JSON**

```rust
pub fn generate_mcp_config(
    gateway_bin: &str,
    database_url: Option<&str>,
    platform: &str,
    chat_id: &str,
) -> Result<std::path::PathBuf, std::io::Error> {
    // Write to /tmp/gw-mcp-{pid}-{chat_hash}.json
    // Content:
    // {
    //   "mcpServers": {
    //     "gateway": {
    //       "command": "/path/to/astra-gateway",
    //       "args": ["mcp-serve"],
    //       "env": {
    //         "GATEWAY_DATABASE_URL": "...",
    //         "GW_MCP_PLATFORM": "...",
    //         "GW_MCP_CHAT_ID": "..."
    //       }
    //     }
    //   }
    // }
}
```

- [ ] **Step 2: Modify cli_pool.rs — add --mcp-config to persistent spawn**

In `build_persistent_command()`, after the system_prompt arg, add:
```rust
if let Some(mcp_config_path) = mcp_config {
    cmd.arg("--mcp-config").arg(mcp_config_path);
}
```

- [ ] **Step 3: Modify cli_bridge.rs — add --mcp-config to per-request spawn**

In the command builder for Claude CLI, add the mcp-config arg.

- [ ] **Step 4: Modify runner.rs — generate and pass mcp config**

Before calling `begin_turn` or `run_cli_with_cancel`, generate the mcp config file and pass the path.

- [ ] **Step 5: Verify it compiles**

Run: `cargo check -p astra-gateway`

---

### Task 8: Slim down system prompt

**Files:**
- Modify: `crates/astra-gateway/skills/gateway.md`
- Modify: `crates/astra-gateway/src/gateway_context.rs`
- Modify: `crates/astra-gateway/src/runner.rs`

- [ ] **Step 1: Replace gateway.md with minimal prompt**

New content (~300 bytes):
```markdown
## Gateway

Astra Gateway on {{platform}}. User: {{user_display_name}} (`{{user_id}}`), CLI: `{{cli_name}}`
{{#if model}}
Model: `{{model}}`
{{/if}}

You have gateway MCP tools available for:
- Scheduling tasks and reminders (gateway_cron_*)
- Managing reusable skills (gateway_skills_*)
- Durable task tracking (gateway_tasks_*)
- Workspace management (gateway_workspace_*)

Use these tools directly when the user asks to set reminders, schedule tasks, save procedures, check task status, or switch projects.

### User Commands (handled by gateway, not you)

/new /status /model /cli /ws /running /kill /cancel /manage /help
{{#if has_session}}
/session list /session switch <id>
{{/if}}

### Notes

- Mobile platform — keep responses concise. Respond in user's language.
- You CAN set reminders/schedules via gateway tools. No raw JSON/code unless asked.
```

- [ ] **Step 2: Simplify gateway_context.rs**

Remove `with_extra_skills()`, `with_cron_jobs()`, `with_active_tasks()`, `with_db_tables()`, `with_projects()` from `to_system_prompt()`. Keep the struct fields (may still be used elsewhere) but stop injecting them into the prompt.

Remove the `{{#each ...}}` template rendering for cron_jobs, active_tasks, db_tables, available_projects.

- [ ] **Step 3: Simplify runner.rs prompt construction**

Remove the section that queries cron jobs and durable tasks just to inject into prompt (lines ~990-1035). The model will query these via MCP tools when needed.

- [ ] **Step 4: Update tests in gateway_context.rs**

Fix tests that assert on removed content (cron section, db_tables section, etc).

- [ ] **Step 5: Verify it compiles and tests pass**

Run: `cargo test -p astra-gateway`

---

### Task 9: Maintain backward compatibility for non-Claude CLIs

**Files:**
- Modify: `crates/astra-gateway/src/runner.rs`
- Modify: `crates/astra-gateway/src/gateway_context.rs`

- [ ] **Step 1: Keep [[GATEWAY:...]] regex path for non-MCP CLIs**

The `execute_gateway_actions_with_policy` function stays — it's still needed for Astra/Copilot CLIs that don't support MCP.

- [ ] **Step 2: Conditional prompt: slim for Claude, full for others**

```rust
let system_prompt = if CliProcessPool::supports_persistent(&cli_profile) {
    // Claude CLI with MCP — slim prompt
    gw_context.to_slim_system_prompt()
} else {
    // Legacy CLIs — full prompt with actions documentation
    gw_context.to_full_system_prompt()
};
```

Keep the old `to_system_prompt()` logic as `to_full_system_prompt()` for the legacy path.

- [ ] **Step 3: Verify both paths compile**

Run: `cargo check -p astra-gateway`

---

### Task 10: Build and integration test

- [ ] **Step 1: Full build**

Run: `cargo build -p astra-gateway`

- [ ] **Step 2: Run all existing tests**

Run: `cargo test -p astra-gateway`

- [ ] **Step 3: Manual smoke test of mcp-serve subcommand**

Run: `echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}' | ./target/debug/astra-gateway mcp-serve`

Expect: JSON response with server capabilities.

- [ ] **Step 4: Test tools/list**

After initialize, send `tools/list` request and verify all gateway tools are listed.

---

## Verification Criteria

1. **`cargo build -p astra-gateway`** succeeds with no errors
2. **`cargo test -p astra-gateway`** — all tests pass
3. **`astra-gateway mcp-serve`** responds to MCP JSON-RPC on stdio
4. **`tools/list`** returns all gateway tools with correct schemas
5. **Claude CLI spawn** includes `--mcp-config` pointing to gateway's mcp-serve
6. **System prompt** for Claude CLI is <500 bytes, contains no dynamic data
7. **Non-Claude CLIs** still get the full prompt with `[[GATEWAY:...]]` documentation
8. **`[[GATEWAY:...]]` regex extraction** still works for non-MCP CLI backends
9. **gw_skills table** is created on startup and CRUD works
