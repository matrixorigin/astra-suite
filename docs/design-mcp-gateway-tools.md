# MCP Gateway Tools — Implementation Summary

## What Was Built

Gateway binary now embeds an MCP stdio server (`astra-gateway mcp-serve`). When spawning Claude CLI, gateway generates a temp mcp-config JSON and passes it via `--mcp-config`. The model interacts with gateway through structured MCP tool calls instead of `[[GATEWAY:...]]` regex tags.

## Architecture

```
Gateway Binary
  ├─ Runner (main loop)
  │    ├─ Generates /tmp/gw-mcp-{hash}.json per conversation
  │    ├─ Spawns Claude CLI with --mcp-config pointing to self
  │    └─ Claude CLI path: slim system prompt (~350 bytes)
  │
  └─ mcp-serve subcommand (spawned by Claude CLI)
       ├─ Connects to same DB (URL passed via env)
       ├─ Scoped to platform/chat_id/user_id
       └─ Exposes 17 MCP tools

Non-Claude CLIs (Astra/Copilot): unchanged, full prompt + [[GATEWAY:...]]
```

## MCP Tools (17 total)

| Category | Tools |
|----------|-------|
| Cron | `gateway_cron_list`, `gateway_cron_add`, `gateway_cron_delete`, `gateway_remind_after` |
| Skills | `gateway_skills_list`, `gateway_skills_read`, `gateway_skills_add`, `gateway_skills_delete` |
| Tasks | `gateway_tasks_list`, `gateway_tasks_create`, `gateway_tasks_status`, `gateway_tasks_complete`, `gateway_tasks_fail`, `gateway_tasks_cancel` |
| Workspace | `gateway_workspace_current`, `gateway_workspace_list`, `gateway_workspace_switch` |

## New Files

| File | Purpose |
|------|---------|
| `src/mcp/mod.rs` | Module root |
| `src/mcp/server.rs` | MCP server struct + all tool definitions via `#[tool_router]` macro |
| `src/mcp/config.rs` | Generates temp mcp-config JSON for CLI spawn |
| `src/mcp/tools_cron.rs` | Cron tool handler logic |
| `src/mcp/tools_skills.rs` | Skills tool handler logic |
| `src/mcp/tools_tasks.rs` | Durable task tool handler logic |
| `src/mcp/tools_workspace.rs` | Workspace tool handler logic |

## Modified Files

| File | Change |
|------|--------|
| `Cargo.toml` | Added `rmcp` dependency |
| `src/main.rs` | Added `mcp-serve` subcommand |
| `src/lib.rs` | Added `pub mod mcp` |
| `src/cli_pool.rs` | `begin_turn` + `spawn` accept `mcp_config: Option<&Path>` |
| `src/runner.rs` | Generates MCP config, uses slim prompt for Claude CLI |
| `src/gateway_context.rs` | Added `to_slim_system_prompt()` |
| `src/store/mod.rs` | Added `SkillRecord` type + 4 skill methods to trait |
| `src/store/sqlite.rs` | Implemented `gw_skills` table + CRUD |
| `src/store/mysql.rs` | Implemented `gw_skills` table + CRUD |
| `src/store/file.rs` | Implemented skill methods (JSON file storage) |

## System Prompt Comparison

**Before (Claude CLI):** ~4KB — includes action format tables, cron job lists, active tasks, project lists, skills full text

**After (Claude CLI):** ~350 bytes — just role info + "you have gateway MCP tools" + user command list

**Non-Claude CLIs:** Unchanged (full prompt with `[[GATEWAY:...]]` documentation)

## Backward Compatibility

- `[[GATEWAY:...]]` regex mechanism fully preserved for non-MCP backends
- Non-Claude CLIs (Astra, Copilot, Custom) unaffected
- Existing tests all pass (750+)
- `/skill` chat commands and `[[GATEWAY:skill_add:...]]` still work (write to filesystem for legacy, DB for MCP path)

## What's NOT Done (Intentionally Left)

1. **`[[GATEWAY:...]]` removal for Claude CLI** — The regex path still exists and will still fire if Claude's response contains those tags. This is a safety net during transition. Can be removed later once MCP is proven stable.

2. **Prompt injection of cron/tasks for non-pool path** — Still happens for Astra/Copilot. Only Claude CLI gets the slim prompt.

3. **`skills_dir` config removal** — Legacy file-based skills loading still works. Users who already have `skills_dir` configured won't break.
