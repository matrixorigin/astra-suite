# astra-gateway

A chat-platform gateway that bridges WeChat / WeCom (and more) to AI agent CLIs —
**Claude Code**, **Codex**, **GitHub Copilot CLI**, and **Astra**. Point it at your chat
bot and talk to an agent CLI from any chat app, with per-user sessions,
scheduled tasks, durable long-running jobs, and full observability.

## Features

| Category           | Description |
|--------------------|-------------|
| **Multi-CLI**      | claude / codex / copilot / astra — switch at runtime via `/cli`. |
| **Multi-model**    | `/model haiku\|sonnet\|opus\|minimax\|deepseek\|qwen\|glm` |
| **Sessions**       | Per-user isolation, auto-reset (daily / idle), history, switch. |
| **Cron & tasks**   | Recurring jobs, one-time reminders, durable tasks with checkpoint / resume. |
| **Workspace**      | `/workspace <path>` to switch project dirs, auto-discovery from configured roots. |
| **Observability**  | `/trace`, `/running`, `/inspect` (harness), `/audit` (decision chain), `/usage` (cost). |
| **Skills**         | Built-in gateway skill + user-defined `.md` files, agent self-authoring. |
| **Access control** | `allowlist` / `open` / `disabled` per gateway. |
| **Groups**         | Per-user session isolation, `@mention` filtering. |
| **WeChat UX**      | Typing indicator, markdown rendering, voice transcription, media. |
| **Reliability**    | Per-conversation queues, durable outbox, traceable retry / cancel, bounded parallelism. |

## Quick start

```bash
# 1. Install (or build from source: cargo build --release -p astra-gateway)
curl -sSL https://raw.githubusercontent.com/matrixorigin/astra-suite/main/scripts/install.sh | sh

# 2. Generate the starter config at ~/.astra-gateway/config.yaml (chmod 600)
astra-gateway init
# Edit the file: fill AWS_BEARER_TOKEN_BEDROCK + wecom.bot_id + wecom.secret.

# 3. (WeChat personal account only) Scan QR to log in
astra-gateway login-weixin

# 4. Run as a background daemon
astra-gateway start
astra-gateway status
astra-gateway stop
```

## Subcommands

| Command                   | Description |
|---------------------------|-------------|
| `astra-gateway init`      | Write `~/.astra-gateway/config.yaml` (WeCom + Claude/Bedrock + SQLite, 0600) |
| `astra-gateway login-weixin` | QR-code login for WeChat personal accounts |
| `astra-gateway start`     | Daemonize and run in background (idempotent) |
| `astra-gateway status`    | Show whether the daemon is running, plus paths |
| `astra-gateway stop`      | Graceful SIGTERM, escalates to SIGKILL after 15s |
| `astra-gateway update`    | Self-replace with the latest release (atomic, with `ghfast.top` fallback) |
| `astra-gateway`           | Run in foreground (Ctrl+C to stop) — handy for debugging |

`update` accepts `--version <tag>` and `--mirror <url>`.

## Backends

### CLI backends

| Type      | Requires                                  |
|-----------|-------------------------------------------|
| `claude`  | `claude` CLI on PATH (Claude Code)        |
| `codex`   | `codex` CLI on PATH                       |
| `copilot` | `copilot` CLI on PATH (GitHub Copilot CLI) |
| `astra`   | An Astra agent server (closed-source today) |
| `custom`  | Any CLI with JSON / plain-text output     |

Users switch at runtime with `/cli claude`, `/cli codex`, `/cli copilot`, etc.

### Storage backends

| Backend     | Use when                                | Supports |
|-------------|-----------------------------------------|----------|
| `sqlite`    | **Default** — single-node, zero-config  | Everything including trace, durable tasks |
| `mysql`     | Multiple gateway instances share a DB   | Same as sqlite |
| `matrixone` | Same as `mysql` (MySQL-protocol alias)  | Same as sqlite |
| `file`      | Local dev / testing only                | Sessions, cron, reminders, usage |
| `none`      | Ephemeral — state lost on restart       | — |

## User commands

| Command                        | Description |
|--------------------------------|-------------|
| `/help`                        | Show all commands |
| `/status`                      | CLI + model + session + harness summary |
| `/new`                         | Start a new conversation |
| `/cli` / `/cli claude`         | Show / switch CLI backend |
| `/reasoning on\|off`           | Toggle explicit reasoning / thinking blocks |
| `/model` / `/model opus`       | Show / switch model |
| `/workspace <path>`            | Switch working directory |
| `/session list\|switch`        | Session history |
| `/inspect`                     | Harness: tokens, cost, tools, warnings |
| `/audit`                       | Decision chain (last N turns) |
| `/running`                     | Queued / running gateway requests |
| `/trace <id>`                  | Request lifecycle and events |
| `/cancel <id>`                 | Cancel a queued request |
| `/task list\|cancel\|resume`   | Durable task management |
| `/cron list\|add\|del`         | Scheduled tasks |
| `/usage`                       | Token / cost statistics |

Natural language also works — the agent can emit `[[GATEWAY:action]]` tags
to schedule cron jobs, set reminders, open durable tasks, or change the
workspace on your behalf.

## Configuration

The `init` template ([`gateway-wecom-claude.yaml`](gateway-wecom-claude.yaml))
is the recommended starting point — minimal WeCom + Claude/Bedrock + SQLite.
For the full reference (all platforms, advanced cron, custom CLIs) see
[`gateway.example.yaml`](gateway.example.yaml).

```yaml
cli:
  type: claude
  bin: claude
  model: sonnet
  env:                                    # injected into the spawned CLI
    CLAUDE_CODE_USE_BEDROCK: "1"
    AWS_REGION: "us-east-1"
    AWS_BEARER_TOKEN_BEDROCK: ""          # ← FILL ME

platforms:
  wecom:
    enabled: true
    bot_id: ""                            # ← FILL ME
    secret: ""                            # ← FILL ME
```

`cli.env` (and `cli.env_file`) work for every CLI backend, so secrets and
runtime flags live in YAML — no shell `export` needed. Treat the file as
sensitive (`init` chmods it to 0600).

Environment variables (see [.env.example](.env.example)) still override
YAML — useful for deployment-specific tweaks.

Reasoning blocks are opt-in per user. `/reasoning on` or
`/cli <name> thinking-chain` forwards only reasoning/thinking events that the
selected CLI explicitly exposes; WeChat renders them as separate text blocks.

## Development

```bash
# From workspace root — see Makefile for all targets
make build          # cargo build --workspace --all-targets
make release        # cargo build --release -p astra-gateway
make check          # format + clippy + test (full CI)
make test           # unit + integration tests (700+)
make lint           # fmt --check + clippy -D warnings
```

Or directly:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo build --release -p astra-gateway
```

## Architecture

```
WeChat/WeCom ──→ PlatformAdapter ──→ GatewayRunner
                                         │
                    ┌────────────────────┤
                    ↓                    ↓
              handle_fast          handle_message (async)
           (slash commands)        (CLI spawn in tokio::spawn)
              instant ↓                  ↓
                                    CLI bridge → claude/codex/copilot/astra
                                          ↓
                                    trace/run/outbox + policy checks
                                         ↓
                                    Store backend (SQLite/MySQL)
                                         ↓
              ←── cli_resp channel ←─────┘
                    ↓
              PlatformAdapter.send_text() ──→ WeChat/WeCom
```

Slash commands respond instantly; regular chat requests serialize per
conversation. Different conversations run concurrently up to
`max_concurrent_runs`. Final responses go through the durable outbox
before platform delivery is acknowledged, so messages survive crashes
and transient platform failures.

## Storage schema

All tables are created automatically on first run. Prefix `gw_`:

| Table                     | Purpose |
|---------------------------|---------|
| `gw_users`                | Profiles + preferences (CLI, model, workspace) |
| `gw_sessions`             | Chat → CLI session mapping (per-CLI isolation) |
| `gw_cron_jobs`            | Recurring + one-time scheduled tasks |
| `gw_durable_tasks`        | Checkpointable long-running tasks |
| `gw_platform_credentials` | WeChat tokens, context tokens, sync cursors |
| `gw_trace_requests`       | User / scheduler request state |
| `gw_trace_runs`           | CLI / runtime attempt state |
| `gw_trace_events`         | Append-only trace / audit event stream |
| `gw_trace_outbox`         | Durable platform delivery queue |
| `gw_usage`                | Per-message token / cost tracking |

## License

MIT — see [LICENSE](../../LICENSE).
