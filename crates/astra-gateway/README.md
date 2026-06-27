# astra-gateway

A chat-platform gateway that bridges WeChat / WeCom / WhatsApp to AI agent CLIs —
**Claude Code**, **Codex**, **GitHub Copilot CLI**, and **Astra**. Point it at your chat
bot and talk to an agent CLI from any chat app, with per-user sessions,
attachments, scheduled tasks, and full observability.

## Features

| Category           | Description |
|--------------------|-------------|
| **Multi-CLI**      | claude / codex / copilot / astra — switch at runtime via `/cli`. |
| **Multi-model**    | `/model haiku\|sonnet\|opus\|minimax\|deepseek\|qwen\|glm` |
| **Sessions**       | Per-user isolation, auto-reset (daily / idle), history, switch. |
| **MCP tools**      | Agent-callable reminders, cron jobs, workspace listing, and file sending. |
| **Cron & tasks**   | Recurring jobs and one-time reminders, via slash commands or MCP tools. |
| **Workspace**      | `/workspace <path>` to switch project dirs, auto-discovery from configured roots. |
| **Observability**  | `/trace`, `/running`, `/inspect` (harness), `/audit` (decision chain), `/usage` (cost). |
| **Skills**         | Built-in gateway skill + user-defined `.md` files, agent self-authoring. |
| **Access control** | `allowlist` / `open` / `disabled` per gateway. |
| **Groups**         | Per-user session isolation, `@mention` filtering. |
| **Attachments**    | Receive chat images/files, guard image input by model vision support, send local files back. |
| **Chat UX**        | Typing indicator, markdown rendering, voice transcription, feedback, media. |
| **Reliability**    | Per-conversation queues, durable outbox, traceable retry / cancel, bounded parallelism. |

## Quick start

```bash
# 1. Install (or build from source: cargo build --release -p astra-gateway)
curl -sSL https://raw.githubusercontent.com/matrixorigin/astra-suite/main/scripts/install.sh | sh

# 2. Generate the starter config at ~/.astra-gateway/config.yaml (chmod 600)
astra-gateway init
# Edit the file: fill AWS_BEARER_TOKEN_BEDROCK + wecom.bot_id + wecom.secret.

# 3. (WeChat personal account only) Scan QR to log in
astra-gateway weixin login

# 4. Run as a background daemon
astra-gateway start
astra-gateway status
astra-gateway stop
```

## Subcommands

| Command                   | Description |
|---------------------------|-------------|
| `astra-gateway init`      | Write `~/.astra-gateway/config.yaml` (WeCom + Claude/Bedrock + SQLite, 0600) |
| `astra-gateway weixin login` | QR-code login for WeChat personal accounts |
| `astra-gateway whatsapp login` | QR-code login for WhatsApp Web sidecar |
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

Gateway automatically writes per-conversation MCP config for Claude/Codex-style
persistent backends when supported. The agent sees gateway tools such as
`gateway_send_attachment`, cron scheduling, reminders, and workspace listing.
State-changing model-generated tool calls are controlled by `action_policy`.

### Storage backends

| Backend     | Use when                                | Supports |
|-------------|-----------------------------------------|----------|
| `sqlite`    | **Default** — single-node, zero-config  | Everything including trace |
| `mysql`     | Multiple gateway instances share a DB   | Same as sqlite |
| `matrixone` | Same as `mysql` (MySQL-protocol alias)  | Same as sqlite |
| `file`      | Local dev / testing only                | Sessions, cron, reminders, usage |
| `none`      | Ephemeral — state lost on restart       | — |

## Platforms

| Platform       | Notes |
|----------------|-------|
| `wecom`        | Enterprise WeChat AI Bot over WebSocket. Supports text, feedback, streaming, and media. |
| `weixin`       | WeChat personal account through iLink Bot API. Run `astra-gateway weixin login` first. |
| `whatsapp`     | WhatsApp Business Cloud API webhook adapter. |
| `whatsapp_web` | WhatsApp Web via bundled Baileys sidecar. Run `astra-gateway whatsapp login` first. |

Inbound attachments are downloaded into the gateway run directory and passed to
the selected CLI as local files. Images are accepted only when the current model
is known or configured as vision-capable; unknown/text-only models get a clear
chat response instead of a backend error. Outbound file sending is exposed to
agents through `gateway_send_attachment`.

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
| `/task list\|cancel`           | Scheduled task / reminder management |
| `/cron list\|add\|del`         | Scheduled tasks |
| `/usage`                       | Token / cost statistics |

Natural language also works — when MCP is enabled, the agent can call gateway
tools to schedule cron jobs or set reminders on your behalf.

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

### System prompt

Gateway builds a system prompt for every normal agent turn. It includes the
current user/chat context and available gateway capabilities. Add local
instructions in either of these ways:

```yaml
system_prompt_extra: |
  Extra gateway instructions here.
```

If `system_prompt_extra` is absent, gateway loads `system_prompt_extra.md` from
the same directory as the config file. The YAML field wins when both exist.

### Model vision allowlist

Gateway has built-in model patterns for common vision-capable and text-only
models. Add local overrides when a deployed model id is not covered:

```yaml
vision_models:
  - qwen2.5-vl
  - my-vision-model
```

Unknown image-capability models are treated as unsupported to avoid sending
image files to backends that may reject them.

### Per-user GitHub tokens

Map chat user ids to GitHub tokens when the agent should operate GitHub as that
user. The matched token is injected into CLI processes as `GH_TOKEN` and
`GITHUB_TOKEN`. `default` is used when no user-specific entry exists.

```yaml
github_tokens:
  default:
    token: ghp_xxx
    remark: matrix-meow
  wom-o3DwAALoVyBSBHfVV03KgIa8BXVw:
    token: ghp_xxx
    remark: aptend
```

Reasoning blocks are opt-in per user. `/reasoning on` or
`/cli <name> thinking-chain` forwards only reasoning/thinking events that the
selected CLI explicitly exposes; chat platforms render them as separate text blocks.

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
WeChat/WeCom/WhatsApp ──→ PlatformAdapter ──→ GatewayRunner
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
              PlatformAdapter.send_text() ──→ WeChat/WeCom/WhatsApp
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
| `gw_platform_credentials` | WeChat tokens, context tokens, sync cursors |
| `gw_trace_requests`       | User / scheduler request state |
| `gw_trace_runs`           | CLI / runtime attempt state |
| `gw_trace_events`         | Append-only trace / audit event stream |
| `gw_trace_outbox`         | Durable platform delivery queue |
| `gw_usage`                | Per-message token / cost tracking |

## License

MIT — see [LICENSE](../../LICENSE).
