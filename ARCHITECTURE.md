# Architecture

High-level overview of the astra-suite workspace, its internal structure,
and extension points.

## Workspace Layout

### astra-gateway

The main binary crate. A chat-platform gateway that bridges messaging
platforms (WeChat, WeCom) to AI agent CLIs (Claude Code, Codex, Copilot,
Astra). Handles sessions, scheduling, durable tasks, trace/audit,
access control, and reliable message delivery.

### astra

An HTTP + SSE client library for the Astra agent server. Used by the
gateway's `astra` CLI backend to communicate with a running Astra
instance. Provides typed request/response models and streaming event
parsing.

### astra-task-store

Defines the `DurableTaskStore` trait and associated types for
checkpointable long-running tasks. Consumed by the gateway and
potentially reusable by other services that need durable job semantics.

## Gateway Architecture

```
WeChat/WeCom ──> PlatformAdapter ──> GatewayRunner
                                         |
                    +--------------------+
                    v                    v
              handle_fast          handle_message (async)
           (slash commands)        (CLI spawn in tokio::spawn)
              instant v                  v
                                    CLI bridge -> claude/codex/copilot/astra
                                         v
                                    trace/run/outbox + policy checks
                                         v
                                    Store backend (SQLite/MySQL)
                                         v
              <-- cli_resp channel <------+
                    v
              PlatformAdapter.send_text() --> WeChat/WeCom
```

### Flow Details

**PlatformAdapter to InboundMessage.** Each platform module converts
raw webhook payloads into a normalized `InboundMessage` and hands it to
the `GatewayRunner`.

**Runner: per-conversation serialized queues.** Messages for the same
conversation are processed sequentially (one CLI run at a time).
Different conversations run concurrently up to `max_concurrent_runs`,
providing bounded parallelism without conversation-level races.

**CliProfile dispatch.** The runner consults the user's active
`CliProfile` to build the CLI `Command` (args, env, working directory).
Each profile variant knows how to construct arguments, parse streamed
output, and advertise its capabilities.

**Trace and Outbox.** Every request creates a trace record
(request, run, events). Final responses enter the durable outbox before
platform delivery, so messages survive crashes and transient failures.

**Scheduler.** A cron polling loop checks `gw_cron_jobs` on a
configurable interval, synthesizes `InboundMessage` payloads for due
jobs, and feeds them back through the runner like normal user messages.

## Extension Points

| Want to add        | Where                                         | How                                                                                      |
|--------------------|-----------------------------------------------|------------------------------------------------------------------------------------------|
| CLI backend        | `cli_bridge.rs` -- `CliProfile` enum          | Add variant, implement `build_command` / `parse_output` / `capabilities`                 |
| Platform           | `platforms/<name>.rs`                         | Implement `PlatformAdapter`, instantiate in `main.rs`                                    |
| Storage backend    | `store/<name>.rs`                             | Implement `GatewayStore` + `DurableTaskStore` + `TraceRepository`, wire in `StorageConfig` + `open_store_bundle` |

## Glossary

**trace** -- Per-request lifecycle tracking. A trace captures request
receipt, CLI run state, intermediate events, and outbox delivery status.

**outbox** -- Durable delivery queue. Messages are written to the outbox
before platform send; delivery is retried until acknowledged. Survives
crashes and transient platform failures.

**harness** -- The inner loop of a CLI run. Manages token counting, tool
call tracking, cost accumulation, and streaming output parsing for a
single invocation.

**session** -- Maps a chat conversation to a CLI session ID, providing
continuity across messages. Sessions are per-user and per-CLI-backend.

**skill** -- A `.md` template file injected into the CLI's system prompt.
Skills provide domain knowledge or behavioral instructions to the agent.

**workspace** -- The working directory passed to the CLI subprocess.
Controls which project the agent operates on; switchable at runtime via
`/workspace`.

**durable task** -- A long-running job with checkpoint/resume semantics.
Tasks persist progress to storage so they can be restarted after crashes
or intentional stops without losing intermediate work.
