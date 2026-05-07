# Astra Suite

> Open-source tools for the [Astra](https://github.com/matrixorigin/astra) agent ecosystem.

Astra is an AI agent runtime with planning, memory, tool orchestration, and multi-model routing. **Astra Suite** provides standalone open-source utilities that extend Astra's reach — each tool works independently and integrates with the broader ecosystem.

---

## Tools

### [Astra Gateway](crates/astra-gateway)

A production-ready **chat-platform gateway** that bridges messaging apps to AI agent CLIs.

```
WeChat / WeCom / (Feishu, WhatsApp planned)
        ↕
   Astra Gateway
        ↕
Claude Code · Codex · Astra · Custom CLI
```

**Highlights:**

- **Zero-config start** — SQLite storage, 3 commands to deploy
- **Multi-backend** — switch between Claude, Codex, Astra at runtime (`/cli`, `/model`)
- **Autonomous scheduling** — cron jobs that invoke the agent ("每晚 10 点检查 PR 列表并总结")
- **Durable tasks** — long-running jobs with checkpoint/resume, crash recovery
- **Full observability** — per-request trace, audit chain, durable outbox with retry
- **Multi-user** — per-user sessions, access control, group chat isolation

**Install:**

```bash
curl -sSL https://raw.githubusercontent.com/matrixorigin/astra-suite/main/scripts/install.sh | sh
```

Or download binaries directly from [GitHub Releases](https://github.com/matrixorigin/astra-suite/releases).

**Quick start:**

```bash
astra-gateway init          # generate gateway.yaml (Claude + SQLite)
astra-gateway login-weixin  # scan QR for WeChat
astra-gateway               # start
```

See [`crates/astra-gateway/README.md`](crates/astra-gateway/README.md) for full documentation.

---

## Repository Structure

```
astra-suite/
├── crates/
│   ├── astra-gateway/     # Gateway binary + library
│   ├── astra/             # HTTP+SSE client for Astra server
│   └── astra-task-store/  # Durable task store trait + types
├── ARCHITECTURE.md        # System design + extension points
├── CONTRIBUTING.md        # Developer workflow
├── Makefile               # Build / test / run targets
└── LICENSE                # MIT
```

## Development

```bash
make build          # compile workspace
make check          # format + clippy + 848 tests (~6s)
make test           # tests only
make format         # auto-format
make lint           # fmt check + clippy (CI)
```

```bash
make run            # start gateway (background)
make stop           # stop
make restart        # stop + start
make log            # tail -f gateway log
```

## Roadmap

- [x] Astra Gateway (WeChat + WeCom)
- [ ] Feishu (飞书) platform adapter
- [ ] WhatsApp platform adapter
- [ ] Copilot CLI backend
- [ ] `astra-bench` — agent evaluation harness
- [ ] `astra-sync` — cross-device session sync

## License

[MIT](LICENSE)
