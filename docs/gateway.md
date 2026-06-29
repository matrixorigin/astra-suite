# Astra Gateway

`astra-gateway` is the open-source chat-platform gateway in
`crates/astra-gateway`. It can run without an Astra server by using Claude,
Codex, Copilot, or a custom CLI backend. SQLite is the default storage backend,
so the basic gateway path has no MatrixOne or Memoria dependency.

Build from source:

```bash
cargo build --release -p astra-gateway
./target/release/astra-gateway init
./target/release/astra-gateway start
./target/release/astra-gateway status
```

Use the Astra backend after starting the local Astra stack:

```yaml
cli:
  type: astra
  bin: astra
  app_server_url: "http://127.0.0.1:17001"
  permission_mode: auto
  model: "qwen-plus"
```

The root `scripts/install.sh` installs the `astra` CLI only. It is useful for
the gateway's Astra backend, but it is not a gateway installer.

For the full gateway reference, see
[`crates/astra-gateway/README.md`](../crates/astra-gateway/README.md).
