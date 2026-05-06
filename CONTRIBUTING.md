# Contributing to astra-suite

Thank you for considering a contribution. This document covers everything you
need to get started.

## Prerequisites

- **Rust stable** (latest recommended)
- **cargo** (ships with Rust)
- SQLite development headers (usually pre-installed on Linux/macOS)

## Building

```bash
cargo build --workspace
```

For a release binary of the gateway:

```bash
cargo build --release -p astra-gateway
```

## Testing

```bash
cargo test --workspace
```

The workspace contains 700+ tests. 13 end-to-end tests are `#[ignore]`
because they require a real LLM backend (claude CLI on PATH with valid
credentials). Run them explicitly when needed:

```bash
cargo test --workspace -- --ignored
```

## Code Style

- Format: `cargo fmt --all`
- Lint: `cargo clippy --workspace --all-targets`
- **Zero warnings policy** -- CI rejects any clippy or compiler warnings.

Run both before pushing:

```bash
cargo fmt --all && cargo clippy --workspace --all-targets
```

## PR Workflow

1. Fork the repository.
2. Create a feature or fix branch off `main`.
3. Make your changes with clear, incremental commits.
4. Open a pull request against `main`.
5. CI must pass (fmt, clippy, tests).

## Commit Style

- Use the imperative mood: "Add X" not "Added X".
- Keep the summary line concise (under 72 characters).
- Add a blank line before any extended description.

## Extension Points

### Adding a CLI backend

See `crates/astra-gateway/src/cli_bridge.rs`. Add a new variant to the
`CliProfile` enum and implement the corresponding logic in
`build_command`, `parse_output`, and `capabilities`.

### Adding a platform

See `crates/astra-gateway/src/platforms/`. Create a new module that
implements the `PlatformAdapter` trait (defined in `platforms/mod.rs`),
then instantiate it in `main.rs`.

### Adding a storage backend

See `crates/astra-gateway/src/store/`. A new backend must implement:

- `GatewayStore` (defined in `store/mod.rs`)
- `DurableTaskStore` (defined in `durable_task_store.rs`)
- `TraceRepository` (defined in `trace_model.rs`)

Wire it into `StorageConfig` and `open_store_bundle` in `store/mod.rs`.

## License

This project is licensed under MIT. By submitting a contribution you agree
that your work will be distributed under the same license. See [LICENSE](LICENSE).
