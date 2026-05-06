.PHONY: build release test test-live check format lint clean run stop restart log init setup login-weixin

GATEWAY_BIN = target/release/astra-gateway
GATEWAY_PID = /tmp/astra-gateway.pid
GATEWAY_LOG = /tmp/astra-gateway.log

# ─── Build ───────────────────────────────────────────────────────────────────

build:
	cargo build --workspace --all-targets

release:
	cargo build --release -p astra-gateway

# ─── Test ────────────────────────────────────────────────────────────────────

## Fast offline tests (~5s, no external deps)
test:
	cargo test --workspace

## Include live tests (requires: astra server, MySQL, claude CLI)
test-live:
	cargo test --workspace -- --include-ignored

# ─── Lint / Format ───────────────────────────────────────────────────────────

## Full pre-commit check: format + clippy + test
check: format lint test

## Auto-format code
format:
	cargo fmt --all

## Check formatting + clippy warnings (CI uses this)
lint:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets

# ─── Run / Stop / Restart ────────────────────────────────────────────────────

## Quick start: make init → make login-weixin → make run
init:
	cargo run -p astra-gateway --release -- init

run: release
	@echo "Starting astra-gateway (log: $(GATEWAY_LOG))..."
	@nohup $(GATEWAY_BIN) --config gateway.yaml > $(GATEWAY_LOG) 2>&1 & echo $$! > /dev/null
	@sleep 1
	@if [ -f $(GATEWAY_PID) ]; then \
		echo "✅ gateway running (pid: $$(cat $(GATEWAY_PID)))"; \
	else \
		echo "❌ failed to start — check $(GATEWAY_LOG)"; \
		tail -5 $(GATEWAY_LOG); \
	fi

stop:
	@if [ -f $(GATEWAY_PID) ]; then \
		kill $$(cat $(GATEWAY_PID)) 2>/dev/null && echo "✅ gateway stopped" || echo "⚠️  process not running"; \
		rm -f $(GATEWAY_PID); \
	else \
		echo "⚠️  no pid file found (not running?)"; \
	fi

restart: stop
	@sleep 1
	@$(MAKE) run

log:
	@tail -f $(GATEWAY_LOG)

login-weixin:
	cargo run -p astra-gateway --release -- login-weixin

setup:
	bash crates/astra-gateway/setup.sh

# ─── Clean ───────────────────────────────────────────────────────────────────

clean:
	cargo clean
