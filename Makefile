.PHONY: help build release test test-live check format lint clean run stop restart log init setup login-weixin

GATEWAY_BIN = target/release/astra-gateway
GATEWAY_PID = /tmp/astra-gateway.pid
GATEWAY_LOG = /tmp/astra-gateway.log

# ─── Help (default) ──────────────────────────────────────────────────────────

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	awk 'BEGIN {FS = ":.*?## "; printf "\033[36m%-20s %s\033[0m\n", "target", "description"} \
	     {printf "\033[33m%-20s\033[0m \033[2m%s\033[0m\n", $$1, $$2}'

# ─── Build ───────────────────────────────────────────────────────────────────

build: ## Build workspace with all targets
	cargo build --workspace --all-targets

release: ## Build astra-gateway in release mode
	cargo build --release -p astra-gateway

# ─── Test ────────────────────────────────────────────────────────────────────

test: ## Run fast offline tests
	cargo test --workspace

test-live: ## Run all tests including live (requires: server, MySQL, Claude CLI)
	cargo test --workspace -- --include-ignored

# ─── Lint / Format ───────────────────────────────────────────────────────────

check: ## Full pre-commit check: format + clippy + test
check: format lint test

format: ## Auto-format all code
	cargo fmt --all

lint: ## Check formatting + clippy (CI gate)
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets -- -D warnings

# ─── Run / Stop / Restart ────────────────────────────────────────────────────

init: release ## Initialize gateway config
	$(GATEWAY_BIN) init

run: release ## Start gateway in background
	@if [ ! -f gateway.yaml ]; then \
		echo "📝 gateway.yaml not found — auto-running init..."; \
		$(GATEWAY_BIN) init; \
	fi
	@if [ -f $(GATEWAY_PID) ] && kill -0 $$(cat $(GATEWAY_PID)) 2>/dev/null; then \
		echo "ℹ️  gateway already running (pid: $$(cat $(GATEWAY_PID)))"; \
	else \
		echo "Starting astra-gateway (log: $(GATEWAY_LOG))..."; \
		nohup $(GATEWAY_BIN) --config gateway.yaml > $(GATEWAY_LOG) 2>&1 & echo $$! > $(GATEWAY_PID); \
		sleep 1; \
		if [ -f $(GATEWAY_PID) ] && kill -0 $$(cat $(GATEWAY_PID)) 2>/dev/null; then \
			echo "✅ gateway running (pid: $$(cat $(GATEWAY_PID)))"; \
		else \
			echo "❌ failed to start — check $(GATEWAY_LOG)"; \
			tail -5 $(GATEWAY_LOG); \
		fi; \
	fi

stop: ## Stop running gateway
	@if [ -f $(GATEWAY_PID) ]; then \
		kill $$(cat $(GATEWAY_PID)) 2>/dev/null && echo "✅ gateway stopped" || echo "⚠️  process not running"; \
		rm -f $(GATEWAY_PID); \
	else \
		echo "⚠️  no pid file found (not running?)"; \
	fi

restart: stop ## Restart gateway
	@sleep 1
	@$(MAKE) run

log: ## Tail gateway log
	@tail -f $(GATEWAY_LOG)

login-weixin: release ## Login via WeChat
	$(GATEWAY_BIN) login-weixin

setup: ## Run gateway setup script
	bash crates/astra-gateway/setup.sh

# ─── Clean ───────────────────────────────────────────────────────────────────

clean: ## Clean all build artifacts
	cargo clean