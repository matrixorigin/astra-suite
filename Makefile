.PHONY: help build release test test-live check format lint prepare-release prepare-release-push clean run stop restart log init setup cli-install stack-env stack-check-env stack-config stack-up stack-down stack-clean stack-status stack-logs stack-smoke

GATEWAY_BIN = target/release/astra-gateway
GATEWAY_PID = /tmp/astra-gateway.pid
GATEWAY_LOG = /tmp/astra-gateway.log

DEFAULT_API_PORT ?= 17001
STACK_DIR := deployment/astra-stack
STACK_ENV := $(STACK_DIR)/.env
STACK_COMPOSE := cd $(STACK_DIR) && HOST_UID=$$(id -u) HOST_GID=$$(id -g) docker compose --env-file $(abspath $(STACK_ENV))
STACK_SECRET_ENV := ASTRA_JWT_SECRET ASTRA_TOKEN_ENCRYPTION_KEY ASTRA_BRIDGE_SECRET MEMORIA_MASTER_KEY
STACK_REQUIRED_ENV := $(STACK_SECRET_ENV) MEMORIA_EMBEDDING_API_KEY MEMORIA_EMBEDDING_BASE_URL

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

# ─── Release ─────────────────────────────────────────────────────────────────

prepare-release: ## Prepare gateway release commit and tag; usage: make prepare-release VERSION=0.3.1
	@test -n "$(VERSION)" || (echo "usage: make prepare-release VERSION=0.3.1" >&2; exit 2)
	scripts/prepare-release.sh $(VERSION)

prepare-release-push: ## Prepare gateway release commit/tag and push; usage: make prepare-release-push VERSION=0.3.1
	@test -n "$(VERSION)" || (echo "usage: make prepare-release-push VERSION=0.3.1" >&2; exit 2)
	scripts/prepare-release.sh $(VERSION) --push

cli-install: ## Install astra CLI from GitHub Releases (VERSION=v0.1.0 optional)
	@args=""; \
	if [ -n "$(VERSION)" ]; then args="$$args -v $(VERSION)"; fi; \
	if [ "$(INIT_MODELS)" = "1" ]; then args="$$args --init-models"; fi; \
	if [ -n "$(MODELS_PATH)" ]; then args="$$args --models-path $(MODELS_PATH)"; fi; \
	sh scripts/install.sh $$args

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

setup: ## Run gateway setup script
	bash crates/astra-gateway/setup.sh

# ─── Astra Stack ─────────────────────────────────────────────────────────────

stack-env: ## Create deployment/astra-stack/.env and generate local secrets
	@if [ -f "$(STACK_ENV)" ]; then \
		echo "$(STACK_ENV) already exists"; \
	else \
		cp $(STACK_DIR)/.env.example $(STACK_ENV); \
		echo "Created $(STACK_ENV)"; \
	fi; \
	if ! command -v openssl >/dev/null 2>&1; then \
		echo "openssl is required to generate stack secrets" >&2; \
		exit 1; \
	fi; \
	has_env_value() { \
		key="$$1"; \
		awk -v key="$$key" ' \
			/^[[:space:]]*#/ { next } \
			{ \
				line = $$0; \
				sub(/^[[:space:]]*/, "", line); \
				if (line ~ "^" key "[[:space:]]*=") { \
					sub(/^[^=]*=/, "", line); \
					sub(/^[[:space:]]*/, "", line); \
					lower = tolower(line); \
					if (line != "" && lower !~ /(change[-_]?me|change-in-production|astra-dev-|dev-master-key|your-|replace-with)/) found = 1; \
				} \
			} \
			END { exit found ? 0 : 1 } \
		' "$(STACK_ENV)"; \
	}; \
	set_env_value() { \
		key="$$1"; \
		value="$$2"; \
		tmp="$$(mktemp)"; \
		awk -v key="$$key" -v value="$$value" ' \
			BEGIN { done = 0 } \
			{ \
				line = $$0; \
				sub(/^[[:space:]]*/, "", line); \
				if (line ~ "^" key "[[:space:]]*=") { \
					print key "=" value; \
					done = 1; \
					next; \
				} \
				print; \
			} \
			END { if (!done) print key "=" value } \
		' "$(STACK_ENV)" > "$$tmp"; \
		mv "$$tmp" "$(STACK_ENV)"; \
	}; \
	ensure_secret() { \
		key="$$1"; \
		if has_env_value "$$key"; then \
			echo "$$key already configured"; \
			return 0; \
		fi; \
		value="$$(openssl rand -hex 32)"; \
		set_env_value "$$key" "$$value"; \
		echo "Generated $$key"; \
	}; \
	ensure_secret ASTRA_JWT_SECRET; \
	ensure_secret ASTRA_TOKEN_ENCRYPTION_KEY; \
	ensure_secret ASTRA_BRIDGE_SECRET; \
	ensure_secret MEMORIA_MASTER_KEY; \
	echo "Edit $(STACK_ENV) and fill MEMORIA_EMBEDDING_API_KEY plus MEMORIA_EMBEDDING_BASE_URL before make stack-up."

stack-check-env: ## Validate required astra-stack configuration
	@if [ ! -f "$(STACK_ENV)" ]; then \
		echo "Missing $(STACK_ENV)" >&2; \
		echo "Run: make stack-env" >&2; \
		exit 1; \
	fi
	@missing=""; \
	for key in $(STACK_REQUIRED_ENV); do \
		if ! awk -v key="$$key" ' \
			/^[[:space:]]*#/ { next } \
			{ \
				line = $$0; \
				sub(/^[[:space:]]*/, "", line); \
				if (line ~ "^" key "[[:space:]]*=") { \
					sub(/^[^=]*=/, "", line); \
					sub(/^[[:space:]]*/, "", line); \
					lower = tolower(line); \
					if (line != "" && lower !~ /(change[-_]?me|change-in-production|astra-dev-|dev-master-key|your-|replace-with)/) found = 1; \
				} \
			} \
			END { exit found ? 0 : 1 } \
		' "$(STACK_ENV)"; then \
			missing="$$missing $$key"; \
		fi; \
	done; \
	if [ -n "$$missing" ]; then \
		echo "Missing or insecure required config in $(STACK_ENV):$$missing" >&2; \
		echo "Run make stack-env to generate secrets, then fill embedding config." >&2; \
		exit 1; \
	fi

stack-config: stack-check-env ## Validate astra-stack docker compose config
	@$(STACK_COMPOSE) config --quiet
	@echo "Compose stack config OK"

stack-up: stack-config ## Start MatrixOne + Memoria + Astra API
	@$(STACK_COMPOSE) up -d
	@API_PORT=$$(awk -F= '/^[[:space:]]*ASTRA_API_PORT[[:space:]]*=/{gsub(/^[[:space:]]+|[[:space:]]+$$/, "", $$2); print $$2}' $(STACK_ENV) | tail -1); \
	echo "Astra stack started"; \
	echo "API: http://127.0.0.1:$${API_PORT:-$(DEFAULT_API_PORT)}"

stack-down: ## Stop astra-stack containers
	@$(STACK_COMPOSE) down
	@echo "Astra stack stopped"

stack-clean: ## Stop astra-stack containers and remove MatrixOne data volume
	@$(STACK_COMPOSE) down -v
	@echo "Astra stack stopped and volumes removed"

stack-status: stack-check-env ## Show astra-stack container status
	@$(STACK_COMPOSE) ps

stack-logs: stack-check-env ## Follow astra-stack logs (SERVICE=api optional)
	@$(STACK_COMPOSE) logs -f $(SERVICE)

stack-smoke: stack-check-env ## Probe the local Astra API health endpoint
	@API_PORT=$$(awk -F= '/^[[:space:]]*ASTRA_API_PORT[[:space:]]*=/{gsub(/^[[:space:]]+|[[:space:]]+$$/, "", $$2); print $$2}' $(STACK_ENV) | tail -1); \
	curl --noproxy '*' -fsS "http://127.0.0.1:$${API_PORT:-$(DEFAULT_API_PORT)}/health"

# ─── Clean ───────────────────────────────────────────────────────────────────

clean: ## Clean all build artifacts
	cargo clean
