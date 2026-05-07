# Multi-stage build for astra-gateway

# ── Stage 1: build ──────────────────────────────────────────────────────────
ARG RUST_VERSION=1.88
FROM rust:${RUST_VERSION}-slim AS builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy manifests first for dependency caching
COPY Cargo.toml Cargo.lock ./
COPY crates/astra/Cargo.toml crates/astra/Cargo.toml
COPY crates/astra-task-store/Cargo.toml crates/astra-task-store/Cargo.toml
COPY crates/astra-gateway/Cargo.toml crates/astra-gateway/Cargo.toml

# Create dummy src files for dep caching
RUN for d in crates/*/; do mkdir -p "$d/src" && echo "" > "$d/src/lib.rs"; done && \
    mkdir -p crates/astra-gateway/src && echo "fn main(){}" > crates/astra-gateway/src/main.rs

# Build deps only (cached layer)
RUN cargo build --release -p astra-gateway || true

# Copy real source and build
COPY crates/ crates/
RUN cargo build --release -p astra-gateway

# ── Stage 2: runtime ───────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/* && \
    useradd -m -u 1000 appuser && \
    mkdir -p /data /config && chown appuser:appuser /data /config

COPY --from=builder /build/target/release/astra-gateway /usr/local/bin/

USER appuser
WORKDIR /config

ENTRYPOINT ["astra-gateway"]
CMD ["--config", "/config/gateway.yaml"]
