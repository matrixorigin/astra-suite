# Astra Gateway — multi-stage Docker build.
#
# Build from the workspace root:
#   docker build -f crates/astra-gateway/Dockerfile -t astra-gateway .
#
# Run:
#   docker run --rm -v "$PWD/gateway.yaml:/app/gateway.yaml" \
#       -v astra-gateway-data:/data astra-gateway

FROM rust:1.85-slim AS builder

WORKDIR /build
COPY Cargo.toml ./
COPY crates/ crates/

RUN cargo build --release -p astra-gateway \
    && strip target/release/astra-gateway

# ── Runtime image ──
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r gateway && useradd -r -g gateway -d /app gateway \
    && mkdir -p /data && chown gateway:gateway /data

COPY --from=builder /build/target/release/astra-gateway /usr/local/bin/astra-gateway

WORKDIR /app
COPY crates/astra-gateway/gateway.example.yaml /app/gateway.yaml
RUN chown -R gateway:gateway /app

USER gateway
ENV RUST_LOG=info,astra_gateway=debug
# SQLite database location — mount a volume here to persist state.
VOLUME /data

ENTRYPOINT ["astra-gateway"]
CMD ["--config", "/app/gateway.yaml"]
