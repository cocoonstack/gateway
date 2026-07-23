# Pinned: successive image builds must embed the same toolchain, not whatever
# `rust:1` floats to on the day of the build.
FROM rust:1 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p gw-server --locked

FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home gateway
COPY --from=builder /app/target/release/gw /usr/local/bin/gw
ENV GW_HOST=0.0.0.0
USER gateway
EXPOSE 8080
# Set GW_CONFIG to a mounted config path; unset uses the embedded demo config.
HEALTHCHECK --interval=30s --timeout=3s CMD curl -sf http://127.0.0.1:8080/health || exit 1
ENTRYPOINT ["/usr/local/bin/gw"]
