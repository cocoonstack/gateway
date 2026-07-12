FROM rust:1 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p ap-server --locked

FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home gateway
COPY --from=builder /app/target/release/ap /usr/local/bin/ap
ENV AP_HOST=0.0.0.0
USER gateway
EXPOSE 8080
# Set AP_GATEWAY_CONF to a mounted config path; unset uses the embedded demo config.
HEALTHCHECK --interval=30s --timeout=3s CMD curl -sf http://127.0.0.1:8080/health || exit 1
ENTRYPOINT ["/usr/local/bin/ap"]
