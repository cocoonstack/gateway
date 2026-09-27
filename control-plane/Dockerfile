FROM node:24.21.0-alpine@sha256:be80f76cf40ec8e42b9bec49f60a55e0660f30af58d3e5a25530785b30ea67e2 AS web
WORKDIR /src/web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM golang:1.27.1-alpine3.24@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414 AS backend
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /control-plane ./cmd/control-plane

FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b
RUN apk add --no-cache ca-certificates \
    && adduser -D -H -u 10001 controlplane
COPY --from=backend /control-plane /usr/local/bin/control-plane
COPY --from=web /src/web/dist /srv/control-plane/web
ENV CP_LISTEN=0.0.0.0:8090 CP_WEB_DIR=/srv/control-plane/web CP_COOKIE_SECURE=true
USER controlplane
EXPOSE 8090
HEALTHCHECK --interval=30s --timeout=3s CMD wget -q -O /dev/null http://127.0.0.1:8090/api/v1/health || exit 1
ENTRYPOINT ["/usr/local/bin/control-plane"]
