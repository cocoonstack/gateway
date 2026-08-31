FROM node:24.20.0-alpine@sha256:e67514e5d0f6c46656005e1b693b2ec9d52e80b641307de684d4a015ba7a4eaf AS web
WORKDIR /src/web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM golang:1.27.0-alpine3.24@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc AS backend
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
