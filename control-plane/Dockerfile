FROM node:24.21.0-alpine@sha256:ebfe2f90462722a7a4de65e91990e97fe0d401c70e0e762c5b53302f905ec1c1 AS web
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

FROM alpine:3.24@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6
RUN apk add --no-cache ca-certificates \
    && adduser -D -H -u 10001 controlplane
COPY --from=backend /control-plane /usr/local/bin/control-plane
COPY --from=web /src/web/dist /srv/control-plane/web
ENV CP_LISTEN=0.0.0.0:8090 CP_WEB_DIR=/srv/control-plane/web CP_COOKIE_SECURE=true
USER controlplane
EXPOSE 8090
HEALTHCHECK --interval=30s --timeout=3s CMD wget -q -O /dev/null http://127.0.0.1:8090/api/v1/health || exit 1
ENTRYPOINT ["/usr/local/bin/control-plane"]
