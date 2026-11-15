FROM node:24.22.2-alpine@sha256:8a32c8cf649a49915a66e81e7a609223c708d0adabf288ee3f4077d765542f85 AS web
WORKDIR /src/web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM golang:1.27.4-alpine3.24@sha256:0a5f7fe796599ff487605987432e7619ae45beeb2fba4c2ce0b907879932aebf AS backend
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
