# syntax=docker/dockerfile:1
#
# annotations — Hanzo Base-native Go service binary (annotation queues, items,
# assignments). Single pure-Go binary: Base (embedded SQLite + vault) + typed
# ZAP router. Replaces three console tRPC routers.
#
# CI builds this multi-arch (linux/amd64 + linux/arm64) on the hanzoai
# self-hosted runners → ghcr.io/hanzoai/annotations. Do NOT build locally.
#
# NOTE on go.mod replaces: the working-copy go.mod carries `replace` directives
# to ../base and ../../zap-proto/go for local dev while those modules' proxy
# availability is flaky. CI builds against the PUBLISHED modules — the release
# pipeline drops the replaces (the modules resolve via GOPROXY) so this build
# context needs only this repo. If you must build with the replaces, use a
# monorepo build context that includes the sibling modules.
FROM golang:1.26.5-alpine AS builder
RUN apk add --no-cache git ca-certificates tzdata
WORKDIR /build
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .

# JSON v2 per SCALE_STANDARD.md §2: error/results bodies serialize via
# encoding/json; jsonv2 trims time + allocs on the hot path.
ARG GO_EXPERIMENT=jsonv2
ENV GOEXPERIMENT=${GO_EXPERIMENT}

# Pure-Go (CGO off): modernc SQLite + luxfi/zap need no C toolchain; the binary
# is statically linked and cross-compiles cleanly for both target arches.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w" \
    -o /build/annotations .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata curl \
    && addgroup -S hanzo && adduser -S hanzo -G hanzo
WORKDIR /app
COPY --from=builder /build/annotations /app/annotations
RUN mkdir -p /data /vaults && chown -R hanzo:hanzo /app /data /vaults
USER hanzo
# 8090 = Base sidecar HTTP (health/metrics); 9996 = typed ZAP capability RPC.
EXPOSE 8090 9996
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8090/healthz || exit 1
ENTRYPOINT ["/app/annotations"]
CMD ["serve", "--http=0.0.0.0:8090", "--zap=0.0.0.0:9996", "--dir=/data", "--vaultDir=/vaults"]
