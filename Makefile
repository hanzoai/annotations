# annotations — build + codegen.
#
# The .zap schema (proto/) is the source of truth; gen/ is its Go projection.
# `make zap-gen` and `make build` are idempotent: re-running produces
# byte-identical output (zapgen is deterministic; CGO is off so the binary is
# pure-Go and reproducible).

# Pure-Go: modernc SQLite + luxfi/zap have no required C deps for this service,
# and CGO pulls blst/accel C headers that need a full toolchain. Off = lean,
# reproducible, cross-compilable.
export CGO_ENABLED := 0
# This repo is self-contained via go.mod replaces; ignore any parent go.work.
export GOWORK := off

BIN      := annotations
SCHEMA   := proto/annotations.zap
GEN_DIR  := gen
ZAPGEN   := github.com/zap-proto/go/cmd/zapgen

.PHONY: all build zap-gen test vet tidy clean run

all: zap-gen build test

## build: compile the service binary (pure-Go, reproducible).
build:
	go build -trimpath -o $(BIN) .

## zap-gen: regenerate Go views from the .zap schema. Idempotent.
zap-gen:
	go run $(ZAPGEN) -out $(GEN_DIR) -single $(SCHEMA)
	gofmt -w $(GEN_DIR)

## test: run the in-process ZAP RPC + permission + pipelining suite.
test:
	go test ./... -count=1

## vet: static checks.
vet:
	go vet ./...

## tidy: resolve module graph.
tidy:
	go mod tidy

## run: start the service (HTTP :8090, ZAP :9996).
run: build
	./$(BIN) serve --http=127.0.0.1:8090 --zap=127.0.0.1:9996

clean:
	rm -f $(BIN)
