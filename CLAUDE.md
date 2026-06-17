# CLAUDE.md — contract for AI helpers

This is the Annotations Hanzo Base-native Go service: annotation queues, queue
items, and queue assignments over typed ZAP capability RPC. Cloned from the
`ui-customization` reference service; keep it lean and exemplary.

## The one rule

**The `.zap` schema is the source of truth.** `proto/annotations.zap` defines
the data structs; `gen/` is its Go projection via `make zap-gen`. Never hand-edit
`gen/`. Change the schema, regenerate, then update `server/`.

## Two schema dialects (do not conflate)

- `proto/annotations.zap` is the **zap-spec dialect** (`package … / Field Type
  @off`) that `github.com/zap-proto/go/cmd/zapgen` compiles to Go. This is what
  THIS repo (a Go service) consumes.
- The console compiles the SAME file with `zapgen --target=ts` to TS View/Builder
  classes. One schema, two code targets. No capnp on either side.

## Build / test

- Pure-Go always: `CGO_ENABLED=0`. CGO pulls blst/accel C deps that need a full
  toolchain and break reproducibility. `make` sets this + `GOWORK=off`.
- `GOWORK=off`: this repo is self-contained via go.mod `replace`s; a parent
  `go.work` must not capture it.
- `make zap-gen` and `make build` are **idempotent** (byte-identical regen,
  reproducible binary). Keep them so.
- `make test` runs the in-process ZAP RPC + per-method + permission + pipelining
  suite. Show it passing; don't claim "done" without it.

## Architecture invariants (DRY, orthogonal, decomplected)

- **Three wire layers, separated:** transport (`luxfi/zap` Node, msgType 207) /
  envelope (`server/wire.go`) / payload (`gen/` typed views). The capability is
  carried as OPAQUE bytes through all three — auth is a value, not a place.
- **One auth chokepoint:** `Server.authorize`, parametrized by the method's
  required permission bit. `permFor` is the method→bit policy in ONE place. Kind +
  the required bit are always enforced; signature verify is gated on a wired
  issuer registry (TODO → SPEC.md §2.3). Do NOT scatter permission checks into
  the handlers.
- **Four permission bits, one per RBAC scope:** `AnnReadQueues` (1<<0),
  `AnnWriteQueues` (1<<1), `AnnReadAssignments` (1<<2), `AnnWriteAssignments`
  (1<<3). Items reuse the queue bits (their source procedures gated on the
  `annotationQueues` scope).
- **One backend:** Hanzo Base. No Prisma, Postgres-as-source-of-truth, Mongo,
  Redis, tRPC, nginx. Data lives in three Base collections (`annotation_queues`,
  `annotation_queue_items`, `annotation_queue_assignments`), encrypted SQLite via
  the vault plugin when `--vaultDir` is set.
- **One scoping place:** `server/query.go` holds the project/queue predicates in
  both the filter-string and dbx-expression forms Base needs.
- **Pipelining is real:** `itemCreateMany` can target `queueCreate`'s promise; the
  server's promise table (`await`/`resolve`) resolves the new queueId and fills it
  into the dependent call. Genuine in-flight pipelining needs the two calls on
  SEPARATE connections (the transport is FIFO per connection) — see
  `Client.PipelineCreateQueueThenItems` and the pipelining test.

## Tenant model

Every record carries `project` (the console projectId). The service scopes every
query to `req.Project` AFTER the cap gate. There is no cross-project read path.

## Do not

- Build Docker images locally (CI does, multi-arch → ghcr.io/hanzoai/annotations).
- Push to GitHub from here unless asked.
- Add plugins this one service doesn't need. Lean binary.
- Touch the console — wiring is documented in README, not done here.
