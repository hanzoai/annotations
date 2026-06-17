# annotations

A Hanzo Base-native Go service binary serving the **Annotations** capability —
annotation queues, queue items, and queue assignments — that the Next.js console
previously ran in-process as three tRPC routers hitting Prisma.

One of 8 parallel service-binary builds in the console tRPC→ZAP migration.

**Pattern:** Go binary built on [Hanzo Base](../base) (embedded encrypted SQLite
+ plugins) exposing a typed [ZAP](../zap) capability-RPC interface. No Prisma, no
Postgres-as-source-of-truth, no Mongo, no tRPC in the backend.

```
.zap schema (source of truth)  ──zapgen──▶  gen/ (Go views)
        │                                        │
        └──zapgen --target=ts──▶ console TS       ▼
                                   server/  ─ ZAP RPC handler (cap-gated)
                                   main.go  ─ base.New() + ZAP router :9996
                                              Base HTTP (health/metrics) :8090
                                              vault → per-org encrypted SQLite
```

## Run

```bash
make build
./annotations serve --http=127.0.0.1:8090 --zap=127.0.0.1:9996
```

Optional per-org encrypted SQLite (vault plugin): add `--vaultDir=/data/vaults`.

## Test

In-process suite (ZAP RPC + per-method coverage + permission gate + pipelining):

```bash
make test
```

## The interface — msgType 207

Nineteen methods across the three migrated routers, on a `CapKindIAMSession`
capability. Each method gates on exactly ONE of four category permission bits,
enforced at the single chokepoint `Server.authorize` (`permFor` is the
method→bit policy in one place).

| Method (ordinal) | Source procedure | Permission bit |
|---|---|---|
| `queueHasAny` @0 | queueRouter.hasAny | `AnnReadQueues` (1<<0) |
| `queueAll` @1 | queueRouter.all | `AnnReadQueues` |
| `queueNamesAndIds` @2 | queueRouter.allNamesAndIds | `AnnReadQueues` |
| `queueCount` @3 | queueRouter.count | `AnnReadQueues` |
| `queueById` @4 | queueRouter.byId | `AnnReadQueues` |
| `queueByObjectId` @5 | queueRouter.byObjectId | `AnnReadQueues` |
| `queueCreate` @6 | queueRouter.create | `AnnWriteQueues` (1<<1) |
| `queueUpdate` @7 | queueRouter.update | `AnnWriteQueues` |
| `queueDelete` @8 | queueRouter.delete | `AnnWriteQueues` |
| `itemTypeById` @9 | queueItemRouter.typeById | `AnnReadQueues` |
| `itemById` @10 | queueItemRouter.byId | `AnnReadQueues` |
| `itemsByQueueId` @11 | queueItemRouter.itemsByQueueId | `AnnReadQueues` |
| `itemUnseenCount` @12 | queueItemRouter.unseenPendingItemCountByQueueId | `AnnReadQueues` |
| `itemCreateMany` @13 | queueItemRouter.createMany | `AnnWriteQueues` |
| `itemDeleteMany` @14 | queueItemRouter.deleteMany | `AnnWriteQueues` |
| `itemComplete` @15 | queueItemRouter.complete | `AnnWriteQueues` |
| `assignCreateMany` @16 | queueAssignmentRouter.createMany | `AnnWriteAssignments` (1<<3) |
| `assignDelete` @17 | queueAssignmentRouter.delete | `AnnWriteAssignments` |
| `assignByQueueId` @18 | queueAssignmentRouter.byQueueId | `AnnReadAssignments` (1<<2) |

Item methods reuse the queue bits because every item procedure in the source
gated on the `annotationQueues:{read,CUD}` scope. Tenant isolation is by
`project` (the console projectId): every record carries it and every query is
scoped to `req.Project` after the cap gate.

Capability auth is the single chokepoint `Server.authorize`: it Wraps the opaque
capability buffer, enforces `Kind == CapKindIAMSession` and the method's required
permission bit, and (when an issuer registry is wired) verifies the signature per
[zap-spec/SPEC.md §2.3](../../zap-proto/zap-spec/SPEC.md). The signature step is
stubbed in bootstrap (TODO in `server.go`) — Kind + Permissions are always
enforced.

## Pipelining

`itemCreateMany` can target `queueCreate`'s promise: ship the create, and ship
the dependent item-create (with an empty queueId) on a SECOND connection
referencing the create's promise id. The server resolves the new queue's id from
the create's answer and substitutes it as the items' queueId — Cap'n Proto
promise pipelining, no round trip to learn the id. See `Client.PipelineCreateQueueThenItems`
and `TestPipeliningCreateQueueThenItems` (which proves, on a shared send log, that
the dependent call ships before the create answers).

## Wiring the console to this service (handoff)

The console keeps its existing TS client (same `.zap` contract, compiled to TS
by `zapgen --target=ts`). Its ZAP bridge substitutes the in-process routers for
a ZAP client to this service:

1. Set the service URL in console's runtime: `ANNOTATIONS_ZAP_URL=tcp://127.0.0.1:9996`.
2. Route the annotation-queue/-item/-assignment tRPC procedures to a ZAP client
   dialed at that URL instead of the in-process routers.
3. The wire envelope is `(method:u32, promiseID:u32, target:u32, cap:bytes,
   payload:bytes)` at ZAP msgType **207**; responses are `(status:u32,
   promiseID:u32, body:bytes)`. Method ordinals match the table above. The TS
   client encodes the cap as the opaque ZAP capability buffer and the body as the
   zapgen-compiled struct bytes.

This repo does **not** edit console.

## Layout

| Path | What |
|------|------|
| `proto/annotations.zap` | Canonical schema (zap-spec dialect → Go). |
| `gen/` | zapgen output (`make zap-gen`). Generated; do not edit. |
| `server/wire.go` | Transport envelope codec + msgType/method/status consts. |
| `server/server.go` | ZAP RPC handler: cap auth, dispatch, 19 Base ops, promise pipelining. |
| `server/collections.go` | Three Base collection schemas (queues/items/assignments). |
| `server/query.go` | Project/queue scoping predicates (one place). |
| `server/client.go` | Typed client wrappers + pipelining chain + `SyntheticCap`. |
| `main.go` | Binary: `base.New()` + vault + ZAP router (NodeID `annotations`, :9996). |

Container registry: `ghcr.io/hanzoai/annotations` (CI-built, multi-arch).
