# annotations.zap — canonical wire schema for the Annotations service.
#
# Dialect: zap-spec (the `package … / Field Type @off` grammar that
# github.com/zap-proto/go/cmd/zapgen consumes). The SAME dialect the capability
# schema (zap-spec/capabilities.zap) is written in. ONE schema, two code targets:
# this Go service binary compiles it with `zapgen` (default target → Go views);
# the console copies it verbatim and compiles with `zapgen --target=ts`. No capnp
# on either side — the field set below is the single byte-for-byte contract.
#
# RPC surface (hand-dispatched in server/, exactly as cap/ hand-writes Verify on
# top of zapgen'd views — zapgen emits data views, never method stubs). Replaces
# THREE console tRPC routers: queueRouter, queueItemRouter, queueAssignmentRouter.
#
#   interface Annotations @ MsgTypeRouterBase (207) {
#     # queueRouter
#     queueHasAny       @0  (ProjectScope)        -> (BoolResult)        # AnnReadQueues
#     queueAll          @1  (Page)                -> (QueueList)         # AnnReadQueues
#     queueNamesAndIds  @2  (ProjectScope)        -> (QueueRefList)      # AnnReadQueues
#     queueCount        @3  (ProjectScope)        -> (CountResult)       # AnnReadQueues
#     queueById         @4  (QueueLookup)         -> (Queue)             # AnnReadQueues
#     queueByObjectId   @5  (ObjectRef)           -> (QueueObjectList)   # AnnReadQueues
#     queueCreate       @6  (QueueWrite)          -> (Queue)             # AnnWriteQueues
#     queueUpdate       @7  (QueueWrite)          -> (Queue)             # AnnWriteQueues
#     queueDelete       @8  (QueueLookup)         -> (Queue)             # AnnWriteQueues
#     # queueItemRouter
#     itemTypeById      @9  (ItemRef)             -> (TextResult)        # AnnReadQueues
#     itemById          @10 (ItemRef)             -> (QueueItem)         # AnnReadQueues
#     itemsByQueueId    @11 (QueuePage)           -> (QueueItemList)     # AnnReadQueues
#     itemUnseenCount   @12 (SeenScope)           -> (CountResult)       # AnnReadQueues
#     itemCreateMany    @13 (ItemCreateMany)      -> (CreateManyResult)  # AnnWriteQueues
#     itemDeleteMany    @14 (ItemIds)             -> (CountResult)       # AnnWriteQueues
#     itemComplete      @15 (ItemRef)             -> (QueueItem)         # AnnWriteQueues
#     # queueAssignmentRouter
#     assignCreateMany  @16 (AssignWrite)         -> (AssignResult)      # AnnWriteAssignments
#     assignDelete      @17 (AssignOne)           -> (BoolResult)        # AnnWriteAssignments
#     assignByQueueId   @18 (QueuePage)           -> (AssignList)        # AnnReadAssignments
#   }
#
# Permission model: the caller's verified Capability (CapKindIAMSession = 0x01)
# carries a u64 Permissions bitmask. Methods gate on one of four category bits
# via the single chokepoint server.requirePermission(cap, bit):
#
#   AnnReadQueues       1 << 0   queue + item reads (items live under the
#                                annotationQueues:read scope in console)
#   AnnWriteQueues      1 << 1   queue + item create/update/delete
#                                (annotationQueues:CUD)
#   AnnReadAssignments  1 << 2   assignment reads (annotationQueueAssignments:read)
#   AnnWriteAssignments 1 << 3   assignment create/delete
#                                (annotationQueueAssignments:CUD)
#
# Org scoping: every record carries `project` (the console projectId — the tenant
# key here); the service scopes all queries to req.Project after the cap gate.

package ann

# ─────────────────────────── entity structs ───────────────────────────
# Queue mirrors the annotation_queues row the queueRouter read. scoreConfigIds is
# a JSON-array-of-strings column, surfaced as list<text>. countCompletedItems /
# countPendingItems are computed (aggregate join in `all`); zero on plain reads.
struct Queue {
    Id                  text         @0
    Project             text         @8
    Name                text         @16
    Description         text         @24
    ScoreConfigIds      list<text>   @32
    CreatedAt           i64          @40    # unix millis
    CountCompletedItems u64          @48
    CountPendingItems   u64          @56
    IsCurrentUserAssign bool         @64
}

# QueueItem mirrors an annotation_queue_items row. Status/ObjectType are the
# Prisma string enums (PENDING|COMPLETED, TRACE|SESSION|OBSERVATION). lockedAt /
# completedAt are unix millis (0 = null). ParentTraceId is the resolved trace for
# OBSERVATION items (empty otherwise).
struct QueueItem {
    Id              text   @0
    Project         text   @8
    QueueId         text   @16
    ObjectId        text   @24
    ObjectType      text   @32
    Status          text   @40
    CreatedAt       i64    @48
    CompletedAt     i64    @56
    LockedAt        i64    @64
    LockedByUserId  text   @72
    LockedByUser    text   @80    # display name of the locking user (empty if unlocked)
    AnnotatorUserId text   @88
    ParentTraceId   text   @96
}

# Assignment is the projected user shape assignByQueueId returns (id/name/email
# of an assigned user) — the assignment join already resolved to its user.
struct Assignment {
    UserId text @0
    Name   text @8
    Email  text @16
}

# QueueRef is the {id,name} projection queueNamesAndIds returns.
struct QueueRef {
    Id   text @0
    Name text @8
}

# QueueObjectHit is one queueByObjectId row: queue id+name plus the first
# matching item's id+status (empty when the queue has no item for the object).
struct QueueObjectHit {
    QueueId   text @0
    QueueName text @8
    ItemId    text @16
    Status    text @24
}

# ─────────────────────────── request structs ───────────────────────────
# ProjectScope is the bare {project} input (queueHasAny, queueNamesAndIds,
# queueCount). Project is the tenant key; the cap gate runs before any read.
struct ProjectScope {
    Project text @0
}

# Page is {project, limit, page} for queueAll. Limit 0 = unbounded (mirrors the
# optional pagination: no LIMIT clause emitted).
struct Page {
    Project text @0
    Limit   u32  @8
    Page    u32  @12
}

# QueueLookup is {project, queueId} for queueById / queueDelete.
struct QueueLookup {
    Project text @0
    QueueId text @8
}

# QueuePage is {project, queueId, limit, page} for itemsByQueueId / assignByQueueId.
struct QueuePage {
    Project text @0
    QueueId text @8
    Limit   u32  @16
    Page    u32  @20
}

# ObjectRef is {project, objectId, objectType} for queueByObjectId.
struct ObjectRef {
    Project    text @0
    ObjectId   text @8
    ObjectType text @16
}

# QueueWrite is the create/update body (CreateQueueData + project [+ queueId on
# update]). Empty QueueId ⇒ create; non-empty ⇒ update that queue.
struct QueueWrite {
    Project        text       @0
    QueueId        text       @8
    Name           text       @16
    Description    text       @24
    ScoreConfigIds list<text> @32
}

# ItemRef is {project, itemId[, queueId]} for itemTypeById / itemById / itemComplete.
struct ItemRef {
    Project text @0
    ItemId  text @8
    QueueId text @16
}

# SeenScope is {project, queueId, seenItemIds} for itemUnseenCount.
struct SeenScope {
    Project     text       @0
    QueueId     text       @8
    SeenItemIds list<text> @16
}

# ItemCreateMany is {project, queueId, objectIds, objectType} for itemCreateMany.
# The console batch-action path is a separate worker concern; this service does
# the direct createMany insert and returns the created count + queue ref.
struct ItemCreateMany {
    Project    text       @0
    QueueId    text       @8
    ObjectIds  list<text> @16
    ObjectType text       @24
}

# ItemIds is {project, itemIds} for itemDeleteMany.
struct ItemIds {
    Project text       @0
    ItemIds list<text> @8
}

# AssignWrite is {project, queueId, userIds} for assignCreateMany.
struct AssignWrite {
    Project text       @0
    QueueId text       @8
    UserIds list<text> @16
}

# AssignOne is {project, queueId, userId} for assignDelete.
struct AssignOne {
    Project text @0
    QueueId text @8
    UserId  text @16
}

# ─────────────────────────── response structs ───────────────────────────
# QueueList is queueAll's {queues, totalCount}. Elements are Queue sub-buffers.
struct QueueList {
    Queues     list<Queue> @0
    TotalCount u64         @8
}

# QueueRefList is queueNamesAndIds' [{id,name}].
struct QueueRefList {
    Refs list<QueueRef> @0
}

# QueueObjectList is queueByObjectId's {queues, totalCount}.
struct QueueObjectList {
    Hits       list<QueueObjectHit> @0
    TotalCount u64                  @8
}

# QueueItemList is itemsByQueueId's {queueItems, totalItems}.
struct QueueItemList {
    Items      list<QueueItem> @0
    TotalCount u64             @8
}

# AssignList is assignByQueueId's {assignments, totalCount}.
struct AssignList {
    Assignments list<Assignment> @0
    TotalCount  u64              @8
}

# CreateManyResult is itemCreateMany's {createdCount, queueName, queueId}.
struct CreateManyResult {
    CreatedCount u64  @0
    QueueName    text @8
    QueueId      text @16
}

# AssignResult is assignCreateMany's {success, addedCount, skippedCount}.
struct AssignResult {
    Success      bool @0
    AddedCount   u64  @8
    SkippedCount u64  @16
}

# Scalar result wrappers — one shape each so every method returns a typed body.
struct BoolResult {
    Value bool @0
}

struct CountResult {
    Count u64 @0
}

struct TextResult {
    Value text @0
}
