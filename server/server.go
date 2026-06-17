package server

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/hanzoai/base/core"
	"github.com/hanzoai/base/tools/types"
	luxlog "github.com/luxfi/log"
	zaplib "github.com/luxfi/zap"
	zapproto "github.com/zap-proto/go"
	zcap "github.com/zap-proto/go/cap"

	gen "github.com/hanzoai/annotations/gen"
)

// AnnPermissions — the four category bits a CapKindIAMSession capability carries
// in its u64 Permissions bitmask, one per RBAC scope the source routers gated
// on. The single chokepoint requirePermission(cap, bit) enforces exactly one of
// these per method (see permFor). Items reuse the queue bits because every item
// procedure in the source gated on the annotationQueues scope.
const (
	AnnReadQueues       uint64 = 1 << 0 // annotationQueues:read   (queue + item reads)
	AnnWriteQueues      uint64 = 1 << 1 // annotationQueues:CUD    (queue + item writes)
	AnnReadAssignments  uint64 = 1 << 2 // annotationQueueAssignments:read
	AnnWriteAssignments uint64 = 1 << 3 // annotationQueueAssignments:CUD
)

// permFor maps a method ordinal to the single permission bit it requires. This
// is the method→bit policy in ONE place — handlers never re-check permissions.
func permFor(method uint32) (uint64, bool) {
	switch method {
	case MethodQueueHasAny, MethodQueueAll, MethodQueueNamesAndIds, MethodQueueCount,
		MethodQueueById, MethodQueueByObjectId,
		MethodItemTypeById, MethodItemById, MethodItemsByQueueId, MethodItemUnseenCount:
		return AnnReadQueues, true
	case MethodQueueCreate, MethodQueueUpdate, MethodQueueDelete,
		MethodItemCreateMany, MethodItemDeleteMany, MethodItemComplete:
		return AnnWriteQueues, true
	case MethodAssignByQueueId:
		return AnnReadAssignments, true
	case MethodAssignCreateMany, MethodAssignDelete:
		return AnnWriteAssignments, true
	default:
		return 0, false
	}
}

// Server implements the Annotations ZAP capability-RPC interface on top of a
// Base app. One method per tRPC procedure across the three source routers, each
// gated on the caller's capability via the single chokepoint requirePermission,
// then reading/writing the Base collections scoped to the request's project.
type Server struct {
	app    core.App
	logger luxlog.Logger

	// verifier validates capability buffers. Wired to ed25519 (the bootstrap
	// scheme); a PQ deployment swaps in an ML-DSA-65 SchemeVerify + the IAM
	// pubkey registry for IssuerKey. See zap-spec/SPEC.md §2.3.
	verifier zcap.Verifier

	// promises is the server-side pipelining table: a call may carry PromiseID,
	// and a later call may Target it. Promises are FUTURES — a dependent call
	// that arrives before its target resolves WAITS on the target (it is not
	// rejected), then dispatches against the resolved answer. This is Cap'n
	// Proto promise pipelining. Entries are short-lived (one connection turn).
	mu       sync.Mutex
	promises map[uint32]*promiseSlot
}

// promiseSlot is a future for a pipelined call's answer. done is closed when the
// slot resolves; the resolved value is then readable. For Annotations the
// pipelined value is the queueId produced by an earlier call (e.g. queueCreate),
// which a dependent itemCreateMany targets. resolvedAt drives reaping.
type promiseSlot struct {
	done       chan struct{}
	value      string // the resolved queueId (or "" when none)
	resolvedAt time.Time
}

// promiseWaitTimeout bounds how long a dependent call waits for its target to
// resolve before failing. Generous relative to a same-connection turn.
const promiseWaitTimeout = 5 * time.Second

// getOrCreate returns the slot for id, creating an unresolved one if absent.
func (s *Server) getOrCreate(id uint32) *promiseSlot {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reapLocked()
	slot, ok := s.promises[id]
	if !ok {
		slot = &promiseSlot{done: make(chan struct{})}
		s.promises[id] = slot
	}
	return slot
}

// reapLocked drops promise slots that resolved more than promiseWaitTimeout ago.
// Caller must hold s.mu.
func (s *Server) reapLocked() {
	cutoff := time.Now().Add(-promiseWaitTimeout)
	for id, slot := range s.promises {
		if !slot.resolvedAt.IsZero() && slot.resolvedAt.Before(cutoff) {
			delete(s.promises, id)
		}
	}
}

// resolve fills a promise slot with its answer and wakes any waiters. Safe to
// call once per slot; a double-resolve is guarded under the lock.
func (s *Server) resolve(id uint32, value string) {
	slot := s.getOrCreate(id)
	s.mu.Lock()
	select {
	case <-slot.done:
		// already resolved — leave as-is
	default:
		slot.value = value
		slot.resolvedAt = time.Now()
		close(slot.done)
	}
	s.mu.Unlock()
}

// await blocks until the target promise resolves or the timeout elapses,
// returning the resolved value.
func (s *Server) await(target uint32) (string, bool) {
	slot := s.getOrCreate(target)
	select {
	case <-slot.done:
		s.mu.Lock()
		v := slot.value
		s.mu.Unlock()
		return v, true
	case <-time.After(promiseWaitTimeout):
		return "", false
	}
}

// NewServer builds an Annotations server. verifier supplies the capability trust
// anchor; pass a Verifier whose IssuerKey resolves your IAM issuer key.
func NewServer(app core.App, logger luxlog.Logger, verifier zcap.Verifier) *Server {
	return &Server{
		app:      app,
		logger:   logger,
		verifier: verifier,
		promises: make(map[uint32]*promiseSlot),
	}
}

// Register wires the server's handler onto a luxfi/zap node at this service's
// message-type slot. Called from main once the node is constructed.
func (s *Server) Register(node *zaplib.Node) {
	node.Handle(MsgTypeRouterBase, s.handle)
}

// handle is the ZAP dispatch entrypoint: decode envelope → authorize → route.
func (s *Server) handle(ctx context.Context, from string, msg *zaplib.Message) (*zaplib.Message, error) {
	req := parseRequest(msg)

	bit, ok := permFor(req.Method)
	if !ok {
		return buildResponse(StatusBadRequest, req.PromiseID, errorBody(fmt.Sprintf("unknown method %d", req.Method)))
	}

	// Single auth chokepoint: Wrap the opaque cap, enforce Kind + the method's
	// required permission bit, and (when an issuer registry is wired) verify the
	// signature. Pipelining elides round trips, never authorization.
	if status, errMsg := s.authorize(req, bit); status != StatusOK {
		s.logger.Debug("ann: auth rejected", "from", from, "method", req.Method, "status", status, "err", errMsg)
		return buildResponse(status, req.PromiseID, errorBody(errMsg))
	}

	// Pipelining join: if this call targets an earlier promise, WAIT for that
	// promise's answer (a queueId produced by, e.g., queueCreate) and pass it to
	// the handler. The dependent call referenced the not-yet-created queue's id
	// before it existed — Cap'n Proto promise pipelining. The transport ships
	// frames FIFO per connection, so genuine overlap needs the two calls on
	// SEPARATE connections (see client.PipelineCreateQueueThenItems); the await
	// then blocks here until the create resolves the promise on its connection.
	pipelinedQueueID := ""
	if req.Target != NoTarget {
		v, ok := s.await(req.Target)
		if !ok {
			return buildResponse(StatusBadRequest, req.PromiseID,
				errorBody(fmt.Sprintf("pipelined target %d did not resolve in time", req.Target)))
		}
		pipelinedQueueID = v
	}

	// route returns the typed body + status; queueID is the value this call's
	// own promise resolves to (a queueId for the pipeline join), resolved here so
	// a dependent call WAITING on it proceeds.
	body, queueID, status, errMsg := s.route(req, pipelinedQueueID)
	if req.PromiseID != NoTarget {
		s.resolve(req.PromiseID, queueID)
	}
	if status != StatusOK {
		return buildResponse(status, req.PromiseID, errorBody(errMsg))
	}
	return buildResponse(StatusOK, req.PromiseID, body)
}

// authorize enforces the capability: it must be a CapKindIAMSession holding the
// required permission bit, and (when an issuer registry is wired) carry a valid
// signature. Returns (StatusOK, "") on success.
func (s *Server) authorize(req Call, requiredBit uint64) (status uint32, errMsg string) {
	c, err := zcap.Wrap(req.Cap)
	if err != nil {
		return StatusBadRequest, "malformed capability: " + err.Error()
	}
	if c.Kind() != uint32(zcap.KindIAMSession) {
		return StatusForbidden, "capability is not a CapKindIAMSession"
	}
	if c.Permissions()&requiredBit == 0 {
		return StatusForbidden, fmt.Sprintf("capability lacks required permission bit 0x%x", requiredBit)
	}
	// Cryptographic verification runs whenever an issuer registry is wired; with
	// no registry (bootstrap/tests) the signature step is skipped but Kind +
	// Permissions above are STILL enforced.
	//
	// TODO(SPEC.md §2.3): bind the cap to the live session via the out-of-band
	// holderSig over a server nonce, and walk the parent chain with
	// verifier.VerifyChain once the IAM pubkey registry is wired here.
	if s.verifier.IssuerKey != nil {
		if err := s.verifier.Verify(c, time.Now().Unix()); err != nil {
			return StatusUnauthorized, "capability verify failed: " + err.Error()
		}
	}
	return StatusOK, ""
}

// route dispatches an authorized call to its handler. Returns the typed body, a
// queueID (the value a pipelined dependent call resolves against — set by the
// queue create/update/delete and item-create paths, "" otherwise), a status,
// and an error message when status != OK. pipelinedQueueID is the resolved
// answer of a targeted promise (empty when the call does not pipeline); only
// itemCreateMany consumes it (it can act on a queue created by an earlier call).
func (s *Server) route(req Call, pipelinedQueueID string) (body []byte, queueID string, status uint32, errMsg string) {
	switch req.Method {
	// queueRouter
	case MethodQueueHasAny:
		return s.queueHasAny(req)
	case MethodQueueAll:
		return s.queueAll(req)
	case MethodQueueNamesAndIds:
		return s.queueNamesAndIds(req)
	case MethodQueueCount:
		return s.queueCount(req)
	case MethodQueueById:
		return s.queueById(req)
	case MethodQueueByObjectId:
		return s.queueByObjectId(req)
	case MethodQueueCreate:
		return s.queueCreate(req)
	case MethodQueueUpdate:
		return s.queueUpdate(req)
	case MethodQueueDelete:
		return s.queueDelete(req)
	// queueItemRouter
	case MethodItemTypeById:
		return s.itemTypeById(req)
	case MethodItemById:
		return s.itemById(req)
	case MethodItemsByQueueId:
		return s.itemsByQueueId(req)
	case MethodItemUnseenCount:
		return s.itemUnseenCount(req)
	case MethodItemCreateMany:
		return s.itemCreateMany(req, pipelinedQueueID)
	case MethodItemDeleteMany:
		return s.itemDeleteMany(req)
	case MethodItemComplete:
		return s.itemComplete(req)
	// queueAssignmentRouter
	case MethodAssignCreateMany:
		return s.assignCreateMany(req)
	case MethodAssignDelete:
		return s.assignDelete(req)
	case MethodAssignByQueueId:
		return s.assignByQueueId(req)
	default:
		return nil, "", StatusBadRequest, fmt.Sprintf("unknown method %d", req.Method)
	}
}

// ──────────────────────────── queueRouter ────────────────────────────

// queueHasAny: does the project have any queue? (queueRouter.hasAny)
func (s *Server) queueHasAny(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapProjectScope(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad ProjectScope: " + err.Error()
	}
	n, err := s.app.CountRecords(ColQueues, byProject(in.Project()))
	if err != nil {
		return nil, "", StatusInternal, "count queues: " + err.Error()
	}
	return gen.NewBoolResult(gen.BoolResultInput{Value: n > 0}), "", StatusOK, ""
}

// queueAll: paginated queues with item counts + assignment flag. (queueRouter.all)
func (s *Server) queueAll(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapPage(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad Page: " + err.Error()
	}
	project := in.Project()
	recs, err := s.app.FindRecordsByFilter(ColQueues, projectFilter, "-created",
		int(in.Limit()), pageOffset(in.Page(), in.Limit()), projectParam(project))
	if err != nil {
		return nil, "", StatusInternal, "list queues: " + err.Error()
	}
	total, err := s.app.CountRecords(ColQueues, byProject(project))
	if err != nil {
		return nil, "", StatusInternal, "count queues: " + err.Error()
	}

	queues := make([][]byte, 0, len(recs))
	for _, rec := range recs {
		completed, pending := s.itemCounts(project, rec.Id)
		queues = append(queues, gen.NewQueue(gen.QueueInput{
			Id:                  rec.Id,
			Project:             project,
			Name:                rec.GetString(qName),
			Description:         rec.GetString(qDescription),
			ScoreConfigIds:      textList(rec.GetStringSlice(qScoreConfigIds)),
			CreatedAt:           createdMillis(rec),
			CountCompletedItems: completed,
			CountPendingItems:   pending,
		}))
	}
	body := gen.NewQueueList(gen.QueueListInput{Queues: queues, TotalCount: uint64(total)})
	return body, "", StatusOK, ""
}

// queueNamesAndIds: [{id,name}] for the project. (queueRouter.allNamesAndIds)
func (s *Server) queueNamesAndIds(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapProjectScope(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad ProjectScope: " + err.Error()
	}
	recs, err := s.app.FindRecordsByFilter(ColQueues, projectFilter, "-created", 0, 0, projectParam(in.Project()))
	if err != nil {
		return nil, "", StatusInternal, "list queues: " + err.Error()
	}
	refs := make([][]byte, 0, len(recs))
	for _, rec := range recs {
		refs = append(refs, gen.NewQueueRef(gen.QueueRefInput{Id: rec.Id, Name: rec.GetString(qName)}))
	}
	return gen.NewQueueRefList(gen.QueueRefListInput{Refs: refs}), "", StatusOK, ""
}

// queueCount: number of queues in the project. (queueRouter.count)
func (s *Server) queueCount(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapProjectScope(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad ProjectScope: " + err.Error()
	}
	n, err := s.app.CountRecords(ColQueues, byProject(in.Project()))
	if err != nil {
		return nil, "", StatusInternal, "count queues: " + err.Error()
	}
	return gen.NewCountResult(gen.CountResultInput{Count: uint64(n)}), "", StatusOK, ""
}

// queueById: a single queue by id within the project. (queueRouter.byId)
func (s *Server) queueById(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapQueueLookup(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad QueueLookup: " + err.Error()
	}
	rec, err := s.queueRecord(in.Project(), in.QueueId())
	if err != nil {
		return nil, "", StatusNotFound, "queue not found"
	}
	return s.queueBody(rec), in.QueueId(), StatusOK, ""
}

// queueByObjectId: for an object, which queues contain it + the first item's
// id/status. (queueRouter.byObjectId)
func (s *Server) queueByObjectId(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapObjectRef(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad ObjectRef: " + err.Error()
	}
	project := in.Project()
	queues, err := s.app.FindRecordsByFilter(ColQueues, projectFilter, "-created", 0, 0, projectParam(project))
	if err != nil {
		return nil, "", StatusInternal, "list queues: " + err.Error()
	}
	hits := make([][]byte, 0, len(queues))
	var total uint64
	for _, q := range queues {
		items, err := s.app.FindRecordsByFilter(ColQueueItems,
			"project = {:project} && queueId = {:queueId} && objectId = {:objectId} && objectType = {:objectType}",
			"created", 0, 0, map[string]any{
				"project": project, "queueId": q.Id,
				"objectId": in.ObjectId(), "objectType": in.ObjectType(),
			})
		if err != nil {
			return nil, "", StatusInternal, "match items: " + err.Error()
		}
		total += uint64(len(items))
		hit := gen.QueueObjectHitInput{QueueId: q.Id, QueueName: q.GetString(qName)}
		if len(items) > 0 { // first item only (source selects [0])
			hit.ItemId = items[0].Id
			hit.Status = items[0].GetString(iStatus)
		}
		hits = append(hits, gen.NewQueueObjectHit(hit))
	}
	return gen.NewQueueObjectList(gen.QueueObjectListInput{Hits: hits, TotalCount: total}), "", StatusOK, ""
}

// queueCreate: create a queue (unique name per project). (queueRouter.create)
func (s *Server) queueCreate(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapQueueWrite(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad QueueWrite: " + err.Error()
	}
	project := in.Project()
	if dup, _ := s.app.FindFirstRecordByFilter(ColQueues,
		"project = {:project} && name = {:name}",
		map[string]any{"project": project, "name": in.Name()}); dup != nil {
		return nil, "", StatusConflict, "a queue with this name already exists in the project"
	}
	col, err := s.app.FindCollectionByNameOrId(ColQueues)
	if err != nil {
		return nil, "", StatusInternal, err.Error()
	}
	rec := core.NewRecord(col)
	rec.Set(qProject, project)
	rec.Set(qName, in.Name())
	rec.Set(qDescription, in.Description())
	rec.Set(qScoreConfigIds, listToStrings(in.ScoreConfigIds()))
	if err := s.app.Save(rec); err != nil {
		return nil, "", StatusInternal, "create queue: " + err.Error()
	}
	return s.queueBody(rec), rec.Id, StatusOK, ""
}

// queueUpdate: update a queue's fields. (queueRouter.update)
func (s *Server) queueUpdate(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapQueueWrite(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad QueueWrite: " + err.Error()
	}
	rec, err := s.queueRecord(in.Project(), in.QueueId())
	if err != nil {
		return nil, "", StatusNotFound, "queue not found in project"
	}
	rec.Set(qName, in.Name())
	rec.Set(qDescription, in.Description())
	rec.Set(qScoreConfigIds, listToStrings(in.ScoreConfigIds()))
	if err := s.app.Save(rec); err != nil {
		return nil, "", StatusInternal, "update queue: " + err.Error()
	}
	return s.queueBody(rec), rec.Id, StatusOK, ""
}

// queueDelete: delete a queue. (queueRouter.delete)
func (s *Server) queueDelete(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapQueueLookup(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad QueueLookup: " + err.Error()
	}
	rec, err := s.queueRecord(in.Project(), in.QueueId())
	if err != nil {
		return nil, "", StatusNotFound, "queue not found in project"
	}
	body := s.queueBody(rec) // capture before delete (source returns the deleted row)
	if err := s.app.Delete(rec); err != nil {
		return nil, "", StatusInternal, "delete queue: " + err.Error()
	}
	return body, in.QueueId(), StatusOK, ""
}

// ──────────────────────────── queueItemRouter ────────────────────────────

// itemTypeById: an item's objectType. (queueItemRouter.typeById)
func (s *Server) itemTypeById(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapItemRef(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad ItemRef: " + err.Error()
	}
	rec, err := s.app.FindFirstRecordByFilter(ColQueueItems,
		"project = {:project} && queueId = {:queueId} && id = {:id}",
		map[string]any{"project": in.Project(), "queueId": in.QueueId(), "id": in.ItemId()})
	if err != nil {
		return gen.NewTextResult(gen.TextResultInput{Value: ""}), in.QueueId(), StatusOK, ""
	}
	return gen.NewTextResult(gen.TextResultInput{Value: rec.GetString(iObjectType)}), in.QueueId(), StatusOK, ""
}

// itemById: a single item with its lock display. (queueItemRouter.byId)
func (s *Server) itemById(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapItemRef(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad ItemRef: " + err.Error()
	}
	rec, err := s.app.FindFirstRecordByFilter(ColQueueItems,
		"project = {:project} && id = {:id}",
		map[string]any{"project": in.Project(), "id": in.ItemId()})
	if err != nil {
		// Source returns null when the item was deleted in-session; model as an
		// empty QueueItem (Id == "" signals absence to the client).
		return gen.NewQueueItem(gen.QueueItemInput{}), in.QueueId(), StatusOK, ""
	}
	return s.itemBody(rec), in.QueueId(), StatusOK, ""
}

// itemsByQueueId: paginated items in a queue. (queueItemRouter.itemsByQueueId)
func (s *Server) itemsByQueueId(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapQueuePage(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad QueuePage: " + err.Error()
	}
	project, queueID := in.Project(), in.QueueId()
	recs, err := s.app.FindRecordsByFilter(ColQueueItems, queueFilter,
		"created,objectId,objectType", int(in.Limit()), pageOffset(in.Page(), in.Limit()),
		queueParams(project, queueID))
	if err != nil {
		return nil, "", StatusInternal, "list items: " + err.Error()
	}
	total, err := s.app.CountRecords(ColQueueItems, byQueue(project, queueID))
	if err != nil {
		return nil, "", StatusInternal, "count items: " + err.Error()
	}
	items := make([][]byte, 0, len(recs))
	for _, rec := range recs {
		items = append(items, s.itemBody(rec))
	}
	return gen.NewQueueItemList(gen.QueueItemListInput{Items: items, TotalCount: uint64(total)}), queueID, StatusOK, ""
}

// itemUnseenCount: pending items not in seenItemIds. (queueItemRouter.unseenPendingItemCountByQueueId)
func (s *Server) itemUnseenCount(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapSeenScope(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad SeenScope: " + err.Error()
	}
	project, queueID := in.Project(), in.QueueId()
	recs, err := s.app.FindRecordsByFilter(ColQueueItems,
		"project = {:project} && queueId = {:queueId} && status = {:status}",
		"", 0, 0, map[string]any{"project": project, "queueId": queueID, "status": StatusPending})
	if err != nil {
		return nil, "", StatusInternal, "count unseen: " + err.Error()
	}
	seen := stringSet(listToStrings(in.SeenItemIds()))
	var count uint64
	for _, rec := range recs {
		if _, ok := seen[rec.Id]; !ok {
			count++
		}
	}
	return gen.NewCountResult(gen.CountResultInput{Count: count}), queueID, StatusOK, ""
}

// itemCreateMany: insert items for objectIds (de-duped). (queueItemRouter.createMany)
// When pipelined off an earlier call (e.g. queueCreate) and the request omits a
// queueId, the resolved promise value (the just-created queue's id) is used —
// the dependent call referenced the queue's id before it was known.
func (s *Server) itemCreateMany(req Call, pipelinedQueueID string) ([]byte, string, uint32, string) {
	in, err := gen.WrapItemCreateMany(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad ItemCreateMany: " + err.Error()
	}
	project, queueID := in.Project(), in.QueueId()
	if queueID == "" {
		queueID = pipelinedQueueID // resolved from the targeted promise
	}
	objectIDs := listToStrings(in.ObjectIds())
	if len(objectIDs) == 0 {
		return nil, "", StatusBadRequest, "minimum 1 object_id is required"
	}
	queue, err := s.queueRecord(project, queueID)
	if err != nil {
		return nil, "", StatusNotFound, "queue not found"
	}
	col, err := s.app.FindCollectionByNameOrId(ColQueueItems)
	if err != nil {
		return nil, "", StatusInternal, err.Error()
	}
	var created uint64
	for _, objectID := range objectIDs {
		// skipDuplicates: an item for this (queue,object,type) already present.
		if dup, _ := s.app.FindFirstRecordByFilter(ColQueueItems,
			"project = {:project} && queueId = {:queueId} && objectId = {:objectId} && objectType = {:objectType}",
			map[string]any{"project": project, "queueId": queueID, "objectId": objectID, "objectType": in.ObjectType()}); dup != nil {
			continue
		}
		rec := core.NewRecord(col)
		rec.Set(iProject, project)
		rec.Set(iQueueId, queueID)
		rec.Set(iObjectId, objectID)
		rec.Set(iObjectType, in.ObjectType())
		rec.Set(iStatus, StatusPending)
		if err := s.app.Save(rec); err != nil {
			return nil, "", StatusInternal, "create item: " + err.Error()
		}
		created++
	}
	body := gen.NewCreateManyResult(gen.CreateManyResultInput{
		CreatedCount: created, QueueName: queue.GetString(qName), QueueId: queueID,
	})
	return body, queueID, StatusOK, ""
}

// itemDeleteMany: delete items by id. (queueItemRouter.deleteMany)
func (s *Server) itemDeleteMany(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapItemIds(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad ItemIds: " + err.Error()
	}
	project := in.Project()
	ids := listToStrings(in.ItemIds())
	if len(ids) == 0 {
		return nil, "", StatusBadRequest, "minimum 1 item_id is required"
	}
	var deleted uint64
	for _, id := range ids {
		rec, err := s.app.FindFirstRecordByFilter(ColQueueItems,
			"project = {:project} && id = {:id}", map[string]any{"project": project, "id": id})
		if err != nil {
			continue // already gone
		}
		if err := s.app.Delete(rec); err != nil {
			return nil, "", StatusInternal, "delete item: " + err.Error()
		}
		deleted++
	}
	return gen.NewCountResult(gen.CountResultInput{Count: deleted}), "", StatusOK, ""
}

// itemComplete: mark a pending item COMPLETED. (queueItemRouter.complete)
func (s *Server) itemComplete(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapItemRef(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad ItemRef: " + err.Error()
	}
	rec, err := s.app.FindFirstRecordByFilter(ColQueueItems,
		"project = {:project} && id = {:id} && status = {:status}",
		map[string]any{"project": in.Project(), "id": in.ItemId(), "status": StatusPending})
	if err != nil {
		return nil, "", StatusNotFound, "the item to complete was not found, it was likely deleted"
	}
	rec.Set(iStatus, StatusCompleted)
	rec.Set(iCompletedAt, types.NowDateTime())
	if err := s.app.Save(rec); err != nil {
		return nil, "", StatusInternal, "complete item: " + err.Error()
	}
	return s.itemBody(rec), in.QueueId(), StatusOK, ""
}

// ──────────────────────────── queueAssignmentRouter ────────────────────────────

// assignCreateMany: assign users to a queue (de-duped). (queueAssignmentRouter.createMany)
func (s *Server) assignCreateMany(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapAssignWrite(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad AssignWrite: " + err.Error()
	}
	project, queueID := in.Project(), in.QueueId()
	if _, err := s.queueRecord(project, queueID); err != nil {
		return nil, "", StatusNotFound, "annotation queue not found"
	}
	col, err := s.app.FindCollectionByNameOrId(ColAssignments)
	if err != nil {
		return nil, "", StatusInternal, err.Error()
	}
	var added, skipped uint64
	for _, userID := range listToStrings(in.UserIds()) {
		if dup, _ := s.app.FindFirstRecordByFilter(ColAssignments,
			"project = {:project} && queueId = {:queueId} && userId = {:userId}",
			map[string]any{"project": project, "queueId": queueID, "userId": userID}); dup != nil {
			skipped++
			continue
		}
		rec := core.NewRecord(col)
		rec.Set(aProject, project)
		rec.Set(aQueueId, queueID)
		rec.Set(aUserId, userID)
		if err := s.app.Save(rec); err != nil {
			return nil, "", StatusInternal, "create assignment: " + err.Error()
		}
		added++
	}
	body := gen.NewAssignResult(gen.AssignResultInput{Success: true, AddedCount: added, SkippedCount: skipped})
	return body, queueID, StatusOK, ""
}

// assignDelete: remove a user's assignment from a queue. (queueAssignmentRouter.delete)
func (s *Server) assignDelete(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapAssignOne(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad AssignOne: " + err.Error()
	}
	project, queueID := in.Project(), in.QueueId()
	if _, err := s.queueRecord(project, queueID); err != nil {
		return nil, "", StatusNotFound, "annotation queue not found"
	}
	recs, err := s.app.FindRecordsByFilter(ColAssignments,
		"project = {:project} && queueId = {:queueId} && userId = {:userId}", "", 0, 0,
		map[string]any{"project": project, "queueId": queueID, "userId": in.UserId()})
	if err != nil {
		return nil, "", StatusInternal, "find assignment: " + err.Error()
	}
	for _, rec := range recs {
		if err := s.app.Delete(rec); err != nil {
			return nil, "", StatusInternal, "delete assignment: " + err.Error()
		}
	}
	return gen.NewBoolResult(gen.BoolResultInput{Value: true}), queueID, StatusOK, ""
}

// assignByQueueId: paginated users assigned to a queue. (queueAssignmentRouter.byQueueId)
func (s *Server) assignByQueueId(req Call) ([]byte, string, uint32, string) {
	in, err := gen.WrapQueuePage(req.Payload)
	if err != nil {
		return nil, "", StatusBadRequest, "bad QueuePage: " + err.Error()
	}
	project, queueID := in.Project(), in.QueueId()
	if _, err := s.queueRecord(project, queueID); err != nil {
		return nil, "", StatusNotFound, "annotation queue not found"
	}
	recs, err := s.app.FindRecordsByFilter(ColAssignments, queueFilter, "-created",
		int(in.Limit()), pageOffset(in.Page(), in.Limit()), queueParams(project, queueID))
	if err != nil {
		return nil, "", StatusInternal, "list assignments: " + err.Error()
	}
	total, err := s.app.CountRecords(ColAssignments, byQueue(project, queueID))
	if err != nil {
		return nil, "", StatusInternal, "count assignments: " + err.Error()
	}
	assigns := make([][]byte, 0, len(recs))
	for _, rec := range recs {
		assigns = append(assigns, gen.NewAssignment(gen.AssignmentInput{
			UserId: rec.GetString(aUserId),
			Name:   rec.GetString(aUserName),
			Email:  rec.GetString(aUserEmail),
		}))
	}
	return gen.NewAssignList(gen.AssignListInput{Assignments: assigns, TotalCount: uint64(total)}), queueID, StatusOK, ""
}

// ──────────────────────────── shared helpers ────────────────────────────

// queueRecord finds a queue by id, scoped to the project.
func (s *Server) queueRecord(project, queueID string) (*core.Record, error) {
	return s.app.FindFirstRecordByFilter(ColQueues,
		"project = {:project} && id = {:id}", map[string]any{"project": project, "id": queueID})
}

// queueBody projects a queue record into its wire view (plain reads: counts 0).
func (s *Server) queueBody(rec *core.Record) []byte {
	completed, pending := s.itemCounts(rec.GetString(qProject), rec.Id)
	return gen.NewQueue(gen.QueueInput{
		Id:                  rec.Id,
		Project:             rec.GetString(qProject),
		Name:                rec.GetString(qName),
		Description:         rec.GetString(qDescription),
		ScoreConfigIds:      textList(rec.GetStringSlice(qScoreConfigIds)),
		CreatedAt:           createdMillis(rec),
		CountCompletedItems: completed,
		CountPendingItems:   pending,
	})
}

// itemBody projects an item record into its wire view.
func (s *Server) itemBody(rec *core.Record) []byte {
	return gen.NewQueueItem(gen.QueueItemInput{
		Id:              rec.Id,
		Project:         rec.GetString(iProject),
		QueueId:         rec.GetString(iQueueId),
		ObjectId:        rec.GetString(iObjectId),
		ObjectType:      rec.GetString(iObjectType),
		Status:          rec.GetString(iStatus),
		CreatedAt:       createdMillis(rec),
		CompletedAt:     dateMillis(rec, iCompletedAt),
		LockedAt:        dateMillis(rec, iLockedAt),
		LockedByUserId:  rec.GetString(iLockedByUserId),
		AnnotatorUserId: rec.GetString(iAnnotatorUserId),
	})
}

// itemCounts returns (completed, pending) item counts for a queue.
func (s *Server) itemCounts(project, queueID string) (completed, pending uint64) {
	c, _ := s.app.CountRecords(ColQueueItems, byQueueStatus(project, queueID, StatusCompleted))
	p, _ := s.app.CountRecords(ColQueueItems, byQueueStatus(project, queueID, StatusPending))
	return uint64(c), uint64(p)
}

// createdMillis reads the Base `created` autodate as unix millis (0 if unset).
func createdMillis(rec *core.Record) int64 { return dateMillis(rec, "created") }

// dateMillis reads a date field as unix millis, 0 when null/zero.
func dateMillis(rec *core.Record, field string) int64 {
	dt := rec.GetDateTime(field)
	if dt.IsZero() {
		return 0
	}
	return dt.Time().UnixMilli()
}

// textList converts a []string into the [][]byte the generated list<text>
// builders consume (each element is the UTF-8 bytes of the string).
func textList(ss []string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

// listToStrings reads a zap-proto/go List of text elements (the type the
// generated payload views expose) into a sorted []string.
func listToStrings(l zapproto.List) []string {
	n := l.Len()
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, string(l.BytesAt(i)))
	}
	sort.Strings(out) // deterministic order for de-dupe iteration
	return out
}

// stringSet builds a presence set from a []string.
func stringSet(ss []string) map[string]struct{} {
	m := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		m[s] = struct{}{}
	}
	return m
}
