package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	zaplib "github.com/luxfi/zap"
	zcap "github.com/zap-proto/go/cap"

	gen "github.com/hanzoai/annotations/gen"
)

// Client is an Annotations ZAP capability-RPC client. It is what console's
// bridge substitutes for the in-process tRPC routers: a thin typed wrapper over
// a luxfi/zap connection that ships the verified capability with every call.
//
// Holds the caller's opaque capability buffer and the transport node. Construct
// with Dial, then call any of the typed method wrappers.
type Client struct {
	node   *zaplib.Node
	peerID string
	capBuf []byte

	promiseSeq uint32 // monotonic PromiseID allocator

	// sendLog records, in order, every call shipped — used by the smoke test to
	// prove a pipelined dependent call ships before the first answer resolves.
	// Optional; nil disables instrumentation.
	logMu   sync.Mutex
	sendLog *[]SendEvent
}

// SendEvent is one entry in the instrumentation log: a call left the client
// (send) or its answer arrived (recv), with a monotonic sequence number.
type SendEvent struct {
	Seq       uint64
	Kind      string // "send" or "recv"
	Method    uint32
	PromiseID uint32
	Target    uint32
	At        time.Time
}

var sendEventSeq uint64

// pipelineIDSeq hands out process-unique promise ids for pipelined call groups.
// Starts high (above any per-client counter) so a pipeline id never collides
// with a plain per-call PromiseID. Promise ids are a cross-connection
// correlation namespace, so global uniqueness is required.
var pipelineIDSeq uint32 = 1 << 20

func nextPipelineID() uint32 { return atomic.AddUint32(&pipelineIDSeq, 1) }

// Dial constructs a Client over an already-started local node, connecting to the
// service at addr (e.g. "127.0.0.1:9996"). capBuf is the caller's opaque
// capability buffer (a zcap.Cap.Bytes()). peerID is the service's ZAP node id.
func Dial(node *zaplib.Node, addr, peerID string, capBuf []byte) (*Client, error) {
	if err := node.ConnectDirect(addr); err != nil {
		return nil, fmt.Errorf("ann client: connect %s: %w", addr, err)
	}
	return &Client{node: node, peerID: peerID, capBuf: capBuf}, nil
}

// WithSendLog attaches an instrumentation slice the client appends send/recv
// events to. Returns the client for chaining.
func (c *Client) WithSendLog(log *[]SendEvent) *Client {
	c.sendLog = log
	return c
}

func (c *Client) record(kind string, method, promiseID, target uint32) {
	if c.sendLog == nil {
		return
	}
	c.logMu.Lock()
	*c.sendLog = append(*c.sendLog, SendEvent{
		Seq:       atomic.AddUint64(&sendEventSeq, 1),
		Kind:      kind,
		Method:    method,
		PromiseID: promiseID,
		Target:    target,
		At:        time.Now(),
	})
	c.logMu.Unlock()
}

func (c *Client) nextPromise() uint32 { return atomic.AddUint32(&c.promiseSeq, 1) }

// call ships one request and blocks for its correlated response.
func (c *Client) call(ctx context.Context, method, promiseID, target uint32, payload []byte) (Response, error) {
	msg, err := buildRequest(Call{
		Method:    method,
		PromiseID: promiseID,
		Target:    target,
		Cap:       c.capBuf,
		Payload:   payload,
	})
	if err != nil {
		return Response{}, err
	}
	c.record("send", method, promiseID, target)
	resp, err := c.node.Call(ctx, c.peerID, msg)
	if err != nil {
		return Response{}, err
	}
	c.record("recv", method, promiseID, target)
	return parseResponse(resp), nil
}

// do is the common path for a non-pipelined call: ship with a fresh promise and
// no target, check status, return the body bytes.
func (c *Client) do(ctx context.Context, method uint32, payload []byte) ([]byte, error) {
	resp, err := c.call(ctx, method, c.nextPromise(), NoTarget, payload)
	if err != nil {
		return nil, err
	}
	if resp.Status != StatusOK {
		return nil, fmt.Errorf("method %d: status %d: %s", method, resp.Status, resp.Body)
	}
	return resp.Body, nil
}

// ──────────────────────────── queueRouter ────────────────────────────

func (c *Client) QueueHasAny(ctx context.Context, project string) (gen.BoolResult, error) {
	b, err := c.do(ctx, MethodQueueHasAny, gen.NewProjectScope(gen.ProjectScopeInput{Project: project}))
	if err != nil {
		return gen.BoolResult{}, err
	}
	return gen.WrapBoolResult(b)
}

func (c *Client) QueueAll(ctx context.Context, project string, limit, page uint32) (gen.QueueList, error) {
	b, err := c.do(ctx, MethodQueueAll, gen.NewPage(gen.PageInput{Project: project, Limit: limit, Page: page}))
	if err != nil {
		return gen.QueueList{}, err
	}
	return gen.WrapQueueList(b)
}

func (c *Client) QueueNamesAndIds(ctx context.Context, project string) (gen.QueueRefList, error) {
	b, err := c.do(ctx, MethodQueueNamesAndIds, gen.NewProjectScope(gen.ProjectScopeInput{Project: project}))
	if err != nil {
		return gen.QueueRefList{}, err
	}
	return gen.WrapQueueRefList(b)
}

func (c *Client) QueueCount(ctx context.Context, project string) (gen.CountResult, error) {
	b, err := c.do(ctx, MethodQueueCount, gen.NewProjectScope(gen.ProjectScopeInput{Project: project}))
	if err != nil {
		return gen.CountResult{}, err
	}
	return gen.WrapCountResult(b)
}

func (c *Client) QueueById(ctx context.Context, project, queueID string) (gen.Queue, error) {
	b, err := c.do(ctx, MethodQueueById, gen.NewQueueLookup(gen.QueueLookupInput{Project: project, QueueId: queueID}))
	if err != nil {
		return gen.Queue{}, err
	}
	return gen.WrapQueue(b)
}

func (c *Client) QueueByObjectId(ctx context.Context, project, objectID, objectType string) (gen.QueueObjectList, error) {
	b, err := c.do(ctx, MethodQueueByObjectId, gen.NewObjectRef(gen.ObjectRefInput{
		Project: project, ObjectId: objectID, ObjectType: objectType,
	}))
	if err != nil {
		return gen.QueueObjectList{}, err
	}
	return gen.WrapQueueObjectList(b)
}

// QueueCreate creates a queue and returns the created row.
func (c *Client) QueueCreate(ctx context.Context, project, name, description string, scoreConfigIDs []string) (gen.Queue, error) {
	b, err := c.do(ctx, MethodQueueCreate, gen.NewQueueWrite(gen.QueueWriteInput{
		Project: project, Name: name, Description: description, ScoreConfigIds: textList(scoreConfigIDs),
	}))
	if err != nil {
		return gen.Queue{}, err
	}
	return gen.WrapQueue(b)
}

func (c *Client) QueueUpdate(ctx context.Context, project, queueID, name, description string, scoreConfigIDs []string) (gen.Queue, error) {
	b, err := c.do(ctx, MethodQueueUpdate, gen.NewQueueWrite(gen.QueueWriteInput{
		Project: project, QueueId: queueID, Name: name, Description: description, ScoreConfigIds: textList(scoreConfigIDs),
	}))
	if err != nil {
		return gen.Queue{}, err
	}
	return gen.WrapQueue(b)
}

func (c *Client) QueueDelete(ctx context.Context, project, queueID string) (gen.Queue, error) {
	b, err := c.do(ctx, MethodQueueDelete, gen.NewQueueLookup(gen.QueueLookupInput{Project: project, QueueId: queueID}))
	if err != nil {
		return gen.Queue{}, err
	}
	return gen.WrapQueue(b)
}

// ──────────────────────────── queueItemRouter ────────────────────────────

func (c *Client) ItemTypeById(ctx context.Context, project, queueID, itemID string) (gen.TextResult, error) {
	b, err := c.do(ctx, MethodItemTypeById, gen.NewItemRef(gen.ItemRefInput{Project: project, QueueId: queueID, ItemId: itemID}))
	if err != nil {
		return gen.TextResult{}, err
	}
	return gen.WrapTextResult(b)
}

func (c *Client) ItemById(ctx context.Context, project, itemID string) (gen.QueueItem, error) {
	b, err := c.do(ctx, MethodItemById, gen.NewItemRef(gen.ItemRefInput{Project: project, ItemId: itemID}))
	if err != nil {
		return gen.QueueItem{}, err
	}
	return gen.WrapQueueItem(b)
}

func (c *Client) ItemsByQueueId(ctx context.Context, project, queueID string, limit, page uint32) (gen.QueueItemList, error) {
	b, err := c.do(ctx, MethodItemsByQueueId, gen.NewQueuePage(gen.QueuePageInput{
		Project: project, QueueId: queueID, Limit: limit, Page: page,
	}))
	if err != nil {
		return gen.QueueItemList{}, err
	}
	return gen.WrapQueueItemList(b)
}

func (c *Client) ItemUnseenCount(ctx context.Context, project, queueID string, seenItemIDs []string) (gen.CountResult, error) {
	b, err := c.do(ctx, MethodItemUnseenCount, gen.NewSeenScope(gen.SeenScopeInput{
		Project: project, QueueId: queueID, SeenItemIds: textList(seenItemIDs),
	}))
	if err != nil {
		return gen.CountResult{}, err
	}
	return gen.WrapCountResult(b)
}

// ItemCreateMany inserts items for objectIDs into queueID and returns the count.
func (c *Client) ItemCreateMany(ctx context.Context, project, queueID, objectType string, objectIDs []string) (gen.CreateManyResult, error) {
	b, err := c.do(ctx, MethodItemCreateMany, gen.NewItemCreateMany(gen.ItemCreateManyInput{
		Project: project, QueueId: queueID, ObjectType: objectType, ObjectIds: textList(objectIDs),
	}))
	if err != nil {
		return gen.CreateManyResult{}, err
	}
	return gen.WrapCreateManyResult(b)
}

func (c *Client) ItemDeleteMany(ctx context.Context, project string, itemIDs []string) (gen.CountResult, error) {
	b, err := c.do(ctx, MethodItemDeleteMany, gen.NewItemIds(gen.ItemIdsInput{Project: project, ItemIds: textList(itemIDs)}))
	if err != nil {
		return gen.CountResult{}, err
	}
	return gen.WrapCountResult(b)
}

func (c *Client) ItemComplete(ctx context.Context, project, itemID string) (gen.QueueItem, error) {
	b, err := c.do(ctx, MethodItemComplete, gen.NewItemRef(gen.ItemRefInput{Project: project, ItemId: itemID}))
	if err != nil {
		return gen.QueueItem{}, err
	}
	return gen.WrapQueueItem(b)
}

// ──────────────────────────── queueAssignmentRouter ────────────────────────────

func (c *Client) AssignCreateMany(ctx context.Context, project, queueID string, userIDs []string) (gen.AssignResult, error) {
	b, err := c.do(ctx, MethodAssignCreateMany, gen.NewAssignWrite(gen.AssignWriteInput{
		Project: project, QueueId: queueID, UserIds: textList(userIDs),
	}))
	if err != nil {
		return gen.AssignResult{}, err
	}
	return gen.WrapAssignResult(b)
}

func (c *Client) AssignDelete(ctx context.Context, project, queueID, userID string) (gen.BoolResult, error) {
	b, err := c.do(ctx, MethodAssignDelete, gen.NewAssignOne(gen.AssignOneInput{Project: project, QueueId: queueID, UserId: userID}))
	if err != nil {
		return gen.BoolResult{}, err
	}
	return gen.WrapBoolResult(b)
}

func (c *Client) AssignByQueueId(ctx context.Context, project, queueID string, limit, page uint32) (gen.AssignList, error) {
	b, err := c.do(ctx, MethodAssignByQueueId, gen.NewQueuePage(gen.QueuePageInput{
		Project: project, QueueId: queueID, Limit: limit, Page: page,
	}))
	if err != nil {
		return gen.AssignList{}, err
	}
	return gen.WrapAssignList(b)
}

// PipelineCreateQueueThenItems issues queueCreate @6 and itemCreateMany @13 such
// that itemCreateMany is in flight at the server BEFORE queueCreate's answer
// resolves — Cap'n Proto promise pipelining. itemCreateMany Targets queueCreate's
// promise and ships an EMPTY queueId in its payload; the server resolves the new
// queue's id from the create's answer and substitutes it as the effective queueId
// (server.itemCreateMany). No intermediate round trip to learn the id.
//
// Transport note (load-bearing): luxfi/zap processes a single connection's frames
// strictly FIFO — one handler runs to completion before the next frame is read.
// So genuine in-flight pipelining requires the two calls on SEPARATE connections,
// where the server runs two dispatch loops and its promise table joins them. `dep`
// is therefore a SECOND client connection over which the dependent itemCreateMany
// is shipped; pass a Client dialed on its own *zaplib.Node.
//
// Proof (on the shared send log both clients append to): itemCreateMany's send
// precedes queueCreate's recv — the dependent call was on the wire before the
// call it depends on had answered. The server's await() blocks itemCreateMany
// until queueCreate resolves the promise, which is the pipelining join.
func (c *Client) PipelineCreateQueueThenItems(
	ctx context.Context, dep *Client,
	project, name, objectType string, objectIDs []string,
) (gen.Queue, gen.CreateManyResult, error) {
	createPromise := nextPipelineID()
	itemPromise := nextPipelineID()

	var (
		queue     gen.Queue
		result    gen.CreateManyResult
		createErr error
		itemErr   error
		wg        sync.WaitGroup
	)
	// barrier releases the dependent send only after the create's send is
	// committed to its wire, so the server resolves the create's promise id
	// before (or concurrently with) the dependent call's await.
	barrier := make(chan struct{})
	wg.Add(2)

	// Call #1: queueCreate @6 on connection c — the promise the dependent targets.
	go func() {
		defer wg.Done()
		close(barrier)
		resp, err := c.call(ctx, MethodQueueCreate, createPromise, NoTarget,
			gen.NewQueueWrite(gen.QueueWriteInput{Project: project, Name: name}))
		if err != nil {
			createErr = err
			return
		}
		if resp.Status != StatusOK {
			createErr = fmt.Errorf("queueCreate: status %d: %s", resp.Status, resp.Body)
			return
		}
		queue, createErr = gen.WrapQueue(resp.Body)
	}()

	// Call #2: itemCreateMany @13 on connection dep, pipelined off the create's
	// promise, shipped with an EMPTY queueId — the server fills it from the
	// resolved promise (the just-created queue's id).
	go func() {
		defer wg.Done()
		<-barrier
		resp, err := dep.call(ctx, MethodItemCreateMany, itemPromise, createPromise,
			gen.NewItemCreateMany(gen.ItemCreateManyInput{
				Project: project, ObjectType: objectType, ObjectIds: textList(objectIDs),
			}))
		if err != nil {
			itemErr = err
			return
		}
		if resp.Status != StatusOK {
			itemErr = fmt.Errorf("itemCreateMany: status %d: %s", resp.Status, resp.Body)
			return
		}
		result, itemErr = gen.WrapCreateManyResult(resp.Body)
	}()

	wg.Wait()
	if createErr != nil {
		return gen.Queue{}, gen.CreateManyResult{}, createErr
	}
	if itemErr != nil {
		return gen.Queue{}, gen.CreateManyResult{}, itemErr
	}
	return queue, result, nil
}

// SyntheticCap mints an in-memory CapKindIAMSession capability for tests and
// bootstrap: ed25519-signed (the SPEC bootstrap scheme), holding the given
// permission bits. The signature is real (ed25519); production wires an
// IAM-issued cap instead. Returns the opaque buffer to pass to Dial.
func SyntheticCap(perms uint64) ([]byte, error) {
	signer, err := zcap.NewEd25519Signer()
	if err != nil {
		return nil, err
	}
	c, err := zcap.Issue(zcap.Issuance{
		Kind:        uint32(zcap.KindIAMSession),
		Holder:      signer.Public(),
		Permissions: perms,
		ExpiresAt:   time.Now().Add(time.Hour).Unix(),
	}, signer)
	if err != nil {
		return nil, err
	}
	return c.Bytes(), nil
}
