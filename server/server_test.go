package server_test

import (
	"context"
	"testing"
	"time"

	basetests "github.com/hanzoai/base/tests"
	luxlog "github.com/luxfi/log"
	zaplib "github.com/luxfi/zap"
	zcap "github.com/zap-proto/go/cap"

	gen "github.com/hanzoai/annotations/gen"
	"github.com/hanzoai/annotations/server"
)

const testProject = "proj-test"

// allPerms is a cap holding every category bit — the common case for a session
// that can do anything across the three routers.
const allPerms = server.AnnReadQueues | server.AnnWriteQueues |
	server.AnnReadAssignments | server.AnnWriteAssignments

// newService spins up a Base test app with the three annotation collections
// provisioned, a ZAP router node listening, and returns the listen address, the
// node id, and a cleanup func.
func newService(t *testing.T, port int) (addr, peerID string, cleanup func()) {
	t.Helper()

	app, err := basetests.NewTestApp()
	if err != nil {
		t.Fatalf("new test app: %v", err)
	}
	if err := server.EnsureCollections(app); err != nil {
		t.Fatalf("ensure collections: %v", err)
	}

	logger := luxlog.New("component", "ann-test")
	node := zaplib.NewNode(zaplib.NodeConfig{
		NodeID:      "ann-test-srv",
		Port:        port,
		NoDiscovery: true,
	})
	srv := server.NewServer(app, logger, zcap.Verifier{})
	srv.Register(node)
	if err := node.Start(); err != nil {
		t.Fatalf("node start: %v", err)
	}

	return "127.0.0.1:" + itoa(port), "ann-test-srv", func() {
		node.Stop()
		app.Cleanup()
	}
}

// newClient dials the service with a synthetic CapKindIAMSession cap holding the
// given permissions, on its own ZAP node (unique port per connection).
func newClient(t *testing.T, addr, peerID string, perms uint64, port int) (*server.Client, func()) {
	t.Helper()
	capBuf, err := server.SyntheticCap(perms)
	if err != nil {
		t.Fatalf("synthetic cap: %v", err)
	}
	cli := zaplib.NewNode(zaplib.NodeConfig{
		NodeID:      "ann-test-cli-" + itoa(port),
		Port:        port,
		NoDiscovery: true,
	})
	if err := cli.Start(); err != nil {
		t.Fatalf("client node start: %v", err)
	}
	c, err := server.Dial(cli, addr, peerID, capBuf)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	time.Sleep(150 * time.Millisecond) // let the handshake settle
	return c, func() { cli.Stop() }
}

func testCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// ──────────────────────────── queue lifecycle ────────────────────────────

// TestQueueLifecycle exercises create → byId → all → update → count → delete,
// asserting each step's wire view, on a single connection.
func TestQueueLifecycle(t *testing.T) {
	addr, peerID, stop := newService(t, 19700)
	defer stop()
	cli, stopCli := newClient(t, addr, peerID, allPerms, 19701)
	defer stopCli()
	ctx, cancel := testCtx()
	defer cancel()

	// Empty project: hasAny false, count 0.
	if has, err := cli.QueueHasAny(ctx, testProject); err != nil || has.Value() {
		t.Fatalf("hasAny empty: %v val=%v", err, has.Value())
	}
	if c, err := cli.QueueCount(ctx, testProject); err != nil || c.Count() != 0 {
		t.Fatalf("count empty: %v c=%d", err, c.Count())
	}

	// Create.
	q, err := cli.QueueCreate(ctx, testProject, "My Queue", "desc", []string{"sc1", "sc2"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if q.Id() == "" || q.Name() != "My Queue" || q.Description() != "desc" {
		t.Fatalf("create view wrong: id=%q name=%q desc=%q", q.Id(), q.Name(), q.Description())
	}
	if q.ScoreConfigIds().Len() != 2 {
		t.Fatalf("scoreConfigIds = %d, want 2", q.ScoreConfigIds().Len())
	}
	queueID := q.Id()

	// Duplicate name → conflict (error from the client).
	if _, err := cli.QueueCreate(ctx, testProject, "My Queue", "", nil); err == nil {
		t.Fatalf("expected conflict on duplicate queue name")
	}

	// hasAny now true; count 1.
	if has, err := cli.QueueHasAny(ctx, testProject); err != nil || !has.Value() {
		t.Fatalf("hasAny after create: %v val=%v", err, has.Value())
	}
	if c, err := cli.QueueCount(ctx, testProject); err != nil || c.Count() != 1 {
		t.Fatalf("count after create: %v c=%d", err, c.Count())
	}

	// byId.
	got, err := cli.QueueById(ctx, testProject, queueID)
	if err != nil || got.Id() != queueID {
		t.Fatalf("byId: %v id=%q", err, got.Id())
	}

	// all: one queue, totalCount 1.
	list, err := cli.QueueAll(ctx, testProject, 0, 0)
	if err != nil || list.TotalCount() != 1 || list.Queues().Len() != 1 {
		t.Fatalf("all: %v total=%d n=%d", err, list.TotalCount(), list.Queues().Len())
	}

	// namesAndIds.
	refs, err := cli.QueueNamesAndIds(ctx, testProject)
	if err != nil || refs.Refs().Len() != 1 {
		t.Fatalf("namesAndIds: %v n=%d", err, refs.Refs().Len())
	}

	// Update.
	upd, err := cli.QueueUpdate(ctx, testProject, queueID, "Renamed", "newdesc", []string{"sc3"})
	if err != nil || upd.Name() != "Renamed" || upd.Description() != "newdesc" {
		t.Fatalf("update: %v name=%q", err, upd.Name())
	}

	// Delete returns the (renamed) row; subsequent byId fails.
	del, err := cli.QueueDelete(ctx, testProject, queueID)
	if err != nil || del.Id() != queueID {
		t.Fatalf("delete: %v id=%q", err, del.Id())
	}
	if _, err := cli.QueueById(ctx, testProject, queueID); err == nil {
		t.Fatalf("expected byId to fail after delete")
	}
}

// ──────────────────────────── item lifecycle ────────────────────────────

// TestItemLifecycle covers createMany (with de-dup) → itemsByQueueId →
// typeById → byId → unseenCount → complete → deleteMany, plus the queue's
// computed completed/pending counts and byObjectId.
func TestItemLifecycle(t *testing.T) {
	addr, peerID, stop := newService(t, 19702)
	defer stop()
	cli, stopCli := newClient(t, addr, peerID, allPerms, 19703)
	defer stopCli()
	ctx, cancel := testCtx()
	defer cancel()

	q, err := cli.QueueCreate(ctx, testProject, "Items Q", "", nil)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	queueID := q.Id()

	// createMany 3 objects; a second call with an overlapping id de-dupes.
	res, err := cli.ItemCreateMany(ctx, testProject, queueID, server.ObjectTrace, []string{"t1", "t2", "t3"})
	if err != nil || res.CreatedCount() != 3 || res.QueueId() != queueID {
		t.Fatalf("createMany: %v created=%d", err, res.CreatedCount())
	}
	res2, err := cli.ItemCreateMany(ctx, testProject, queueID, server.ObjectTrace, []string{"t3", "t4"})
	if err != nil || res2.CreatedCount() != 1 { // t3 dup-skipped, t4 new
		t.Fatalf("createMany dedup: %v created=%d (want 1)", err, res2.CreatedCount())
	}

	// itemsByQueueId: 4 items total.
	items, err := cli.ItemsByQueueId(ctx, testProject, queueID, 0, 0)
	if err != nil || items.TotalCount() != 4 || items.Items().Len() != 4 {
		t.Fatalf("itemsByQueueId: %v total=%d n=%d", err, items.TotalCount(), items.Items().Len())
	}

	// Pull the first item from the wire list to drive typeById/byId/complete.
	// List elements were built with AddObjectBytes (self-contained NewQueueItem
	// buffers), so BytesAt(i) yields exactly such a buffer for WrapQueueItem.
	firstItem, err := gen.WrapQueueItem(items.Items().BytesAt(0))
	if err != nil {
		t.Fatalf("wrap first item: %v", err)
	}
	itemID := firstItem.Id()
	if itemID == "" {
		t.Fatalf("first item id empty")
	}

	// typeById → TRACE.
	if ty, err := cli.ItemTypeById(ctx, testProject, queueID, itemID); err != nil || ty.Value() != server.ObjectTrace {
		t.Fatalf("typeById: %v ty=%q", err, ty.Value())
	}

	// byId → status PENDING.
	one, err := cli.ItemById(ctx, testProject, itemID)
	if err != nil || one.Id() != itemID || one.Status() != server.StatusPending {
		t.Fatalf("byId: %v id=%q status=%q", err, one.Id(), one.Status())
	}

	// unseenCount with 1 item already seen → 3 (4 pending - 1 seen).
	if u, err := cli.ItemUnseenCount(ctx, testProject, queueID, []string{itemID}); err != nil || u.Count() != 3 {
		t.Fatalf("unseenCount: %v c=%d (want 3)", err, u.Count())
	}

	// complete the item → status COMPLETED, completedAt set.
	done, err := cli.ItemComplete(ctx, testProject, itemID)
	if err != nil || done.Status() != server.StatusCompleted || done.CompletedAt() == 0 {
		t.Fatalf("complete: %v status=%q completedAt=%d", err, done.Status(), done.CompletedAt())
	}

	// Queue counts now reflect 1 completed, 3 pending.
	gotQ, err := cli.QueueById(ctx, testProject, queueID)
	if err != nil || gotQ.CountCompletedItems() != 1 || gotQ.CountPendingItems() != 3 {
		t.Fatalf("queue counts: %v completed=%d pending=%d", err, gotQ.CountCompletedItems(), gotQ.CountPendingItems())
	}

	// completing an already-completed item → not found.
	if _, err := cli.ItemComplete(ctx, testProject, itemID); err == nil {
		t.Fatalf("expected complete to fail on already-completed item")
	}

	// byObjectId for t1: one hit in this queue.
	hits, err := cli.QueueByObjectId(ctx, testProject, "t1", server.ObjectTrace)
	if err != nil || hits.TotalCount() != 1 || hits.Hits().Len() != 1 {
		t.Fatalf("byObjectId: %v total=%d n=%d", err, hits.TotalCount(), hits.Hits().Len())
	}

	// deleteMany the completed item.
	if d, err := cli.ItemDeleteMany(ctx, testProject, []string{itemID}); err != nil || d.Count() != 1 {
		t.Fatalf("deleteMany: %v deleted=%d", err, d.Count())
	}

	// byId on a deleted item → empty view (Id == "").
	if gone, err := cli.ItemById(ctx, testProject, itemID); err != nil || gone.Id() != "" {
		t.Fatalf("byId deleted: %v id=%q (want empty)", err, gone.Id())
	}
}

// ──────────────────────────── assignment lifecycle ────────────────────────────

// TestAssignmentLifecycle covers createMany (with de-dup) → byQueueId →
// delete, and the not-found path when the queue is absent.
func TestAssignmentLifecycle(t *testing.T) {
	addr, peerID, stop := newService(t, 19704)
	defer stop()
	cli, stopCli := newClient(t, addr, peerID, allPerms, 19705)
	defer stopCli()
	ctx, cancel := testCtx()
	defer cancel()

	q, err := cli.QueueCreate(ctx, testProject, "Assign Q", "", nil)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	queueID := q.Id()

	// createMany 2 users; second call overlaps one → de-dup.
	r, err := cli.AssignCreateMany(ctx, testProject, queueID, []string{"u1", "u2"})
	if err != nil || !r.Success() || r.AddedCount() != 2 {
		t.Fatalf("assignCreateMany: %v added=%d", err, r.AddedCount())
	}
	r2, err := cli.AssignCreateMany(ctx, testProject, queueID, []string{"u2", "u3"})
	if err != nil || r2.AddedCount() != 1 || r2.SkippedCount() != 1 {
		t.Fatalf("assignCreateMany dedup: %v added=%d skipped=%d", err, r2.AddedCount(), r2.SkippedCount())
	}

	// byQueueId → 3 assignments.
	list, err := cli.AssignByQueueId(ctx, testProject, queueID, 0, 0)
	if err != nil || list.TotalCount() != 3 || list.Assignments().Len() != 3 {
		t.Fatalf("assignByQueueId: %v total=%d n=%d", err, list.TotalCount(), list.Assignments().Len())
	}

	// delete u1 → success; count drops to 2.
	if d, err := cli.AssignDelete(ctx, testProject, queueID, "u1"); err != nil || !d.Value() {
		t.Fatalf("assignDelete: %v val=%v", err, d.Value())
	}
	if list, err := cli.AssignByQueueId(ctx, testProject, queueID, 0, 0); err != nil || list.TotalCount() != 2 {
		t.Fatalf("assignByQueueId after delete: %v total=%d", err, list.TotalCount())
	}

	// createMany against a missing queue → not found (client error).
	if _, err := cli.AssignCreateMany(ctx, testProject, "nope", []string{"u9"}); err == nil {
		t.Fatalf("expected not-found assigning to missing queue")
	}
}

// ──────────────────────────── permission gating ────────────────────────────

// TestPermissionDenied proves the bitmask chokepoint per category: a cap missing
// the required bit is rejected before any data op, while a cap holding only the
// matching bit succeeds.
func TestPermissionDenied(t *testing.T) {
	addr, peerID, stop := newService(t, 19706)
	defer stop()
	ctx, cancel := testCtx()
	defer cancel()

	// Read-queues only: a queue read works, a queue write is denied.
	roCli, stopRO := newClient(t, addr, peerID, server.AnnReadQueues, 19707)
	defer stopRO()
	if _, err := roCli.QueueCount(ctx, testProject); err != nil {
		t.Fatalf("read-queues cap should allow queueCount: %v", err)
	}
	if _, err := roCli.QueueCreate(ctx, testProject, "X", "", nil); err == nil {
		t.Fatalf("read-queues cap must NOT allow queueCreate")
	}

	// Seed a queue with a fully-permissioned client for the cross-category checks.
	wc, stopWC := newClient(t, addr, peerID, allPerms, 19708)
	defer stopWC()
	seed, err := wc.QueueCreate(ctx, testProject, "Seed", "", nil)
	if err != nil {
		t.Fatalf("seed queue: %v", err)
	}
	queueID := seed.Id()

	// Read-queues must not allow assignment read (different category).
	if _, err := roCli.AssignByQueueId(ctx, testProject, queueID, 0, 0); err == nil {
		t.Fatalf("read-queues cap must NOT allow assignByQueueId (needs AnnReadAssignments)")
	}

	// Write-assignments only: assignment write works, queue read denied.
	waCli, stopWA := newClient(t, addr, peerID, server.AnnWriteAssignments, 19709)
	defer stopWA()
	if _, err := waCli.AssignCreateMany(ctx, testProject, queueID, []string{"u1"}); err != nil {
		t.Fatalf("write-assignments cap should allow assignCreateMany: %v", err)
	}
	if _, err := waCli.QueueCount(ctx, testProject); err == nil {
		t.Fatalf("write-assignments cap must NOT allow queueCount (needs AnnReadQueues)")
	}

	// No bits at all: everything denied.
	noCli, stopNo := newClient(t, addr, peerID, 0, 19710)
	defer stopNo()
	if _, err := noCli.QueueCount(ctx, testProject); err == nil {
		t.Fatalf("zero-perm cap must be rejected")
	}
}

// ──────────────────────────── pipelining proof ────────────────────────────

// TestPipeliningCreateQueueThenItems is the load-bearing pipelining proof: the
// dependent itemCreateMany (which targets queueCreate's promise and ships an
// EMPTY queueId) must be on the wire BEFORE queueCreate's answer resolves, and
// the server must fill the items' queueId from the resolved promise (the
// just-created queue's id). We assert BOTH on the instrumented send log (two
// sends before the first recv) AND on the data (items landed in the new queue).
func TestPipeliningCreateQueueThenItems(t *testing.T) {
	addr, peerID, stop := newService(t, 19711)
	defer stop()

	var log []server.SendEvent
	cli, stopCli := newClient(t, addr, peerID, allPerms, 19712)
	defer stopCli()
	cli.WithSendLog(&log)
	dep, stopDep := newClient(t, addr, peerID, allPerms, 19713)
	defer stopDep()
	dep.WithSendLog(&log)

	ctx, cancel := testCtx()
	defer cancel()

	queue, result, err := cli.PipelineCreateQueueThenItems(
		ctx, dep, testProject, "Pipelined Q", server.ObjectObservation, []string{"o1", "o2"})
	if err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	if queue.Id() == "" {
		t.Fatalf("pipeline: queue not created")
	}
	if result.CreatedCount() != 2 {
		t.Fatalf("pipeline: createdCount = %d, want 2", result.CreatedCount())
	}
	// The promise join worked: the items' queueId equals the created queue's id.
	if result.QueueId() != queue.Id() {
		t.Fatalf("pipeline join failed: itemCreateMany queueId=%q, created queueId=%q",
			result.QueueId(), queue.Id())
	}

	// Verify on the data plane: the new queue has exactly the 2 pipelined items.
	items, err := cli.ItemsByQueueId(ctx, testProject, queue.Id(), 0, 0)
	if err != nil || items.TotalCount() != 2 {
		t.Fatalf("pipelined items not in queue: %v total=%d", err, items.TotalCount())
	}

	// Ordering proof: two "send" events precede the first "recv".
	firstRecv := -1
	sendsBeforeFirstRecv := 0
	for i, e := range log {
		if e.Kind == "recv" {
			firstRecv = i
			break
		}
		if e.Kind == "send" {
			sendsBeforeFirstRecv++
		}
	}
	t.Logf("send log: %s", formatLog(log))
	if firstRecv == -1 {
		t.Fatalf("no recv events recorded")
	}
	if sendsBeforeFirstRecv < 2 {
		t.Fatalf("pipelining violated: only %d sends before first answer resolved; want 2",
			sendsBeforeFirstRecv)
	}
	t.Logf("PIPELINING PROVEN: %d calls shipped before the first answer resolved; promise join filled queueId=%q",
		sendsBeforeFirstRecv, result.QueueId())
}

// ──────────────────────────── tiny helpers ────────────────────────────

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [12]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func formatLog(log []server.SendEvent) string {
	out := ""
	for _, e := range log {
		method := "queueCreate"
		if e.Method == server.MethodItemCreateMany {
			method = "itemCreateMany"
		}
		out += e.Kind + "(" + method + ",p=" + itoa(int(e.PromiseID)) + ",t=" + itoa(int(e.Target)) + ") "
	}
	return out
}
