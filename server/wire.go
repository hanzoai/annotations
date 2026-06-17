// Package server implements the Annotations ZAP capability-RPC service.
//
// Wire model — three concentric, decoupled layers (values, not places):
//
//  1. Transport: github.com/luxfi/zap Node. Frames are length-prefixed,
//     request/response-correlated, and routed by msgType = flags>>8. This
//     service owns MsgTypeRouterBase (207), disjoint from Base's generic ORM
//     plugin (100–103) and from the other migrated service routers. One TCP
//     listener, port 9996.
//
//  2. Envelope: a luxfi/zap object carrying the call shape. Request =
//     (Method u32, PromiseID u32, Target u32, Cap bytes, Payload bytes);
//     response = (Status u32, PromiseID u32, Body bytes). The Cap field is
//     the OPAQUE zap-proto/go capability buffer — re-Wrapped server-side,
//     never decoded by the transport.
//
//  3. Payload/Body: zap-proto/go typed views generated from the .zap schema
//     (gen/). These are the contract both Go and the console's TS client
//     agree on. The transport carries them as opaque bytes — the contract
//     (data) is fully separate from the transport (place).
//
// Pipelining: PromiseID + Target let a caller reference the not-yet-resolved
// answer of an earlier call. A second call shipped with Target = the first
// call's PromiseID is dispatched against that promise's resolved value —
// Cap'n Proto-style promise pipelining (see client.go Pipeline()).
package server

import (
	"fmt"

	zaplib "github.com/luxfi/zap"
)

// MsgTypeRouterBase is this service's ZAP message-type slot. Base's generic
// ORM transport plugin uses 100–103; typed capability routers start at 200.
// Annotations owns 207.
const MsgTypeRouterBase uint16 = 207

// Method identifiers — one per .zap interface method (the `@n` ordinals).
// Grouped by the source tRPC router each came from.
const (
	// queueRouter
	MethodQueueHasAny      uint32 = 0 // Annotations.queueHasAny @0
	MethodQueueAll         uint32 = 1 // Annotations.queueAll @1
	MethodQueueNamesAndIds uint32 = 2 // Annotations.queueNamesAndIds @2
	MethodQueueCount       uint32 = 3 // Annotations.queueCount @3
	MethodQueueById        uint32 = 4 // Annotations.queueById @4
	MethodQueueByObjectId  uint32 = 5 // Annotations.queueByObjectId @5
	MethodQueueCreate      uint32 = 6 // Annotations.queueCreate @6
	MethodQueueUpdate      uint32 = 7 // Annotations.queueUpdate @7
	MethodQueueDelete      uint32 = 8 // Annotations.queueDelete @8

	// queueItemRouter
	MethodItemTypeById    uint32 = 9  // Annotations.itemTypeById @9
	MethodItemById        uint32 = 10 // Annotations.itemById @10
	MethodItemsByQueueId  uint32 = 11 // Annotations.itemsByQueueId @11
	MethodItemUnseenCount uint32 = 12 // Annotations.itemUnseenCount @12
	MethodItemCreateMany  uint32 = 13 // Annotations.itemCreateMany @13
	MethodItemDeleteMany  uint32 = 14 // Annotations.itemDeleteMany @14
	MethodItemComplete    uint32 = 15 // Annotations.itemComplete @15

	// queueAssignmentRouter
	MethodAssignCreateMany uint32 = 16 // Annotations.assignCreateMany @16
	MethodAssignDelete     uint32 = 17 // Annotations.assignDelete @17
	MethodAssignByQueueId  uint32 = 18 // Annotations.assignByQueueId @18
)

// NoTarget is the Target value for a call that does not pipeline off an
// earlier promise (i.e. it acts on the root capability directly).
const NoTarget uint32 = 0

// Request-frame field offsets within the envelope object's fixed section.
const (
	reqMethodOff    = 0  // u32: which interface method
	reqPromiseIDOff = 4  // u32: caller-assigned id this call's answer resolves to
	reqTargetOff    = 8  // u32: promise this call pipelines off (NoTarget = root)
	reqCapOff       = 12 // bytes: opaque zap-proto/go capability buffer
	reqPayloadOff   = 20 // bytes: zap-proto/go-encoded method params
	reqFixedSize    = 28
)

// Response-frame field offsets.
const (
	respStatusOff    = 0  // u32: 200 ok, else error
	respPromiseIDOff = 4  // u32: echoes the request's PromiseID
	respBodyOff      = 12 // bytes: zap-proto/go-encoded results (or error JSON)
	respFixedSize    = 20
)

// Status codes mirror the HTTP-ish convention Base's ZAP plugin already uses.
const (
	StatusOK           uint32 = 200
	StatusBadRequest   uint32 = 400
	StatusUnauthorized uint32 = 401
	StatusForbidden    uint32 = 403
	StatusNotFound     uint32 = 404
	StatusConflict     uint32 = 409
	StatusInternal     uint32 = 500
)

// Call is the decoded request envelope.
type Call struct {
	Method    uint32
	PromiseID uint32
	Target    uint32
	Cap       []byte // opaque capability buffer
	Payload   []byte // opaque zap-proto/go params
}

// buildRequest encodes a Call into a luxfi/zap message tagged for this router.
func buildRequest(c Call) (*zaplib.Message, error) {
	b := zaplib.NewBuilder(len(c.Cap) + len(c.Payload) + reqFixedSize + 64)
	ob := b.StartObject(reqFixedSize)
	ob.SetUint32(reqMethodOff, c.Method)
	ob.SetUint32(reqPromiseIDOff, c.PromiseID)
	ob.SetUint32(reqTargetOff, c.Target)
	ob.SetBytes(reqCapOff, c.Cap)
	ob.SetBytes(reqPayloadOff, c.Payload)
	ob.FinishAsRoot()
	data := b.FinishWithFlags(MsgTypeRouterBase << 8)
	return zaplib.Parse(data)
}

// parseRequest decodes a luxfi/zap message into a Call.
func parseRequest(msg *zaplib.Message) Call {
	root := msg.Root()
	return Call{
		Method:    root.Uint32(reqMethodOff),
		PromiseID: root.Uint32(reqPromiseIDOff),
		Target:    root.Uint32(reqTargetOff),
		Cap:       root.Bytes(reqCapOff),
		Payload:   root.Bytes(reqPayloadOff),
	}
}

// buildResponse encodes a status + body into a router-tagged luxfi/zap message.
func buildResponse(status, promiseID uint32, body []byte) (*zaplib.Message, error) {
	b := zaplib.NewBuilder(len(body) + respFixedSize + 64)
	ob := b.StartObject(respFixedSize)
	ob.SetUint32(respStatusOff, status)
	ob.SetUint32(respPromiseIDOff, promiseID)
	ob.SetBytes(respBodyOff, body)
	ob.FinishAsRoot()
	data := b.FinishWithFlags(MsgTypeRouterBase << 8)
	return zaplib.Parse(data)
}

// Response is the decoded response envelope.
type Response struct {
	Status    uint32
	PromiseID uint32
	Body      []byte
}

// parseResponse decodes a luxfi/zap response message.
func parseResponse(msg *zaplib.Message) Response {
	root := msg.Root()
	return Response{
		Status:    root.Uint32(respStatusOff),
		PromiseID: root.Uint32(respPromiseIDOff),
		Body:      root.Bytes(respBodyOff),
	}
}

// errorBody is a minimal JSON error body, matching the shape Base's plugin
// returns ({"error": "..."}) so a single client error path covers both.
func errorBody(msg string) []byte {
	return []byte(fmt.Sprintf(`{"error":%q}`, msg))
}
