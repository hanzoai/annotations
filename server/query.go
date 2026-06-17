package server

import (
	"github.com/hanzoai/dbx"
)

// Query helpers — the project/queue scoping predicates in ONE place, shared by
// every handler. Two flavors, because Base exposes two query surfaces:
//
//   - FindRecordsByFilter(col, filter, sort, limit, offset, params): a filter
//     STRING with {:name} placeholders + dbx.Params. The *Filter consts + the
//     *Params builders below feed it.
//   - CountRecords(col, ...dbx.Expression): typed dbx expressions. The by*
//     builders below feed it.
//
// Keeping both in sync here means a scoping change happens once.

// Filter strings for FindRecordsByFilter (placeholder form).
const (
	projectFilter = "project = {:project}"
	queueFilter   = "project = {:project} && queueId = {:queueId}"
)

// projectParam builds the params for projectFilter.
func projectParam(project string) dbx.Params {
	return dbx.Params{"project": project}
}

// queueParams builds the params for queueFilter.
func queueParams(project, queueID string) dbx.Params {
	return dbx.Params{"project": project, "queueId": queueID}
}

// byProject is the typed expression form of projectFilter (for CountRecords).
func byProject(project string) dbx.Expression {
	return dbx.HashExp{"project": project}
}

// byQueue is the typed expression form of queueFilter (for CountRecords).
func byQueue(project, queueID string) dbx.Expression {
	return dbx.HashExp{"project": project, "queueId": queueID}
}

// byQueueStatus scopes to a queue + status (item-count predicates).
func byQueueStatus(project, queueID, status string) dbx.Expression {
	return dbx.HashExp{"project": project, "queueId": queueID, "status": status}
}

// pageOffset converts a (page, limit) pair into a row offset. Limit 0 means
// unbounded — offset is then irrelevant (FindRecordsByFilter applies no LIMIT,
// so OFFSET is a no-op) and we return 0. Mirrors the source's
// `page && limit ? OFFSET page*limit : (none)`.
func pageOffset(page, limit uint32) int {
	if limit == 0 || page == 0 {
		return 0
	}
	return int(page * limit)
}
