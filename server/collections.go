package server

import (
	"github.com/hanzoai/base/core"
	"github.com/hanzoai/base/tools/hook"
)

// The three Base collections backing the Annotations service — one per Prisma
// model the migrated tRPC routers read/wrote. All rows carry `project` (the
// console projectId, the tenant key); the service scopes every query to the
// project in the verified capability's request. Backed by Base's encrypted
// SQLite (vault plugin KEK per org) when vault is registered; plain SQLite
// otherwise.
const (
	ColQueues      = "annotation_queues"
	ColQueueItems  = "annotation_queue_items"
	ColAssignments = "annotation_queue_assignments"
)

// Queue fields — mirror the annotation_queues Prisma model. `id`, `created`,
// `updated` are Base system fields; the rest mirror the source columns.
const (
	qProject        = "project"
	qName           = "name"
	qDescription    = "description"
	qScoreConfigIds = "scoreConfigIds" // JSON array of strings
)

// QueueItem fields — mirror annotation_queue_items. Status/ObjectType are the
// Prisma string enums; lockedAt/completedAt are nullable dates.
const (
	iProject         = "project"
	iQueueId         = "queueId"
	iObjectId        = "objectId"
	iObjectType      = "objectType"      // TRACE | SESSION | OBSERVATION
	iStatus          = "status"          // PENDING | COMPLETED
	iCompletedAt     = "completedAt"     // nullable
	iLockedAt        = "lockedAt"        // nullable
	iLockedByUserId  = "lockedByUserId"  // nullable
	iAnnotatorUserId = "annotatorUserId" // nullable
)

// Assignment fields — mirror annotation_queue_assignments. The user identity
// (name/email) is denormalized here so this service is self-contained: the
// console resolved it from the users table via a join; we store it on assign.
const (
	aProject   = "project"
	aQueueId   = "queueId"
	aUserId    = "userId"
	aUserName  = "userName"
	aUserEmail = "userEmail"
)

// AnnotationQueueStatus / ObjectType enum string values (mirror the Prisma
// enums the source used). Stored verbatim; no custom enum kind in the schema.
const (
	StatusPending   = "PENDING"
	StatusCompleted = "COMPLETED"

	ObjectTrace       = "TRACE"
	ObjectSession     = "SESSION"
	ObjectObservation = "OBSERVATION"
)

// RegisterCollections ensures all three collections exist. Idempotent:
// re-running finds the existing collections and no-ops. Wired via OnBootstrap
// so it runs once at startup, before the ZAP listener accepts calls.
func RegisterCollections(app core.App) {
	app.OnBootstrap().Bind(&hook.Handler[*core.BootstrapEvent]{
		Id: "annotationCollections",
		Func: func(e *core.BootstrapEvent) error {
			// Run the rest of bootstrap first so the DB is initialized.
			if err := e.Next(); err != nil {
				return err
			}
			return EnsureCollections(app)
		},
	})
}

// EnsureCollections creates the three collections if absent. Exported so callers
// that bootstrap the app themselves (tests, one-shot migrations) can provision
// them directly rather than via the OnBootstrap hook. Idempotent.
func EnsureCollections(app core.App) error {
	if err := ensureQueues(app); err != nil {
		return err
	}
	if err := ensureQueueItems(app); err != nil {
		return err
	}
	return ensureAssignments(app)
}

func ensureQueues(app core.App) error {
	if _, err := app.FindCollectionByNameOrId(ColQueues); err == nil {
		return nil
	}
	col := core.NewBaseCollection(ColQueues)
	col.Fields.Add(&core.TextField{Name: qProject, Required: true, Max: 255})
	col.Fields.Add(&core.TextField{Name: qName, Required: true, Max: 1024})
	col.Fields.Add(&core.TextField{Name: qDescription, Max: 65536})
	col.Fields.Add(&core.JSONField{Name: qScoreConfigIds, MaxSize: 65536})
	col.Fields.Add(&core.AutodateField{Name: "created", OnCreate: true})
	col.Fields.Add(&core.AutodateField{Name: "updated", OnCreate: true, OnUpdate: true})

	// One queue name per project (source enforces uniqueness on create).
	col.AddIndex("idx_aq_project_name", true, "project, name", "")
	col.AddIndex("idx_aq_project", false, qProject, "")
	return app.Save(col)
}

func ensureQueueItems(app core.App) error {
	if _, err := app.FindCollectionByNameOrId(ColQueueItems); err == nil {
		return nil
	}
	col := core.NewBaseCollection(ColQueueItems)
	col.Fields.Add(&core.TextField{Name: iProject, Required: true, Max: 255})
	col.Fields.Add(&core.TextField{Name: iQueueId, Required: true, Max: 255})
	col.Fields.Add(&core.TextField{Name: iObjectId, Required: true, Max: 255})
	col.Fields.Add(&core.TextField{Name: iObjectType, Required: true, Max: 32})
	col.Fields.Add(&core.TextField{Name: iStatus, Required: true, Max: 32})
	col.Fields.Add(&core.DateField{Name: iCompletedAt})
	col.Fields.Add(&core.DateField{Name: iLockedAt})
	col.Fields.Add(&core.TextField{Name: iLockedByUserId, Max: 255})
	col.Fields.Add(&core.TextField{Name: iAnnotatorUserId, Max: 255})
	col.Fields.Add(&core.AutodateField{Name: "created", OnCreate: true})
	col.Fields.Add(&core.AutodateField{Name: "updated", OnCreate: true, OnUpdate: true})

	// De-dupe the same object into the same queue (source createMany
	// skipDuplicates on this tuple).
	col.AddIndex("idx_aqi_dedupe", true, "project, queueId, objectId, objectType", "")
	col.AddIndex("idx_aqi_queue", false, "project, queueId", "")
	return app.Save(col)
}

func ensureAssignments(app core.App) error {
	if _, err := app.FindCollectionByNameOrId(ColAssignments); err == nil {
		return nil
	}
	col := core.NewBaseCollection(ColAssignments)
	col.Fields.Add(&core.TextField{Name: aProject, Required: true, Max: 255})
	col.Fields.Add(&core.TextField{Name: aQueueId, Required: true, Max: 255})
	col.Fields.Add(&core.TextField{Name: aUserId, Required: true, Max: 255})
	col.Fields.Add(&core.TextField{Name: aUserName, Max: 1024})
	col.Fields.Add(&core.TextField{Name: aUserEmail, Max: 1024})
	col.Fields.Add(&core.AutodateField{Name: "created", OnCreate: true})
	col.Fields.Add(&core.AutodateField{Name: "updated", OnCreate: true, OnUpdate: true})

	// One assignment per (project, queue, user) — source createMany
	// skipDuplicates on this tuple.
	col.AddIndex("idx_aqa_dedupe", true, "project, queueId, userId", "")
	col.AddIndex("idx_aqa_queue", false, "project, queueId", "")
	return app.Save(col)
}
