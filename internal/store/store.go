// Package store persists calibration revisions in PostgreSQL.
//
// Data model:
//
//   - heads:         one row per equipment, pointing at the current revision.
//   - segments:      append-only snapshot of the full base batch-segment list
//     of every revision; rows are never updated or deleted
//     (enforced by trigger), so every historical revision
//     stays recomputable.
//   - overlay_rects: append-only snapshot of every revision's temperature
//     overlay rectangles (batch×temperature). Both kinds of
//     publish (base calibration and temperature overlay)
//     snapshot both layers, so a single revision always
//     preserves the base calibration as it stood then together
//     with every overlay then in effect.
//   - operations:    idempotency ledger mapping (equipment, operation_id) to
//     the publish kind, request hash and stored first
//     response. An operation id is equipment-unique across
//     both kinds.
//
// Every publish runs in a single transaction that locks the equipment's head
// row with SELECT ... FOR UPDATE. The lock serializes publishers per
// equipment, so two publishes racing on the same seen revision cannot both
// succeed, and a failed transaction (a failed rectangle split included)
// leaves no partial snapshot behind.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/example/calibsvc/internal/rectsplit"
	"github.com/example/calibsvc/internal/split"
)

// Publish kinds recorded in the idempotency ledger.
const (
	KindBase    = "base"
	KindOverlay = "overlay"
)

// Stable domain errors, mapped to HTTP status codes by the API layer.
var (
	ErrStaleRevision     = errors.New("seen revision does not match the current revision")
	ErrOperationConflict = errors.New("operation id already used with different parameters")
	ErrEquipmentNotFound = errors.New("equipment not found")
	ErrRevisionNotFound  = errors.New("revision not found")
	ErrBatchNotCovered   = errors.New("batch not covered by any calibration segment")
)

// Store provides transactional access to the calibration database.
type Store struct {
	db *sql.DB
}

// New returns a Store backed by db.
func New(db *sql.DB) *Store { return &Store{db: db} }

// Ping reports whether the database is reachable (used by /health).
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// Migrate creates the schema if it does not exist yet. It is idempotent.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

const schema = `
CREATE TABLE IF NOT EXISTS heads (
    equipment TEXT PRIMARY KEY,
    revision BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS segments (
    equipment TEXT NOT NULL,
    revision BIGINT NOT NULL,
    lower    BIGINT NOT NULL,
    upper    BIGINT NOT NULL,
    content  TEXT NOT NULL,
    PRIMARY KEY (equipment, revision, lower),
    CHECK (lower < upper)
);

CREATE TABLE IF NOT EXISTS overlay_rects (
    equipment TEXT NOT NULL,
    revision  BIGINT NOT NULL,
    batch_lo  BIGINT NOT NULL,
    batch_hi  BIGINT NOT NULL,
    temp_lo   BIGINT NOT NULL,
    temp_hi   BIGINT NOT NULL,
    content   TEXT NOT NULL,
    PRIMARY KEY (equipment, revision, batch_lo, batch_hi, temp_lo),
    CHECK (batch_lo < batch_hi),
    CHECK (temp_lo < temp_hi)
);

CREATE TABLE IF NOT EXISTS operations (
    equipment    TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    kind         TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    revision     BIGINT NOT NULL,
    response     TEXT NOT NULL,
    PRIMARY KEY (equipment, operation_id)
);

-- Deployments predating the overlay feature lack operations.kind; add it.
ALTER TABLE operations ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'base';

-- Historical revisions and the idempotency ledger are immutable: any attempt
-- to rewrite or delete them is rejected at the database level.
CREATE OR REPLACE FUNCTION reject_history_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'history is immutable: % on % is not allowed', TG_OP, TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'segments_immutable') THEN
        CREATE TRIGGER segments_immutable BEFORE UPDATE OR DELETE ON segments
            FOR EACH ROW EXECUTE FUNCTION reject_history_mutation();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'overlay_rects_immutable') THEN
        CREATE TRIGGER overlay_rects_immutable BEFORE UPDATE OR DELETE ON overlay_rects
            FOR EACH ROW EXECUTE FUNCTION reject_history_mutation();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'operations_immutable') THEN
        CREATE TRIGGER operations_immutable BEFORE UPDATE OR DELETE ON operations
            FOR EACH ROW EXECUTE FUNCTION reject_history_mutation();
    END IF;
END $$;
`

// PublishParams describes one validated base-calibration publish call.
type PublishParams struct {
	Equipment    string
	OperationID  string
	SeenRevision int64
	Lower        int64
	Upper        int64
	Content      string // canonical JSON
	RequestHash  string // hash over all request parameters, kind included
}

// OverlayParams describes one validated temperature-overlay publish call.
type OverlayParams struct {
	Equipment    string
	OperationID  string
	SeenRevision int64
	BatchLo      int64
	BatchHi      int64
	TempLo       int64
	TempHi       int64
	Content      string // canonical JSON
	RequestHash  string // hash over all request parameters, kind included
}

// PublishResult is the outcome of a successful (or replayed) publish.
type PublishResult struct {
	Equipment  string
	Revision   int64
	Segments   []split.Segment
	Rectangles []rectsplit.Rect
	Response   []byte // exact response body, stored for idempotent replay
	Replayed   bool   // true when the result comes from the idempotency ledger
}

// Publish atomically validates and applies a base-calibration publish call.
//
// The new revision snapshots both the freshly split base segments and the
// overlay rectangles unchanged, so overlay state is preserved across base
// publishes.
func (s *Store) Publish(ctx context.Context, p PublishParams) (*PublishResult, error) {
	return s.publish(ctx, ledgerParams{
		Equipment:    p.Equipment,
		OperationID:  p.OperationID,
		Kind:         KindBase,
		SeenRevision: p.SeenRevision,
		RequestHash:  p.RequestHash,
	}, func(tx *sql.Tx, head int64, segs []split.Segment, rects []rectsplit.Rect) (
		[]split.Segment, []rectsplit.Rect, []byte, error,
	) {
		nextSegs := split.Apply(segs, p.Lower, p.Upper, p.Content)
		resp := marshalPublishResponse(p.Equipment, head+1, nextSegs)
		return nextSegs, rects, resp, nil
	})
}

// PublishOverlay atomically validates and applies a temperature-overlay
// publish call. The rectangle partition is split/merged by rectsplit.Apply;
// the base segments are snapshotted unchanged.
func (s *Store) PublishOverlay(ctx context.Context, p OverlayParams) (*PublishResult, error) {
	return s.publish(ctx, ledgerParams{
		Equipment:    p.Equipment,
		OperationID:  p.OperationID,
		Kind:         KindOverlay,
		SeenRevision: p.SeenRevision,
		RequestHash:  p.RequestHash,
	}, func(tx *sql.Tx, head int64, segs []split.Segment, rects []rectsplit.Rect) (
		[]split.Segment, []rectsplit.Rect, []byte, error,
	) {
		nextRects := rectsplit.Apply(rects, p.BatchLo, p.BatchHi, p.TempLo, p.TempHi, p.Content)
		if err := rectsplit.Validate(nextRects); err != nil {
			return nil, nil, nil, err
		}
		resp := marshalOverlayResponse(p.Equipment, head+1, nextRects)
		return segs, nextRects, resp, nil
	})
}

// ledgerParams is the part of a publish that interacts with the idempotency
// ledger and the equipment head.
type ledgerParams struct {
	Equipment    string
	OperationID  string
	Kind         string
	SeenRevision int64
	RequestHash  string
}

// applyFn computes the next revision's segment snapshot, rectangle snapshot
// and response body from the current head state. Any error it returns aborts
// (rolls back) the whole transaction.
type applyFn func(tx *sql.Tx, head int64, segs []split.Segment, rects []rectsplit.Rect) (
	nextSegs []split.Segment, nextRects []rectsplit.Rect, response []byte, err error)

// publish holds the transaction skeleton shared by both publish kinds.
//
// Ordering inside the transaction matters: the idempotency ledger is checked
// before the revision check so that a legitimate retry of an already applied
// operation replays its first result instead of failing with a stale
// revision error.
func (s *Store) publish(ctx context.Context, p ledgerParams, apply applyFn) (*PublishResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() // no-op after Commit

	// Create the head row on first contact, then lock it. Concurrent
	// publishers for the same equipment serialize on this row lock.
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO heads (equipment, revision) VALUES ($1, 0)
		 ON CONFLICT (equipment) DO NOTHING`, p.Equipment); err != nil {
		return nil, err
	}
	var head int64
	if err := tx.QueryRowContext(ctx,
		`SELECT revision FROM heads WHERE equipment = $1 FOR UPDATE`, p.Equipment,
	).Scan(&head); err != nil {
		return nil, err
	}

	// Idempotency: same operation id replays the first result if the
	// parameters (kind included) match, and is rejected as a conflict if
	// they do not.
	var (
		existingKind     string
		existingHash     string
		existingRevision int64
		existingResponse []byte
	)
	err = tx.QueryRowContext(ctx,
		`SELECT kind, request_hash, revision, response FROM operations
		 WHERE equipment = $1 AND operation_id = $2`,
		p.Equipment, p.OperationID,
	).Scan(&existingKind, &existingHash, &existingRevision, &existingResponse)
	switch {
	case err == nil:
		if existingKind != p.Kind || existingHash != p.RequestHash {
			return nil, ErrOperationConflict
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return &PublishResult{
			Equipment: p.Equipment,
			Revision:  existingRevision,
			Response:  existingResponse,
			Replayed:  true,
		}, nil
	case !errors.Is(err, sql.ErrNoRows):
		return nil, err
	}

	// Optimistic concurrency: the caller must have seen the current head.
	if p.SeenRevision != head {
		return nil, ErrStaleRevision
	}

	segs, err := loadSegments(ctx, tx, p.Equipment, head)
	if err != nil {
		return nil, err
	}
	rects, err := loadRects(ctx, tx, p.Equipment, head)
	if err != nil {
		return nil, err
	}

	nextSegs, nextRects, response, err := apply(tx, head, segs, rects)
	if err != nil {
		return nil, err
	}
	newRevision := head + 1

	// Both layers are snapshotted at every revision, regardless of which
	// kind of publish produced it.
	for _, sg := range nextSegs {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO segments (equipment, revision, lower, upper, content)
			 VALUES ($1, $2, $3, $4, $5)`,
			p.Equipment, newRevision, sg.Lower, sg.Upper, sg.Content); err != nil {
			return nil, err
		}
	}
	for _, rc := range nextRects {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO overlay_rects
			 (equipment, revision, batch_lo, batch_hi, temp_lo, temp_hi, content)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			p.Equipment, newRevision, rc.BatchLo, rc.BatchHi, rc.TempLo, rc.TempHi, rc.Content); err != nil {
			return nil, err
		}
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE heads SET revision = $2 WHERE equipment = $1`,
		p.Equipment, newRevision); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO operations (equipment, operation_id, kind, request_hash, revision, response)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		p.Equipment, p.OperationID, p.Kind, p.RequestHash, newRevision, response); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &PublishResult{
		Equipment:  p.Equipment,
		Revision:   newRevision,
		Segments:   nextSegs,
		Rectangles: nextRects,
		Response:   response,
	}, nil
}

// QueryResult is the unique segment effective for a batch at a revision.
type QueryResult struct {
	Equipment string
	Batch     int64
	Revision  int64
	Lower     int64
	Upper     int64
	Content   string // canonical JSON
}

// Query returns the base segment covering batch at the given revision, or at
// the current head revision when revision is nil. Temperature overlays are
// not considered; this is the unchanged one-dimensional query path used when
// the caller omits the temperature parameter.
func (s *Store) Query(ctx context.Context, equipment string, batch int64, revision *int64) (*QueryResult, error) {
	rev, err := s.resolveRevision(ctx, equipment, revision)
	if err != nil {
		return nil, err
	}

	var r QueryResult
	err = s.db.QueryRowContext(ctx,
		`SELECT lower, upper, content FROM segments
		 WHERE equipment = $1 AND revision = $2 AND lower <= $3 AND $3 < upper`,
		equipment, rev, batch,
	).Scan(&r.Lower, &r.Upper, &r.Content)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrBatchNotCovered
	}
	if err != nil {
		return nil, err
	}
	r.Equipment = equipment
	r.Batch = batch
	r.Revision = rev
	return &r, nil
}

// QueryResult2D is the unique calibration effective for a (batch,
// temperature) point at a revision: an overlay rectangle when the point is
// covered, otherwise the base batch calibration ("fallback").
type QueryResult2D struct {
	Equipment   string
	Batch       int64
	Temperature int64
	Revision    int64
	Source      string // KindOverlay or KindBase
	Lower       int64  // batch span: rectangle or base segment
	Upper       int64
	TempLower   int64 // 0 when Source == KindBase
	TempUpper   int64 // 0 when Source == KindBase
	Content     string
}

// Query2D returns the calibration effective at (batch, temperature) at the
// given revision (or the current head when revision is nil). Overlay
// rectangles take precedence; a point no rectangle covers falls back to the
// base segment for that batch.
func (s *Store) Query2D(ctx context.Context, equipment string, batch, temperature int64, revision *int64) (*QueryResult2D, error) {
	rev, err := s.resolveRevision(ctx, equipment, revision)
	if err != nil {
		return nil, err
	}

	var r QueryResult2D
	err = s.db.QueryRowContext(ctx,
		`SELECT batch_lo, batch_hi, temp_lo, temp_hi, content FROM overlay_rects
		 WHERE equipment = $1 AND revision = $2
		   AND batch_lo <= $3 AND $3 < batch_hi
		   AND temp_lo <= $4 AND $4 < temp_hi`,
		equipment, rev, batch, temperature,
	).Scan(&r.Lower, &r.Upper, &r.TempLower, &r.TempUpper, &r.Content)
	switch {
	case err == nil:
		r.Equipment = equipment
		r.Batch = batch
		r.Temperature = temperature
		r.Revision = rev
		r.Source = KindOverlay
		return &r, nil
	case !errors.Is(err, sql.ErrNoRows):
		return nil, err
	}

	// Overlay miss: fall back to the base batch calibration.
	err = s.db.QueryRowContext(ctx,
		`SELECT lower, upper, content FROM segments
		 WHERE equipment = $1 AND revision = $2 AND lower <= $3 AND $3 < upper`,
		equipment, rev, batch,
	).Scan(&r.Lower, &r.Upper, &r.Content)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrBatchNotCovered
	}
	if err != nil {
		return nil, err
	}
	r.Equipment = equipment
	r.Batch = batch
	r.Temperature = temperature
	r.Revision = rev
	r.Source = KindBase
	return &r, nil
}

// resolveRevision maps an optional revision selector to a concrete revision,
// validating equipment and revision existence.
func (s *Store) resolveRevision(ctx context.Context, equipment string, revision *int64) (int64, error) {
	var head int64
	err := s.db.QueryRowContext(ctx,
		`SELECT revision FROM heads WHERE equipment = $1`, equipment,
	).Scan(&head)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrEquipmentNotFound
	}
	if err != nil {
		return 0, err
	}
	if revision != nil {
		if *revision < 1 || *revision > head {
			return 0, ErrRevisionNotFound
		}
		return *revision, nil
	}
	return head, nil
}

func loadSegments(ctx context.Context, tx *sql.Tx, equipment string, revision int64) ([]split.Segment, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT lower, upper, content FROM segments
		 WHERE equipment = $1 AND revision = $2 ORDER BY lower`,
		equipment, revision)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []split.Segment
	for rows.Next() {
		var sg split.Segment
		if err := rows.Scan(&sg.Lower, &sg.Upper, &sg.Content); err != nil {
			return nil, err
		}
		out = append(out, sg)
	}
	return out, rows.Err()
}

func loadRects(ctx context.Context, tx *sql.Tx, equipment string, revision int64) ([]rectsplit.Rect, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT batch_lo, batch_hi, temp_lo, temp_hi, content FROM overlay_rects
		 WHERE equipment = $1 AND revision = $2
		 ORDER BY batch_lo, temp_lo`,
		equipment, revision)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []rectsplit.Rect
	for rows.Next() {
		var rc rectsplit.Rect
		if err := rows.Scan(&rc.BatchLo, &rc.BatchHi, &rc.TempLo, &rc.TempHi, &rc.Content); err != nil {
			return nil, err
		}
		out = append(out, rc)
	}
	return out, rows.Err()
}

func marshalPublishResponse(equipment string, revision int64, segs []split.Segment) []byte {
	type segmentJSON struct {
		Lower   int64           `json:"lower"`
		Upper   int64           `json:"upper"`
		Content json.RawMessage `json:"content"`
	}
	type responseJSON struct {
		Equipment string        `json:"equipment"`
		Revision  int64         `json:"revision"`
		Segments  []segmentJSON `json:"segments"`
	}
	resp := responseJSON{
		Equipment: equipment,
		Revision:  revision,
		Segments:  make([]segmentJSON, 0, len(segs)),
	}
	for _, sg := range segs {
		resp.Segments = append(resp.Segments, segmentJSON{
			Lower:   sg.Lower,
			Upper:   sg.Upper,
			Content: json.RawMessage(sg.Content),
		})
	}
	b, _ := json.Marshal(resp) // cannot fail: all fields are marshalable
	return b
}

func marshalOverlayResponse(equipment string, revision int64, rects []rectsplit.Rect) []byte {
	type rectJSON struct {
		BatchLower int64           `json:"batch_lower"`
		BatchUpper int64           `json:"batch_upper"`
		TempLower  int64           `json:"temp_lower"`
		TempUpper  int64           `json:"temp_upper"`
		Content    json.RawMessage `json:"content"`
	}
	type responseJSON struct {
		Equipment  string     `json:"equipment"`
		Revision   int64      `json:"revision"`
		Rectangles []rectJSON `json:"rectangles"`
	}
	resp := responseJSON{
		Equipment:  equipment,
		Revision:   revision,
		Rectangles: make([]rectJSON, 0, len(rects)),
	}
	for _, rc := range rects {
		resp.Rectangles = append(resp.Rectangles, rectJSON{
			BatchLower: rc.BatchLo,
			BatchUpper: rc.BatchHi,
			TempLower:  rc.TempLo,
			TempUpper:  rc.TempHi,
			Content:    json.RawMessage(rc.Content),
		})
	}
	b, _ := json.Marshal(resp) // cannot fail: all fields are marshalable
	return b
}
