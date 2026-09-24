// Package store persists calibration revisions in PostgreSQL.
//
// Data model:
//
//   - heads:            one row per equipment, pointing at the current
//     revision.
//   - segments:         append-only snapshot of the full base batch
//     segment list of every revision.
//   - temperature_zones: append-only snapshot of the full temperature-zone
//     rectangle list of every revision.
//   - operations:       idempotency ledger mapping (equipment,
//     operation_id) to the request hash and the stored
//     first response. Base calibrations and temperature
//     zones share the same ledger namespace, so an
//     operation id cannot be reused across the two entry
//     points either.
//
// Every revision carries a complete snapshot of BOTH layers: publishing a
// base calibration copies the current zone snapshot forward, and
// publishing a temperature zone copies the base snapshot forward. Rows are
// never updated or deleted (enforced by trigger), so every historical
// revision stays recomputable in either dimension.
//
// Every publish runs in a single transaction that locks the equipment's
// head row with SELECT ... FOR UPDATE. The lock serializes publishers per
// equipment, so two publishes racing on the same seen revision cannot both
// succeed, and a failed transaction leaves no partial split behind.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/example/calibsvc/internal/split"
	"github.com/example/calibsvc/internal/split2d"
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

CREATE TABLE IF NOT EXISTS temperature_zones (
    equipment   TEXT NOT NULL,
    revision    BIGINT NOT NULL,
    batch_lower BIGINT NOT NULL,
    batch_upper BIGINT NOT NULL,
    temp_lower  BIGINT NOT NULL,
    temp_upper  BIGINT NOT NULL,
    content     TEXT NOT NULL,
    PRIMARY KEY (equipment, revision, batch_lower, temp_lower),
    CHECK (batch_lower < batch_upper AND temp_lower < temp_upper)
);

CREATE TABLE IF NOT EXISTS operations (
    equipment    TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    revision     BIGINT NOT NULL,
    response     TEXT NOT NULL,
    PRIMARY KEY (equipment, operation_id)
);

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
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'temperature_zones_immutable') THEN
        CREATE TRIGGER temperature_zones_immutable BEFORE UPDATE OR DELETE ON temperature_zones
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
	RequestHash  string // hash over all request parameters, incl. kind
}

// ZonePublishParams describes one validated temperature-zone publish call.
type ZonePublishParams struct {
	Equipment    string
	OperationID  string
	SeenRevision int64
	BatchLower   int64
	BatchUpper   int64
	TempLower    int64
	TempUpper    int64
	Content      string // canonical JSON
	RequestHash  string // hash over all request parameters, incl. kind
}

// PublishResult is the outcome of a successful (or replayed) publish of
// either kind. Response holds the exact response body stored for idempotent
// replay; Segments/Zones are populated only for non-replayed calls.
type PublishResult struct {
	Equipment string
	Revision  int64
	Segments  []split.Segment
	Zones     []split2d.Rect
	Response  []byte // exact response body, stored for idempotent replay
	Replayed  bool   // true when the result comes from the idempotency ledger
}

// Publish atomically validates and applies a base-calibration publish
// call. The current temperature-zone snapshot is copied into the new
// revision unchanged.
//
// Ordering inside the transaction matters: the idempotency ledger is
// checked before the revision check so that a legitimate retry of an
// already applied operation replays its first result instead of failing
// with a stale revision error.
func (s *Store) Publish(ctx context.Context, p PublishParams) (*PublishResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() // no-op after Commit

	head, err := lockHead(ctx, tx, p.Equipment)
	if err != nil {
		return nil, err
	}
	if rev, resp, err := replayLedger(ctx, tx, p.Equipment, p.OperationID, p.RequestHash); err != nil {
		return nil, err
	} else if resp != nil {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return &PublishResult{Equipment: p.Equipment, Revision: rev, Response: resp, Replayed: true}, nil
	}
	if p.SeenRevision != head {
		return nil, ErrStaleRevision
	}

	currentSegs, err := loadSegments(ctx, tx, p.Equipment, head)
	if err != nil {
		return nil, err
	}
	nextSegs := split.Apply(currentSegs, p.Lower, p.Upper, p.Content)
	newRevision := head + 1

	if err := insertSegments(ctx, tx, p.Equipment, newRevision, nextSegs); err != nil {
		return nil, err
	}
	// The zone layer is unchanged by a base publish, but the new revision
	// must still carry its full snapshot.
	if err := copyLayer(ctx, tx, "temperature_zones", p.Equipment, head, p.Equipment, newRevision); err != nil {
		return nil, err
	}
	if err := advanceHead(ctx, tx, p.Equipment, newRevision); err != nil {
		return nil, err
	}

	response := marshalPublishResponse(p.Equipment, newRevision, nextSegs)
	if err := recordOperation(ctx, tx, p.Equipment, p.OperationID, p.RequestHash, newRevision, response); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &PublishResult{
		Equipment: p.Equipment,
		Revision:  newRevision,
		Segments:  nextSegs,
		Response:  response,
	}, nil
}

// PublishZone atomically validates and applies a temperature-zone publish
// call. The current base segment snapshot is copied into the new revision
// unchanged.
func (s *Store) PublishZone(ctx context.Context, p ZonePublishParams) (*PublishResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	head, err := lockHead(ctx, tx, p.Equipment)
	if err != nil {
		return nil, err
	}
	if rev, resp, err := replayLedger(ctx, tx, p.Equipment, p.OperationID, p.RequestHash); err != nil {
		return nil, err
	} else if resp != nil {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return &PublishResult{Equipment: p.Equipment, Revision: rev, Response: resp, Replayed: true}, nil
	}
	if p.SeenRevision != head {
		return nil, ErrStaleRevision
	}

	currentZones, err := loadZones(ctx, tx, p.Equipment, head)
	if err != nil {
		return nil, err
	}
	nextZones := split2d.Apply(currentZones,
		p.BatchLower, p.BatchUpper, p.TempLower, p.TempUpper, p.Content)
	newRevision := head + 1

	// The base layer is unchanged by a zone publish, but the new revision
	// must still carry its full snapshot.
	if err := copyLayer(ctx, tx, "segments", p.Equipment, head, p.Equipment, newRevision); err != nil {
		return nil, err
	}
	if err := insertZones(ctx, tx, p.Equipment, newRevision, nextZones); err != nil {
		return nil, err
	}
	if err := advanceHead(ctx, tx, p.Equipment, newRevision); err != nil {
		return nil, err
	}

	response := marshalZonePublishResponse(p.Equipment, newRevision, nextZones)
	if err := recordOperation(ctx, tx, p.Equipment, p.OperationID, p.RequestHash, newRevision, response); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &PublishResult{
		Equipment: p.Equipment,
		Revision:  newRevision,
		Zones:     nextZones,
		Response:  response,
	}, nil
}

// QueryResult is the unique calibration effective at a query point.
//
// A base-calibration hit fills Lower/Upper only. A temperature-zone hit
// additionally fills TempLower/TempUpper and reports Source == "zone".
type QueryResult struct {
	Equipment   string
	Batch       int64
	Temperature *int64
	Revision    int64
	Source      string // "base" or "zone"
	Lower       int64
	Upper       int64
	TempLower   int64
	TempUpper   int64
	Content     string // canonical JSON
}

// Query returns the calibration effective at (batch) — or at
// (batch, temperature) when temperature is non-nil — at the given
// revision, or at the current head revision when revision is nil.
//
// With no temperature given, only the base batch calibration is consulted
// and the historical one-dimensional semantics are unchanged. With a
// temperature given, the temperature-zone layer is consulted first; a
// batch covered only outside the point's temperature (or by no zone at
// all) falls back to the base batch calibration.
func (s *Store) Query(ctx context.Context, equipment string, batch, temperature, revision *int64) (*QueryResult, error) {
	var head int64
	err := s.db.QueryRowContext(ctx,
		`SELECT revision FROM heads WHERE equipment = $1`, equipment,
	).Scan(&head)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrEquipmentNotFound
	}
	if err != nil {
		return nil, err
	}

	rev := head
	if revision != nil {
		if *revision < 1 || *revision > head {
			return nil, ErrRevisionNotFound
		}
		rev = *revision
	}

	if temperature != nil {
		var z struct {
			bl, bu, tl, tu int64
			content        string
		}
		err := s.db.QueryRowContext(ctx,
			`SELECT batch_lower, batch_upper, temp_lower, temp_upper, content
			 FROM temperature_zones
			 WHERE equipment = $1 AND revision = $2
			   AND batch_lower <= $3 AND $3 < batch_upper
			   AND temp_lower <= $4 AND $4 < temp_upper`,
			equipment, rev, batch, *temperature,
		).Scan(&z.bl, &z.bu, &z.tl, &z.tu, &z.content)
		switch {
		case err == nil:
			return &QueryResult{
				Equipment:   equipment,
				Batch:       *batch,
				Temperature: temperature,
				Revision:    rev,
				Source:      "zone",
				Lower:       z.bl,
				Upper:       z.bu,
				TempLower:   z.tl,
				TempUpper:   z.tu,
				Content:     z.content,
			}, nil
		case !errors.Is(err, sql.ErrNoRows):
			return nil, err
		}
		// Zone miss: fall through to the base calibration.
	}

	var r QueryResult
	err = s.db.QueryRowContext(ctx,
		`SELECT lower, upper, content FROM segments
		 WHERE equipment = $1 AND revision = $2 AND lower <= $3 AND $3 < upper`,
		equipment, rev, *batch,
	).Scan(&r.Lower, &r.Upper, &r.Content)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrBatchNotCovered
	}
	if err != nil {
		return nil, err
	}
	r.Equipment = equipment
	r.Batch = *batch
	r.Temperature = temperature
	r.Revision = rev
	r.Source = "base"
	return &r, nil
}

// lockHead creates the head row on first contact and locks it for the
// transaction; concurrent publishers for the same equipment serialize on
// this row lock.
func lockHead(ctx context.Context, tx *sql.Tx, equipment string) (int64, error) {
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO heads (equipment, revision) VALUES ($1, 0)
		 ON CONFLICT (equipment) DO NOTHING`, equipment); err != nil {
		return 0, err
	}
	var head int64
	if err := tx.QueryRowContext(ctx,
		`SELECT revision FROM heads WHERE equipment = $1 FOR UPDATE`, equipment,
	).Scan(&head); err != nil {
		return 0, err
	}
	return head, nil
}

// replayLedger returns the stored first response (and its revision) when
// the operation id already exists with the same hash. A nil response with
// no error means the operation is new.
func replayLedger(ctx context.Context, tx *sql.Tx, equipment, operationID, requestHash string) (int64, []byte, error) {
	var (
		existingHash     string
		existingRevision int64
		existingResponse []byte
	)
	err := tx.QueryRowContext(ctx,
		`SELECT request_hash, revision, response FROM operations
		 WHERE equipment = $1 AND operation_id = $2`,
		equipment, operationID,
	).Scan(&existingHash, &existingRevision, &existingResponse)
	switch {
	case err == nil:
		if existingHash != requestHash {
			return 0, nil, ErrOperationConflict
		}
		return existingRevision, existingResponse, nil
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil, nil
	default:
		return 0, nil, err
	}
}

func advanceHead(ctx context.Context, tx *sql.Tx, equipment string, newRevision int64) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE heads SET revision = $2 WHERE equipment = $1`, equipment, newRevision)
	return err
}

func recordOperation(ctx context.Context, tx *sql.Tx, equipment, operationID, requestHash string, revision int64, response []byte) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO operations (equipment, operation_id, request_hash, revision, response)
		 VALUES ($1, $2, $3, $4, $5)`,
		equipment, operationID, requestHash, revision, response)
	return err
}

func insertSegments(ctx context.Context, tx *sql.Tx, equipment string, revision int64, segs []split.Segment) error {
	for _, sg := range segs {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO segments (equipment, revision, lower, upper, content)
			 VALUES ($1, $2, $3, $4, $5)`,
			equipment, revision, sg.Lower, sg.Upper, sg.Content); err != nil {
			return err
		}
	}
	return nil
}

func insertZones(ctx context.Context, tx *sql.Tx, equipment string, revision int64, zones []split2d.Rect) error {
	for _, z := range zones {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO temperature_zones
			 (equipment, revision, batch_lower, batch_upper, temp_lower, temp_upper, content)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			equipment, revision, z.BatchLower, z.BatchUpper, z.TempLower, z.TempUpper, z.Content); err != nil {
			return err
		}
	}
	return nil
}

// copyLayer duplicates one snapshot layer (table must be "segments" or
// "temperature_zones") of srcRevision into dstRevision. The table name is
// not user input; all values are bind parameters.
func copyLayer(ctx context.Context, tx *sql.Tx, table, srcEquipment string, srcRevision int64, dstEquipment string, dstRevision int64) error {
	switch table {
	case "segments":
		_, err := tx.ExecContext(ctx,
			`INSERT INTO segments (equipment, revision, lower, upper, content)
			 SELECT $1, $2, lower, upper, content FROM segments
			 WHERE equipment = $3 AND revision = $4`,
			dstEquipment, dstRevision, srcEquipment, srcRevision)
		return err
	case "temperature_zones":
		_, err := tx.ExecContext(ctx,
			`INSERT INTO temperature_zones
			 (equipment, revision, batch_lower, batch_upper, temp_lower, temp_upper, content)
			 SELECT $1, $2, batch_lower, batch_upper, temp_lower, temp_upper, content
			 FROM temperature_zones
			 WHERE equipment = $3 AND revision = $4`,
			dstEquipment, dstRevision, srcEquipment, srcRevision)
		return err
	default:
		return errors.New("unknown snapshot layer: " + table)
	}
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

func loadZones(ctx context.Context, tx *sql.Tx, equipment string, revision int64) ([]split2d.Rect, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT batch_lower, batch_upper, temp_lower, temp_upper, content
		 FROM temperature_zones
		 WHERE equipment = $1 AND revision = $2
		 ORDER BY batch_lower, temp_lower`,
		equipment, revision)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []split2d.Rect
	for rows.Next() {
		var z split2d.Rect
		if err := rows.Scan(&z.BatchLower, &z.BatchUpper, &z.TempLower, &z.TempUpper, &z.Content); err != nil {
			return nil, err
		}
		out = append(out, z)
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

func marshalZonePublishResponse(equipment string, revision int64, zones []split2d.Rect) []byte {
	type zoneJSON struct {
		BatchLower int64           `json:"batch_lower"`
		BatchUpper int64           `json:"batch_upper"`
		TempLower  int64           `json:"temp_lower"`
		TempUpper  int64           `json:"temp_upper"`
		Content    json.RawMessage `json:"content"`
	}
	type responseJSON struct {
		Equipment string     `json:"equipment"`
		Revision  int64      `json:"revision"`
		Zones     []zoneJSON `json:"zones"`
	}
	resp := responseJSON{
		Equipment: equipment,
		Revision:  revision,
		Zones:     make([]zoneJSON, 0, len(zones)),
	}
	for _, z := range zones {
		resp.Zones = append(resp.Zones, zoneJSON{
			BatchLower: z.BatchLower,
			BatchUpper: z.BatchUpper,
			TempLower:  z.TempLower,
			TempUpper:  z.TempUpper,
			Content:    json.RawMessage(z.Content),
		})
	}
	b, _ := json.Marshal(resp) // cannot fail: all fields are marshalable
	return b
}
