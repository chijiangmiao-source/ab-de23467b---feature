package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// These integration tests run only when CALIB_TEST_DATABASE_URL points at a
// reachable PostgreSQL instance (the verify compose service provisions one).
func testStore(t *testing.T) (*Store, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("CALIB_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("CALIB_TEST_DATABASE_URL not set; skipping PostgreSQL integration tests")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	st := New(db)
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return st, db
}

func uniqueEq(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("eq-it-%d-%s", time.Now().UnixNano(), t.Name())
}

func TestIntegrationPublishAnd2DQuery(t *testing.T) {
	st, _ := testStore(t)
	ctx := context.Background()
	eq := uniqueEq(t)

	pub := func(op string, seen, lo, hi int64, content string) int64 {
		t.Helper()
		res, err := st.Publish(ctx, PublishParams{
			Equipment: eq, OperationID: op, SeenRevision: seen,
			Lower: lo, Upper: hi, Content: content,
			RequestHash: "base-" + op,
		})
		if err != nil {
			t.Fatalf("base publish %s: %v", op, err)
		}
		return res.Revision
	}
	ovl := func(op string, seen, bl, bh, tl, th int64, content string) int64 {
		t.Helper()
		res, err := st.PublishOverlay(ctx, OverlayParams{
			Equipment: eq, OperationID: op, SeenRevision: seen,
			BatchLo: bl, BatchHi: bh, TempLo: tl, TempHi: th, Content: content,
			RequestHash: "overlay-" + op,
		})
		if err != nil {
			t.Fatalf("overlay publish %s: %v", op, err)
		}
		return res.Revision
	}

	pub("b1", 0, 0, 200, `"BASE"`)       // rev 1
	ovl("o1", 1, 0, 200, 0, 100, `"LO"`) // rev 2
	ovl("o2", 2, 50, 150, 40, 60, `"X"`) // rev 3: cross split

	q2 := func(batch, temp int64, rev *int64) *QueryResult2D {
		t.Helper()
		r, err := st.Query2D(ctx, eq, batch, temp, rev)
		if err != nil {
			t.Fatalf("query2d (%d,%d): %v", batch, temp, err)
		}
		return r
	}
	ptr := func(v int64) *int64 { return &v }

	r := q2(100, 50, ptr(3))
	if r.Source != KindOverlay || r.Content != `"X"` || r.Lower != 50 || r.Upper != 150 ||
		r.TempLower != 40 || r.TempUpper != 60 {
		t.Fatalf("cross center = %+v", r)
	}
	// Temperature 60 is the inclusive lower boundary of the high-temp
	// residual, so it still hits "LO".
	r = q2(100, 60, ptr(3))
	if r.Source != KindOverlay || r.Content != `"LO"` ||
		r.Lower != 50 || r.Upper != 150 || r.TempLower != 60 || r.TempUpper != 100 {
		t.Fatalf("high-temp residual = %+v", r)
	}
	// Half-open upper temperature boundary: overlay miss -> base fallback.
	r = q2(100, 100, ptr(3))
	if r.Source != KindBase || r.Content != `"BASE"` || r.TempLower != 0 || r.TempUpper != 0 {
		t.Fatalf("temperature boundary fallback = %+v", r)
	}
	r = q2(10, 50, ptr(3))
	if r.Source != KindOverlay || r.Content != `"LO"` {
		t.Fatalf("left residual = %+v", r)
	}

	// One-dimensional query ignores overlays at every revision.
	q1, err := st.Query(ctx, eq, 100, ptr(3))
	if err != nil || q1.Content != `"BASE"` {
		t.Fatalf("1D query at rev3 = %+v, err %v", q1, err)
	}

	// A later base publish snapshots overlays unchanged.
	pub("b2", 3, 0, 200, `"BASE2"`) // rev 4
	r = q2(100, 50, ptr(4))
	if r.Source != KindOverlay || r.Content != `"X"` {
		t.Fatalf("overlay lost across base publish: %+v", r)
	}
	r = q2(100, 100, ptr(4))
	if r.Source != KindBase || r.Content != `"BASE2"` {
		t.Fatalf("base update not visible in fallback: %+v", r)
	}

	// Historical revisions recompute uniquely.
	if h := q2(100, 50, ptr(2)); h.Source != KindOverlay || h.Content != `"LO"` {
		t.Fatalf("rev2 history = %+v", h)
	}
	if h := q2(100, 50, ptr(1)); h.Source != KindBase || h.Content != `"BASE"` {
		t.Fatalf("rev1 history = %+v", h)
	}

	// Uncovered batch (base miss and overlay miss).
	if _, err := st.Query2D(ctx, eq, 300, 50, ptr(4)); !errors.Is(err, ErrBatchNotCovered) {
		t.Fatalf("uncovered 2D point err = %v, want ErrBatchNotCovered", err)
	}
}

func TestIntegrationIdempotencyAndConflict(t *testing.T) {
	st, _ := testStore(t)
	ctx := context.Background()
	eq := uniqueEq(t)

	first, err := st.PublishOverlay(ctx, OverlayParams{
		Equipment: eq, OperationID: "op1", SeenRevision: 0,
		BatchLo: 0, BatchHi: 100, TempLo: 0, TempHi: 100,
		Content: `{"v":1}`, RequestHash: "h1",
	})
	if err != nil {
		t.Fatalf("first overlay: %v", err)
	}
	replay, err := st.PublishOverlay(ctx, OverlayParams{
		Equipment: eq, OperationID: "op1", SeenRevision: 0,
		BatchLo: 0, BatchHi: 100, TempLo: 0, TempHi: 100,
		Content: `{"v":1}`, RequestHash: "h1",
	})
	if err != nil || !replay.Replayed || replay.Revision != first.Revision ||
		string(replay.Response) != string(first.Response) {
		t.Fatalf("replay = rev %d replayed %v err %v", replay.Revision, replay.Replayed, err)
	}

	// Different parameters under the same operation id.
	_, err = st.PublishOverlay(ctx, OverlayParams{
		Equipment: eq, OperationID: "op1", SeenRevision: 0,
		BatchLo: 0, BatchHi: 100, TempLo: 0, TempHi: 200,
		Content: `{"v":1}`, RequestHash: "h2",
	})
	if !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("param reuse err = %v, want ErrOperationConflict", err)
	}
	// Same operation id, different publish kind.
	_, err = st.Publish(ctx, PublishParams{
		Equipment: eq, OperationID: "op1", SeenRevision: 0,
		Lower: 0, Upper: 100, Content: `{"v":1}`, RequestHash: "h1",
	})
	if !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("cross-kind reuse err = %v, want ErrOperationConflict", err)
	}
	// Stale seen revision.
	_, err = st.PublishOverlay(ctx, OverlayParams{
		Equipment: eq, OperationID: "op2", SeenRevision: 5,
		BatchLo: 0, BatchHi: 100, TempLo: 0, TempHi: 100,
		Content: `"Z"`, RequestHash: "h3",
	})
	if !errors.Is(err, ErrStaleRevision) {
		t.Fatalf("stale err = %v, want ErrStaleRevision", err)
	}
	// Legitimate retry still replays after the stale attempt; head is still 1.
	if _, err := st.Query2D(ctx, eq, 1, 1, nil); err != nil {
		t.Fatalf("state changed after failed attempts: %v", err)
	}
}

func TestIntegrationHistoryIsImmutable(t *testing.T) {
	st, db := testStore(t)
	ctx := context.Background()
	eq := uniqueEq(t)

	if _, err := st.Publish(ctx, PublishParams{
		Equipment: eq, OperationID: "b", SeenRevision: 0,
		Lower: 0, Upper: 10, Content: `"B"`, RequestHash: "hb",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PublishOverlay(ctx, OverlayParams{
		Equipment: eq, OperationID: "o", SeenRevision: 1,
		BatchLo: 0, BatchHi: 10, TempLo: 0, TempHi: 10,
		Content: `"O"`, RequestHash: "ho",
	}); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		stmt string
	}{
		{`UPDATE segments SET content='"X"' WHERE equipment = '` + eq + `'`},
		{`DELETE FROM segments WHERE equipment = '` + eq + `'`},
		{`UPDATE overlay_rects SET content='"X"' WHERE equipment = '` + eq + `'`},
		{`DELETE FROM overlay_rects WHERE equipment = '` + eq + `'`},
		{`UPDATE operations SET response='{}' WHERE equipment = '` + eq + `'`},
	} {
		if _, err := db.ExecContext(ctx, tc.stmt); err == nil {
			t.Fatalf("%s succeeded, want immutability error", tc.stmt)
		} else if !strings.Contains(err.Error(), "immutable") {
			t.Fatalf("%s rejected with unexpected error: %v", tc.stmt, err)
		}
	}
}
