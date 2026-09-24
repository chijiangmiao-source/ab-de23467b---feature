package split2d

import (
	"reflect"
	"testing"
)

func rect(bl, bu, tl, tu int64, c string) Rect {
	return Rect{bl, bu, tl, tu, c}
}

func TestApply(t *testing.T) {
	tests := []struct {
		name           string
		rects          []Rect
		bl, bu, tl, tu int64
		content        string
		want           []Rect
	}{
		{
			name:  "empty state",
			rects: nil,
			bl:    10, bu: 20, tl: 0, tu: 5, content: "A",
			want: []Rect{rect(10, 20, 0, 5, "A")},
		},
		{
			name:  "disjoint keeps both",
			rects: []Rect{rect(0, 10, 0, 10, "X")},
			bl:    20, bu: 30, tl: 0, tu: 10, content: "A",
			want: []Rect{rect(0, 10, 0, 10, "X"), rect(20, 30, 0, 10, "A")},
		},
		{
			name:  "cross punch keeps eight-neighbour residuals",
			rects: []Rect{rect(0, 100, 0, 100, "X")},
			bl:    40, bu: 60, tl: 40, tu: 60, content: "A",
			want: []Rect{
				rect(0, 40, 0, 100, "X"),
				rect(40, 60, 0, 40, "X"),
				rect(40, 60, 40, 60, "A"),
				rect(40, 60, 60, 100, "X"),
				rect(60, 100, 0, 100, "X"),
			},
		},
		{
			name: "cross over two existing rectangles splits all overlapped",
			rects: []Rect{
				rect(0, 50, 0, 100, "L"),
				rect(50, 100, 0, 100, "R"),
			},
			bl: 25, bu: 75, tl: 25, tu: 75, content: "A",
			want: []Rect{
				rect(0, 25, 0, 100, "L"),
				rect(25, 50, 0, 25, "L"),
				rect(25, 75, 25, 75, "A"),
				rect(25, 50, 75, 100, "L"),
				rect(50, 75, 0, 25, "R"),
				rect(50, 75, 75, 100, "R"),
				rect(75, 100, 0, 100, "R"),
			},
		},
		{
			name:  "batch neighbours with full shared edge merge",
			rects: []Rect{rect(0, 100, 0, 10, "A")},
			bl:    100, bu: 200, tl: 0, tu: 10, content: "A",
			want: []Rect{rect(0, 200, 0, 10, "A")},
		},
		{
			name:  "temperature neighbours with full shared edge merge",
			rects: []Rect{rect(0, 100, 0, 10, "A")},
			bl:    0, bu: 100, tl: 10, tu: 20, content: "A",
			want: []Rect{rect(0, 100, 0, 20, "A")},
		},
		{
			name:  "partial edge does not merge",
			rects: []Rect{rect(0, 10, 0, 10, "A")},
			bl:    10, bu: 20, tl: 5, tu: 15, content: "A",
			want: []Rect{rect(0, 10, 0, 10, "A"), rect(10, 20, 5, 15, "A")},
		},
		{
			name: "republishing neighbour content bridges equal rectangles",
			rects: []Rect{
				rect(0, 10, 0, 10, "A"),
				rect(10, 20, 0, 10, "B"),
				rect(20, 30, 0, 10, "A"),
			},
			bl: 10, bu: 20, tl: 0, tu: 10, content: "A",
			want: []Rect{rect(0, 30, 0, 10, "A")},
		},
		{
			name: "bridging across temperature axis merges chain",
			rects: []Rect{
				rect(0, 10, 0, 10, "A"),
				rect(0, 10, 20, 30, "A"),
			},
			bl: 0, bu: 10, tl: 10, tu: 20, content: "A",
			want: []Rect{rect(0, 10, 0, 30, "A")},
		},
		{
			name:  "exact cover replaces",
			rects: []Rect{rect(0, 100, 0, 100, "X")},
			bl:    0, bu: 100, tl: 0, tu: 100, content: "A",
			want: []Rect{rect(0, 100, 0, 100, "A")},
		},
		{
			name:  "cover one temperature band of a rectangle",
			rects: []Rect{rect(0, 100, 0, 100, "X")},
			bl:    0, bu: 100, tl: 40, tu: 60, content: "A",
			want: []Rect{
				rect(0, 100, 0, 40, "X"),
				rect(0, 100, 40, 60, "A"),
				rect(0, 100, 60, 100, "X"),
			},
		},
		{
			name:  "disjoint temperature bands in same batch range coexist",
			rects: []Rect{rect(0, 100, 0, 10, "X")},
			bl:    0, bu: 100, tl: 20, tu: 30, content: "Y",
			want: []Rect{
				rect(0, 100, 0, 10, "X"),
				rect(0, 100, 20, 30, "Y"),
			},
		},
		{
			name:  "negative coordinates extend into uncovered space",
			rects: []Rect{rect(-100, 0, -10, 0, "X")},
			bl:    -50, bu: 50, tl: -5, tu: 5, content: "A",
			want: []Rect{
				rect(-100, -50, -10, 0, "X"),
				rect(-50, 0, -10, -5, "X"),
				rect(-50, 50, -5, 5, "A"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Apply(tt.rects, tt.bl, tt.bu, tt.tl, tt.tu, tt.content)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Apply() =\n%+v\nwant\n%+v", got, tt.want)
			}
			assertInvariants(t, got)
		})
	}
}

func TestLookup(t *testing.T) {
	rects := []Rect{
		rect(0, 100, 0, 100, "X"),
		rect(0, 100, 100, 200, "Y"),
		rect(100, 200, 0, 100, "Z"),
	}
	for _, tc := range []struct {
		batch, temp int64
		want        string
		ok          bool
	}{
		{50, 50, "X", true},
		{50, 150, "Y", true},
		{150, 50, "Z", true},
		{150, 150, "", false}, // uncovered corner falls back to base
		{0, 0, "X", true},     // lower bounds inclusive
		{100, 100, "", false}, // upper bounds exclusive
	} {
		got, ok := Lookup(rects, tc.batch, tc.temp)
		if ok != tc.ok || (ok && got.Content != tc.want) {
			t.Fatalf("Lookup(%d,%d) = %q,%v want %q,%v",
				tc.batch, tc.temp, got.Content, ok, tc.want, tc.ok)
		}
	}
}

// assertInvariants verifies that rects are sorted, pairwise
// non-overlapping, non-degenerate and free of mergeable neighbours.
func assertInvariants(t *testing.T, rects []Rect) {
	t.Helper()
	for i, r := range rects {
		if r.BatchLower >= r.BatchUpper || r.TempLower >= r.TempUpper {
			t.Fatalf("degenerate rectangle %+v", r)
		}
		if i > 0 {
			p := rects[i-1]
			if p.BatchLower > r.BatchLower ||
				(p.BatchLower == r.BatchLower && p.TempLower >= r.TempLower) {
				t.Fatalf("rectangles not sorted: %+v before %+v", p, r)
			}
		}
		for j := i + 1; j < len(rects); j++ {
			q := rects[j]
			if r.BatchLower < q.BatchUpper && q.BatchLower < r.BatchUpper &&
				r.TempLower < q.TempUpper && q.TempLower < r.TempUpper {
				t.Fatalf("rectangles overlap: %+v and %+v", r, q)
			}
			if r.Content == q.Content {
				fullBatchEdge := (r.BatchUpper == q.BatchLower || q.BatchUpper == r.BatchLower) &&
					r.TempLower == q.TempLower && r.TempUpper == q.TempUpper
				fullTempEdge := r.BatchLower == q.BatchLower && r.BatchUpper == q.BatchUpper &&
					(r.TempUpper == q.TempLower || q.TempUpper == r.TempLower)
				if fullBatchEdge || fullTempEdge {
					t.Fatalf("mergeable equal-content rectangles not merged: %+v and %+v", r, q)
				}
			}
		}
	}
}
