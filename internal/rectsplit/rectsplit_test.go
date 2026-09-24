package rectsplit

import (
	"reflect"
	"testing"
)

func r(bl, bh, tl, th int64, c string) Rect {
	return Rect{BatchLo: bl, BatchHi: bh, TempLo: tl, TempHi: th, Content: c}
}

func TestApply(t *testing.T) {
	tests := []struct {
		name           string
		rects          []Rect
		bl, bh, tl, th int64
		content        string
		want           []Rect
	}{
		{
			name:  "empty state",
			rects: nil,
			bl:    0, bh: 10, tl: 20, th: 30, content: "A",
			want: []Rect{r(0, 10, 20, 30, "A")},
		},
		{
			name:  "cross overlap splits into four residuals around the new rect",
			rects: []Rect{r(0, 100, 0, 100, "X")},
			bl:    40, bh: 60, tl: 40, th: 60, content: "A",
			want: []Rect{
				r(0, 40, 0, 100, "X"),
				r(40, 60, 0, 40, "X"),
				r(40, 60, 40, 60, "A"),
				r(40, 60, 60, 100, "X"),
				r(60, 100, 0, 100, "X"),
			},
		},
		{
			name: "cross over multiple existing rectangles",
			rects: []Rect{
				r(0, 50, 0, 100, "L"),
				r(50, 100, 0, 100, "R"),
			},
			bl: 25, bh: 75, tl: 25, th: 75, content: "A",
			want: []Rect{
				r(0, 25, 0, 100, "L"),
				r(25, 50, 0, 25, "L"),
				r(25, 75, 25, 75, "A"),
				r(25, 50, 75, 100, "L"),
				r(50, 75, 0, 25, "R"),
				r(50, 75, 75, 100, "R"),
				r(75, 100, 0, 100, "R"),
			},
		},
		{
			name:  "disjoint rectangle is untouched",
			rects: []Rect{r(0, 10, 0, 10, "X")},
			bl:    20, bh: 30, tl: 20, th: 30, content: "A",
			want: []Rect{r(0, 10, 0, 10, "X"), r(20, 30, 20, 30, "A")},
		},
		{
			name:  "touching only is disjoint",
			rects: []Rect{r(0, 10, 0, 10, "X")},
			bl:    10, bh: 20, tl: 0, th: 10, content: "X",
			want: []Rect{r(0, 20, 0, 10, "X")}, // full-edge neighbours, equal -> merge
		},
		{
			name:  "exact cover replaces",
			rects: []Rect{r(0, 10, 0, 10, "X")},
			bl:    0, bh: 10, tl: 0, th: 10, content: "A",
			want: []Rect{r(0, 10, 0, 10, "A")},
		},
		{
			name:  "cover whole batch band keeps two temperature residuals",
			rects: []Rect{r(0, 100, 0, 100, "X")},
			bl:    0, bh: 100, tl: 40, th: 60, content: "A",
			want: []Rect{
				r(0, 100, 0, 40, "X"),
				r(0, 100, 40, 60, "A"),
				r(0, 100, 60, 100, "X"),
			},
		},
		{
			name:  "adjacent horizontal equal rectangles merge",
			rects: []Rect{r(0, 10, 0, 10, "A")},
			bl:    10, bh: 20, tl: 0, th: 10, content: "A",
			want: []Rect{r(0, 20, 0, 10, "A")},
		},
		{
			name:  "adjacent vertical equal rectangles merge",
			rects: []Rect{r(0, 10, 10, 20, "A")},
			bl:    0, bh: 10, tl: 0, th: 10, content: "A",
			want: []Rect{r(0, 10, 0, 20, "A")},
		},
		{
			name:  "adjacent different content does not merge",
			rects: []Rect{r(0, 10, 0, 10, "A")},
			bl:    10, bh: 20, tl: 0, th: 10, content: "B",
			want: []Rect{
				r(0, 10, 0, 10, "A"),
				r(10, 20, 0, 10, "B"),
			},
		},
		{
			name: "partial-edge neighbours do not merge",
			// The two A rectangles overlap in batch but only touch in
			// temperature, so they are disjoint; the shared edge is only
			// partial ([5,10)) and must not be merged away.
			rects: []Rect{r(0, 10, 0, 10, "A")},
			bl:    5, bh: 15, tl: 10, th: 20, content: "A",
			want: []Rect{
				r(0, 10, 0, 10, "A"),
				r(5, 15, 10, 20, "A"),
			},
		},
		{
			name: "partial edge created by a split does not merge",
			// Publishing A above only part of an X band leaves an X residual
			// whose edge the new A rect shares only partially: no merge.
			rects: []Rect{r(0, 10, 0, 10, "X")},
			bl:    5, bh: 10, tl: 10, th: 20, content: "A",
			want: []Rect{
				r(0, 10, 0, 10, "X"),
				r(5, 10, 10, 20, "A"),
			},
		},
		{
			name: "new rectangle bridges two equal bands vertically",
			rects: []Rect{
				r(0, 10, 0, 5, "A"),
				r(0, 10, 5, 10, "B"),
			},
			bl: 0, bh: 10, tl: 5, th: 10, content: "A",
			want: []Rect{r(0, 10, 0, 10, "A")},
		},
		{
			name: "residuals of same content merge across the new band",
			rects: []Rect{
				r(0, 20, 0, 10, "A"),
				r(0, 20, 10, 20, "B"),
				r(0, 20, 20, 30, "A"),
			},
			bl: 0, bh: 20, tl: 10, th: 20, content: "C",
			want: []Rect{
				// The two "A" bands are stacked but separated by "C"; only a
				// full-edge equal pair merges, so they stay apart.
				r(0, 20, 0, 10, "A"),
				r(0, 20, 10, 20, "C"),
				r(0, 20, 20, 30, "A"),
			},
		},
		{
			name: "L-shaped same-content neighbours do not merge",
			// Two A rectangles sharing only part of an edge.
			rects: []Rect{
				r(0, 10, 0, 10, "A"),
				r(10, 20, 0, 5, "A"),
			},
			bl: 20, bh: 30, tl: 0, th: 10, content: "B",
			want: []Rect{
				r(0, 10, 0, 10, "A"),
				r(10, 20, 0, 5, "A"),
				r(20, 30, 0, 10, "B"),
			},
		},
		{
			name:  "negative coordinates",
			rects: []Rect{r(-100, 0, -50, 50, "X")},
			bl:    -50, bh: 50, tl: -25, th: 25, content: "A",
			want: []Rect{
				r(-100, -50, -50, 50, "X"),
				r(-50, 0, -50, -25, "X"),
				r(-50, 50, -25, 25, "A"),
				r(-50, 0, 25, 50, "X"),
			},
		},
		{
			name:  "same-content cross split merges all residuals back",
			rects: []Rect{r(0, 100, 0, 100, "A")},
			bl:    40, bh: 60, tl: 40, th: 60, content: "A",
			want: []Rect{r(0, 100, 0, 100, "A")},
		},
		{
			name: "same-content band inside equal band keeps partial-edge L apart",
			rects: []Rect{
				r(0, 20, 0, 10, "A"),
				r(0, 20, 10, 20, "B"),
			},
			bl: 5, bh: 15, tl: 0, th: 20, content: "A",
			// The A residuals share only part of the new rectangle's edge
			// (L-shaped union), so they stay separate; the B band keeps its
			// left/right residuals.
			want: []Rect{
				r(0, 5, 0, 10, "A"),
				r(0, 5, 10, 20, "B"),
				r(5, 15, 0, 20, "A"),
				r(15, 20, 0, 10, "A"),
				r(15, 20, 10, 20, "B"),
			},
		},
		{
			name:  "republish identical rectangle is identity",
			rects: []Rect{r(0, 10, 0, 10, "A"), r(10, 20, 0, 10, "B")},
			bl:    0, bh: 10, tl: 0, th: 10, content: "A",
			want: []Rect{r(0, 10, 0, 10, "A"), r(10, 20, 0, 10, "B")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Apply(tt.rects, tt.bl, tt.bh, tt.tl, tt.th, tt.content)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Apply() = %+v\nwant     %+v", got, tt.want)
			}
			if err := Validate(got); err != nil {
				t.Fatalf("result violates invariants: %v: %+v", err, got)
			}
			assertNoMergeablePair(t, got)
		})
	}
}

func TestApplyIsOrderIndependent(t *testing.T) {
	rects := []Rect{
		r(0, 100, 0, 100, "X"),
		r(100, 200, 0, 100, "Y"),
		r(0, 100, 100, 200, "Z"),
	}
	a := Apply(rects, 50, 150, 50, 150, "A")
	b := Apply([]Rect{rects[2], rects[0], rects[1]}, 50, 150, 50, 150, "A")
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("Apply depends on input order:\n%+v\n%+v", a, b)
	}
}

func TestAtAndBoundaryFallthrough(t *testing.T) {
	rects := Apply(nil, 0, 100, 40, 60, "OVL")
	// The state consists of the single overlay rect; everything else is
	// uncovered (the caller then falls back to the base calibration).
	for _, tc := range []struct {
		batch, temp int64
		covered     bool
	}{
		{0, 40, true},
		{99, 59, true},
		{50, 50, true},
		{0, 39, false},   // temperature strictly below the band
		{0, 60, false},   // half-open upper temperature: boundary falls through
		{100, 50, false}, // half-open upper batch boundary falls through
		{-1, 50, false},
	} {
		_, ok := At(rects, tc.batch, tc.temp)
		if ok != tc.covered {
			t.Fatalf("At(%d,%d) covered=%v, want %v", tc.batch, tc.temp, ok, tc.covered)
		}
	}
}

func TestValidate(t *testing.T) {
	if err := Validate([]Rect{r(0, 10, 0, 10, "A"), r(10, 20, 0, 10, "A")}); err != nil {
		t.Fatalf("touching rectangles must be valid: %v", err)
	}
	for _, bad := range [][]Rect{
		{r(10, 10, 0, 10, "A")}, // zero-width batch span
		{r(0, 10, 5, 5, "A")},   // zero-width temperature span
		{
			r(0, 10, 0, 10, "A"),
			r(5, 15, 0, 10, "B"), // overlapping batch spans
		},
		{
			r(0, 10, 0, 10, "A"),
			r(0, 10, 5, 15, "B"), // overlapping temperature spans
		},
	} {
		if err := Validate(bad); err == nil {
			t.Fatalf("invalid partition accepted: %+v", bad)
		}
	}
}

// assertNoMergeablePair mirrors the API's post-condition: no two equal-content
// rectangles may still share a full edge.
func assertNoMergeablePair(t *testing.T, rects []Rect) {
	t.Helper()
	for i := range rects {
		for j := i + 1; j < len(rects); j++ {
			if _, ok := tryMerge(rects[i], rects[j]); ok {
				t.Fatalf("unmerged full-edge neighbours: %+v %+v", rects[i], rects[j])
			}
		}
	}
}
