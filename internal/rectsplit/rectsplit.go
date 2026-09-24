// Package rectsplit implements the rectangle arithmetic behind temperature
// overlay calibrations: publishing a half-open batch×temperature rectangle
// [BL,BH)×[TL,TH) replaces the intersecting parts of every existing overlay
// rectangle (which are sliced into at most four residual rectangles), keeps
// the unaffected area untouched, and then coalesces adjacent rectangles that
// share a full edge and carry identical content.
//
// A list of non-overlapping (touching is allowed), non-degenerate, axis-aligned
// rectangles describes the full overlay state of one equipment revision.
// Batch/temperature points not covered by any rectangle fall back to the base
// batch calibration.
package rectsplit

import "sort"

// Rect is a half-open batch×temperature rectangle
// [BatchLo,BatchHi)×[TempLo,TempHi) carrying a calibration content payload
// (canonical JSON).
type Rect struct {
	BatchLo int64
	BatchHi int64
	TempLo  int64
	TempHi  int64
	Content string
}

// Apply returns the rectangle partition that results from publishing the
// half-open rectangle [bl,bh)×[tl,th) with the given content on top of
// rects.
//
// Every rectangle intersecting the published one leaves up to four residuals
// (left, right, low-temperature, high-temperature); the intersection itself
// is replaced by the new rectangle. Disjoint rectangles are kept untouched.
// Afterwards, rectangles with identical content whose union is again a
// rectangle (full-edge neighbours, transitively) are merged.
//
// rects must be pairwise non-overlapping and non-degenerate; the result keeps
// the same invariants and is sorted by (BatchLo, TempLo). bl must be strictly
// less than bh and tl strictly less than th.
func Apply(rects []Rect, bl, bh, tl, th int64, content string) []Rect {
	next := Rect{BatchLo: bl, BatchHi: bh, TempLo: tl, TempHi: th, Content: content}
	// Work on a canonically ordered copy so the output is a pure function of
	// the partition, independent of the input slice order.
	work := append([]Rect(nil), rects...)
	sortRects(work)
	out := make([]Rect, 0, len(work)+4)
	for _, r := range work {
		out = append(out, subtract(r, next)...)
	}
	out = append(out, next)
	sortRects(out)
	out = mergeRects(out)
	sortRects(out)
	return out
}

// subtract returns r minus p as at most four axis-aligned rectangles. When r
// and p are disjoint, r is returned unchanged.
func subtract(r, p Rect) []Rect {
	ixLo, ixHi := max(r.BatchLo, p.BatchLo), min(r.BatchHi, p.BatchHi)
	iyLo, iyHi := max(r.TempLo, p.TempLo), min(r.TempHi, p.TempHi)
	if ixLo >= ixHi || iyLo >= iyHi {
		return []Rect{r} // disjoint
	}
	var out []Rect
	if r.BatchLo < ixLo { // full-height left residual
		out = append(out, Rect{BatchLo: r.BatchLo, BatchHi: ixLo, TempLo: r.TempLo, TempHi: r.TempHi, Content: r.Content})
	}
	if ixHi < r.BatchHi { // full-height right residual
		out = append(out, Rect{BatchLo: ixHi, BatchHi: r.BatchHi, TempLo: r.TempLo, TempHi: r.TempHi, Content: r.Content})
	}
	if r.TempLo < iyLo { // low-temperature residual between the side cuts
		out = append(out, Rect{BatchLo: ixLo, BatchHi: ixHi, TempLo: r.TempLo, TempHi: iyLo, Content: r.Content})
	}
	if iyHi < r.TempHi { // high-temperature residual between the side cuts
		out = append(out, Rect{BatchLo: ixLo, BatchHi: ixHi, TempLo: iyHi, TempHi: r.TempHi, Content: r.Content})
	}
	return out
}

// mergeRects repeatedly coalesces a pair of equal-content rectangles whenever
// their union is itself a rectangle (full-edge neighbours), until no such
// pair remains. Pair selection is deterministic (lowest indices after the
// fixed order) so equal inputs always produce equal outputs.
func mergeRects(rects []Rect) []Rect {
	a := append([]Rect(nil), rects...)
	for {
		merged := false
		for i := 0; i < len(a) && !merged; i++ {
			for j := i + 1; j < len(a); j++ {
				if u, ok := tryMerge(a[i], a[j]); ok {
					a[i] = u
					a = append(a[:j], a[j+1:]...)
					merged = true
					break
				}
			}
		}
		if !merged {
			return a
		}
	}
}

// tryMerge reports whether two equal-content rectangles share a full edge
// (their union is a rectangle) and returns the union.
func tryMerge(x, y Rect) (Rect, bool) {
	if x.Content != y.Content {
		return Rect{}, false
	}
	// Side-by-side: identical temperature span, batch spans touch.
	if x.TempLo == y.TempLo && x.TempHi == y.TempHi {
		switch {
		case x.BatchHi == y.BatchLo:
			return Rect{BatchLo: x.BatchLo, BatchHi: y.BatchHi, TempLo: x.TempLo, TempHi: x.TempHi, Content: x.Content}, true
		case y.BatchHi == x.BatchLo:
			return Rect{BatchLo: y.BatchLo, BatchHi: x.BatchHi, TempLo: x.TempLo, TempHi: x.TempHi, Content: x.Content}, true
		}
	}
	// Stacked: identical batch span, temperature spans touch.
	if x.BatchLo == y.BatchLo && x.BatchHi == y.BatchHi {
		switch {
		case x.TempHi == y.TempLo:
			return Rect{BatchLo: x.BatchLo, BatchHi: x.BatchHi, TempLo: x.TempLo, TempHi: y.TempHi, Content: x.Content}, true
		case y.TempHi == x.TempLo:
			return Rect{BatchLo: x.BatchLo, BatchHi: x.BatchHi, TempLo: y.TempLo, TempHi: x.TempHi, Content: x.Content}, true
		}
	}
	return Rect{}, false
}

func sortRects(rects []Rect) {
	sort.Slice(rects, func(i, j int) bool {
		a, b := rects[i], rects[j]
		if a.BatchLo != b.BatchLo {
			return a.BatchLo < b.BatchLo
		}
		if a.TempLo != b.TempLo {
			return a.TempLo < b.TempLo
		}
		if a.BatchHi != b.BatchHi {
			return a.BatchHi < b.BatchHi
		}
		return a.TempHi < b.TempHi
	})
}

// At returns the rectangle covering the point (batch, temperature), if any.
// With a valid partition at most one rectangle matches.
func At(rects []Rect, batch, temperature int64) (Rect, bool) {
	for _, r := range rects {
		if r.BatchLo <= batch && batch < r.BatchHi &&
			r.TempLo <= temperature && temperature < r.TempHi {
			return r, true
		}
	}
	return Rect{}, false
}

// Validate checks the partition invariants: no degenerate rectangle and no
// two rectangles overlap (touching is allowed).
func Validate(rects []Rect) error {
	for i, r := range rects {
		if r.BatchLo >= r.BatchHi || r.TempLo >= r.TempHi {
			return &InvalidPartitionError{Reason: "degenerate rectangle", Rect: r}
		}
		for j := 0; j < i; j++ {
			o := rects[j]
			if r.BatchLo < o.BatchHi && o.BatchLo < r.BatchHi &&
				r.TempLo < o.TempHi && o.TempLo < r.TempHi {
				return &InvalidPartitionError{Reason: "overlapping rectangles", Rect: r}
			}
		}
	}
	return nil
}

// InvalidPartitionError is returned when a rectangle list violates the
// partition invariants.
type InvalidPartitionError struct {
	Reason string
	Rect   Rect
}

func (e *InvalidPartitionError) Error() string {
	return "rectsplit: " + e.Reason
}
