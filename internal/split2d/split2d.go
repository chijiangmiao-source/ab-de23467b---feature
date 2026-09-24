// Package split2d implements the batch-by-temperature rectangle arithmetic
// behind temperature-zone overlays.
//
// A zone is a half-open rectangle [BatchLower, BatchUpper) x
// [TempLower, TempUpper) carrying a calibration content payload (canonical
// JSON). A sorted, non-overlapping list of rectangles describes the full
// temperature-overlay state of one equipment revision; batch/temperature
// points not covered by any rectangle fall back to the base batch
// calibration.
package split2d

import "sort"

// Rect is a half-open batch x temperature rectangle carrying a content
// payload (canonical JSON).
type Rect struct {
	BatchLower int64
	BatchUpper int64
	TempLower  int64
	TempUpper  int64
	Content    string
}

// Apply returns the rectangle partition that results from publishing the
// half-open rectangle [bl, bu) x [tl, tu) with the given content on top of
// rects.
//
// Every rectangle overlapped by the published rectangle is cut along the
// published batch/temperature boundaries into non-overlapping residual
// rectangles; only the overlap is replaced. Rectangles disjoint from the
// published rectangle are kept untouched. Afterwards, rectangles that
// carry identical content and share a complete edge (identical extent along
// the other axis) are merged, possibly repeatedly.
//
// rects must be sorted (by BatchLower, then TempLower), pairwise
// non-overlapping and non-degenerate; the result keeps the same
// invariants. bl < bu and tl < tu are required.
func Apply(rects []Rect, bl, bu, tl, tu int64, content string) []Rect {
	// Collect every batch boundary (the published ones plus every existing
	// rectangle's edges): the resulting strips are the finest batch
	// partition the answer can need.
	xs := make([]int64, 0, 2+2*len(rects))
	xs = append(xs, bl, bu)
	for _, r := range rects {
		xs = append(xs, r.BatchLower, r.BatchUpper)
	}
	xs = dedupe(xs)

	out := make([]Rect, 0, len(rects)+4)
	for i := 0; i+1 < len(xs); i++ {
		lo, hi := xs[i], xs[i+1]
		if lo >= hi {
			continue
		}
		switch {
		case hi <= bl || lo >= bu:
			// Batch strip outside the published rectangle: keep the
			// (at most one, due to the boundary grid) covering rectangle.
			for _, r := range rects {
				if r.BatchLower < hi && r.BatchUpper > lo {
					out = append(out, Rect{
						BatchLower: lo, BatchUpper: hi,
						TempLower: r.TempLower, TempUpper: r.TempUpper,
						Content: r.Content,
					})
				}
			}
		default:
			// Batch strip inside [bl, bu): cut its temperature line on the
			// published temperature edges and every intersecting
			// rectangle's temperature edges.
			ys := []int64{tl, tu}
			for _, r := range rects {
				if r.BatchLower < hi && r.BatchUpper > lo {
					ys = append(ys, r.TempLower, r.TempUpper)
				}
			}
			ys = dedupe(ys)
			for j := 0; j+1 < len(ys); j++ {
				ylo, yhi := ys[j], ys[j+1]
				if ylo >= yhi {
					continue
				}
				if ylo >= tl && yhi <= tu {
					// Inside the published rectangle: replaced.
					out = append(out, Rect{
						BatchLower: lo, BatchUpper: hi,
						TempLower: ylo, TempUpper: yhi,
						Content: content,
					})
					continue
				}
				// Outside the published temperature range: keep the
				// unique rectangle covering this point.
				for _, r := range rects {
					if r.BatchLower < hi && r.BatchUpper > lo &&
						r.TempLower <= ylo && yhi <= r.TempUpper {
						out = append(out, Rect{
							BatchLower: lo, BatchUpper: hi,
							TempLower: ylo, TempUpper: yhi,
							Content: r.Content,
						})
						break
					}
				}
			}
		}
	}
	return Merge(out)
}

// Merge coalesces rectangles that carry identical content and share a
// complete edge: either [a,b)x[c,d) next to [b,e)x[c,d) (batch
// neighbours with the exact same temperature extent) or [a,b)x[c,d)
// below [a,b)x[d,f) (temperature neighbours with the exact same batch
// extent). Chains of merges collapse in repeated passes, so an L-shaped
// equal-content region whose pieces become mergeable only after an
// earlier merge ends up as a single rectangle.
//
// rects must be pairwise non-overlapping. The result is sorted by
// BatchLower and then TempLower.
func Merge(rects []Rect) []Rect {
	cur := sortedCopy(rects)
	for {
		next := mergeOnce(cur)
		if len(next) == len(cur) {
			return cur
		}
		cur = next
	}
}

func mergeOnce(rects []Rect) []Rect {
	removed := make([]bool, len(rects))
	out := make([]Rect, 0, len(rects))
	for i := range rects {
		if removed[i] {
			continue
		}
		ri := rects[i]
		// Only partners with a higher index are considered; repeated passes
		// handle chains that need more than one merge.
		for j := i + 1; j < len(rects); j++ {
			if removed[j] {
				continue
			}
			rj := rects[j]
			if rj.Content != ri.Content {
				continue
			}
			switch {
			case ri.BatchUpper == rj.BatchLower &&
				ri.TempLower == rj.TempLower && ri.TempUpper == rj.TempUpper:
				ri.BatchUpper = rj.BatchUpper
				removed[j] = true
			case ri.BatchLower == rj.BatchLower && ri.BatchUpper == rj.BatchUpper &&
				ri.TempUpper == rj.TempLower:
				ri.TempUpper = rj.TempUpper
				removed[j] = true
			}
		}
		out = append(out, ri)
	}
	return sortedCopy(out)
}

// Lookup returns the rectangle covering the point (batch, temperature), or
// false when the point is not covered by any rectangle.
func Lookup(rects []Rect, batch, temperature int64) (Rect, bool) {
	for _, r := range rects {
		if r.BatchLower <= batch && batch < r.BatchUpper &&
			r.TempLower <= temperature && temperature < r.TempUpper {
			return r, true
		}
	}
	return Rect{}, false
}

func sortedCopy(rects []Rect) []Rect {
	out := make([]Rect, len(rects))
	copy(out, rects)
	sort.Slice(out, func(i, j int) bool {
		if out[i].BatchLower != out[j].BatchLower {
			return out[i].BatchLower < out[j].BatchLower
		}
		return out[i].TempLower < out[j].TempLower
	})
	return out
}

func dedupe(vals []int64) []int64 {
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	out := vals[:0]
	for _, v := range vals {
		if len(out) == 0 || out[len(out)-1] != v {
			out = append(out, v)
		}
	}
	return out
}
