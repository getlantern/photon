package stream

import "github.com/getlantern/photon/internal/wire"

type closedRange struct{ first, last uint64 }

// Closed stores merged stream-ordinal ranges per stream type. Memory depends
// on holes (active streams), not the total number of completed streams.
// Concurrency is controlled by negotiated stream limits and local application
// opening. No fixed range cap may reject otherwise legal stream completion.
type Closed struct{ ranges [4][]closedRange }

func (c *Closed) Contains(id uint64) bool {
	if id > wire.MaxVarint {
		return false
	}
	ranges := c.ranges[id&3]
	ordinal := id / 4
	lo, hi := 0, len(ranges)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if ranges[mid].last < ordinal {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo < len(ranges) && ranges[lo].first <= ordinal
}
func (c *Closed) Add(id uint64) error {
	if id > wire.MaxVarint {
		return ErrStreamState
	}
	if c.Contains(id) {
		return nil
	}
	typ := id & 3
	value := closedRange{id / 4, id / 4}
	old := c.ranges[typ]
	out := make([]closedRange, 0, len(old)+1)
	inserted := false
	for _, r := range old {
		if r.last+1 < value.first {
			out = append(out, r)
			continue
		}
		if value.last+1 < r.first {
			if !inserted {
				out = append(out, value)
				inserted = true
			}
			out = append(out, r)
			continue
		}
		value.first = min(value.first, r.first)
		value.last = max(value.last, r.last)
	}
	if !inserted {
		out = append(out, value)
	}
	c.ranges[typ] = out
	return nil
}
