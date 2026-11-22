package recovery

import "github.com/getlantern/photon/internal/wire"

// useACKLookup requires validated, disjoint ACK ranges. Enumerate packet
// numbers only when their total is bounded by the retained entry count.
// Otherwise scan the ledger, including for enormous peer-provided ranges.
func useACKLookup(frame wire.AckFrame, entries int) bool {
	if entries <= 0 {
		return false
	}
	var covered uint64
	for _, span := range frame.Ranges {
		covered += span.Largest - span.Smallest + 1
		if covered > uint64(entries) {
			return false
		}
	}
	return true
}
