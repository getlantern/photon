package transport

import (
	"slices"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

// sentNumbers lists, ascending, the packet numbers from first on whose
// frames s.sent still holds: the numbers packets took, skips excluded.
func sentNumbers(s *numberSpace, first uint64) []uint64 {
	var pns []uint64
	for pn := range s.sent {
		if pn >= first {
			pns = append(pns, pn)
		}
	}
	slices.Sort(pns)
	return pns
}

// ackRange acknowledges lo through hi, less the numbers sent skipped.
func ackRange(sent *recovery.Sent, lo, hi uint64) wire.AckFrame {
	var pns []uint64
	skipped := sent.Skipped(recovery.Application)
	for pn := lo; pn <= hi; pn++ {
		if !slices.Contains(skipped, pn) {
			pns = append(pns, pn)
		}
	}
	return ackFor(pns...)
}

// ackFor acknowledges exactly pns, in ranges that stop at every gap, so it
// never claims a skipped packet number.
func ackFor(pns ...uint64) wire.AckFrame {
	sorted := slices.Clone(pns)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)
	var f wire.AckFrame
	for i := len(sorted) - 1; i >= 0; i-- {
		if n := len(f.Ranges); n > 0 && f.Ranges[n-1].Smallest == sorted[i]+1 {
			f.Ranges[n-1].Smallest = sorted[i]
			continue
		}
		f.Ranges = append(f.Ranges, wire.AckRange{Smallest: sorted[i], Largest: sorted[i]})
	}
	return f
}
