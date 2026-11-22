package transport

import (
	"github.com/getlantern/photon/internal/wire"
	"time"
)

// Candidate challenge processing must not acknowledge withheld STREAM data.
// Keep its replay history separate, but retain it across binding commits so
// replaying the old packet can admit data without answering its challenge twice.
func (a *pathApplication) recordCandidateChallenges(pn uint64, frames []wire.Frame, now time.Time) (bool, error) {
	hasChallenge := false
	for _, f := range frames {
		if _, ok := f.(wire.PathChallengeFrame); ok {
			hasChallenge = true
			break
		}
	}
	if !hasChallenge {
		return false, nil
	}
	if receivedPacketNumber(a.space.received, pn) {
		return false, nil
	}
	if a.candidateChallenges == nil {
		a.candidateChallenges, _ = NewReceived(64)
	}
	receipt, err := a.candidateChallenges.Record(pn, now)
	return receipt == NewPacket, err
}

func receivedPacketNumber(r *Received, pn uint64) bool {
	if r == nil {
		return false
	}
	if pn < r.floor {
		return true
	}
	for _, interval := range r.ranges {
		if pn >= interval.Smallest && pn <= interval.Largest {
			return true
		}
	}
	return false
}
