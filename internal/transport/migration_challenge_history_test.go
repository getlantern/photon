package transport

import (
	"github.com/getlantern/photon/internal/wire"
	"testing"
	"time"
)

func TestCandidateChallengeHistoryBound(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	a := c.multipath.paths[1].application
	frames := []wire.Frame{wire.PathChallengeFrame{}}
	now := time.Unix(101, 0)
	for pn := uint64(0); pn < 200; pn += 2 {
		if fresh, err := a.recordCandidateChallenges(pn, frames, now); err != nil || !fresh {
			t.Fatal(pn, err)
		}
	}
	if len(a.candidateChallenges.ranges) != 64 || a.space.received.Expected() != 0 || a.space.ackPending {
		t.Fatal("history bound/admission")
	}
	for _, pn := range []uint64{0, 1, 198} {
		if fresh, err := a.recordCandidateChallenges(pn, frames, now); err != nil || fresh {
			t.Fatal("replayed or forgotten challenge", pn, err)
		}
	}
	if fresh, err := a.recordCandidateChallenges(197, frames, now); err != nil || !fresh {
		t.Fatal("retained gap rejected", err)
	}
}
