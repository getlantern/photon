package transport

import (
	"github.com/getlantern/photon/internal/recovery"

	"testing"
	"time"
)

// Compare the same idle one-path state through generic and base-only scheduling.
// This isolates scheduler CPU overhead, not network throughput or active output.
func BenchmarkBasePathIdleScheduler(b *testing.B) {
	for _, base := range []bool{false, true} {
		name := "generic"
		if base {
			name = "base"
		}
		b.Run(name, func(b *testing.B) {
			c := &Conn{started: true, confirmed: true}
			c.multipath = &pathCIDs{baseOnly: base, paths: map[uint64]*pathCIDState{0: {application: newPathApplication(recovery.NewRenoAlgorithm), peer: newPeerIDs([]byte("peer-cid"), 2)}}}
			now := time.Unix(101, 0)
			b.ReportAllocs()
			for b.Loop() {
				_, packet, err := c.nextMultipathPacket(now)
				if err != nil || packet != nil {
					b.Fatal("idle scheduler produced output", err)
				}
			}
		})
	}
}
