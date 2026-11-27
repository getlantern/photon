package recovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// One cumulative ACK of the oldest packet in flight, with inFlight packets
// outstanding: the cost a sender pays per ACK as its window grows. In the
// lossy case every 50th packet is ECN-marked and never acknowledged, so lost
// packets' history and ECN evidence linger, and each ACK has a gap at every
// loss, as an honest peer's would.
func BenchmarkCumulativeACK(b *testing.B) {
	for _, lossy := range []bool{false, true} {
		for _, inFlight := range []int{1000, 30000} {
			b.Run(fmt.Sprintf("lossy=%t/%d", lossy, inFlight), func(b *testing.B) {
				r, err := NewSent(65536)
				if err != nil {
					b.Fatal(err)
				}
				now := time.Unix(100, 0)
				next := uint64(0)
				send := func() {
					p := Packet{Number: next, SentAt: now, Bytes: 1200, AckEliciting: true, InFlight: true}
					if lossy && next%50 == 0 {
						p.ECN = ECT0
					}
					if err := r.OnSent(Application, p); err != nil {
						b.Fatal(err)
					}
					next++
				}
				for range inFlight {
					send()
				}
				oldest := uint64(1)
				var f wire.AckFrame
				b.ResetTimer()
				for range b.N {
					send()
					f.Ranges = f.Ranges[:0]
					if lossy {
						// Ranges between the losses, newest first, back 32 ranges.
						hi := oldest
						for len(f.Ranges) < 32 && hi > 0 {
							lo := hi - hi%50 + 1
							if hi%50 == 0 {
								hi--
								continue
							}
							f.Ranges = append(f.Ranges, wire.AckRange{Smallest: lo, Largest: hi})
							if lo < 51 {
								break
							}
							hi = lo - 2
						}
					} else {
						f.Ranges = append(f.Ranges, wire.AckRange{Smallest: 0, Largest: oldest})
					}
					if _, err := r.OnACK(Application, f, now, 0, 0, true); err != nil {
						b.Fatal(err)
					}
					oldest++
				}
			})
		}
	}
}
