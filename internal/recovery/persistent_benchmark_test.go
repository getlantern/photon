package recovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func BenchmarkCongestionHistoryACK(b *testing.B) {
	h := congestionHistory{capacity: 64}
	now := time.Unix(100, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pn := uint64(i)
		h.sent(Application, Packet{Number: pn, SentAt: now, AckEliciting: true}, true)
		h.ack(Application, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: pn, Largest: pn}}}, []Packet{{Number: pn}})
	}
}

// Keep a history near capacity with unresolved packets, loss callbacks and late ACKs.
// One operation adds a packet and retires old evidence under capacity pressure.
func BenchmarkCongestionHistoryOutstanding(b *testing.B) {
	for _, capacity := range []int{32, 1024} {
		b.Run(fmt.Sprint(capacity), func(b *testing.B) {
			h := congestionHistory{capacity: capacity}
			start := time.Unix(100, 0)
			send := func(pn uint64) {
				h.sent(Application, Packet{Number: pn, SentAt: start.Add(time.Duration(pn) * time.Microsecond), AckEliciting: true}, true)
			}
			for pn := 0; pn < capacity; pn++ {
				send(uint64(pn))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pn := uint64(i + capacity)
				send(pn)
				h.lost(Application, []Packet{{Number: pn - uint64(capacity/4)}})
				n := pn - uint64(capacity/2)
				if n%2 == 0 {
					h.ack(Application, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: n, Largest: n}}}, []Packet{{Number: n}})
				}
				if i%64 == 0 {
					h.detect(time.Second)
				}
			}
			b.StopTimer()
			if h.order.Len() < capacity-1 || h.order.Len() > capacity {
				b.Fatalf("history drained: %d", h.order.Len())
			}
		})
	}
}
