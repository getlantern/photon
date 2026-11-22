package recovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// Keep the send/ACK cycle in the measurement so per-batch setup is identical.
func BenchmarkACKBatch(b *testing.B) {
	for _, batch := range []int{1, 16, 64, 256} {
		b.Run(fmt.Sprint(batch), func(b *testing.B) {
			r, _ := NewSent(1024)
			at := time.Unix(100, 0)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				first := uint64(i) * uint64(batch)
				for j := 0; j < batch; j++ {
					if err := r.OnSent(Application, Packet{Number: first + uint64(j), SentAt: at, Bytes: 1200, InFlight: true, AckEliciting: true}); err != nil {
						b.Fatal(err)
					}
				}
				f := wire.AckFrame{Ranges: []wire.AckRange{{Smallest: first, Largest: first + uint64(batch) - 1}}}
				events, err := r.OnACK(Application, f, at.Add(time.Millisecond), 0, 0, true)
				if err != nil || len(events.Acked) != batch || len(events.Lost) != 0 || r.BytesInFlight != 0 {
					b.Fatal(events, err)
				}
			}
		})
	}
}
