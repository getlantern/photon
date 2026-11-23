package recovery

import (
	"testing"
	"time"
)

// Retain many lost markings while newly sent packets receive single-packet
// ACKs. This models a long-lived connection with sparse late-ACK evidence.
func BenchmarkECNACKRetainedHistory(b *testing.B) {
	r, _ := NewSent(4096)
	at := time.Unix(100, 0)
	for pn := uint64(0); pn < 4096; pn++ {
		if err := r.OnSent(Application, Packet{Number: pn, ECN: ECT0, SentAt: at}); err != nil {
			b.Fatal(err)
		}
	}
	first := ack(4095, 4095)
	first.ECN = &[3]uint64{1, 0, 0}
	if _, err := r.OnACK(Application, first, at.Add(time.Millisecond), 0, 0, true); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pn := uint64(4096 + i)
		if err := r.OnSent(Application, Packet{Number: pn, ECN: ECT0, SentAt: at}); err != nil {
			b.Fatal(err)
		}
		frame := ack(pn, pn)
		frame.ECN = &[3]uint64{uint64(i + 2), 0, 0}
		ev, err := r.OnACK(Application, frame, at.Add(time.Millisecond), 0, 0, true)
		if err != nil || ev.NewlyECT != [2]uint64{1, 0} || ev.ECN.Failed {
			b.Fatal(ev, err)
		}
	}
}
