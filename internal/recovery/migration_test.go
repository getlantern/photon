package recovery

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestPathChangeRetainsNumbersAndIsolatesOldACK(t *testing.T) {
	r, _ := NewSent(8)
	now := time.Unix(101, 0)
	r.RTT.Update(20*time.Millisecond, 0, 0, true)
	for pn := uint64(0); pn < 3; pn++ {
		if err := r.OnSent(Application, Packet{Number: pn, SentAt: now, Bytes: 1200, AckEliciting: true, InFlight: true}); err != nil {
			t.Fatal(err)
		}
	}
	outstanding := r.ResetForPathChange()
	if len(outstanding[Application]) != 3 || r.BytesInFlight != 0 || r.RTT.Sampled {
		t.Fatal("migration did not reset network evidence")
	}
	for i, p := range outstanding[Application] {
		if p.Number != uint64(i) {
			t.Fatal("outstanding order")
		}
	}
	if _, err := r.OnACK(Application, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 3, Largest: 3}}}, now, 0, 0, true); err != ErrACK {
		t.Fatal("unsent ACK accepted", err)
	}
	if err := r.OnSent(Application, Packet{Number: 3, SentAt: now.Add(time.Second), Bytes: 1200, AckEliciting: true, InFlight: true}); err != nil {
		t.Fatal(err)
	}
	events, err := r.OnACK(Application, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 2}}}, now.Add(2*time.Second), 0, 0, true)
	if err != nil || len(events.Acked) != 0 || len(events.Lost) != 0 || r.RTT.Sampled || r.BytesInFlight != 1200 || r.PersistentCongestion(0) {
		t.Fatal("old ACK influenced new recovery", events, err)
	}
	events, err = r.OnACK(Application, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 3}}}, now.Add(2*time.Second), 0, 0, true)
	if err != nil || len(events.Acked) != 1 || events.Acked[0].Number != 3 || r.BytesInFlight != 0 || r.RTT.Smoothed != time.Second {
		t.Fatal("new context ACK not measured", events, err)
	}
}
