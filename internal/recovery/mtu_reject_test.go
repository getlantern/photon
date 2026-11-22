package recovery

import (
	"errors"
	"github.com/getlantern/photon/internal/wire"
	"testing"
	"time"
)

func TestRejectMTUProbe(t *testing.T) {
	r, _ := NewSent(2)
	now := time.Unix(100, 0)
	send := func(n uint64, probe bool) {
		t.Helper()
		if err := r.OnSent(Application, Packet{Number: n, SentAt: now.Add(time.Duration(n) * time.Second), Bytes: 1300, AckEliciting: true, InFlight: true, MTUProbe: probe}); err != nil {
			t.Fatal(err)
		}
	}
	send(0, false)
	if r.RejectMTUProbe(0) {
		t.Fatal("ordinary data canceled")
	}
	send(1, true)
	if !r.RejectMTUProbe(1) || r.RejectMTUProbe(1) || r.BytesInFlight != 1300 {
		t.Fatal("rejection accounting")
	}
	if r.spaces[Application].lastEliciting != now {
		t.Fatal("failed probe postponed PTO")
	}
	if len(r.history.index) != 1 {
		t.Fatal("failed probe retained congestion evidence")
	}
	for _, reset := range []func(){func() {}, func() { r.ResetForPathChange() }} {
		reset()
		_, err := r.OnACK(Application, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 1, Largest: 1}}}, now.Add(time.Second), 0, 0, true)
		if !errors.Is(err, ErrACK) {
			t.Fatal("ACK of locally unsent packet accepted")
		}
	}
	send(2, true)
	if !r.RejectMTUProbe(2) {
		t.Fatal("second rejection")
	}
	send(3, true)
	if r.RejectMTUProbe(3) || r.BytesInFlight != 1300 {
		t.Fatal("bounded ledger failure not atomic")
	}
}

func TestRejectLargePacket(t *testing.T) {
	for _, size := range []int{1200, 1201} {
		r, _ := NewSent(2)
		now := time.Unix(100, 0)
		for n := uint64(0); n < 2; n++ {
			if err := r.OnSent(Application, Packet{Number: n, SentAt: now.Add(time.Duration(n) * time.Second), Bytes: size, AckEliciting: true, InFlight: true}); err != nil {
				t.Fatal(err)
			}
		}
		if r.RejectLargePacket(0) {
			t.Fatal("rejected older packet")
		}
		if got := r.RejectLargePacket(1); got != (size > 1200) {
			t.Fatal("size eligibility", size, got)
		}
		if size <= 1200 {
			continue
		}
		if r.BytesInFlight != uint64(size) || r.spaces[Application].lastEliciting != now || len(r.history.index) != 1 || r.RejectLargePacket(1) {
			t.Fatal("non-loss rollback")
		}
		r.ResetForPathChange()
		if _, err := r.OnACK(Application, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 1, Largest: 1}}}, now, 0, 0, true); !errors.Is(err, ErrACK) {
			t.Fatal("rejected ACK after migration", err)
		}
	}
}
