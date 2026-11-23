package recovery

import (
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestRejectMigrationProbe(t *testing.T) {
	r, _ := NewSent(2)
	now := time.Unix(100, 0)
	for number := uint64(0); number < 2; number++ {
		if err := r.OnSent(Application, Packet{Number: number, SentAt: now.Add(time.Duration(number) * time.Second), Bytes: 1200, AckEliciting: true, InFlight: true}); err != nil {
			t.Fatal(err)
		}
	}
	if r.RejectMigrationProbe(0) || !r.RejectMigrationProbe(1) || r.RejectMigrationProbe(1) {
		t.Fatal("latest packet rejection not single use")
	}
	if r.BytesInFlight != 1200 || r.spaces[Application].lastEliciting != now || len(r.history.index) != 1 {
		t.Fatal("probe retained flight/PTO/congestion state")
	}
	if _, err := r.OnACK(Application, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 1, Largest: 1}}}, now.Add(2*time.Second), 0, 0, true); !errors.Is(err, ErrACK) {
		t.Fatal("unsent packet accepted", err)
	}
	for number := uint64(2); number < 4; number++ {
		if err := r.OnSent(Application, Packet{Number: number, SentAt: now.Add(time.Duration(number) * time.Second), Bytes: 1200, AckEliciting: true, InFlight: true}); err != nil {
			t.Fatal(err)
		}
		if rejected := r.RejectMigrationProbe(number); rejected != (number == 2) {
			t.Fatal("rejection ledger not bounded")
		}
	}
}
