package recovery

import (
	"reflect"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// A cumulative ACK can cover mostly retired history. Returned storage must
// depend on retained transmissions, remain independently owned, and preserve
// exactly-once accounting across sparse and dense ACK traversal.
func TestACKBatchRetainedOwnership(t *testing.T) {
	for _, sparse := range []bool{false, true} {
		r, _ := NewSent(128)
		at := time.Unix(100, 0)
		for pn := uint64(0); pn < 128; pn++ {
			if err := r.OnSent(Application, Packet{Number: pn, SentAt: at, Bytes: 1200, InFlight: true, AckEliciting: true}); err != nil {
				t.Fatal(err)
			}
		}
		var ranges []wire.AckRange
		if sparse {
			for pn := uint64(126); ; pn -= 2 {
				ranges = append(ranges, wire.AckRange{Smallest: pn, Largest: pn})
				if pn == 0 {
					break
				}
			}
		} else {
			ranges = []wire.AckRange{{Smallest: 0, Largest: 127}}
		}
		events, err := r.OnACK(Application, wire.AckFrame{Ranges: ranges}, at.Add(time.Millisecond), 0, 0, true)
		if err != nil {
			t.Fatal(err)
		}
		want := 128
		if sparse {
			want = 64
		}
		if len(events.Acked) != want {
			t.Fatal("wrong newly acknowledged count", len(events.Acked), want)
		}
		for i, packet := range events.Acked {
			pn := uint64(i)
			if sparse {
				pn *= 2
			}
			if packet.Number != pn {
				t.Fatal("wrong packet/order", packet.Number, pn)
			}
		}
		saved := append([]Packet(nil), events.Acked...)
		late, err := r.OnACK(Application, ack(0, 127), at.Add(2*time.Millisecond), 0, 0, true)
		if err != nil || r.BytesInFlight != 0 {
			t.Fatal("late ACK", late, err, r.BytesInFlight)
		}
		if !reflect.DeepEqual(events.Acked, saved) {
			t.Fatal("later ACK overwrote returned packets")
		}
		duplicate, err := r.OnACK(Application, ack(0, 127), at.Add(3*time.Millisecond), 0, 0, true)
		if err != nil || duplicate.Acked != nil || len(duplicate.Lost) != 0 {
			t.Fatal("duplicate produced events", duplicate, err)
		}
	}
}
