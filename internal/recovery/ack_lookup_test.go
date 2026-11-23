package recovery

import (
	"slices"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestACKLookupBounds(t *testing.T) {
	for _, tc := range []struct {
		frame   wire.AckFrame
		entries int
		want    bool
	}{
		{ack(0, wire.MaxVarint), 65536, false},
		{ack(wire.MaxVarint, wire.MaxVarint), 1, true},
		{ack(0, 0), 0, false},
		{wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 10, Largest: 11}, {Smallest: 1, Largest: 1}}}, 2, false},
		{wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 10, Largest: 11}, {Smallest: 1, Largest: 1}}}, 3, true},
	} {
		if got := useACKLookup(tc.frame, tc.entries); got != tc.want {
			t.Fatal(tc, got)
		}
	}
}

func TestCongestionHistoryACKLookupSpaceAndDenseRange(t *testing.T) {
	h := congestionHistory{capacity: 8}
	at := time.Unix(100, 0)
	h.sent(Initial, Packet{Number: wire.MaxVarint, SentAt: at}, true)
	h.sent(Application, Packet{Number: wire.MaxVarint, SentAt: at}, true)
	h.sent(Application, Packet{Number: 0, SentAt: at}, true)
	h.ack(Application, ack(wire.MaxVarint, wire.MaxVarint))
	if h.index[transmissionID{Initial, wire.MaxVarint}].state != 0 {
		t.Fatal("ACK crossed spaces")
	}
	if h.index[transmissionID{Application, wire.MaxVarint}].state != 2 {
		t.Fatal("sparse lookup missed packet")
	}
	h.ack(Application, ack(0, wire.MaxVarint))
	if h.order.Len() != 3 || h.index[transmissionID{Application, 0}].state != 2 {
		t.Fatal("dense scan changed history ordering")
	}
	h.ack(Initial, ack(wire.MaxVarint, wire.MaxVarint))
	if h.order.Len() != 0 || len(h.index) != 0 {
		t.Fatal("acknowledged prefix not trimmed")
	}
}

// Mixed new/duplicate ranges must preserve event ordering, loss accounting,
// and RTT sampling under both bounded lookup and retained-ledger scanning.
func TestACKLookupOrderedRetirement(t *testing.T) {
	r := tracker(t)
	at := time.Unix(100, 0)
	for pn := uint64(0); pn < 12; pn++ {
		sent(t, r, Application, pn, at, true, true)
	}
	if _, err := r.OnACK(Application, ack(1, 1), at.Add(100*time.Millisecond), 0, 0, true); err != nil {
		t.Fatal(err)
	}
	sparse := wire.AckFrame{Ranges: []wire.AckRange{
		{Smallest: 8, Largest: 9}, {Smallest: 4, Largest: 5}, {Smallest: 1, Largest: 2},
	}}
	if !useACKLookup(sparse, len(r.spaces[Application].packets)) {
		t.Fatal("fixture must exercise bounded lookup")
	}
	check := func(f wire.AckFrame, wantAck, wantLost []uint64, flight uint64) {
		t.Helper()
		ev, err := r.OnACK(Application, f, at.Add(100*time.Millisecond), 0, 0, true)
		numbers := func(packets []Packet) []uint64 {
			var out []uint64
			for _, p := range packets {
				out = append(out, p.Number)
			}
			return out
		}
		if err != nil || !slices.Equal(numbers(ev.Acked), wantAck) || !slices.Equal(numbers(ev.Lost), wantLost) || r.BytesInFlight != flight || r.RTT.Latest != 100*time.Millisecond {
			t.Fatalf("events=%+v err=%v flight=%d RTT=%+v", ev, err, r.BytesInFlight, r.RTT)
		}
	}
	check(sparse, []uint64{2, 4, 5, 8, 9}, []uint64{0, 3, 6}, 3600)
	before := r.RTT
	check(sparse, nil, nil, 3600)
	if r.RTT != before {
		t.Fatal("duplicate ACK changed RTT")
	}
	dense := ack(0, 11)
	if useACKLookup(dense, len(r.spaces[Application].packets)) {
		t.Fatal("fixture must exercise ledger scan")
	}
	check(dense, []uint64{7, 10, 11}, nil, 0)
	check(dense, nil, nil, 0)
}
