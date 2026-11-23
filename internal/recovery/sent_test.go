package recovery

import (
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func ack(lo, hi uint64) wire.AckFrame {
	return wire.AckFrame{Ranges: []wire.AckRange{{Smallest: lo, Largest: hi}}}
}
func tracker(t *testing.T) *Sent {
	t.Helper()
	r, err := NewSent(16)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func sent(t *testing.T, r *Sent, s Space, pn uint64, at time.Time, eliciting, flight bool) {
	t.Helper()
	if err := r.OnSent(s, Packet{Number: pn, SentAt: at, Bytes: 1200, AckEliciting: eliciting, InFlight: flight}); err != nil {
		t.Fatal(err)
	}
}

func TestACKRTTSampling(t *testing.T) {
	start := time.Unix(1000, 0)
	r := tracker(t)
	sent(t, r, Application, 0, start, true, true)
	sent(t, r, Application, 1, start.Add(10*time.Millisecond), false, false)
	ev, err := r.OnACK(Application, ack(0, 1), start.Add(100*time.Millisecond), 0, 0, true)
	if err != nil || len(ev.Acked) != 2 || r.RTT.Latest != 90*time.Millisecond || r.BytesInFlight != 0 {
		t.Fatalf("ACK-only largest: %+v %+v %v", ev, r, err)
	}
	before := r.RTT
	r.OnACK(Application, ack(0, 1), start.Add(time.Second), 0, 0, true)
	if r.RTT != before {
		t.Fatal("duplicate ACK sampled RTT")
	}
	sent(t, r, Application, 2, start, false, false)
	r.OnACK(Application, ack(2, 2), start.Add(2*time.Second), 0, 0, true)
	if r.RTT != before {
		t.Fatal("ACK-only transmission sampled RTT")
	}
	// Encryption levels use the same path estimator, ignoring ACK delay outside application space.
	sent(t, r, Handshake, 0, start, true, true)
	r.OnACK(Handshake, ack(0, 0), start.Add(100*time.Millisecond), 50*time.Millisecond, time.Second, true)
	if r.RTT.Smoothed != 91250*time.Microsecond {
		t.Fatalf("handshake ACK delay applied: %+v", r.RTT)
	}
}

func TestPacketAndTimeThreshold(t *testing.T) {
	start := time.Unix(1000, 0)
	r := tracker(t)
	for pn := uint64(0); pn < 6; pn++ {
		sent(t, r, Application, pn, start, true, true)
	}
	ev, err := r.OnACK(Application, ack(4, 4), start.Add(100*time.Millisecond), 0, 0, true)
	if err != nil || len(ev.Lost) != 2 || ev.Lost[0].Number != 0 || ev.Lost[1].Number != 1 || r.BytesInFlight != 3600 {
		t.Fatalf("packet threshold: %+v %v flight=%d", ev, err, r.BytesInFlight)
	}
	at, space, ok := r.LossTime()
	if !ok || space != Application || !at.Equal(start.Add(112500*time.Microsecond)) {
		t.Fatalf("deadline %v %v %v", at, space, ok)
	}
	if p := r.DetectLoss(Application, at.Add(-time.Nanosecond)); len(p) != 0 {
		t.Fatal("early loss", p)
	}
	lost := r.DetectLoss(Application, at)
	if len(lost) != 2 || lost[0].Number != 2 || lost[1].Number != 3 || r.BytesInFlight != 1200 {
		t.Fatal("time threshold", lost, r.BytesInFlight)
	}
	if _, _, ok := r.LossTime(); ok {
		t.Fatal("tail has loss deadline")
	}
	if p := r.DetectLoss(Application, start.Add(time.Hour)); len(p) != 0 {
		t.Fatal("tail lost without later ACK", p)
	}
	ev, err = r.OnACK(Application, ack(0, 4), start.Add(time.Hour), 0, 0, true)
	if err != nil || len(ev.Acked) != 0 || r.BytesInFlight != 1200 {
		t.Fatal("late ACK debited twice", ev, err)
	}
	r.Discard(Application)
	if r.BytesInFlight != 0 {
		t.Fatal("discard retained flight")
	}
}

func TestACKValidationAndCapacity(t *testing.T) {
	r, _ := NewSent(1)
	now := time.Unix(1000, 0)
	sent(t, r, Initial, 0, now, false, true) // PADDING counts toward flight, even without ack-eliciting data.
	if r.BytesInFlight != 1200 || r.CanTrack(Initial) {
		t.Fatal("flight/capacity")
	}
	for _, f := range []wire.AckFrame{ack(0, 1), ack(2, 1), {}, {Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}, {Smallest: 0, Largest: 0}}}} {
		if _, err := r.OnACK(Initial, f, now, 0, 0, false); !errors.Is(err, ErrACK) {
			t.Fatal("accepted invalid ACK", f, err)
		}
		if r.BytesInFlight != 1200 {
			t.Fatal("invalid ACK mutated state")
		}
	}
	if _, err := r.OnACK(Handshake, ack(0, 0), now, 0, 0, false); !errors.Is(err, ErrACK) {
		t.Fatal("ACK crossed spaces")
	}
	if err := r.OnSent(Initial, Packet{Number: 1, InFlight: true}); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	r.OnACK(Initial, ack(0, 0), now, 0, 0, false)
	if r.RTT.Sampled || r.BytesInFlight != 0 || !r.CanTrack(Initial) {
		t.Fatal("padding ACK semantics")
	}
	if err := r.OnSent(Initial, Packet{Number: 2}); err == nil {
		t.Fatal("accepted skipped PN")
	}
	sent(t, r, Initial, 1, now, true, true)
	if p := r.DetectLoss(Initial, now.Add(time.Hour)); len(p) != 0 {
		t.Fatal("tail declared lost", p)
	}
}

func TestACKRangeSearch(t *testing.T) {
	f := wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 100, Largest: wire.MaxVarint}, {Smallest: 10, Largest: 20}, {Smallest: 0, Largest: 0}}}
	for _, pn := range []uint64{0, 10, 15, 20, 100, wire.MaxVarint} {
		if !f.Acknowledges(pn) {
			t.Fatal("missing", pn)
		}
	}
	for _, pn := range []uint64{1, 9, 21, 99, wire.MaxVarint + 1} {
		if f.Acknowledges(pn) {
			t.Fatal("false match", pn)
		}
	}
	if (wire.AckFrame{}).Acknowledges(0) {
		t.Fatal("empty matches")
	}
}

func TestACKOnlyHistoryCannotExhaustRecovery(t *testing.T) {
	r, _ := NewSent(4)
	now := time.Unix(1000, 0)
	sent(t, r, Application, 0, now, true, true)
	for pn := uint64(1); pn < 10000; pn++ {
		if err := r.OnSent(Application, Packet{Number: pn, SentAt: now, Bytes: 40}); err != nil {
			t.Fatal(pn, err)
		}
		if len(r.spaces[Application].packets) > 4 || r.spaces[Application].ackOnly.Len() > 3 {
			t.Fatal("history exceeded capacity")
		}
	}
	if r.BytesInFlight != 1200 {
		t.Fatal("eviction changed flight")
	}
	if _, ok := r.spaces[Application].packets[0]; !ok {
		t.Fatal("in-flight packet evicted")
	}
	ev, err := r.OnACK(Application, ack(0, 9999), now.Add(time.Second), 0, 0, true)
	if err != nil || len(ev.Acked) != 4 || r.BytesInFlight != 0 {
		t.Fatal(ev, err)
	}
	if r.spaces[Application].ackOnly.Len() != 0 || len(r.spaces[Application].ackNodes) != 0 {
		t.Fatal("ACK index not cleared")
	}
	if _, err := r.OnACK(Application, ack(10000, 10000), now, 0, 0, true); !errors.Is(err, ErrACK) {
		t.Fatal("forgotten history permitted unsent ACK", err)
	}
}
func TestFullFlightLedgerStillAllowsPureACK(t *testing.T) {
	r, _ := NewSent(1)
	now := time.Unix(1000, 0)
	sent(t, r, Initial, 0, now, false, true) // Padding is flight even though not ACK-eliciting.
	if r.CanRecord(Initial, true) || !r.CanRecord(Initial, false) {
		t.Fatal("recording gate")
	}
	if err := r.OnSent(Initial, Packet{Number: 1, SentAt: now, Bytes: 40}); err != nil {
		t.Fatal(err)
	}
	if len(r.spaces[Initial].packets) != 1 || r.BytesInFlight != 1200 {
		t.Fatal("padded flight displaced")
	}
	if _, err := r.OnACK(Initial, ack(1, 1), now, 0, 0, false); err != nil {
		t.Fatal("ACK of unretained sent packet rejected", err)
	}
	r.Discard(Initial)
	if r.BytesInFlight != 0 || r.spaces[Initial].ackOnly.Len() != 0 {
		t.Fatal("discard accounting")
	}
}
