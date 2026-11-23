package recovery

import (
	"testing"
	"time"
)

func TestECNSentLateACK(t *testing.T) {
	r := tracker(t)
	at := time.Unix(100, 0)
	for pn := uint64(0); pn < 4; pn++ {
		if err := r.OnSent(Application, Packet{Number: pn, SentAt: at, Bytes: 1200, AckEliciting: true, InFlight: true, ECN: ECT0}); err != nil {
			t.Fatal(err)
		}
	}
	ev, err := r.OnACK(Application, ack(3, 3), at.Add(time.Millisecond), 0, 0, true)
	if err != nil || len(ev.Lost) != 1 || ev.NewlyECT != [2]uint64{1, 0} {
		t.Fatalf("first ACK: %+v %v", ev, err)
	}
	ev, err = r.OnACK(Application, ack(0, 0), at.Add(2*time.Millisecond), 0, 0, true)
	if err != nil || len(ev.Acked) != 0 || ev.NewlyECT != [2]uint64{1, 0} {
		t.Fatalf("late ACK: %+v %v", ev, err)
	}
	ev, err = r.OnACK(Application, ack(0, 0), at.Add(3*time.Millisecond), 0, 0, true)
	if err != nil || ev.NewlyECT != [2]uint64{} || r.ECNSent(Application) != [2]uint64{4, 0} {
		t.Fatalf("duplicate: %+v %v", ev, err)
	}
}

func TestECNSentCapacityAndEviction(t *testing.T) {
	r, _ := NewSent(1)
	if err := r.OnSent(Application, Packet{ECN: ECT1}); err != nil {
		t.Fatal(err)
	}
	if r.CanMarkECN(Application) {
		t.Fatal("unbounded ledger")
	}
	if err := r.OnSent(Application, Packet{Number: 1, ECN: ECT0}); err != ErrCapacity {
		t.Fatal(err)
	}
	if err := r.OnSent(Application, Packet{Number: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.OnACK(Application, ack(0, 2), time.Now(), 0, 0, true); err != ErrACK {
		t.Fatal(err)
	}
	ev, err := r.OnACK(Application, ack(0, 0), time.Now(), 0, 0, true)
	if err != nil || len(ev.Acked) != 0 || ev.NewlyECT != [2]uint64{0, 1} || !r.CanMarkECN(Application) {
		t.Fatalf("evicted marking: %+v %v", ev, err)
	}
	if r.ECNSent(Initial) != [2]uint64{} {
		t.Fatal("space isolation")
	}
}

func TestECNSentRejectAndReset(t *testing.T) {
	for _, probe := range []bool{false, true} {
		r := tracker(t)
		if err := r.OnSent(Application, Packet{ECN: ECT0, Bytes: 1400, InFlight: true, AckEliciting: true, MTUProbe: probe}); err != nil {
			t.Fatal(err)
		}
		var ok bool
		if probe {
			ok = r.RejectMTUProbe(0)
		} else {
			ok = r.RejectLargePacket(0)
		}
		if !ok || r.ECNSent(Application) != [2]uint64{} || len(r.spaces[Application].ecnPending) != 0 {
			t.Fatal("rejected marking retained")
		}
		if _, err := r.OnACK(Application, ack(0, 0), time.Now(), 0, 0, true); err != ErrACK {
			t.Fatal(err)
		}
	}
	for _, reset := range []func(*Sent){func(r *Sent) { r.Discard(Application) }, func(r *Sent) { r.ResetForRetry() }} {
		r := tracker(t)
		if err := r.OnSent(Application, Packet{ECN: ECT1}); err != nil {
			t.Fatal(err)
		}
		reset(r)
		if r.ECNSent(Application) != [2]uint64{} || len(r.spaces[Application].ecnPending) != 0 {
			t.Fatal("stale context")
		}
		if err := r.OnSent(Application, Packet{Number: 1, ECN: ECT0}); err != nil {
			t.Fatal(err)
		}
	}
	r := tracker(t)
	if err := r.OnSent(Application, Packet{ECN: 3}); err == nil {
		t.Fatal("originated CE")
	}
	if err := r.OnSent(Application, Packet{ECN: ECT0}); err != nil {
		t.Fatal("invalid marking consumed packet number", err)
	}
}

func TestECNSentMigrationCumulativeFeedback(t *testing.T) {
	r := tracker(t)
	var feedback ECNFeedback
	at := time.Unix(100, 0)
	for pn := uint64(0); pn < 2; pn++ {
		if err := r.OnSent(Application, Packet{Number: pn, SentAt: at, Bytes: 1200, InFlight: true, AckEliciting: true, ECN: ECT0}); err != nil {
			t.Fatal(err)
		}
	}
	ev, err := r.OnACK(Application, ack(0, 0), at.Add(time.Millisecond), 0, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := feedback.Check(0, &[3]uint64{1, 0, 0}, r.ECNSent(Application), ev.NewlyECT); !got.Validated || got.Failed {
		t.Fatal(got)
	}
	outstanding := r.ResetForPathChange()
	if len(outstanding[Application]) != 1 || r.BytesInFlight != 0 || r.RTT.Sampled {
		t.Fatal("old recovery context survived migration")
	}
	if r.ECNSent(Application) != [2]uint64{2, 0} {
		t.Fatal("migration forgot cumulative sends")
	}
	// An old-path late ACK must retain its marking without creating a new-path
	// RTT sample or acknowledging any new-path transmission.
	ev, err = r.OnACK(Application, ack(1, 1), at.Add(2*time.Millisecond), 0, 0, true)
	if err != nil || len(ev.Acked) != 0 || ev.NewlyECT != [2]uint64{1, 0} || r.RTT.Sampled {
		t.Fatalf("late old-path ACK: %+v %v", ev, err)
	}
	if got := feedback.Check(1, &[3]uint64{2, 0, 0}, r.ECNSent(Application), ev.NewlyECT); got.Failed {
		t.Fatal(got)
	}
	if err := r.OnSent(Application, Packet{Number: 2, SentAt: at.Add(3 * time.Millisecond), Bytes: 1200, InFlight: true, AckEliciting: true, ECN: ECT0}); err != nil {
		t.Fatal(err)
	}
	ev, err = r.OnACK(Application, ack(0, 2), at.Add(4*time.Millisecond), 0, 0, true)
	if err != nil || ev.NewlyECT != [2]uint64{1, 0} {
		t.Fatalf("new path ACK: %+v %v", ev, err)
	}
	if got := feedback.Check(2, &[3]uint64{2, 0, 1}, r.ECNSent(Application), ev.NewlyECT); !got.Validated || got.Failed || got.CE != 1 {
		t.Fatal("cumulative feedback rejected", got)
	}
}
