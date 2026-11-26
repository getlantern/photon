package recovery

import (
	"testing"
	"time"
)

func TestACKChecksECNAfterPacketValidation(t *testing.T) {
	r := tracker(t)
	now := time.Unix(100, 0)
	if err := r.OnSent(Application, Packet{ECN: ECT0, SentAt: now, Bytes: 1200, InFlight: true, AckEliciting: true}); err != nil {
		t.Fatal(err)
	}
	invalid := ack(0, 1)
	invalid.ECN = &[3]uint64{}
	if ev, err := r.OnACK(Application, invalid, now, 0, 0, true); err != ErrACK || ev.ECN.Failed {
		t.Fatal("invalid ACK affected ECN", ev, err)
	}
	valid := ack(0, 0)
	valid.ECN = &[3]uint64{0, 0, 1}
	ev, err := r.OnACK(Application, valid, now.Add(time.Millisecond), 0, 0, true)
	if err != nil || !ev.ECN.Validated || ev.ECN.Failed || ev.ECN.CE != 1 {
		t.Fatal(ev, err)
	}
	ev, err = r.OnACK(Application, valid, now.Add(2*time.Millisecond), 0, 0, true)
	if err != nil || ev.ECN != (ECNFeedbackResult{}) {
		t.Fatal("repeated CE", ev, err)
	}
}

func TestACKBadECNStillAcknowledgesData(t *testing.T) {
	r := tracker(t)
	now := time.Unix(100, 0)
	if err := r.OnSent(Application, Packet{ECN: ECT0, SentAt: now, Bytes: 1200, InFlight: true, AckEliciting: true}); err != nil {
		t.Fatal(err)
	}
	ev, err := r.OnACK(Application, ack(0, 0), now.Add(time.Millisecond), 0, 0, true)
	if err != nil || len(ev.Acked) != 1 || r.BytesInFlight != 0 || !ev.ECN.Failed || ev.ECN.CE != 0 {
		t.Fatal(ev, err)
	}
	if err := r.OnSent(Handshake, Packet{ECN: ECT0, SentAt: now}); err != nil {
		t.Fatal(err)
	}
	frame := ack(0, 0)
	frame.ECN = &[3]uint64{1, 0, 0}
	ev, err = r.OnACK(Handshake, frame, now.Add(time.Millisecond), 0, 0, true)
	if err != nil || ev.ECN.Failed || !ev.ECN.Validated {
		t.Fatal("feedback leaked between spaces", ev, err)
	}
}

func TestACKMigrationPreservesFeedbackBaseline(t *testing.T) {
	r := tracker(t)
	now := time.Unix(100, 0)
	for pn := uint64(0); pn < 2; pn++ {
		if err := r.OnSent(Application, Packet{Number: pn, ECN: ECT0, SentAt: now}); err != nil {
			t.Fatal(err)
		}
		frame := ack(pn, pn)
		frame.ECN = &[3]uint64{0, 0, pn + 1}
		ev, err := r.OnACK(Application, frame, now, 0, 0, true)
		if err != nil || ev.ECN.CE != 1 || ev.ECN.Failed {
			t.Fatal("old CE repeated or valid feedback failed", ev, err)
		}
		r.ResetForPathChange()
	}
	r.Discard(Application)
	if r.spaces[Application].ecnFeedback != (ECNFeedback{}) {
		t.Fatal("discard retained validation")
	}
}

func TestACKOldPathECNIsNotNewPathCongestion(t *testing.T) {
	r := tracker(t)
	r.EnableECN()
	at := time.Unix(100, 0)
	send := func(pn uint64, sent time.Time) {
		t.Helper()
		if err := r.OnSent(Application, Packet{Number: pn, ECN: ECT0, SentAt: sent, Bytes: 1200, InFlight: true, AckEliciting: true}); err != nil {
			t.Fatal(err)
		}
	}
	send(0, at)
	r.ResetForPathChange()
	frame := ack(0, 0)
	frame.ECN = &[3]uint64{0, 0, 1}
	ev, err := r.OnACK(Application, frame, at.Add(time.Millisecond), 0, 0, true)
	if err != nil || ev.ECN.CE != 1 || ev.ECNCongestion {
		t.Fatal("old path CE affected new congestion", ev, err)
	}
	// The new path validates on its own ECT reports, beyond the one the old
	// packet could account for, then its own mark starts a recovery period
	// from its packet's send time.
	send(1, at.Add(2*time.Millisecond))
	send(2, at.Add(2*time.Millisecond))
	frame = ack(1, 2)
	frame.ECN = &[3]uint64{2, 0, 1}
	if _, err := r.OnACK(Application, frame, at.Add(3*time.Millisecond), 0, 0, true); err != nil || r.ECNState() != ECNCapable {
		t.Fatal("new path not validated", err)
	}
	send(3, at.Add(4*time.Millisecond))
	frame = ack(3, 3)
	frame.ECN = &[3]uint64{2, 0, 2}
	ev, err = r.OnACK(Application, frame, at.Add(5*time.Millisecond), 0, 0, true)
	if err != nil || !ev.ECNCongestion || ev.ECNSentAt != at.Add(4*time.Millisecond) {
		t.Fatal("new path CE suppressed", ev, err)
	}
}

func TestACKCEAfterTimestampEviction(t *testing.T) {
	r, _ := NewSent(1)
	r.EnableECN()
	// The path has passed validation; this tests the send time surviving
	// metadata eviction.
	r.ecnPath.state = ECNCapable
	at := time.Unix(100, 0)
	if err := r.OnSent(Application, Packet{Number: 0, ECN: ECT0, SentAt: at}); err != nil {
		t.Fatal(err)
	}
	if err := r.OnSent(Application, Packet{Number: 1, SentAt: at.Add(time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	frame := ack(0, 0)
	frame.ECN = &[3]uint64{0, 0, 1}
	ev, err := r.OnACK(Application, frame, at.Add(2*time.Millisecond), 0, 0, true)
	if err != nil || len(ev.Acked) != 0 || !ev.ECNCongestion || ev.ECNSentAt != at {
		t.Fatal("marked timestamp was evicted", ev, err)
	}
}

// After a path change, an old packet's CE reported alongside a new packet
// doesn't start a recovery period on the new path, though the ACK's largest
// packet is new.
func TestACKOldPathCEWithNewLargestIsNotCongestion(t *testing.T) {
	r := tracker(t)
	r.EnableECN()
	at := time.Unix(100, 0)
	send := func(pn uint64) {
		t.Helper()
		if err := r.OnSent(Application, Packet{Number: pn, ECN: ECT0, SentAt: at, Bytes: 1200, InFlight: true, AckEliciting: true}); err != nil {
			t.Fatal(err)
		}
	}
	send(0)
	r.ResetForPathChange()
	send(1)
	send(2)
	frame := ack(1, 2)
	frame.ECN = &[3]uint64{2, 0, 0}
	if _, err := r.OnACK(Application, frame, at.Add(time.Millisecond), 0, 0, true); err != nil || r.ECNState() != ECNCapable {
		t.Fatal("new path not validated", err)
	}
	send(3)
	frame = ack(0, 3)
	frame.ECN = &[3]uint64{3, 0, 1}
	ev, err := r.OnACK(Application, frame, at.Add(2*time.Millisecond), 0, 0, true)
	if err != nil || ev.ECNCongestion {
		t.Fatal("the old packet's CE started recovery on the new path", ev, err)
	}
}
