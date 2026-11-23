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
	at := time.Unix(100, 0)
	if err := r.OnSent(Application, Packet{Number: 0, ECN: ECT0, SentAt: at, Bytes: 1200, InFlight: true, AckEliciting: true}); err != nil {
		t.Fatal(err)
	}
	r.ResetForPathChange()
	frame := ack(0, 0)
	frame.ECN = &[3]uint64{0, 0, 1}
	ev, err := r.OnACK(Application, frame, at.Add(time.Millisecond), 0, 0, true)
	if err != nil || ev.ECN.CE != 1 || ev.ECNCongestion {
		t.Fatal("old path CE affected new congestion", ev, err)
	}
	if err := r.OnSent(Application, Packet{Number: 1, ECN: ECT0, SentAt: at.Add(2 * time.Millisecond), Bytes: 1200, InFlight: true, AckEliciting: true}); err != nil {
		t.Fatal(err)
	}
	frame = ack(1, 1)
	frame.ECN = &[3]uint64{0, 0, 2}
	ev, err = r.OnACK(Application, frame, at.Add(3*time.Millisecond), 0, 0, true)
	if err != nil || !ev.ECNCongestion || ev.ECNSentAt != at.Add(2*time.Millisecond) {
		t.Fatal("new path CE suppressed", ev, err)
	}
}

func TestACKCEAfterTimestampEviction(t *testing.T) {
	r, _ := NewSent(1)
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
