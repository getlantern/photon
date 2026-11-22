package recovery

import (
	"testing"
	"time"
)

func TestECNTestingTimeBudget(t *testing.T) {
	r := tracker(t)
	r.EnableECN()
	at := time.Unix(100, 0)
	budget := 3 * r.RTT.PTO(false, false, 0, 0)
	if err := r.OnSent(Application, Packet{ECN: ECT0, SentAt: at}); err != nil {
		t.Fatal(err)
	}
	if r.ECNMarkingAt(Application, at.Add(budget-time.Nanosecond)) != ECT0 {
		t.Fatal("stopped early")
	}
	// Later RTT estimates must not push the original deadline out.
	r.RTT.Smoothed = time.Hour
	if r.ECNMarkingAt(Application, at.Add(budget)) != NotECT || r.ECNState() != ECNUnknown {
		t.Fatal("unbounded testing")
	}
	frame := ack(0, 0)
	frame.ECN = &[3]uint64{1, 0, 0}
	if _, err := r.OnACK(Application, frame, at.Add(budget+time.Millisecond), 0, 0, true); err != nil {
		t.Fatal(err)
	}
	if r.ECNMarkingAt(Application, at.Add(time.Hour)) != ECT0 {
		t.Fatal("valid late feedback did not establish capability")
	}
	r.ResetForPathChange()
	if r.ECNMarkingAt(Application, at.Add(time.Hour)) != ECT0 {
		t.Fatal("new path inherited old deadline")
	}
}

func TestECNTimeBudgetWithAllProbesLost(t *testing.T) {
	p := NewECNPath()
	p.Sent(Initial, 0)
	p.Lost(Initial, 0)
	if p.State() != ECNTesting {
		t.Fatal("failed before probe window ended")
	}
	p.endTesting()
	if p.State() != ECNFailed || p.Marking() != NotECT {
		t.Fatal("short test flight loss ignored")
	}
}

func TestECNRejectedOnlyProbeDoesNotStartTimeBudget(t *testing.T) {
	for _, probe := range []bool{false, true} {
		r := tracker(t)
		r.EnableECN()
		at := time.Unix(100, 0)
		if err := r.OnSent(Application, Packet{ECN: ECT0, SentAt: at, Bytes: 1400, InFlight: true, AckEliciting: true, MTUProbe: probe}); err != nil {
			t.Fatal(err)
		}
		var rejected bool
		if probe {
			rejected = r.RejectMTUProbe(0)
		} else {
			rejected = r.RejectLargePacket(0)
		}
		if !rejected {
			t.Fatal("local rejection failed")
		}
		later := at.Add(time.Minute)
		if r.ECNMarkingAt(Application, later) != ECT0 {
			t.Fatal("unsent probe expired ECN testing")
		}
		if err := r.OnSent(Application, Packet{Number: 1, ECN: ECT0, SentAt: later}); err != nil {
			t.Fatal(err)
		}
		if r.ECNMarkingAt(Application, later.Add(time.Millisecond)) != ECT0 {
			t.Fatal("replacement inherited deadline")
		}
		if r.ECNMarkingAt(Application, later.Add(3*r.RTT.PTO(false, false, 0, 0))) != NotECT {
			t.Fatal("replacement did not start deadline")
		}
	}
}

func TestECNRejectLaterProbeDoesNotExtendDeadline(t *testing.T) {
	r := tracker(t)
	r.EnableECN()
	at := time.Unix(100, 0)
	for pn := uint64(0); pn < 2; pn++ {
		if err := r.OnSent(Application, Packet{Number: pn, ECN: ECT0, SentAt: at.Add(time.Duration(pn) * time.Second), Bytes: 1400, InFlight: true, AckEliciting: true}); err != nil {
			t.Fatal(err)
		}
	}
	if !r.RejectLargePacket(1) {
		t.Fatal("local rejection failed")
	}
	if r.ECNMarkingAt(Application, at.Add(3*r.RTT.PTO(false, false, 0, 0))) != NotECT {
		t.Fatal("remaining probe deadline extended")
	}
}

func TestECNDiscardOnlyProbeAllowsNewSpaceTesting(t *testing.T) {
	r := tracker(t)
	r.EnableECN()
	at := time.Unix(100, 0)
	if err := r.OnSent(Initial, Packet{ECN: ECT0, SentAt: at}); err != nil {
		t.Fatal(err)
	}
	r.Discard(Initial)
	if r.ECNMarkingAt(Handshake, at.Add(time.Minute)) != ECT0 {
		t.Fatal("discarded space prevented testing")
	}
}
