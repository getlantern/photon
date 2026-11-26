package recovery

import (
	"testing"
	"time"
)

func TestECNPathProbeBudgetAndLoss(t *testing.T) {
	var disabled ECNPath
	if disabled.Marking() != NotECT {
		t.Fatal("zero policy enabled ECN")
	}
	p := NewECNPath()
	for pn := uint64(0); pn < 10; pn++ {
		if p.Marking() != ECT0 {
			t.Fatal("stopped testing early", pn)
		}
		p.Sent(Application, pn)
		p.Sent(Application, pn) // duplicate commit cannot consume the budget
		if pn < 9 {
			p.Lost(Application, pn)
		}
	}
	if p.State() != ECNUnknown || p.Marking() != NotECT {
		t.Fatal("probe budget exceeded")
	}
	p.Lost(Initial, 9)
	p.Lost(Application, 99)
	if p.State() != ECNUnknown {
		t.Fatal("unrelated losses failed path")
	}
	p.Lost(Application, 9)
	if p.State() != ECNFailed {
		t.Fatal("all probes lost without fallback")
	}
	p.ACK(Application, ack(0, 9), ECNFeedbackResult{Validated: true})
	if p.State() != ECNFailed || p.Marking() != NotECT {
		t.Fatal("failed path revived")
	}
}

func TestECNPathFeedbackAndIsolation(t *testing.T) {
	p := NewECNPath()
	p.Sent(Application, 100)
	p.Sent(Application, 101)
	p.ACK(Application, ack(0, 99), ECNFeedbackResult{Validated: true})
	p.ACK(Initial, ack(100, 100), ECNFeedbackResult{Validated: true})
	p.ACK(Application, ack(100, 100), ECNFeedbackResult{})
	if p.State() != ECNTesting {
		t.Fatal("old path, wrong space, or unvalidated feedback established capability")
	}
	// Probe 100's first acknowledgment carried no validated feedback, so it
	// has no outcome; probe 101's establishes capability.
	p.ACK(Application, ack(100, 100), ECNFeedbackResult{Validated: true, ECT: 1})
	if p.State() != ECNTesting {
		t.Fatal("a probe already acknowledged without feedback established capability")
	}
	p.ACK(Application, ack(100, 101), ECNFeedbackResult{Validated: true, ECT: 1})
	if p.State() != ECNCapable || p.Marking() != ECT0 {
		t.Fatal("valid feedback did not enable ECN")
	}
	p.Lost(Application, 100)
	if p.State() != ECNCapable {
		t.Fatal("ordinary loss disabled validated ECN")
	}
	p.ACK(Handshake, ack(0, 0), ECNFeedbackResult{Failed: true})
	if p.State() != ECNFailed {
		t.Fatal("failure did not span spaces")
	}
	other := NewECNPath()
	if other.State() != ECNTesting {
		t.Fatal("path state leaked")
	}
}

func TestECNPathLedgerFeedback(t *testing.T) {
	for _, bleached := range []bool{false, true} {
		r := tracker(t)
		p := NewECNPath()
		at := time.Unix(100, 0)
		for pn := uint64(0); pn < 10; pn++ {
			marking := p.Marking()
			if err := r.OnSent(Application, Packet{Number: pn, ECN: marking, SentAt: at, Bytes: 1200, InFlight: true, AckEliciting: true}); err != nil {
				t.Fatal(err)
			}
			p.Sent(Application, pn)
		}
		frame := ack(0, 9)
		counts := [3]uint64{9, 0, 1}
		if bleached {
			counts = [3]uint64{}
		}
		frame.ECN = &counts
		events, err := r.OnACK(Application, frame, at.Add(time.Millisecond), 0, 0, true)
		if err != nil {
			t.Fatal(err)
		}
		result := events.ECN
		p.ACK(Application, frame, result)
		if bleached {
			if p.State() != ECNFailed || p.Marking() != NotECT {
				t.Fatal("bleaching accepted")
			}
		} else if p.State() != ECNCapable || result.CE != 1 {
			t.Fatal("valid feedback rejected", result)
		}
	}
}

func FuzzECNPathTransitions(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3})
	f.Fuzz(func(t *testing.T, actions []byte) {
		p := NewECNPath()
		var pn uint64
		for _, action := range actions {
			before := p.State()
			switch action % 5 {
			case 0:
				p.Sent(Application, pn)
				pn++
			case 1:
				p.Lost(Application, uint64(action/5))
			case 2:
				p.ACK(Application, ack(0, pn), ECNFeedbackResult{Validated: true})
			case 3:
				p.ACK(Application, ack(0, pn), ECNFeedbackResult{Failed: true})
			case 4:
				p.ACK(Application, ack(0, pn), ECNFeedbackResult{})
			}
			if p.count > 10 {
				t.Fatal("unbounded probe history")
			}
			if before == ECNFailed && p.State() != ECNFailed {
				t.Fatal("failure revived")
			}
			if p.Marking() == ECT0 && p.State() != ECNTesting && p.State() != ECNCapable {
				t.Fatal("marking without permission")
			}
			if p.State() == ECNCapable && p.count == 0 {
				t.Fatal("capability without a probe")
			}
		}
	})
}

// A path that remarks every test packet CE fails validation, as quic-go
// does, rather than reading as constant congestion; one probe that arrives
// still ECT establishes capability.
func TestECNPathRemarkedToCE(t *testing.T) {
	for _, oneECT := range []bool{false, true} {
		p := NewECNPath()
		for n := uint64(0); n < 10; n++ {
			p.Sent(Application, n)
		}
		p.ACK(Application, ack(0, 0), ECNFeedbackResult{Validated: true, CE: 1})
		if p.State() != ECNUnknown {
			t.Fatal("CE-only feedback for one probe decided the path:", p.State())
		}
		p.Lost(Application, 1)
		for n := uint64(2); n < 9; n++ {
			p.ACK(Application, ack(n, n), ECNFeedbackResult{Validated: true, CE: 1})
		}
		last := ECNFeedbackResult{Validated: true, CE: 1}
		if oneECT {
			last = ECNFeedbackResult{Validated: true, ECT: 1}
		}
		p.ACK(Application, ack(9, 9), last)
		if want := map[bool]ECNPathState{false: ECNFailed, true: ECNCapable}[oneECT]; p.State() != want {
			t.Fatalf("one ECT probe %t: state %d, want %d", oneECT, p.State(), want)
		}
	}
}

// After a path change, an old-path packet reported ECT, acknowledged with
// the new path's probe reported CE, doesn't make the new path capable: the
// counts can't say which arrived how, so the old packet takes the ECT.
func TestECNNewPathIgnoresOldPathECT(t *testing.T) {
	r := tracker(t)
	r.EnableECN()
	at := time.Unix(100, 0)
	if err := r.OnSent(Application, Packet{Number: 0, SentAt: at, Bytes: 1200, AckEliciting: true, InFlight: true, ECN: ECT0}); err != nil {
		t.Fatal(err)
	}
	r.ResetForPathChange()
	if err := r.OnSent(Application, Packet{Number: 1, SentAt: at, Bytes: 1200, AckEliciting: true, InFlight: true, ECN: r.ECNMarkingAt(Application, at)}); err != nil {
		t.Fatal(err)
	}
	frame := ack(0, 1)
	frame.ECN = &[3]uint64{1, 0, 1}
	if _, err := r.OnACK(Application, frame, at.Add(time.Millisecond), 0, 0, true); err != nil {
		t.Fatal(err)
	}
	if r.ECNState() == ECNCapable {
		t.Fatal("old-path ECT established the new path's capability")
	}
}

// A probe acknowledged with no ECT or CE share of the feedback, as after a
// path change where the old path's packets take both, isn't remarked: the
// path doesn't fail on it.
func TestECNPathAmbiguousProbeNotRemarked(t *testing.T) {
	p := NewECNPath()
	for n := uint64(0); n < 10; n++ {
		p.Sent(Application, n)
	}
	p.ACK(Application, ack(0, 0), ECNFeedbackResult{Validated: true})
	for n := uint64(1); n < 10; n++ {
		p.Lost(Application, n)
	}
	if p.State() == ECNFailed {
		t.Fatal("an ambiguous probe failed the path as remarked")
	}
}

// Evidence for packets lost and never acknowledged doesn't fill the ledger
// and end marking: at capacity, untracked packets' evidence is reclaimed.
func TestECNLostEvidenceReclaimed(t *testing.T) {
	r, _ := NewSent(16)
	r.EnableECN()
	at := time.Unix(100, 0)
	send := func(pn uint64) {
		t.Helper()
		if err := r.OnSent(Application, Packet{Number: pn, SentAt: at, Bytes: 1200, AckEliciting: true, InFlight: true, ECN: r.ECNMarkingAt(Application, at)}); err != nil {
			t.Fatal(err)
		}
	}
	send(0)
	frame := ack(0, 0)
	frame.ECN = &[3]uint64{1, 0, 0}
	if _, err := r.OnACK(Application, frame, at.Add(time.Millisecond), 0, 0, true); err != nil || r.ECNState() != ECNCapable {
		t.Fatal("path not validated", err)
	}
	// Rounds of four packets, the last acknowledged: the first is lost.
	ect := uint64(1)
	for round := uint64(0); round < 20; round++ {
		base := 1 + 4*round
		for pn := base; pn < base+4; pn++ {
			if r.ECNMarkingAt(Application, at) != ECT0 {
				t.Fatalf("round %d: marking stopped with lost evidence filling the ledger", round)
			}
			send(pn)
		}
		ect++
		frame := ack(base+3, base+3)
		frame.ECN = &[3]uint64{ect, 0, 0}
		if _, err := r.OnACK(Application, frame, at.Add(time.Millisecond), 0, 0, true); err != nil {
			t.Fatal(err)
		}
		// The two after the lost one are acknowledged too.
		ect += 2
		frame = ack(base+1, base+3)
		frame.ECN = &[3]uint64{ect, 0, 0}
		if _, err := r.OnACK(Application, frame, at.Add(2*time.Millisecond), 0, 0, true); err != nil {
			t.Fatal(err)
		}
	}
}
