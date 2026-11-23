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
	p.ACK(Application, ack(0, 99), ECNFeedbackResult{Validated: true})
	p.ACK(Initial, ack(100, 100), ECNFeedbackResult{Validated: true})
	p.ACK(Application, ack(100, 100), ECNFeedbackResult{})
	if p.State() != ECNTesting {
		t.Fatal("old path, wrong space, or unvalidated feedback established capability")
	}
	p.ACK(Application, ack(100, 100), ECNFeedbackResult{Validated: true, CE: 1})
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
