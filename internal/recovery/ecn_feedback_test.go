package recovery

import (
	"github.com/getlantern/photon/internal/wire"
	"testing"
)

func TestECNFeedbackAdvancingAndReordered(t *testing.T) {
	var e ECNFeedback
	sent := [2]uint64{10, 2}
	first := [3]uint64{2, 1, 1}
	got := e.Check(4, &first, sent, [2]uint64{2, 1})
	if !got.Validated || got.Failed || got.CE != 1 {
		t.Fatal(got)
	}
	// Neither contradictory counts nor missing counts on an older/equal ACK
	// may disable ECN or repeat the congestion signal.
	for _, largest := range []uint64{3, 4} {
		bad := [3]uint64{wire.MaxVarint, wire.MaxVarint, wire.MaxVarint}
		if got := e.Check(largest, &bad, sent, [2]uint64{1, 0}); got != (ECNFeedbackResult{}) {
			t.Fatal(got)
		}
		if got := e.Check(largest, nil, sent, [2]uint64{1, 0}); got != (ECNFeedbackResult{}) {
			t.Fatal(got)
		}
	}
	next := [3]uint64{5, 1, 2} // More feedback than newly ACKed is legal after ACK loss.
	if got := e.Check(8, &next, sent, [2]uint64{2, 0}); !got.Validated || got.CE != 1 || got.Failed {
		t.Fatal(got)
	}
	if got := e.Check(9, nil, sent, [2]uint64{}); got != (ECNFeedbackResult{}) {
		t.Fatal(got)
	}
}

func TestECNFeedbackFailure(t *testing.T) {
	tests := []struct {
		name        string
		counts      *[3]uint64
		sent, newly [2]uint64
	}{
		{"missing", nil, [2]uint64{1, 0}, [2]uint64{1, 0}},
		{"bleached", &[3]uint64{}, [2]uint64{1, 0}, [2]uint64{1, 0}},
		{"remarked", &[3]uint64{0, 1, 0}, [2]uint64{1, 0}, [2]uint64{1, 0}},
		{"ect0 excess", &[3]uint64{2, 0, 0}, [2]uint64{1, 0}, [2]uint64{1, 0}},
		{"ce excess", &[3]uint64{0, 0, 2}, [2]uint64{1, 0}, [2]uint64{1, 0}},
		{"shared ce insufficient", &[3]uint64{0, 0, 1}, [2]uint64{1, 1}, [2]uint64{1, 1}},
		{"range", &[3]uint64{wire.MaxVarint + 1, 0, 0}, [2]uint64{1, 0}, [2]uint64{1, 0}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var e ECNFeedback
			got := e.Check(0, tc.counts, tc.sent, tc.newly)
			if !got.Failed || got.Validated || got.CE != 0 {
				t.Fatal(got)
			}
			valid := [3]uint64{1, 0, 0}
			if got := e.Check(1, &valid, [2]uint64{1, 0}, [2]uint64{1, 0}); !got.Failed {
				t.Fatal("failed state revived")
			}
		})
	}
}

func TestECNFeedbackDecreaseAndIsolation(t *testing.T) {
	var first, second ECNFeedback
	sent := [2]uint64{2, 0}
	counts := [3]uint64{1, 0, 0}
	if got := first.Check(0, &counts, sent, [2]uint64{1, 0}); !got.Validated {
		t.Fatal(got)
	}
	counts = [3]uint64{}
	if got := first.Check(1, &counts, sent, [2]uint64{}); !got.Failed {
		t.Fatal("decreasing counter accepted")
	}
	counts = [3]uint64{0, 0, 1}
	if got := second.Check(0, &counts, sent, [2]uint64{1, 0}); !got.Validated || got.CE != 1 {
		t.Fatal("space/path state leaked", got)
	}
}

func TestECNFeedbackCounterBoundsAndOwnership(t *testing.T) {
	var e ECNFeedback
	counts := [3]uint64{wire.MaxVarint, wire.MaxVarint, 0}
	sent := [2]uint64{wire.MaxVarint, wire.MaxVarint}
	if got := e.Check(0, &counts, sent, sent); !got.Validated || got.Failed {
		t.Fatal("overflow in bounded sums", got)
	}
	counts[0] = 0
	original := [3]uint64{wire.MaxVarint, wire.MaxVarint, 0}
	if got := e.Check(1, &original, sent, [2]uint64{}); got.Failed {
		t.Fatal("borrowed caller counters")
	}
	original[2] = 1
	if got := e.Check(2, &original, sent, [2]uint64{}); !got.Failed || got.CE != 0 {
		t.Fatal("excess total escaped boundary check", got)
	}
}

func FuzzECNFeedback(f *testing.F) {
	f.Add(uint64(1), uint64(0), uint64(0), uint64(1), uint64(0), uint64(1), uint64(0))
	f.Fuzz(func(t *testing.T, c0, c1, ce, s0, s1, a0, a1 uint64) {
		var e ECNFeedback
		counts := [3]uint64{c0, c1, ce}
		sent, newly := [2]uint64{s0, s1}, [2]uint64{a0, a1}
		got := e.Check(0, &counts, sent, newly)
		if got.Failed {
			if got.CE != 0 || got.Validated {
				t.Fatal("failure emitted congestion/validation")
			}
			if next := e.Check(1, &[3]uint64{}, [2]uint64{}, [2]uint64{}); !next.Failed {
				t.Fatal("failure revived")
			}
			return
		}
		if got.CE != ce {
			t.Fatal("first CE increase")
		}
		if duplicate := e.Check(0, nil, sent, newly); duplicate != (ECNFeedbackResult{}) {
			t.Fatal("duplicate feedback changed state")
		}
	})
}
