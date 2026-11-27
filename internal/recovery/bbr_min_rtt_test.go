package recovery

import (
	"testing"
	"time"
)

// min_rtt keeps the draft's two-level filter: when its ten-second filter
// expires it takes the lowest RTT of the current ProbeRTT interval, not the
// next sample, which a standing queue would inflate.
func TestBBRMinRTTExpiryTakesIntervalMinimum(t *testing.T) {
	b := NewBBR(1200)
	start := time.Unix(100, 0)
	ms := time.Millisecond
	sample := func(after, rtt time.Duration) {
		b.at(start.Add(after))
		b.updateMinRTT(RateSample{RTT: rtt}, start.Add(after))
	}
	sample(0, 50*ms)
	// A ProbeRTT interval begins at 6 s; its lowest RTT is 60 ms.
	sample(6*time.Second, 80*ms)
	sample(7*time.Second, 60*ms)
	sample(9*time.Second, 90*ms)
	if b.minRTT != 50*ms {
		t.Fatal("before expiry", b.minRTT)
	}
	// At 10.5 s min_rtt has expired: a 95 ms sample doesn't become it.
	sample(10500*ms, 95*ms)
	if b.minRTT != 60*ms {
		t.Fatalf("expired min_rtt %v, want the interval's 60ms", b.minRTT)
	}
}

// Seeing min_rtt again refreshes it: after an ACK gap, a min_rtt confirmed
// five seconds ago hasn't expired, and a queued sample doesn't replace it.
func TestBBRMinRTTRefreshedWhenSeenAgain(t *testing.T) {
	b := NewBBR(1200)
	start := time.Unix(100, 0)
	ms := time.Millisecond
	sample := func(after, rtt time.Duration) {
		b.at(start.Add(after))
		b.updateMinRTT(RateSample{RTT: rtt}, start.Add(after))
	}
	sample(0, 50*ms)
	sample(5300*ms, 50*ms)
	sample(10400*ms, 90*ms)
	if b.minRTT != 50*ms {
		t.Fatalf("min_rtt %v after an ACK gap, want the 50ms seen 5 s before", b.minRTT)
	}
}

// ProbeRTT runs after five seconds without a lower RTT, as the draft's
// ProbeRTTInterval has it, not ten: on a clean path, every five seconds.
func TestBBRProbeRTTInterval(t *testing.T) {
	b := newBBRFlow(3)().(*BBR)
	entries := 0
	in := false
	simulate(t, b, newLink(50e6, 40*time.Millisecond, 0), 31*time.Second, func(time.Time) {
		if b.state == bbrProbeRTT && !in {
			entries++
		}
		in = b.state == bbrProbeRTT
	})
	if entries < 5 || entries > 6 {
		t.Fatalf("%d ProbeRTTs in 31 s, want 5 or 6", entries)
	}
}

// Beside NewReno, whose sawtooth drains its queue now and then, min_rtt
// stays near the path's: the interval's lowest RTT catches a trough. Taking
// the sample after expiry, min_rtt reached 78-90 ms on this 50 ms path with
// a 1 BDP buffer, and 126-132 ms with 2.
func TestBBRMinRTTBesideNewReno(t *testing.T) {
	for _, tc := range []struct {
		bdps float64
		max  time.Duration
	}{{1, 55 * time.Millisecond}, {2, 75 * time.Millisecond}} {
		b := newBBRFlow(3)().(*BBR)
		var top time.Duration
		flows := []*simFlow{{c: b, observe: func(time.Time) {
			if b.minRTT < time.Hour {
				top = max(top, b.minRTT)
			}
		}}, {c: newRenoFlow(), start: simStart.Add(3 * time.Second)}}
		simulateFlows(t, bufferedLink(100e6, 50*time.Millisecond, tc.bdps), 60*time.Second, flows...)
		t.Logf("%v BDP: min_rtt at most %v", tc.bdps, top.Round(time.Millisecond))
		if top > tc.max {
			t.Fatalf("%v BDP: min_rtt reached %v on a 50 ms path, want at most %v", tc.bdps, top, tc.max)
		}
	}
}
