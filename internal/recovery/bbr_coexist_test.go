package recovery

import (
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"
)

// coexistFlow names a controller for a coexistence run.
type coexistFlow struct {
	name       string
	newC       func() Controller
	start      time.Duration // after the run starts
	extraDelay time.Duration // one way, on top of the link's
	ecn        bool          // marks its packets ECN-capable
}

func newBBRFlow(seed uint64) func() Controller {
	return func() Controller {
		b := NewBBR(1200)
		b.rng = rand.New(rand.NewPCG(seed, seed+1))
		return b
	}
}

func newRenoFlow() Controller {
	c, _ := NewCongestion(1200)
	return c
}

// coexist runs flows over link for d and returns each one's goodput, in
// bits per second, over the window from measureFrom to the end, and the
// fraction of the packets it sent in that window that the link dropped.
func coexist(t *testing.T, link *simLink, d, measureFrom time.Duration, specs ...coexistFlow) (goodput, loss []float64) {
	t.Helper()
	flows := make([]*simFlow, len(specs))
	from := simStart.Add(measureFrom)
	for i, s := range specs {
		flows[i] = &simFlow{c: s.newC(), start: simStart.Add(s.start), extraDelay: s.extraDelay, ecn: s.ecn, snapshotAt: from}
	}
	simulateFlows(t, link, d, flows...)
	window := (d - measureFrom).Seconds()
	for _, f := range flows {
		base := f.snapshot
		goodput = append(goodput, float64(f.res.delivered-base.delivered)*8/window)
		loss = append(loss, float64(f.res.dropped-base.dropped)/float64(max(f.res.sent-base.sent, 1)))
	}
	return goodput, loss
}

func bufferedLink(bps float64, rtt time.Duration, bdps float64) *simLink {
	l := newLink(bps, rtt, 0)
	l.queue = time.Duration(bdps * float64(rtt))
	return l
}

func shares(goodput []float64) string {
	total := 0.0
	for _, g := range goodput {
		total += g
	}
	s := ""
	for i, g := range goodput {
		if i > 0 {
			s += " / "
		}
		s += fmt.Sprintf("%.1f Mbit/s (%.0f%%)", g/1e6, 100*g/total)
	}
	return s
}

// Two BBR flows that start together on a 100 Mbit/s, 50 ms path with a
// 2 BDP queue converge to near-equal shares.
func TestBBRFlowsShareFairly(t *testing.T) {
	g, _ := coexist(t, bufferedLink(100e6, 50*time.Millisecond, 2), 40*time.Second, 20*time.Second,
		coexistFlow{newC: newBBRFlow(3)}, coexistFlow{newC: newBBRFlow(7)})
	t.Logf("BBR vs BBR: %s", shares(g))
	if lo, hi := min(g[0], g[1]), max(g[0], g[1]); lo <= 0 || lo < 0.7*hi {
		t.Fatalf("BBR flows split %s, want within 30%% of each other", shares(g))
	}
}

// With a queue of 2 BDP, a NewReno flow keeps a substantial share against
// BBR. (In queues of 1 BDP or less BBR takes most of the link from NewReno,
// whose every loss halves its window, while BBR's own loss stays under
// draft-07's 2% threshold; see TestBBRCoexistenceSurvey.)
func TestBBRLeavesNewRenoAShareInDeepBuffers(t *testing.T) {
	g, _ := coexist(t, bufferedLink(100e6, 50*time.Millisecond, 2), 40*time.Second, 10*time.Second,
		coexistFlow{newC: newBBRFlow(3)}, coexistFlow{newC: newRenoFlow, start: 2 * time.Second})
	t.Logf("BBR vs NewReno: %s", shares(g))
	if g[0] <= 0 || g[1] < 0.3*(g[0]+g[1]) {
		t.Fatalf("NewReno got %s, want at least 30%%", shares(g))
	}
}

// In a queue of a tenth of a BDP, BBR alone still fills most of the link
// and keeps its loss rate under draft-07's 2% loss threshold.
func TestBBRShallowBuffer(t *testing.T) {
	g, l := coexist(t, bufferedLink(100e6, 50*time.Millisecond, 0.1), 20*time.Second, 5*time.Second, coexistFlow{newC: newBBRFlow(3)})
	t.Logf("BBR, 0.1 BDP queue: %.1f Mbit/s, loss %.2f%%", g[0]/1e6, 100*l[0])
	if g[0] < 0.8*100e6 || l[0] > 0.02 {
		t.Fatalf("BBR %.1f Mbit/s with %.2f%% loss, want at least 80 Mbit/s under 2%%", g[0]/1e6, 100*l[0])
	}
}

// Behind a 10 Mbit/s token-bucket policer on a 100 Mbit/s link, BBR settles
// near the policed rate rather than repeatedly overrunning it.
func TestBBRPolicer(t *testing.T) {
	link := bufferedLink(100e6, 50*time.Millisecond, 1)
	link.policer = &simPolicer{rate: 10e6 / 8, burst: 64 * 1024}
	g, l := coexist(t, link, 30*time.Second, 10*time.Second, coexistFlow{newC: newBBRFlow(3)})
	t.Logf("BBR behind a 10 Mbit/s policer: %.1f Mbit/s, loss %.2f%%", g[0]/1e6, 100*l[0])
	if g[0] < 0.85*10e6 || l[0] > 0.03 {
		t.Fatalf("BBR %.1f Mbit/s with %.2f%% loss, want at least 8.5 Mbit/s under 3%%", g[0]/1e6, 100*l[0])
	}
}

// TestBBRCoexistenceSurvey logs how BBR and NewReno share a bottleneck across
// queue sizes, RTTs and a policer. It takes over a minute, so it runs only
// with PHOTON_BBR_SURVEY=1.
func TestBBRCoexistenceSurvey(t *testing.T) {
	if os.Getenv("PHOTON_BBR_SURVEY") != "1" {
		t.Skip("set PHOTON_BBR_SURVEY=1 to run")
	}
	const d, from = 40 * time.Second, 10 * time.Second
	rtt := 50 * time.Millisecond
	for _, bdps := range []float64{0.1, 0.5, 1, 2, 4} {
		for _, pair := range []struct {
			name string
			a, b coexistFlow
		}{
			{"reno-reno", coexistFlow{name: "reno", newC: newRenoFlow}, coexistFlow{name: "reno", newC: newRenoFlow, start: 2 * time.Second}},
			{"bbr-bbr", coexistFlow{name: "bbr", newC: newBBRFlow(3)}, coexistFlow{name: "bbr", newC: newBBRFlow(7), start: 2 * time.Second}},
			{"bbr-reno", coexistFlow{name: "bbr", newC: newBBRFlow(3)}, coexistFlow{name: "reno", newC: newRenoFlow, start: 2 * time.Second}},
			{"reno-bbr", coexistFlow{name: "reno", newC: newRenoFlow}, coexistFlow{name: "bbr", newC: newBBRFlow(3), start: 2 * time.Second}},
		} {
			g, l := coexist(t, bufferedLink(100e6, rtt, bdps), d, from, pair.a, pair.b)
			t.Logf("buffer %.1f BDP %-9s %s  loss %.2f%% / %.2f%%", bdps, pair.name, shares(g), 100*l[0], 100*l[1])
		}
	}
	// RTT unfairness: BBR at 20 ms and 100 ms.
	g, l := coexist(t, bufferedLink(100e6, 20*time.Millisecond, 2.5), d, from,
		coexistFlow{newC: newBBRFlow(3)}, coexistFlow{newC: newBBRFlow(7), extraDelay: 40 * time.Millisecond})
	t.Logf("rtt 20 vs 100 ms bbr: %s loss %.2f%% / %.2f%%", shares(g), 100*l[0], 100*l[1])
	g, l = coexist(t, bufferedLink(100e6, 20*time.Millisecond, 2.5), d, from,
		coexistFlow{newC: newRenoFlow}, coexistFlow{newC: newRenoFlow, extraDelay: 40 * time.Millisecond})
	t.Logf("rtt 20 vs 100 ms reno: %s loss %.2f%% / %.2f%%", shares(g), 100*l[0], 100*l[1])
	// A policer at 10 Mbit/s on a 100 Mbit/s link.
	for _, f := range []coexistFlow{{name: "bbr", newC: newBBRFlow(3)}, {name: "reno", newC: newRenoFlow}} {
		link := bufferedLink(100e6, rtt, 1)
		link.policer = &simPolicer{rate: 10e6 / 8, burst: 64 * 1024}
		g, l := coexist(t, link, d, from, f)
		t.Logf("policer 10 Mbit/s %s: %.1f Mbit/s loss %.2f%%", f.name, g[0]/1e6, 100*l[0])
	}
}

// ceBlind is BBR as it was before it answered CE marks: its rate samples
// report none.
type ceBlind struct{ Controller }

func (c ceBlind) OnACK(e ACKEvent) {
	e.Rate.CE, e.Rate.NewCE = 0, 0
	c.Controller.OnACK(e)
}

func newCEBlindBBRFlow(seed uint64) func() Controller {
	return func() Controller { return ceBlind{newBBRFlow(seed)()} }
}

// redLink is a 100 Mbit/s, 50 ms path whose 2 BDP queue runs RED: it acts
// on packets from 5 ms of queueing delay, with a probability rising to 10%
// at 25 ms, marking ECN-capable ones and dropping others.
func redLink() *simLink {
	l := bufferedLink(100e6, 50*time.Millisecond, 2)
	l.red = &simRED{min: 5 * time.Millisecond, max: 25 * time.Millisecond, maxP: 0.1}
	return l
}

// Behind a RED queue that marks, BBR answers the marks: it keeps the queue
// to under half the delay that BBR blind to them builds, loses nothing, and
// still fills most of the link.
func TestBBRAnswersCEMarks(t *testing.T) {
	const d, from = 30 * time.Second, 10 * time.Second
	link := redLink()
	g, l := coexist(t, link, d, from, coexistFlow{newC: newBBRFlow(3), ecn: true})
	blindLink := redLink()
	blind, _ := coexist(t, blindLink, d, from, coexistFlow{newC: newCEBlindBBRFlow(3), ecn: true})
	t.Logf("BBR %.1f Mbit/s, queueing %v, drops %.2f%%; blind to CE %.1f Mbit/s, queueing %v", g[0]/1e6, link.meanQueueing(), 100*l[0], blind[0]/1e6, blindLink.meanQueueing())
	if link.meanQueueing() > blindLink.meanQueueing()/2 {
		t.Fatalf("queueing %v answering CE, %v blind to it", link.meanQueueing(), blindLink.meanQueueing())
	}
	if l[0] > 0 || g[0] < 0.85*100e6 {
		t.Fatalf("BBR %.1f Mbit/s with %.2f%% drops, want at least 85 Mbit/s with none", g[0]/1e6, 100*l[0])
	}
}

// Behind a RED queue, BBR answers marks as it answers the drops RED makes
// of packets that aren't ECN-capable: a NewReno flow keeps at least half
// the share against BBR with ECN that it keeps when both flows' packets are
// dropped. BBR blind to marks starves it.
func TestBBRWithECNSharesAsWithDrops(t *testing.T) {
	const d, from = 30 * time.Second, 10 * time.Second
	run := func(bbr func() Controller, ecn bool) float64 {
		g, _ := coexist(t, redLink(), d, from, coexistFlow{newC: bbr, ecn: ecn}, coexistFlow{newC: newRenoFlow, ecn: ecn, start: 2 * time.Second})
		return g[1] / (g[0] + g[1])
	}
	marked, dropped, blind := run(newBBRFlow(3), true), run(newBBRFlow(3), false), run(newCEBlindBBRFlow(3), true)
	t.Logf("NewReno's share against BBR: %.0f%% with ECN, %.0f%% with drops, %.0f%% against BBR blind to CE", 100*marked, 100*dropped, 100*blind)
	if marked < dropped/2 {
		t.Fatalf("NewReno's share %.0f%% with ECN, %.0f%% with drops", 100*marked, 100*dropped)
	}
}
