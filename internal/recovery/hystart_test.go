package recovery

import (
	"math"
	"testing"
	"time"
)

// hystartDriver acknowledges NewReno's slow start one round at a time: each
// round, a window of packets sent after the previous round began, each
// acknowledged by its own ACK carrying rtt.
type hystartDriver struct {
	t   *testing.T
	c   *NewReno
	now time.Time
}

func newHystartDriver(t *testing.T) *hystartDriver {
	c, err := NewCongestion(1200)
	if err != nil {
		t.Fatal(err)
	}
	return &hystartDriver{t: t, c: c, now: time.Unix(100, 0)}
}

// round acknowledges one window of packets at rtt and returns the window's
// growth over the round.
func (d *hystartDriver) round(rtt time.Duration) uint64 {
	d.t.Helper()
	before := d.c.Window()
	sent := d.now.Add(time.Microsecond)
	d.now = sent.Add(rtt)
	for range before / 1200 {
		d.c.OnACK(ACKEvent{Acked: []Packet{{SentAt: sent, Bytes: 1200, InFlight: true}}, Rate: RateSample{RTT: rtt}, Now: d.now})
	}
	return d.c.Window() - before
}

func (d *hystartDriver) inSlowStart() bool { return d.c.Window() < d.c.Threshold() }

// A rising RTT enters conservative slow start, which grows a quarter as fast,
// and after its rounds ends slow start, with no loss.
func TestHyStartEndsSlowStartOnRisingRTT(t *testing.T) {
	d := newHystartDriver(t)
	for range 3 {
		if grew, window := d.round(100*time.Millisecond), d.c.Window(); grew != window/2 {
			t.Fatal("slow start not doubling", grew, window)
		}
	}
	// Rises by more than the threshold, min(max(100/8, 4), 16) = 12.5 ms.
	d.round(115 * time.Millisecond)
	if !d.c.hystart.css {
		t.Fatal("rising RTT did not enter CSS")
	}
	before := d.c.Window()
	grew := d.round(115 * time.Millisecond)
	if grew > before/4+1200 || grew < before/4-1200 {
		t.Fatal("CSS growth", grew, "window", before)
	}
	// RFC 9406 runs CSS for five rounds, counting the one it began in: three
	// more full rounds stay in slow start, and the next round's first ACK
	// ends it.
	for round := range 3 {
		d.round(115 * time.Millisecond)
		if !d.inSlowStart() {
			t.Fatal("slow start ended early, after CSS round", round+3)
		}
	}
	d.round(115 * time.Millisecond)
	if d.inSlowStart() || d.c.Threshold() == math.MaxUint64 {
		t.Fatal("slow start did not end after CSS rounds", d.c.Window(), d.c.Threshold())
	}
}

// An RTT that falls back below the rise was a spurious rise: slow start
// resumes at full speed.
func TestHyStartSpuriousRiseResumesSlowStart(t *testing.T) {
	d := newHystartDriver(t)
	for range 3 {
		d.round(100 * time.Millisecond)
	}
	d.round(130 * time.Millisecond)
	if !d.c.hystart.css {
		t.Fatal("rising RTT did not enter CSS")
	}
	d.round(100 * time.Millisecond)
	if d.c.hystart.css || !d.inSlowStart() {
		t.Fatal("spurious rise kept CSS")
	}
	if grew, window := d.round(100*time.Millisecond), d.c.Window(); grew != window/2 {
		t.Fatal("slow start not resumed", grew, window)
	}
}

// A loss during CSS ends HyStart++ and its quarter-speed growth: a later
// slow start grows at full speed.
func TestHyStartLossDuringCSS(t *testing.T) {
	d := newHystartDriver(t)
	for range 3 {
		d.round(100 * time.Millisecond)
	}
	d.round(130 * time.Millisecond)
	if !d.c.hystart.css {
		t.Fatal("rising RTT did not enter CSS")
	}
	d.c.OnLoss(LossEvent{Lost: []Packet{{SentAt: d.now, Bytes: 1200, InFlight: true}}, Now: d.now})
	d.c.OnPersistentCongestion()
	d.now = d.now.Add(time.Second)
	if grew, window := d.round(100*time.Millisecond), d.c.Window(); !d.inSlowStart() || grew != window/2 {
		t.Fatal("slow start after a loss in CSS not at full speed", grew, window)
	}
}

// The spurious-rise check needs only the current round's samples: a CSS
// round with no RTT samples doesn't keep the next from resuming slow start.
func TestHyStartSpuriousAfterUnsampledRound(t *testing.T) {
	d := newHystartDriver(t)
	for range 3 {
		d.round(100 * time.Millisecond)
	}
	d.round(130 * time.Millisecond)
	if !d.c.hystart.css {
		t.Fatal("rising RTT did not enter CSS")
	}
	d.round(0) // ACKs with no RTT sample
	if !d.c.hystart.css {
		t.Fatal("unsampled round left CSS")
	}
	d.round(100 * time.Millisecond)
	if d.c.hystart.css || !d.inSlowStart() {
		t.Fatal("RTT fell back after an unsampled round, but CSS continued")
	}
}

// HyStart++ covers only the first slow start: after a loss, a later slow
// start, toward the halved threshold, ignores the RTT.
func TestHyStartOnlyInitialSlowStart(t *testing.T) {
	d := newHystartDriver(t)
	for range 3 {
		d.round(100 * time.Millisecond)
	}
	d.c.OnLoss(LossEvent{Lost: []Packet{{SentAt: d.now, Bytes: 1200, InFlight: true}}, Now: d.now})
	d.c.OnPersistentCongestion()
	d.now = d.now.Add(time.Second)
	for range 3 {
		d.round(100 * time.Millisecond)
	}
	d.round(200 * time.Millisecond)
	if d.c.hystart.css {
		t.Fatal("HyStart++ ran after the first slow start")
	}
}

// A steady RTT never leaves slow start early.
func TestHyStartSteadyRTTStaysInSlowStart(t *testing.T) {
	d := newHystartDriver(t)
	for range 10 {
		d.round(100 * time.Millisecond)
	}
	if d.c.hystart.css || !d.inSlowStart() {
		t.Fatal("steady RTT left slow start")
	}
}

// On a path whose buffer holds one bandwidth-delay product, plain slow start
// doubles the window past what the path holds before the first loss, a
// queue's delay late, reports it. HyStart++ sees the RTT rise first and
// drops about 40% fewer packets, at no cost in goodput. It can't do better:
// an ACK measures a packet sent a round earlier, so the rise shows a round
// after the window passes the path's capacity, by when it has doubled again.
func TestHyStartCutsSlowStartOvershoot(t *testing.T) {
	const d = 10 * time.Second
	run := func(hystart bool) simResult {
		c, err := NewCongestion(1200)
		if err != nil {
			t.Fatal(err)
		}
		c.hystart.done = !hystart
		return simulate(t, c, newLink(50e6, 300*time.Millisecond, 0), d, nil)
	}
	plain, with := run(false), run(true)
	t.Logf("plain: dropped %d, %.1f Mbit/s; HyStart++: dropped %d, %.1f Mbit/s", plain.dropped, goodput(plain, d)/1e6, with.dropped, goodput(with, d)/1e6)
	if with.dropped*10 > plain.dropped*7 {
		t.Fatalf("HyStart++ dropped %d, plain slow start %d: want at least 30%% fewer", with.dropped, plain.dropped)
	}
	if goodput(with, d) < 0.95*goodput(plain, d) {
		t.Fatalf("HyStart++ goodput %.1f Mbit/s, plain %.1f", goodput(with, d)/1e6, goodput(plain, d)/1e6)
	}
}
