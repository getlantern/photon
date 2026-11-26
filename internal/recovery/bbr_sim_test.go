package recovery

import (
	"container/heap"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// simLink is one direction of a bottleneck: a link of rate bytes per second
// with a drop-tail queue holding at most queue of backlog, seeded random
// loss, and a fixed one-way propagation delay. ACKs return over an
// uncongested, lossless path with the same delay.
type simLink struct {
	rate      func(time.Time) float64
	delay     time.Duration
	queue     time.Duration
	loss      float64
	rng       *rand.Rand
	busyUntil time.Time
}

// simACK is an acknowledgment that reaches the sender at at.
type simACK struct {
	at time.Time
	pn uint64
}

type ackHeap []simACK

func (h ackHeap) Len() int           { return len(h) }
func (h ackHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h ackHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *ackHeap) Push(x any)        { *h = append(*h, x.(simACK)) }
func (h *ackHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// simResult is what a simulated bulk transfer measured.
type simResult struct {
	delivered uint64 // bytes acknowledged
	sent      int    // packets sent
	lost      int    // packets declared lost
}

// simulate runs a bulk transfer of 1200-byte packets through link for d,
// driving a real Sent tracker, pacer and controller as the transport does.
// observe, if set, is called after every ACK.
func simulate(t *testing.T, c Controller, link *simLink, d time.Duration, observe func(time.Time)) simResult {
	t.Helper()
	const size = 1200
	sent, _ := NewSent(65536)
	var pacer Pacer
	var acks ackHeap
	var res simResult
	start := time.Unix(1000, 0)
	now := start
	end := start.Add(d)
	var pn uint64
	for now.Before(end) {
		// Deliver due ACKs.
		for acks.Len() > 0 && !acks[0].at.After(now) {
			a := heap.Pop(&acks).(simACK)
			events, err := sent.OnACK(Application, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: a.pn, Largest: a.pn}}}, a.at, 0, 0, true)
			if err != nil {
				t.Fatal(err)
			}
			if len(events.Lost) > 0 {
				res.lost += len(events.Lost)
				c.OnLoss(LossEvent{Lost: events.Lost, TotalLost: sent.LostBytes(), TotalDelivered: sent.DeliveredBytes(), Flight: sent.BytesInFlight, Now: a.at})
			}
			c.OnACK(ACKEvent{Acked: events.Acked, Rate: events.Rate, Flight: sent.BytesInFlight, Now: a.at})
			if observe != nil {
				observe(a.at)
			}
		}
		// Time-threshold loss detection.
		if lt, _, ok := sent.LossTime(); ok && !lt.After(now) {
			if lost := sent.DetectLoss(Application, now); len(lost) > 0 {
				res.lost += len(lost)
				c.OnLoss(LossEvent{Lost: lost, TotalLost: sent.LostBytes(), TotalDelivered: sent.DeliveredBytes(), Flight: sent.BytesInFlight, Now: now})
			}
		}
		// Send what the window and pacer allow: the application always has
		// data, so the sender is never application limited.
		next := end
		for c.CanSend(sent.BytesInFlight, size, true, false) {
			if wait := pacer.DelayFor(now, c, sent.RTT.Smoothed, size); wait > 0 {
				next = now.Add(wait)
				break
			}
			e := SentEvent{Bytes: size, Flight: sent.BytesInFlight, Now: now}
			if err := sent.OnSent(Application, Packet{Number: pn, SentAt: now, Bytes: size, AckEliciting: true, InFlight: true}); err != nil {
				t.Fatal(err)
			}
			c.OnPacketSent(e)
			pacer.Sent(size)
			res.sent++
			if at, ok := link.transmit(now, size); ok {
				heap.Push(&acks, simACK{at: at.Add(link.delay), pn: pn})
			}
			pn++
		}
		if acks.Len() > 0 && acks[0].at.Before(next) {
			next = acks[0].at
		}
		if lt, _, ok := sent.LossTime(); ok && lt.Before(next) {
			next = lt
		}
		if !next.After(now) {
			next = now.Add(time.Microsecond)
		}
		now = next
	}
	res.delivered = sent.rate.delivered
	return res
}

// transmit queues a packet sent at now, returning when it reaches the
// receiver, or false if it is dropped.
func (l *simLink) transmit(now time.Time, size int) (time.Time, bool) {
	if l.loss > 0 && l.rng.Float64() < l.loss {
		return time.Time{}, false
	}
	departure := now
	if l.busyUntil.After(now) {
		departure = l.busyUntil
	}
	if departure.Sub(now) > l.queue {
		return time.Time{}, false
	}
	l.busyUntil = departure.Add(time.Duration(float64(size) / l.rate(now) * float64(time.Second)))
	return l.busyUntil.Add(l.delay), true
}

func constantRate(bps float64) func(time.Time) float64 {
	return func(time.Time) float64 { return bps / 8 }
}

func newLink(bps float64, rtt time.Duration, loss float64) *simLink {
	return &simLink{rate: constantRate(bps), delay: rtt / 2, queue: rtt, loss: loss, rng: rand.New(rand.NewPCG(1, 2))}
}

func goodput(r simResult, d time.Duration) float64 {
	return float64(r.delivered) * 8 / d.Seconds()
}

// On a clean 100 Mbit/s, 50 ms path, BBR fills the link, leaves Startup for
// ProbeBW, and measures the path's RTT.
func TestBBRFillsCleanPath(t *testing.T) {
	b := NewBBR(1200)
	b.rng = rand.New(rand.NewPCG(3, 4))
	const d = 5 * time.Second
	r := simulate(t, b, newLink(100e6, 50*time.Millisecond, 0), d, nil)
	if got := goodput(r, d); got < 0.85*100e6 {
		t.Fatalf("goodput %.1f Mbit/s, want at least 85 of 100", got/1e6)
	}
	if !b.fullBwReached || !b.inProbeBW() && b.state != bbrProbeRTT {
		t.Fatalf("state %d, full bandwidth reached %t", b.state, b.fullBwReached)
	}
	if b.minRTT < 50*time.Millisecond || b.minRTT > 55*time.Millisecond {
		t.Fatalf("min RTT %v, want about 50ms", b.minRTT)
	}
}

// With 1% random loss, BBR keeps most of the link, where NewReno, which
// treats each loss as congestion, collapses.
func TestBBRToleratesRandomLoss(t *testing.T) {
	const d = 10 * time.Second
	b := NewBBR(1200)
	b.rng = rand.New(rand.NewPCG(3, 4))
	bbr := goodput(simulate(t, b, newLink(100e6, 50*time.Millisecond, 0.01), d, nil), d)
	reno, _ := NewCongestion(1200)
	newReno := goodput(simulate(t, reno, newLink(100e6, 50*time.Millisecond, 0.01), d, nil), d)
	t.Logf("1%% loss, 100 Mbit/s, 50 ms: BBR %.1f Mbit/s, NewReno %.1f Mbit/s", bbr/1e6, newReno/1e6)
	// Outside bandwidth probes, BBRv3's short-term bound decays a little in
	// each round with loss and resets at the next probe, so random loss
	// costs it some throughput, not most.
	if bbr < 0.5*100e6 {
		t.Fatalf("BBR goodput %.1f Mbit/s under 1%% loss, want at least 50", bbr/1e6)
	}
	if bbr < 5*newReno {
		t.Fatalf("BBR %.1f Mbit/s not well above NewReno's %.1f", bbr/1e6, newReno/1e6)
	}
}

// Every 10 seconds without a lower RTT sample, BBR enters ProbeRTT to drain
// the queue and refresh its minimum RTT, then returns to ProbeBW.
func TestBBRProbesRTT(t *testing.T) {
	b := NewBBR(1200)
	b.rng = rand.New(rand.NewPCG(3, 4))
	entered, left := false, false
	simulate(t, b, newLink(50e6, 40*time.Millisecond, 0), 13*time.Second, func(time.Time) {
		if b.state == bbrProbeRTT {
			entered = true
		} else if entered && b.inProbeBW() {
			left = true
		}
	})
	if !entered || !left {
		t.Fatalf("entered ProbeRTT %t, returned to ProbeBW %t", entered, left)
	}
}

// When the bottleneck slows tenfold, BBR's bandwidth estimate follows it
// down within a few probing cycles rather than overrunning the queue.
func TestBBRAdaptsToBandwidthDrop(t *testing.T) {
	b := NewBBR(1200)
	b.rng = rand.New(rand.NewPCG(3, 4))
	link := newLink(100e6, 50*time.Millisecond, 0)
	drop := time.Unix(1000, 0).Add(4 * time.Second)
	link.rate = func(now time.Time) float64 {
		if now.Before(drop) {
			return 100e6 / 8
		}
		return 10e6 / 8
	}
	simulate(t, b, link, 16*time.Second, nil)
	if got := b.maxBw * 8; got > 15e6 || got < 7e6 {
		t.Fatalf("bandwidth estimate %.1f Mbit/s after the drop to 10", got/1e6)
	}
}
