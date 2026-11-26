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
// loss, an optional token-bucket policer in front of the queue, and a fixed
// one-way propagation delay. ACKs return over an uncongested, lossless path
// with the same delay.
type simLink struct {
	rate      func(time.Time) float64
	delay     time.Duration
	queue     time.Duration
	loss      float64
	rng       *rand.Rand
	busyUntil time.Time
	policer   *simPolicer
	red       *simRED
	// queued and admitted total the queueing delay of the packets the
	// queue admitted.
	queued   time.Duration
	admitted int
}

// simRED is a RED-style AQM on the queue's delay: below min it leaves
// packets alone; from min to max it acts on each with a probability rising
// linearly to maxP; above max, on all. Acting marks an ECN-capable packet
// CE and drops any other, as RFC 3168 has it.
type simRED struct {
	min, max time.Duration
	maxP     float64
}

func (r *simRED) acts(delay time.Duration, rng *rand.Rand) bool {
	switch {
	case delay < r.min:
		return false
	case delay >= r.max:
		return true
	}
	return rng.Float64() < r.maxP*float64(delay-r.min)/float64(r.max-r.min)
}

// simPolicer is a token bucket: it admits a packet if it holds a token for
// each byte, and drops it otherwise, as a traffic policer does.
type simPolicer struct {
	rate   float64 // bytes per second
	burst  float64 // bucket depth in bytes
	tokens float64
	at     time.Time
}

func (p *simPolicer) admit(now time.Time, size int) bool {
	if p.at.IsZero() {
		p.tokens = p.burst
	} else {
		p.tokens = min(p.burst, p.tokens+p.rate*now.Sub(p.at).Seconds())
	}
	p.at = now
	if p.tokens < float64(size) {
		return false
	}
	p.tokens -= float64(size)
	return true
}

// simACK is an acknowledgment that reaches the sender at at, for a packet
// that was sent ECN-capable (ect) and marked CE on the way (ce).
type simACK struct {
	at      time.Time
	pn      uint64
	ect, ce bool
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
	dropped   int    // packets the link dropped
	marked    int    // packets the link marked CE
}

// simFlow is one bulk sender on a shared link, driving a real Sent tracker,
// pacer and controller as the transport does. It starts at start and stops
// sending at stop (if set).
type simFlow struct {
	c           Controller
	start, stop time.Time
	extraDelay  time.Duration // added to the link's delay in each direction
	observe     func(time.Time)
	// ecn marks the flow's packets ECN-capable, as Sent's ECN state allows,
	// and has its ACKs report the receiver's cumulative ECN counts.
	ecn         bool
	rxECT, rxCE uint64
	// snapshotAt, if set, is when to record the flow's counts in snapshot,
	// before any event at or after it.
	snapshotAt time.Time
	snapshot   simResult
	snapped    bool

	sent     *Sent
	pacer    Pacer
	acks     ackHeap
	pn       uint64
	res      simResult
	lastSend time.Time
	ptoCount int
}

const simPacketSize = 1200

// simulate runs one flow's bulk transfer through link for d. observe, if
// set, is called after every ACK.
func simulate(t *testing.T, c Controller, link *simLink, d time.Duration, observe func(time.Time)) simResult {
	t.Helper()
	f := &simFlow{c: c, observe: observe}
	simulateFlows(t, link, d, f)
	return f.res
}

// simulateFlows runs flows over one shared link for d from simStart. A flow
// with no start begins at simStart.
func simulateFlows(t *testing.T, link *simLink, d time.Duration, flows ...*simFlow) {
	t.Helper()
	now := simStart
	end := simStart.Add(d)
	for _, f := range flows {
		f.sent, _ = NewSent(65536)
		if f.ecn {
			f.sent.EnableECN()
		}
		if f.start.IsZero() {
			f.start = simStart
		}
	}
	// Flows step in rotating order, so none always reaches the shared queue
	// first when several send at the same instant.
	for tick := 0; now.Before(end); tick++ {
		next := end
		for _, f := range flows {
			if !f.snapped && !f.snapshotAt.IsZero() && !now.Before(f.snapshotAt) {
				f.snapped = true
				f.snapshot = f.res
				f.snapshot.delivered = f.sent.rate.delivered
			}
		}
		for k := range flows {
			f := flows[(tick+k)%len(flows)]
			if now.Before(f.start) {
				next = minTime(next, f.start)
				continue
			}
			next = minTime(next, f.step(t, link, now))
		}
		if !next.After(now) {
			next = now.Add(time.Microsecond)
		}
		now = next
	}
	for _, f := range flows {
		f.res.delivered = f.sent.rate.delivered
		// No event reached the snapshot time, so every processed event
		// precedes it.
		if !f.snapped && !f.snapshotAt.IsZero() && f.snapshotAt.Before(end) {
			f.snapped, f.snapshot = true, f.res
		}
	}
}

var simStart = time.Unix(1000, 0)

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

// step processes f's due ACKs, loss detection and PTO at now, sends what it
// can, and returns when it next needs to run.
func (f *simFlow) step(t *testing.T, link *simLink, now time.Time) time.Time {
	sent, c := f.sent, f.c
	for f.acks.Len() > 0 && !f.acks[0].at.After(now) {
		a := heap.Pop(&f.acks).(simACK)
		frame := wire.AckFrame{Ranges: []wire.AckRange{{Smallest: a.pn, Largest: a.pn}}}
		if f.ecn {
			switch {
			case a.ce:
				f.rxCE++
			case a.ect:
				f.rxECT++
			}
			frame.ECN = &[3]uint64{f.rxECT, 0, f.rxCE}
		}
		events, err := sent.OnACK(Application, frame, a.at, 0, 0, true)
		if err != nil {
			t.Fatal(err)
		}
		if events.ECNCongestion {
			c.OnECN(events.ECNSentAt, a.at)
		}
		if len(events.Acked) > 0 {
			f.ptoCount = 0
		}
		if len(events.Lost) > 0 {
			f.res.lost += len(events.Lost)
			c.OnLoss(LossEvent{Lost: events.Lost, TotalLost: sent.LostBytes(), TotalDelivered: sent.DeliveredBytes(), TotalCE: sent.CEBytes(), Flight: sent.BytesInFlight, Now: a.at})
		}
		c.OnACK(ACKEvent{Acked: events.Acked, Rate: events.Rate, Flight: sent.BytesInFlight, SmoothedRTT: sent.SmoothedRTT(), Lost: len(events.Lost) > 0, Now: a.at})
		if c.ForcesAppLimited() {
			sent.MarkAppLimited()
		}
		if f.observe != nil {
			f.observe(a.at)
		}
	}
	// Time-threshold loss detection.
	if lt, _, ok := sent.LossTime(); ok && !lt.After(now) {
		if lost := sent.DetectLoss(Application, now); len(lost) > 0 {
			f.res.lost += len(lost)
			c.OnLoss(LossEvent{Lost: lost, TotalLost: sent.LostBytes(), TotalDelivered: sent.DeliveredBytes(), TotalCE: sent.CEBytes(), Flight: sent.BytesInFlight, Now: now})
		}
	}
	next := time.Time{}
	if f.stop.IsZero() || now.Before(f.stop) {
		// A probe timeout: data is in flight but nothing will be
		// acknowledged, so send one packet outside the window.
		if pto := f.ptoAt(); !pto.IsZero() && !pto.After(now) {
			f.send(link, now)
			f.ptoCount++
		}
		// Send what the window and pacer allow: the application always
		// has data, so only the controller (in ProbeRTT) marks the sender
		// application limited.
		for c.CanSend(sent.BytesInFlight, simPacketSize, true, false) {
			if wait := f.pacer.DelayFor(now, c, sent.RTT.Smoothed, simPacketSize); wait > 0 {
				next = now.Add(wait)
				break
			}
			f.send(link, now)
		}
	}
	if f.acks.Len() > 0 && (next.IsZero() || f.acks[0].at.Before(next)) {
		next = f.acks[0].at
	}
	if lt, _, ok := sent.LossTime(); ok && (next.IsZero() || lt.Before(next)) {
		next = lt
	}
	if pto := f.ptoAt(); !pto.IsZero() && (f.stop.IsZero() || now.Before(f.stop)) && (next.IsZero() || pto.Before(next)) {
		next = pto
	}
	if next.IsZero() {
		next = now.Add(time.Millisecond)
	}
	return next
}

// ptoAt is when a probe timeout fires: when bytes are in flight and no ACK
// is on its way, the smoothed RTT plus four variations after the last send,
// doubled for each consecutive timeout. Unlike RFC 9002's PTO, it waits for
// ACKs already in transit, which the simulator knows will arrive; it exists
// so a flow whose remaining packets were all dropped doesn't stall.
func (f *simFlow) ptoAt() time.Time {
	if f.acks.Len() > 0 || f.sent.BytesInFlight == 0 {
		return time.Time{}
	}
	if _, _, ok := f.sent.LossTime(); ok {
		return time.Time{}
	}
	rtt := f.sent.RTT
	pto := (rtt.Smoothed + max(4*rtt.Variation, time.Millisecond)) << min(f.ptoCount, 6)
	return f.lastSend.Add(pto)
}

func (f *simFlow) send(link *simLink, now time.Time) {
	e := SentEvent{Bytes: simPacketSize, Flight: f.sent.BytesInFlight, AppLimited: f.sent.AppLimited(), Now: now}
	marking := NotECT
	if f.ecn {
		marking = f.sent.ECNMarkingAt(Application, now)
	}
	if err := f.sent.OnSent(Application, Packet{Number: f.pn, SentAt: now, Bytes: simPacketSize, AckEliciting: true, InFlight: true, ECN: marking}); err != nil {
		panic(err)
	}
	f.c.OnPacketSent(e)
	f.pacer.Sent(simPacketSize)
	f.res.sent++
	f.lastSend = now
	// The flow's extra delay is after the bottleneck, so the shared queue
	// sees packets in the order they are sent.
	ect := marking != NotECT
	if at, ok, ce := link.transmit(now, simPacketSize, ect); ok {
		heap.Push(&f.acks, simACK{at: at.Add(link.delay + 2*f.extraDelay), pn: f.pn, ect: ect, ce: ce})
		if ce {
			f.res.marked++
		}
	} else {
		f.res.dropped++
	}
	f.pn++
}

// transmit queues a packet sent at now, returning when it reaches the
// receiver, or false if it is dropped.
func (l *simLink) transmit(now time.Time, size int, ect bool) (at time.Time, ok, ce bool) {
	if l.loss > 0 && l.rng.Float64() < l.loss {
		return time.Time{}, false, false
	}
	if l.policer != nil && !l.policer.admit(now, size) {
		return time.Time{}, false, false
	}
	departure := now
	if l.busyUntil.After(now) {
		departure = l.busyUntil
	}
	delay := departure.Sub(now)
	if delay > l.queue {
		return time.Time{}, false, false
	}
	if l.red != nil && l.red.acts(delay, l.rng) {
		if !ect {
			return time.Time{}, false, false
		}
		ce = true
	}
	l.queued += delay
	l.admitted++
	l.busyUntil = departure.Add(time.Duration(float64(size) / l.rate(now) * float64(time.Second)))
	return l.busyUntil.Add(l.delay), true, ce
}

// meanQueueing is the mean queueing delay of the packets the link admitted.
func (l *simLink) meanQueueing() time.Duration {
	if l.admitted == 0 {
		return 0
	}
	return l.queued / time.Duration(l.admitted)
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
	drop := simStart.Add(4 * time.Second)
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
