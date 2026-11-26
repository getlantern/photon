package recovery

import (
	"math/rand/v2"
	"testing"
	"time"
)

func testBBR() *BBR {
	b := NewBBR(1200)
	b.rng = rand.New(rand.NewPCG(5, 6))
	return b
}

// A lost MTU probe shows the path is smaller, not congested: it neither
// starts recovery nor counts as a loss, in BBR or in the sampler's total.
func TestBBRIgnoresLostMTUProbe(t *testing.T) {
	b := testBBR()
	probe := Packet{Number: 1, Bytes: 1452, AckEliciting: true, InFlight: true, MTUProbe: true}
	b.OnLoss(LossEvent{Lost: []Packet{probe}, Now: time.Unix(1, 0)})
	if b.inRecovery || b.lossInRound || b.lossEventsInRound != 0 {
		t.Fatal("a lost MTU probe counted as congestion")
	}
	r, _ := NewSent(16)
	start := time.Unix(100, 0)
	for i := range uint64(5) {
		p := Packet{Number: i, SentAt: start, Bytes: 1200, AckEliciting: true, InFlight: true}
		if i == 0 {
			p.Bytes, p.MTUProbe = 1452, true
		}
		if err := r.OnSent(Application, p); err != nil {
			t.Fatal(err)
		}
	}
	events := rateACK(t, r, 1, 4, start.Add(50*time.Millisecond))
	if len(events.Lost) != 1 || r.LostBytes() != 0 || events.Rate.Lost != 0 {
		t.Fatal("lost MTU probe counted in the loss total", r.LostBytes(), events.Rate.Lost)
	}
}

// The first smoothed RTT replaces the initial pacing rate, which assumed a
// 1 ms RTT, with the initial window over that RTT; the raw sample, inflated
// by the peer's ACK delay, isn't used.
func TestBBRPacingRateFromFirstRTT(t *testing.T) {
	b := testBBR()
	b.OnACK(ACKEvent{Rate: RateSample{NewlyAcked: 1200, RTT: 125 * time.Millisecond, TotalDelivered: 1200}, SmoothedRTT: 100 * time.Millisecond, Now: time.Unix(1, 0)})
	if want := uint64(bbrStartupPacingGain * float64(b.initialWindow) / 0.1); b.PacingRate() != want {
		t.Fatalf("pacing rate %d after a 100ms RTT, want %d", b.PacingRate(), want)
	}
}

// C.is_cwnd_limited is whether a send filled the window in the last
// packet-timed round trip.
func TestBBRCwndLimitedLastsARound(t *testing.T) {
	b := testBBR()
	now := time.Unix(1, 0)
	b.OnPacketSent(SentEvent{Bytes: 1200, Flight: b.cwnd - 1200, Now: now})
	if !b.cwndLimited {
		t.Fatal("a send that filled the window didn't mark it")
	}
	// A round ends with no further full-window send: the mark lasts it.
	b.delivered, b.nextRoundDelivered = 1200, 0
	b.OnACK(ACKEvent{Rate: RateSample{NewlyAcked: 1200, PriorDelivered: 0, TotalDelivered: 1200}, Now: now})
	if !b.cwndLimited {
		t.Fatal("cwnd-limited mark didn't last the round it was set in")
	}
	b.OnACK(ACKEvent{Rate: RateSample{NewlyAcked: 1200, PriorDelivered: 1200, TotalDelivered: 2400}, Now: now})
	if b.cwndLimited {
		t.Fatal("cwnd-limited mark outlived a round with no full-window send")
	}
}

// A detection that declares several packets lost is one loss event, and each
// packet is charged only the loss up to its own turn, so a burst doesn't
// collapse inflight_longterm.
func TestBBRLossBatch(t *testing.T) {
	var lost []Packet
	for i := range uint64(10) {
		lost = append(lost, Packet{Number: i, Bytes: 1200, InFlight: true, Delivery: DeliveryState{InFlight: 120000}})
	}
	b := testBBR()
	b.OnLoss(LossEvent{Lost: lost, TotalLost: 12000, TotalDelivered: 50000, Now: time.Unix(1, 0)})
	if b.lossEventsInRound != 1 {
		t.Fatal("loss events", b.lossEventsInRound, "want 1 for one detection")
	}
	if b.delivered != 50000 || b.lossRoundDelivered != 50000 {
		t.Fatal("loss round marked at delivered", b.lossRoundDelivered, "want the current 50000")
	}
	// In ProbeBW_UP the burst is too much loss, and moves BBR down.
	b = testBBR()
	b.state, b.checkProbeLoss = bbrProbeBWUp, true
	b.OnLoss(LossEvent{Lost: lost, TotalLost: 12000, TotalDelivered: 50000, Now: time.Unix(1, 0)})
	// Losses cross 2% of 120000 bytes at the third packet: the estimate is
	// just under the 118800 bytes in flight before it.
	if b.inflightLongterm < 118000 || b.inflightLongterm > 120000 || b.state != bbrProbeBWDown {
		t.Fatal("inflight_longterm", b.inflightLongterm, "want just under 118800, state", b.state)
	}
}

// ProbeRTT holds the sender below the path, so BBR asks the owner to mark it
// application limited; recovery ends only on an in-flight packet sent after
// it began.
func TestBBRProbeRTTForcesAppLimitedAndRecoveryNeedsData(t *testing.T) {
	b := testBBR()
	if b.ForcesAppLimited() {
		t.Fatal("Startup forced app limited")
	}
	b.enterProbeRTT()
	if !b.ForcesAppLimited() {
		t.Fatal("ProbeRTT didn't force app limited")
	}
	b = testBBR()
	start := time.Unix(1, 0)
	b.OnLoss(LossEvent{Lost: []Packet{{Bytes: 1200, InFlight: true}}, TotalLost: 1200, Now: start})
	later := start.Add(time.Millisecond)
	b.OnACK(ACKEvent{Acked: []Packet{{SentAt: later, Bytes: 50}}, Rate: RateSample{NewlyAcked: 1200, TotalDelivered: 1200}, Now: later})
	if !b.inRecovery {
		t.Fatal("an ACK-only packet ended recovery")
	}
	b.OnACK(ACKEvent{Acked: []Packet{{SentAt: later, Bytes: 1200, InFlight: true}}, Rate: RateSample{NewlyAcked: 1200, TotalDelivered: 2400}, Now: later})
	if b.inRecovery {
		t.Fatal("an in-flight packet sent after the loss didn't end recovery")
	}
}

// Persistent congestion restarts recovery, so the window it cuts is restored
// only once a packet sent after it is acknowledged, not by an earlier one in
// the same ACK.
func TestBBRPersistentCongestionRestartsRecovery(t *testing.T) {
	b := testBBR()
	start := time.Unix(1, 0)
	b.OnLoss(LossEvent{Lost: []Packet{{Bytes: 1200, InFlight: true}}, TotalLost: 1200, Now: start})
	b.cwnd = 100000
	blackout := start.Add(5 * time.Second)
	b.OnLoss(LossEvent{Lost: []Packet{{Bytes: 1200, InFlight: true}}, TotalLost: 2400, Flight: 2400, Now: blackout})
	b.OnPersistentCongestion()
	cut := b.cwnd
	// A packet sent between the first loss and persistent congestion.
	b.OnACK(ACKEvent{Acked: []Packet{{SentAt: start.Add(time.Second), Bytes: 1200, InFlight: true}}, Rate: RateSample{NewlyAcked: 1200, TotalDelivered: 1200}, Flight: 1200, Now: blackout})
	if !b.inRecovery || b.cwnd > max(cut, b.minPipeCwnd())+1200 {
		t.Fatalf("recovery ended or window %d restored by a packet sent before persistent congestion (cut to %d)", b.cwnd, cut)
	}
}

// The ACK that starts a loss round in Startup can exit Startup on high loss,
// even though the same ACK ends recovery.
func TestBBRStartupExitsOnHighLoss(t *testing.T) {
	b := testBBR()
	start := time.Unix(1, 0)
	b.delivered = 100000
	// The first loss starts recovery before the packet is sent; the rest are
	// detected after it, so its sample counts them.
	sentAt := start.Add(time.Millisecond)
	for i := range bbrStartupFullLossCnt {
		at := start
		if i > 0 {
			at = sentAt.Add(time.Millisecond)
		}
		b.OnLoss(LossEvent{Lost: []Packet{{Number: uint64(i), Bytes: 1200, InFlight: true}}, TotalLost: uint64(i+1) * 1200, TotalDelivered: 100000, Now: at})
	}
	// The in-flight packet, acknowledged with 5% of its flight lost since it
	// was sent.
	later := start.Add(50 * time.Millisecond)
	b.OnACK(ACKEvent{Acked: []Packet{{SentAt: sentAt, Bytes: 1200, InFlight: true}},
		Rate:   RateSample{NewlyAcked: 1200, PriorDelivered: 100000, TotalDelivered: 101200, Lost: uint64(bbrStartupFullLossCnt-1) * 1200, InFlight: 120000, RTT: 50 * time.Millisecond},
		Flight: 100000, Now: later})
	if !b.fullBwReached || b.state == bbrStartup {
		t.Fatal("Startup didn't exit on high loss, state", b.state)
	}
	if b.inRecovery {
		t.Fatal("the ACK didn't also end recovery")
	}
}

// Restarting from idle starts a new ACK-aggregation interval with nothing
// delivered in it, so one small ACK after the restart isn't aggregation.
func TestBBRIdleRestartClearsExtraAcked(t *testing.T) {
	b := testBBR()
	b.bw, b.cwnd, b.extraAckedDelivered = 1e6, 1_000_000, 100000
	restart := time.Unix(10, 0)
	b.OnPacketSent(SentEvent{Bytes: 1200, AppLimited: true, Now: restart})
	b.updateACKAggregation(RateSample{NewlyAcked: 1200}, restart.Add(50*time.Millisecond))
	if b.extraAcked > 1200 {
		t.Fatal("extra_acked", b.extraAcked, "after an idle restart, want at most the ACK's 1200")
	}
}

// A send that ends ProbeRTT bounds the restored window by ProbeBW_CRUISE's
// cap at once, not at the next ACK.
func TestBBRProbeRTTExitOnSendBoundsWindow(t *testing.T) {
	b := testBBR()
	b.fullBwReached, b.bw, b.minRTT = true, 1e6, 100*time.Millisecond
	b.inflightLongterm, b.priorCwnd = 100000, 120000
	b.enterProbeRTT()
	b.cwnd = b.minPipeCwnd()
	start := time.Unix(10, 0)
	b.probeRTTDoneStamp = start
	b.OnPacketSent(SentEvent{Bytes: 1200, AppLimited: true, Now: start.Add(time.Millisecond)})
	if b.state != bbrProbeBWCruise {
		t.Fatal("state", b.state, "want ProbeBW_CRUISE")
	}
	if want := b.inflightWithHeadroom(); b.cwnd != want {
		t.Fatal("window", b.cwnd, "after leaving ProbeRTT, want the Cruise cap", want)
	}
}

// A loss that ends a bandwidth probe applies ProbeBW_DOWN's pacing rate and
// the lowered inflight_longterm at once: a timer-detected loss can be
// followed by sends with no ACK in between.
func TestBBRProbeLossAppliesDownControls(t *testing.T) {
	b := testBBR()
	b.fullBwReached, b.bw, b.minRTT, b.pacingRate = true, 1e6, 100*time.Millisecond, 1.25e6
	b.startProbeBWUp(RateSample{})
	b.checkProbeLoss = true
	b.cwnd, b.inflightLongterm = 200000, 200000
	b.OnLoss(LossEvent{Lost: []Packet{{Bytes: 12000, InFlight: true, Delivery: DeliveryState{InFlight: 120000}}}, TotalLost: 12000, Now: time.Unix(10, 0)})
	if b.state != bbrProbeBWDown {
		t.Fatal("state", b.state, "want ProbeBW_DOWN")
	}
	if b.cwnd > b.inflightLongterm {
		t.Fatal("window", b.cwnd, "above the lowered inflight_longterm", b.inflightLongterm)
	}
	if b.pacingRate >= 1e6 {
		t.Fatal("pacing rate", b.pacingRate, "still probing")
	}
}

// Persistent congestion resets BBR as for a new path, from the minimum
// window, and a later ACK doesn't restore the pre-collapse window.
func TestBBRPersistentCongestionResetsModel(t *testing.T) {
	b := testBBR()
	start := time.Unix(1, 0)
	b.maxBw, b.bw, b.fullBwReached, b.cwnd = 12.5e6, 12.5e6, true, 1_000_000
	b.state = bbrProbeBWCruise
	b.OnLoss(LossEvent{Lost: []Packet{{Bytes: 1200, InFlight: true}}, TotalLost: 1200, Flight: 0, Now: start})
	b.OnPersistentCongestion()
	if b.state != bbrStartup || b.fullBwReached || b.maxBw != 0 || b.cwnd != b.minPipeCwnd() {
		t.Fatalf("after persistent congestion: state %d, full bw %t, max bw %v, cwnd %d", b.state, b.fullBwReached, b.maxBw, b.cwnd)
	}
	later := start.Add(time.Second)
	b.OnACK(ACKEvent{Acked: []Packet{{SentAt: later, Bytes: 1200, InFlight: true}}, Rate: RateSample{NewlyAcked: 1200, TotalDelivered: 1200, RTT: 50 * time.Millisecond}, Flight: 0, Now: later.Add(50 * time.Millisecond)})
	if b.cwnd > 100000 {
		t.Fatal("pre-collapse window restored:", b.cwnd)
	}
}

// The max_bw filter remembers the current and previous ProbeBW cycles: a
// sample is gone once two cycles have passed without a higher one.
func TestBBRMaxBwFilterHoldsTwoCycles(t *testing.T) {
	b := testBBR()
	b.updateMaxBw(RateSample{Valid: true, Rate: 1000})
	b.advanceMaxBwFilter()
	b.updateMaxBw(RateSample{Valid: true, Rate: 400})
	if b.maxBw != 1000 {
		t.Fatal("previous cycle's max forgotten after one cycle:", b.maxBw)
	}
	b.advanceMaxBwFilter()
	b.updateMaxBw(RateSample{Valid: true, Rate: 300})
	if b.maxBw != 400 {
		t.Fatal("max", b.maxBw, "after two cycles, want the previous cycle's 400")
	}
}
