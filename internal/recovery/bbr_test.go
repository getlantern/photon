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
	if len(events.Lost) != 1 || r.LostBytes() != 0 || events.Rate.NewlyLost != 0 {
		t.Fatal("lost MTU probe counted in the loss total", r.LostBytes(), events.Rate.NewlyLost)
	}
}

// The first RTT sample replaces the initial pacing rate, which assumed a 1 ms
// RTT, with the initial window over that RTT.
func TestBBRPacingRateFromFirstRTT(t *testing.T) {
	b := testBBR()
	b.OnACK(ACKEvent{Rate: RateSample{NewlyAcked: 1200, RTT: 100 * time.Millisecond, TotalDelivered: 1200}, Now: time.Unix(1, 0)})
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
