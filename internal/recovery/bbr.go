package recovery

import (
	"math"
	"math/rand/v2"
	"time"
)

// BBR is the BBRv3 congestion controller of draft-ietf-ccwg-bbr-07: it
// models the path's bottleneck bandwidth and minimum RTT, rather than
// treating each loss as congestion, and paces at its bandwidth estimate. Names
// follow the draft so the code can be checked against it; functions carry
// the draft's names in lower case.
//
// Departures from the draft, for QUIC:
//   - Fast Recovery is an epoch that starts with a loss and ends when a packet
//     sent after it is acknowledged, as RFC 9002 section 7.3.2 defines
//     recovery; there is no separate RTO recovery. Persistent congestion
//     (RFC 9002 section 7.6) stands in for the draft's RTO.
//   - QUIC acknowledgments are selective, so C.has_selective_acks is true.
//   - A loss event, for the Startup loss exit, is one loss detection.
//   - The initial pacing rate is recomputed when the first smoothed RTT
//     arrives, as the draft's prose and Linux do; its pseudocode computes it
//     only at init, where a new QUIC path has no RTT and would assume 1 ms.
//   - Persistent congestion resets the controller as for a new path, from
//     BBR's minimum window, rather than restoring the pre-collapse window
//     once recovery ends (RFC 9002 section 7.6.2).
//   - Restarting from idle also clears extra_acked_delivered, as Linux does,
//     so data delivered before the idle period isn't counted as aggregation.
//   - Leaving ProbeRTT on a send, or ending a bandwidth probe on a loss,
//     applies the new state's window bound (and, after a probe, its pacing
//     rate) at once: QUIC can send again before the next ACK.
//   - Spurious loss recovery is not undone (the draft's section 5.5.11): the
//     transport has no signal that a whole recovery episode was spurious.
//   - ECN-CE: the draft requires an ECN-capable sender to treat CE marks as
//     congestion but specifies no response. Marks are answered as loss, as
//     RFC 3168 asks, since a classic AQM marks the packets it would
//     otherwise drop: an ACK reporting new marks is a loss event, marked
//     bytes count with lost ones toward the 2% threshold for the Startup
//     exit (which a round with marks reaches without recovery), and a
//     bandwidth probe checks that threshold on the ACK, as Linux does,
//     since marks belong to no lost packet. Marks start no recovery epoch,
//     since nothing needs retransmitting.
//
// As in draft-07, and unlike earlier BBR versions, recovery doesn't cut the
// window for each loss or conserve packets: it saves the window on entering
// recovery and restores it on leaving, and the loss response is the model's
// (inflight_longterm, bw_shortterm, inflight_shortterm). ProbeRTT starts when
// the 10 s min_rtt window expires.
type BBR struct {
	now           time.Time // the current event's time
	smss          uint64    // maximum datagram size
	initialWindow uint64
	cwnd          uint64
	pacingRate    float64 // bytes per second
	rng           *rand.Rand

	state                bbrState
	pacingGain, cwndGain float64
	roundCount           uint64
	roundStart           bool
	nextRoundDelivered   uint64
	idleRestart          bool
	drainStartRound      uint64
	delivered, inflight  uint64 // the path's C.delivered and C.inflight
	// cwndLimited is C.is_cwnd_limited: whether a send filled the window
	// in the last packet-timed round trip. cwndLimitedInRound tracks the
	// current round.
	cwndLimited, cwndLimitedInRound bool
	hasSeenRTT                      bool

	bwHi                [2]float64 // max bandwidth in the current and previous ProbeBW cycles
	cycleCount          uint64
	maxBw, bw           float64
	bwShortterm         float64
	bwLatest            float64
	inflightLatest      uint64
	minRTT              time.Duration
	minRTTStamp         time.Time
	minRTTExpired       bool
	bdp                 uint64
	extraAcked          uint64
	extraAckedFilter    maxFilter
	extraAckedStart     time.Time
	extraAckedDelivered uint64
	maxInflight         uint64
	inflightLongterm    uint64
	inflightShortterm   uint64

	fullBwReached, fullBwNow bool
	fullBw                   float64
	fullBwCount              int

	baseline lossBaseline

	lossRoundDelivered uint64
	lossRoundStart     bool
	lossInRound        bool
	ceInRound          bool // CE marks in the loss round, which no baseline discounts
	lossEventsInRound  int
	lossNoted          bool // OnLoss noted a loss event since the last OnACK
	inRecovery         bool
	recoveryStart      time.Time

	bwProbeStopping, checkProbeLoss          bool
	bwProbeUpAcked, probeUpAckedPerInc       uint64
	bwProbeUpRounds                          int
	roundsSinceProbeUp                       uint64
	bwProbeWait                              time.Duration
	cycleStamp                               time.Time
	prevProbeTooHigh, prevProbePrecautionary bool

	probeRTTDoneStamp time.Time
	probeRTTRoundDone bool
	probeRTTCwnd      uint64
	priorCwnd         uint64
}

type bbrState uint8

const (
	bbrNone bbrState = iota
	bbrStartup
	bbrDrain
	bbrProbeBWDown
	bbrProbeBWCruise
	bbrProbeBWRefill
	bbrProbeBWUp
	bbrProbeRTT
)

// Constants from draft-ietf-ccwg-bbr-07 section 2 and the summary of control
// behavior in section 5.6.1.
const (
	bbrStartupPacingGain   = 2.77 // 4 * ln(2)
	bbrDrainPacingGain     = 0.5
	bbrDefaultCwndGain     = 2
	bbrPacingMarginPercent = 1
	bbrLossThresh          = 0.02
	bbrBeta                = 0.7
	bbrHeadroom            = 0.15
	bbrExtraAckedFilterLen = 10 // round trips
	bbrMinRTTFilterLen     = 10 * time.Second
	bbrProbeRTTCwndGain    = 0.5
	bbrProbeRTTDuration    = 200 * time.Millisecond
	bbrStartupFullLossCnt  = 6
	bbrInfinity            = math.MaxUint64
)

// NewBBR returns a BBR controller for maxDatagram-byte packets, with the
// same initial window as NewReno.
func NewBBR(maxDatagram uint64) *BBR {
	b := new(BBR)
	b.Reset(maxDatagram)
	return b
}

// Reset is the draft's OnInit, for a new path.
func (b *BBR) Reset(maxDatagram uint64) {
	maxDatagram = min(max(maxDatagram, 1200), 65527)
	rng := b.rng
	if rng == nil {
		rng = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	*b = BBR{smss: maxDatagram, rng: rng}
	b.initialWindow = min(10*maxDatagram, max(14720, 2*maxDatagram))
	b.cwnd = b.initialWindow
	b.minRTT = time.Duration(math.MaxInt64)
	b.probeRTTCwnd = bbrInfinity
	b.inflightLongterm = bbrInfinity
	b.probeUpAckedPerInc = bbrInfinity
	b.resetCongestionSignals()
	b.resetShortTermModel()
	b.resetFullBW()
	// InitPacingRate, with no RTT yet: the initial window per millisecond.
	b.pacingRate = bbrStartupPacingGain * float64(b.initialWindow) / time.Millisecond.Seconds()
	b.enterStartup()
}

func (b *BBR) Window() uint64     { return b.cwnd }
func (b *BBR) PacingRate() uint64 { return uint64(b.pacingRate) }

// CanSend allows a packet within the window. Packets not in flight, and
// probes, are always allowed.
func (b *BBR) CanSend(flight uint64, size int, inFlight, probe bool) bool {
	if size < 0 {
		return false
	}
	if !inFlight || probe {
		return true
	}
	return flight <= b.cwnd && uint64(size) <= b.cwnd-flight
}

// at records an event's time. Reset has no clock, so the draft's OnInit
// stamping of the min_rtt window happens at the first event.
func (b *BBR) at(now time.Time) {
	b.now = now
	if b.minRTTStamp.IsZero() {
		b.minRTTStamp = now
		b.extraAckedStart = now
	}
}

// ForcesAppLimited is true in ProbeRTT, whose reduced window holds the
// sender below what the path can carry (the draft's MarkConnectionAppLimited
// in HandleProbeRTT).
func (b *BBR) ForcesAppLimited() bool { return b.state == bbrProbeRTT }

// OnPacketSent is the draft's OnTransmit: HandleRestartFromIdle. It also
// notes whether this send filled the window.
func (b *BBR) OnPacketSent(e SentEvent) {
	b.at(e.Now)
	if e.Flight+uint64(e.Bytes)+b.smss > b.cwnd {
		b.cwndLimited, b.cwndLimitedInRound = true, true
	}
	if e.Flight == 0 && e.AppLimited {
		b.idleRestart = true
		b.extraAckedStart, b.extraAckedDelivered = e.Now, 0
		if b.inProbeBW() {
			b.setPacingRateWithGain(1)
		} else if b.state == bbrProbeRTT {
			b.checkProbeRTTDone(e.Now)
			// The ACK that would bound the restored window may not come
			// before the next send.
			b.boundCwndForModel()
		}
	}
}

// OnECN does nothing: BBR answers CE marks in OnACK, from the rate
// sample's marked bytes (see BBR).
func (b *BBR) OnECN(sent, now time.Time) {}

// OnPersistentCongestion stands in for the draft's OnEnterRTO, departing
// from it: BBR resets as for a new path, from the minimum window, and starts
// a recovery epoch that only packets sent from now on end.
func (b *BBR) OnPersistentCongestion() {
	// The path stopped delivering for long enough that its model can't be
	// trusted: start over as on a new path, from the minimum window, and
	// don't restore the old one (RFC 9002 section 7.6.2).
	// The reset forgets the loss events so far, the one that led here
	// included, so any marks on the same ACK count as the fresh model's.
	now, inflight := b.now, b.inflight
	b.Reset(b.smss)
	b.at(now)
	b.inflight = inflight
	b.cwnd = b.minPipeCwnd()
	b.inRecovery, b.recoveryStart = true, now
}

// OnLoss runs HandleLostPacket for each lost in-flight packet, entering Fast
// Recovery if not already in it. One detection is one loss event. MTU probes
// are skipped: their loss is not congestion.
func (b *BBR) OnLoss(e LossEvent) {
	b.at(e.Now)
	b.inflight, b.delivered = e.Flight, e.TotalDelivered
	var batch uint64
	for _, p := range e.Lost {
		if p.InFlight && !p.MTUProbe {
			batch += uint64(p.Bytes)
		}
	}
	// Whether this detection was a loss event, for the ACK that may follow.
	b.lossNoted = batch > 0
	b.baseline.noteLost(batch)
	if batch == 0 {
		return
	}
	if !b.inRecovery {
		// OnEnterFastRecovery.
		b.saveCwnd()
		b.saveStateUponLoss()
		b.inRecovery, b.recoveryStart = true, e.Now
	}
	b.noteLoss()
	state := b.state
	// Each packet sees the loss as it stood at its turn: the total before
	// this batch, plus the packets up to and including it.
	lost := e.TotalLost - batch
	for _, p := range e.Lost {
		if !p.InFlight || p.MTUProbe {
			continue
		}
		lost += uint64(p.Bytes)
		b.handleLostPacket(p, lost, e.TotalCE)
	}
	if b.state != state {
		// A probe ended: apply ProbeBW_DOWN's pacing rate and window now,
		// since a timer-detected loss can be followed by sends with no ACK.
		b.setPacingRate()
		b.boundCwndForModel()
	}
}

// OnACK is the draft's UpdateOnACK, for an ACK that acknowledged in-flight
// data.
func (b *BBR) OnACK(e ACKEvent) {
	rs := e.Rate
	// A loss this ACK's detection noted makes its marks the same loss event.
	lossNoted := e.Lost && b.lossNoted
	b.lossNoted = false
	if rs.NewlyAcked == 0 {
		return
	}
	b.at(e.Now)
	b.inflight, b.delivered = e.Flight, rs.TotalDelivered
	if rs.NewCE > 0 {
		// Marks answered as loss: a loss event, unless this ACK's loss
		// detection already noted one, and for a probe, too much in flight if
		// marks and losses since this packet's send cross the threshold.
		if !lossNoted {
			b.noteLoss()
		}
		b.ceInRound = true
		if congested := rs.Lost + rs.CE; b.checkProbeLoss && b.isInflightTooHigh(rs.Lost, rs.CE, rs.InFlight) {
			b.handleInflightTooHigh(rs.AppLimited, inflightAtLoss(min(rs.NewCE, congested), rs.InFlight, congested, b.lossThresh()))
		}
	}
	if rtt := e.SmoothedRTT; !b.hasSeenRTT && rtt > 0 {
		// InitPacingRate, now that there is an RTT to pace over: the smoothed
		// RTT, which the peer's ACK delay doesn't inflate.
		b.hasSeenRTT = true
		b.pacingRate = bbrStartupPacingGain * float64(b.initialWindow) / rtt.Seconds()
	}
	b.baseline.noteRTT(rs.RTT)
	// UpdateModelAndState.
	b.updateLatestDeliverySignals(rs)
	b.updateCongestionSignals(rs)
	b.updateACKAggregation(rs, e.Now)
	b.checkFullBWReached(rs)
	b.checkStartupDone(rs)
	b.checkDrainDone()
	b.updateProbeBWCyclePhase(rs, e.Now)
	b.updateMinRTT(rs, e.Now)
	b.checkProbeRTT(rs, e.Now)
	b.advanceLatestDeliverySignals(rs)
	b.boundBWForModel()
	// Leaving recovery comes after the model update, which checks whether
	// the ACK arrived in recovery (the Startup loss exit), and before the
	// window is set, which bounds the restored window by the model.
	if b.inRecovery {
		for _, p := range e.Acked {
			if p.InFlight && p.SentAt.After(b.recoveryStart) {
				b.inRecovery = false
				b.restoreCwnd()
				break
			}
		}
	}
	// UpdateControlParameters.
	b.setPacingRate()
	b.setCwnd(rs)
}

func (b *BBR) minPipeCwnd() uint64 { return 4 * b.smss }

// --- State machine (section 5.3) ---

func (b *BBR) enterStartup() {
	b.state = bbrStartup
	b.pacingGain = bbrStartupPacingGain
	b.cwndGain = bbrDefaultCwndGain
}

func (b *BBR) checkStartupDone(rs RateSample) {
	b.checkStartupHighLoss(rs)
	if b.state == bbrStartup && b.fullBwReached {
		b.enterDrain()
	}
}

func (b *BBR) resetFullBW() {
	b.fullBw = 0
	b.fullBwCount = 0
	b.fullBwNow = false
}

func (b *BBR) checkFullBWReached(rs RateSample) {
	if b.fullBwNow || !b.roundStart || rs.AppLimited || !rs.Valid {
		return
	}
	rate := float64(rs.Rate)
	if rate >= b.fullBw*1.25 {
		b.resetFullBW()
		b.fullBw = rate
		return
	}
	b.fullBwCount++
	b.fullBwNow = b.fullBwCount >= 3
	if b.fullBwNow {
		b.fullBwReached = true
	}
}

func (b *BBR) checkStartupHighLoss(rs RateSample) {
	if b.fullBwReached || !b.lossRoundStart || !b.inRecovery && rs.CE == 0 {
		return
	}
	if b.lossEventsInRound < bbrStartupFullLossCnt {
		return
	}
	// Without a baseline yet, loss ends Startup only with a queue behind it,
	// since random loss shows none, or above any rate the baseline would
	// discount, as an overflowing buffer too shallow to show a queue gives.
	lost := rs.Lost + rs.CE
	if !b.isInflightTooHigh(rs.Lost, rs.CE, rs.InFlight) ||
		rs.CE == 0 && !b.queueing(rs.RTT) && float64(lost) <= float64(rs.InFlight)*(bbrMaxLossBaseline+bbrLossThresh) {
		return
	}
	b.fullBwReached = true
	b.inflightLongterm = max(b.bdp, b.inflightLatest)
}

func (b *BBR) enterDrain() {
	b.state = bbrDrain
	b.pacingGain = bbrDrainPacingGain
	b.cwndGain = bbrDefaultCwndGain
	b.drainStartRound = b.roundCount
}

// checkDrainDone also leaves Drain after three rounds, as the draft's
// CheckDrainDone does, in case Startup overestimated the bandwidth.
func (b *BBR) checkDrainDone() {
	if b.state == bbrDrain && (b.inflight <= b.inflightGain(1) || b.roundCount > b.drainStartRound+3) {
		b.enterProbeBW()
	}
}

func (b *BBR) enterProbeBW() {
	b.cwndGain = bbrDefaultCwndGain
	b.startProbeBWDown()
}

func (b *BBR) inProbeBW() bool {
	return b.state >= bbrProbeBWDown && b.state <= bbrProbeBWUp
}

func (b *BBR) isProbingBW() bool {
	return b.state == bbrStartup || b.state == bbrProbeBWRefill || b.state == bbrProbeBWUp
}

func (b *BBR) startProbeBWDown() {
	b.resetCongestionSignals()
	b.probeUpAckedPerInc = bbrInfinity
	b.pickProbeWait()
	b.cycleStamp = b.now
	b.bwProbeStopping = true
	b.startRound()
	b.state = bbrProbeBWDown
	b.pacingGain, b.cwndGain = 0.9, bbrDefaultCwndGain
}

func (b *BBR) startProbeBWCruise() {
	b.state = bbrProbeBWCruise
	b.pacingGain, b.cwndGain = 1, bbrDefaultCwndGain
}

func (b *BBR) startProbeBWRefill() {
	b.resetShortTermModel()
	b.bwProbeUpRounds = 0
	b.bwProbeUpAcked = 0
	b.prevProbePrecautionary = false
	b.bwProbeStopping = false
	b.startRound()
	b.state = bbrProbeBWRefill
	b.pacingGain, b.cwndGain = 1, bbrDefaultCwndGain
}

func (b *BBR) startProbeBWUp(rs RateSample) {
	b.startRound()
	b.resetFullBW()
	b.fullBw = float64(rs.Rate)
	b.state = bbrProbeBWUp
	b.pacingGain, b.cwndGain = 1.25, 2.25
	b.raiseInflightLongtermSlope()
}

func (b *BBR) updateProbeBWCyclePhase(rs RateSample, now time.Time) {
	if !b.fullBwReached {
		return
	}
	if b.adaptLongTermModel(rs) {
		return
	}
	if !b.inProbeBW() {
		return
	}
	switch b.state {
	case bbrProbeBWDown:
		if b.isTimeToProbeBW(now) {
			return
		}
		if b.isTimeToCruise() {
			b.startProbeBWCruise()
		}
	case bbrProbeBWCruise:
		b.isTimeToProbeBW(now)
	case bbrProbeBWRefill:
		if b.roundStart {
			b.checkProbeLoss = true
			b.startProbeBWUp(rs)
		}
	case bbrProbeBWUp:
		if b.isTimeToGoDown(rs) {
			b.prevProbeTooHigh = false
			b.startProbeBWDown()
		}
	}
}

func (b *BBR) isTimeToProbeBW(now time.Time) bool {
	if now.Sub(b.cycleStamp) > b.bwProbeWait || b.isRenoCoexistenceProbeTime() {
		b.startProbeBWRefill()
		return true
	}
	return false
}

// pickProbeWait randomizes how long to wait before probing again: 0 or 1
// rounds, and 2 to 3 seconds.
func (b *BBR) pickProbeWait() {
	b.roundsSinceProbeUp = uint64(b.rng.IntN(2))
	b.bwProbeWait = 2*time.Second + time.Duration(b.rng.Float64()*float64(time.Second))
}

func (b *BBR) isRenoCoexistenceProbeTime() bool {
	renoRounds := b.targetInflight() / b.smss
	return b.roundsSinceProbeUp >= min(renoRounds, 63)
}

func (b *BBR) targetInflight() uint64 { return min(b.bdp, b.cwnd) }

func (b *BBR) isTimeToCruise() bool {
	if b.inflight > b.inflightWithHeadroom() {
		return false
	}
	return b.inflight <= b.inflightGain(1)
}

func (b *BBR) isTimeToGoDown(rs RateSample) bool {
	if b.prevProbeTooHigh && b.inflight >= b.inflightLongterm {
		b.prevProbePrecautionary = true
		return true
	}
	if b.cwndLimited && b.cwnd >= b.inflightLongterm {
		b.resetFullBW()
		b.fullBw = float64(rs.Rate)
	} else if b.fullBwNow {
		return true
	}
	return false
}

func (b *BBR) inflightWithHeadroom() uint64 {
	if b.inflightLongterm == bbrInfinity {
		return bbrInfinity
	}
	headroom := max(b.smss, uint64(bbrHeadroom*float64(b.inflightLongterm)))
	if headroom >= b.inflightLongterm {
		return b.minPipeCwnd()
	}
	return max(b.inflightLongterm-headroom, b.minPipeCwnd())
}

func (b *BBR) raiseInflightLongtermSlope() {
	growth := uint64(1) << b.bwProbeUpRounds
	b.bwProbeUpRounds = min(b.bwProbeUpRounds+1, 30)
	b.probeUpAckedPerInc = max(b.cwnd/growth, b.smss)
}

func (b *BBR) probeInflightLongtermUpward(rs RateSample) {
	if !b.cwndLimited || b.cwnd < b.inflightLongterm {
		return
	}
	b.bwProbeUpAcked += rs.NewlyAcked
	if b.bwProbeUpAcked >= b.probeUpAckedPerInc {
		delta := b.bwProbeUpAcked / b.probeUpAckedPerInc
		b.bwProbeUpAcked -= delta * b.probeUpAckedPerInc
		b.inflightLongterm += delta * b.smss
	}
	if b.roundStart {
		b.raiseInflightLongtermSlope()
	}
}

func (b *BBR) adaptLongTermModel(rs RateSample) bool {
	if b.bwProbeStopping && b.roundStart {
		b.checkProbeLoss = false
		b.bwProbeStopping = false
		if b.inProbeBW() && !rs.AppLimited {
			b.advanceMaxBwFilter()
		}
		if b.inProbeBW() && b.prevProbePrecautionary && !b.prevProbeTooHigh {
			b.startProbeBWRefill()
			return true
		}
	}
	if !b.isInflightTooHigh(rs.Lost, rs.CE, rs.InFlight) {
		if b.inflightLongterm == bbrInfinity {
			return false
		}
		if rs.InFlight > b.inflightLongterm {
			b.inflightLongterm = rs.InFlight
		}
		if b.state == bbrProbeBWUp {
			b.probeInflightLongtermUpward(rs)
		}
	}
	return false
}

// --- ProbeRTT (section 5.3.4) ---

func (b *BBR) updateMinRTT(rs RateSample, now time.Time) {
	b.minRTTExpired = now.After(b.minRTTStamp.Add(bbrMinRTTFilterLen))
	if b.minRTTExpired {
		b.probeRTTCwnd = b.bdpMultiple(bbrProbeRTTCwndGain)
	}
	if rs.RTT > 0 && (rs.RTT < b.minRTT || b.minRTTExpired) {
		b.minRTT = rs.RTT
		b.minRTTStamp = now
	}
}

func (b *BBR) checkProbeRTT(rs RateSample, now time.Time) {
	if b.state != bbrProbeRTT && b.minRTTExpired && !b.idleRestart {
		b.saveCwnd()
		b.enterProbeRTT()
		b.probeRTTDoneStamp = time.Time{}
		b.bwProbeStopping = true
		b.startRound()
	}
	if b.state == bbrProbeRTT {
		b.handleProbeRTT(now)
	}
	if rs.Delivered > 0 {
		b.idleRestart = false
	}
}

func (b *BBR) enterProbeRTT() {
	b.state = bbrProbeRTT
	b.pacingGain = 1
	b.cwndGain = bbrProbeRTTCwndGain
}

func (b *BBR) handleProbeRTT(now time.Time) {
	// The owner marks the path application limited when it can't fill the
	// window, which ProbeRTT's reduced window makes so (MarkConnectionAppLimited).
	if b.probeRTTDoneStamp.IsZero() && b.inflight <= b.probeRTTCwndTarget() {
		b.probeRTTDoneStamp = now.Add(bbrProbeRTTDuration)
		b.probeRTTRoundDone = false
		b.startRound()
	} else if !b.probeRTTDoneStamp.IsZero() {
		if b.roundStart {
			b.probeRTTRoundDone = true
		}
		if b.probeRTTRoundDone {
			b.checkProbeRTTDone(now)
		}
	}
}

func (b *BBR) checkProbeRTTDone(now time.Time) {
	if !b.probeRTTDoneStamp.IsZero() && now.After(b.probeRTTDoneStamp) {
		b.minRTTStamp = now
		b.restoreCwnd()
		b.exitProbeRTT(now)
	}
}

func (b *BBR) exitProbeRTT(now time.Time) {
	b.resetShortTermModel()
	if b.fullBwReached {
		b.startProbeBWDown()
		b.startProbeBWCruise()
	} else {
		b.enterStartup()
	}
}

func (b *BBR) probeRTTCwndTarget() uint64 {
	return max(min(b.bdpMultiple(bbrProbeRTTCwndGain), b.probeRTTCwnd), b.minPipeCwnd())
}

// --- Model (section 5.5) ---

func (b *BBR) startRound() { b.nextRoundDelivered = b.delivered }

func (b *BBR) updateRound(rs RateSample) {
	if rs.PriorDelivered >= b.nextRoundDelivered {
		b.startRound()
		b.baseline.endRound(b.delivered, b.state == bbrProbeBWDown || b.state == bbrProbeBWCruise, b.queueing)
		b.roundCount++
		b.roundsSinceProbeUp++
		b.roundStart = true
		b.cwndLimited, b.cwndLimitedInRound = b.cwndLimitedInRound, false
	} else {
		b.roundStart = false
	}
}

// advanceMaxBwFilter starts a new ProbeBW cycle in the max_bw filter, which
// holds the current and previous cycles' maxima.
func (b *BBR) advanceMaxBwFilter() {
	b.cycleCount++
	b.bwHi[1], b.bwHi[0] = b.bwHi[0], 0
	b.maxBw = max(b.bwHi[0], b.bwHi[1])
}

func (b *BBR) updateMaxBw(rs RateSample) {
	b.updateRound(rs)
	if !rs.Valid || rs.Rate == 0 {
		return
	}
	rate := float64(rs.Rate)
	if rate >= b.maxBw || !rs.AppLimited {
		b.bwHi[0] = max(b.bwHi[0], rate)
		b.maxBw = max(b.bwHi[0], b.bwHi[1])
	}
}

func (b *BBR) updateACKAggregation(rs RateSample, now time.Time) {
	interval := now.Sub(b.extraAckedStart)
	expected := uint64(b.bw * interval.Seconds())
	if b.extraAckedDelivered <= expected {
		b.extraAckedDelivered = 0
		b.extraAckedStart = now
		expected = 0
	}
	b.extraAckedDelivered += rs.NewlyAcked
	extra := min(b.extraAckedDelivered-expected, b.cwnd)
	window := uint64(1)
	if b.fullBwReached {
		window = bbrExtraAckedFilterLen
	}
	b.extraAcked = uint64(b.extraAckedFilter.update(float64(extra), b.roundCount, window))
}

// --- Responding to loss (section 5.5.10) ---

func (b *BBR) handleInflightTooHigh(appLimited bool, txInFlight uint64) {
	b.prevProbeTooHigh = true
	b.checkProbeLoss = false
	if !appLimited {
		b.inflightLongterm = max(txInFlight, uint64(float64(b.targetInflight())*bbrBeta))
	}
	if b.state == bbrProbeBWUp {
		b.startProbeBWDown()
	}
}

func (b *BBR) noteLoss() {
	if !b.lossInRound {
		b.lossRoundDelivered = b.delivered
		b.saveStateUponLoss()
	}
	b.lossInRound = true
	b.lossEventsInRound++
}

func (b *BBR) handleLostPacket(p Packet, totalLost, totalCE uint64) {
	if !b.checkProbeLoss {
		return
	}
	txInFlight := p.Delivery.InFlight
	// Marks since the packet was sent count with losses (see BBR).
	lost, ce := totalLost-p.Delivery.Lost, totalCE-p.Delivery.CE
	if b.isInflightTooHigh(lost, ce, txInFlight) {
		b.handleInflightTooHigh(p.Delivery.AppLimited, inflightAtLoss(uint64(p.Bytes), txInFlight, lost+ce, b.lossThresh()))
	}
}

// inflightAtLoss estimates the bytes in flight at which losses crossed the
// threshold, within packet p's own bytes.
func inflightAtLoss(size, txInFlight, lost uint64, thresh float64) uint64 {
	inflightPrev := float64(txInFlight) - float64(size)
	lostPrev := float64(lost) - float64(size)
	lostPrefix := (thresh*inflightPrev - lostPrev) / (1 - thresh)
	return uint64(max(inflightPrev+lostPrefix, 0))
}

func (b *BBR) updateLatestDeliverySignals(rs RateSample) {
	b.lossRoundStart = false
	if rs.Valid {
		b.bwLatest = max(b.bwLatest, float64(rs.Rate))
	}
	b.inflightLatest = max(b.inflightLatest, rs.Delivered)
	if rs.PriorDelivered >= b.lossRoundDelivered {
		b.lossRoundDelivered = b.delivered
		b.lossRoundStart = true
	}
}

func (b *BBR) advanceLatestDeliverySignals(rs RateSample) {
	if b.lossRoundStart {
		b.bwLatest = 0
		if rs.Valid {
			b.bwLatest = float64(rs.Rate)
		}
		b.inflightLatest = rs.Delivered
		b.lossEventsInRound = 0
	}
}

func (b *BBR) resetCongestionSignals() {
	b.lossInRound, b.ceInRound = false, false
	b.lossEventsInRound = 0
	b.bwLatest = 0
	b.inflightLatest = 0
}

func (b *BBR) updateCongestionSignals(rs RateSample) {
	b.updateMaxBw(rs)
	if !b.lossRoundStart {
		return
	}
	b.adaptLowerBoundsFromCongestion()
	b.lossInRound, b.ceInRound = false, false
}

func (b *BBR) adaptLowerBoundsFromCongestion() {
	if b.isProbingBW() {
		return
	}
	if b.lossInRound && b.roundCongested() {
		// InitLowerBounds.
		if math.IsInf(b.bwShortterm, 1) {
			b.bwShortterm = b.maxBw
		}
		if b.inflightShortterm == bbrInfinity {
			b.inflightShortterm = b.cwnd
		}
		// LossLowerBounds.
		b.bwShortterm = max(b.bwLatest, bbrBeta*b.bwShortterm)
		b.inflightShortterm = max(b.inflightLatest, uint64(bbrBeta*float64(b.inflightShortterm)))
	}
}

func (b *BBR) resetShortTermModel() {
	b.bwShortterm = math.Inf(1)
	b.inflightShortterm = bbrInfinity
}

func (b *BBR) boundBWForModel() { b.bw = min(b.maxBw, b.bwShortterm) }

// saveStateUponLoss saves the window to restore when recovery ends. The
// draft also saves model state to undo a spurious recovery, which this
// controller doesn't do (see BBR).
func (b *BBR) saveStateUponLoss() {
	b.saveCwnd()
}

// --- Control parameters (section 5.6) ---

func (b *BBR) setPacingRateWithGain(gain float64) {
	rate := gain * b.bw * (100 - bbrPacingMarginPercent) / 100
	if rate > 0 && (b.fullBwReached || rate > b.pacingRate) {
		b.pacingRate = rate
	}
}

func (b *BBR) setPacingRate() { b.setPacingRateWithGain(b.pacingGain) }

// sendQuantum is 1 ms at the pacing rate, from two packets to 64 KB.
func (b *BBR) sendQuantum() uint64 {
	return min(max(uint64(b.pacingRate*time.Millisecond.Seconds()), 2*b.smss), 64*1024)
}

func (b *BBR) bdpMultiple(gain float64) uint64 {
	if b.minRTT == time.Duration(math.MaxInt64) {
		return b.initialWindow
	}
	b.bdp = uint64(b.bw * b.minRTT.Seconds())
	return uint64(gain * float64(b.bdp))
}

// quantizationBudget adds room for send bursts: the QUIC offload budget is
// the send quantum.
func (b *BBR) quantizationBudget(inflightCap uint64) uint64 {
	inflightCap = max(inflightCap, b.sendQuantum(), b.minPipeCwnd())
	if b.state == bbrProbeBWUp {
		inflightCap += 2 * b.smss
	}
	return inflightCap
}

// inflightGain is the draft's Inflight(gain).
func (b *BBR) inflightGain(gain float64) uint64 {
	return b.quantizationBudget(b.bdpMultiple(gain))
}

func (b *BBR) updateMaxInflight() {
	b.maxInflight = b.quantizationBudget(b.bdpMultiple(b.cwndGain) + b.extraAcked)
}

func (b *BBR) saveCwnd() {
	if !b.inRecovery && b.state != bbrProbeRTT {
		b.priorCwnd = b.cwnd
	} else {
		b.priorCwnd = max(b.priorCwnd, b.cwnd)
	}
}

func (b *BBR) restoreCwnd() { b.cwnd = max(b.cwnd, b.priorCwnd) }

func (b *BBR) setCwnd(rs RateSample) {
	b.updateMaxInflight()
	if b.fullBwReached {
		b.cwnd = min(b.cwnd+rs.NewlyAcked, b.maxInflight)
	} else if b.cwnd < b.maxInflight || b.delivered < b.initialWindow {
		b.cwnd += rs.NewlyAcked
	}
	b.cwnd = max(b.cwnd, b.minPipeCwnd())
	if b.state == bbrProbeRTT {
		b.cwnd = min(b.cwnd, b.probeRTTCwndTarget())
	}
	b.boundCwndForModel()
}

func (b *BBR) boundCwndForModel() {
	cap := uint64(bbrInfinity)
	if b.inProbeBW() && b.state != bbrProbeBWCruise {
		cap = b.inflightLongterm
	} else if b.state == bbrProbeRTT || b.state == bbrProbeBWCruise {
		cap = b.inflightWithHeadroom()
	}
	cap = min(cap, b.inflightShortterm)
	cap = max(cap, b.minPipeCwnd())
	b.cwnd = min(b.cwnd, cap)
}

// maxFilter is a windowed running maximum over the latest window units of
// time, keeping the best, second-best and third-best samples from successive
// subwindows, as Linux's lib/minmax.c does for BBR.
type maxFilter struct {
	s [3]struct {
		t uint64
		v float64
	}
}

func (m *maxFilter) reset(t uint64, v float64) float64 {
	for i := range m.s {
		m.s[i].t, m.s[i].v = t, v
	}
	return v
}

func (m *maxFilter) update(v float64, t, window uint64) float64 {
	if v >= m.s[0].v || t-m.s[2].t > window {
		return m.reset(t, v)
	}
	sample := struct {
		t uint64
		v float64
	}{t, v}
	if v >= m.s[1].v {
		m.s[1], m.s[2] = sample, sample
	} else if v >= m.s[2].v {
		m.s[2] = sample
	}
	dt := t - m.s[0].t
	if dt > window {
		m.s[0], m.s[1], m.s[2] = m.s[1], m.s[2], sample
		if t-m.s[0].t > window {
			m.s[0], m.s[1], m.s[2] = m.s[1], m.s[2], sample
		}
	} else if m.s[1].t == m.s[0].t && dt > window/4 {
		m.s[1], m.s[2] = sample, sample
	} else if m.s[2].t == m.s[1].t && dt > window/2 {
		m.s[2] = sample
	}
	return m.s[0].v
}

var _ Controller = (*BBR)(nil)
