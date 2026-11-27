package recovery

import (
	"math"
	"time"
)

// A path's baseline loss is the loss it shows without congestion: random
// loss, on a lossy link or a wireless hop. BBR's loss responses (the
// short-term bounds, the probe's in-flight limit and Startup's exit) treat
// any loss above a 2% threshold, or for the short-term bounds any loss at
// all, as congestion. On a path that loses packets at random, every round
// has loss, so those responses hold BBR far below the path's capacity:
// draft-ietf-ccwg-bbr-07's model of loss as congestion is a departure here.
// Instead loss counts as congestion only above the baseline: the loss rate
// measured in rounds when BBR isn't probing for bandwidth (ProbeBW_DOWN and
// ProbeBW_CRUISE) and wasn't the round before either, since loss shows
// about a round after the packet was sent, so it builds no queue of its
// own, and when no other flow holds one either: the round's lowest RTT is
// at the path's minimum. A clean path and an overflowing buffer show no
// loss in those rounds, and a queue another flow fills keeps its rounds
// out, so the baseline stays at zero and BBR responds as the draft has it.
// Random loss shows in every round, and is discounted. Two costs remain. A
// policer drops with no queue to show it, so BBR cruising at the policed
// rate learns a small baseline from its overruns. And a loss-based flow
// that random loss keeps from holding a queue can't keep its losses out:
// BBR then takes more of what that flow, already held down by the random
// loss, would have had.
const (
	// bbrBaselineRounds is how many quiet rounds the baseline averages.
	bbrBaselineRounds = 16
	// bbrMinBaselineRounds is how many quiet rounds it takes before there is
	// a baseline, so one unlucky round doesn't make one.
	bbrMinBaselineRounds = 4
	// bbrStaleBaselineRounds is how many rounds in a row without a quiet one
	// drop the baseline: a path that has held a queue that long may have
	// stopped losing packets at random, and the queue keeps that from
	// showing.
	bbrStaleBaselineRounds = 64
	// bbrMaxLossBaseline caps the loss discounted as random.
	bbrMaxLossBaseline = 0.15
	// bbrMaxLossMargin caps the allowance for a short round's sampling
	// noise, so a path with few packets a round still sees congestion.
	bbrMaxLossMargin = 0.05
)

type lossBaseline struct {
	lost, sent  [bbrBaselineRounds]uint64 // quiet rounds' lost and sent bytes
	next, count int
	rate        float64

	// The round in progress: its lost bytes, the delivered count it
	// started at, and whether BBR was quiet when it began.
	roundLost      uint64
	roundDelivered uint64
	roundQuiet     bool
	roundMinRTT    time.Duration
	// baseRTT is the path's lowest RTT, the reference for telling a queue.
	// Unlike BBR's min_rtt it never expires: on expiry min_rtt takes a
	// fresh sample, which another flow's standing queue can inflate, and
	// that queue would then pass for none.
	baseRTT time.Duration
	// stale counts the rounds since the last quiet one.
	stale int
	// learned is whether the path has had a baseline since the connection
	// began or its baseline was last dropped as stale.
	learned bool
	// prevQuiet is whether the round before began quiet: loss shows about a
	// round after the packet was sent, so a round's loss is from the last.
	prevQuiet bool
}

// noteLost adds bytes declared lost to the round in progress.
func (l *lossBaseline) noteLost(bytes uint64) { l.roundLost += bytes }

// noteRTT notes an RTT sample from the round in progress.
func (l *lossBaseline) noteRTT(rtt time.Duration) {
	if rtt > 0 && (l.roundMinRTT == 0 || rtt < l.roundMinRTT) {
		l.roundMinRTT = rtt
	}
	if rtt > 0 && (l.baseRTT == 0 || rtt < l.baseRTT) {
		l.baseRTT = rtt
	}
}

// endRound finishes the round in progress, at delivered bytes, and starts
// the next. The finished round joins the baseline if BBR wasn't probing as
// it or the round before began, and no queue showed in it (queueing, of the
// round's lowest RTT). quietNext reports whether BBR is quiet as the next
// round begins. It reports whether the path has its first baseline, since
// the connection began or its last baseline was dropped as stale.
func (l *lossBaseline) endRound(delivered uint64, quietNext bool, queueing func(time.Duration) bool) (learned bool) {
	began := l.roundQuiet
	if !l.prevQuiet || l.roundMinRTT > 0 && queueing(l.roundMinRTT) {
		l.roundQuiet = false
	}
	sent := delivered - l.roundDelivered + l.roundLost
	if sent > 0 {
		if !l.roundQuiet {
			if l.stale++; l.stale >= bbrStaleBaselineRounds {
				l.lost, l.sent, l.next, l.count, l.rate, l.learned = [bbrBaselineRounds]uint64{}, [bbrBaselineRounds]uint64{}, 0, 0, 0, false
			}
		} else {
			l.stale = 0
			l.lost[l.next], l.sent[l.next] = l.roundLost, sent
			l.next = (l.next + 1) % bbrBaselineRounds
			l.count = min(l.count+1, bbrBaselineRounds)
			var lost, all uint64
			for i := range l.count {
				lost += l.lost[i]
				all += l.sent[i]
			}
			if l.count >= bbrMinBaselineRounds {
				l.rate = min(float64(lost)/float64(all), bbrMaxLossBaseline)
			}
		}
	}
	l.roundLost, l.roundDelivered, l.roundQuiet, l.roundMinRTT, l.prevQuiet = 0, delivered, quietNext, 0, began
	if l.learned || l.rate == 0 {
		return false
	}
	l.learned = true
	return true
}

// lossThresh is the share of in-flight data whose loss marks congestion:
// draft-ietf-ccwg-bbr-07's 2%, above the path's baseline.
func (b *BBR) lossThresh() float64 { return bbrLossThresh + b.baseline.rate }

// isInflightTooHigh reports whether lost and CE-marked bytes exceed
// draft-07's 2% of what was in flight, once the path's baseline loss is
// discounted from the lost bytes. CE marks are never random, so the
// baseline doesn't discount them. QUIC acknowledgments are selective.
func (b *BBR) isInflightTooHigh(lost, ce, inflight uint64) bool {
	lostAbove := max(float64(lost)-float64(inflight)*b.baseline.rate, 0)
	return lostAbove+float64(ce) > float64(inflight)*bbrLossThresh
}

// roundCongested reports whether the loss round just ended, which had loss
// or CE marks, marks congestion, for the short-term bounds. CE marks
// always do, and so does any loss without a baseline, as the draft has it.
func (b *BBR) roundCongested() bool {
	if b.ceInRound {
		return true
	}
	sent := b.lossRoundLost
	if b.delivered > b.lossRoundFrom {
		sent += b.delivered - b.lossRoundFrom
	}
	return b.lossCongested(b.lossRoundLost, sent)
}

// lossCongested reports whether lost of sent bytes marks congestion. With
// a baseline, the loss must exceed it by the loss threshold, and by three
// standard deviations of that many packets' loss count at the baseline
// rate, up to bbrMaxLossMargin: a short round's random loss varies enough
// that the threshold alone would mistake it for congestion.
func (b *BBR) lossCongested(lost, sent uint64) bool {
	p := b.baseline.rate
	if p == 0 || sent == 0 {
		return true
	}
	packets := float64(sent) / float64(max(b.smss, 1))
	margin := min(3*math.Sqrt(p*(1-p)/max(packets, 1)), bbrMaxLossMargin)
	return float64(lost)/float64(sent) > b.lossThresh()+margin
}

// inflightAtCongestion estimates the bytes in flight at which lost and
// CE-marked bytes crossed isInflightTooHigh's threshold, within the last
// size bytes. Once the loss exceeds the baseline's discount, the crossing
// is the combined bytes' at the baseline plus 2%; below it, only the marks
// count, at 2%.
func (b *BBR) inflightAtCongestion(size, txInFlight, lost, ce uint64) uint64 {
	if float64(lost) <= float64(txInFlight)*b.baseline.rate {
		return inflightAtLoss(min(size, ce), txInFlight, ce, bbrLossThresh)
	}
	return inflightAtLoss(size, txInFlight, lost+ce, b.lossThresh())
}

// queueing reports whether the latest RTT shows a queue: above the path's
// lowest RTT by an eighth of it, at least 4 ms and at most 16 ms, as HyStart++ judges
// a rise. Random loss comes without one; a buffer overflowing comes with
// one.
func (b *BBR) queueing(rtt time.Duration) bool {
	base := b.baseline.baseRTT
	if rtt <= 0 || base == 0 {
		return true
	}
	return rtt >= base+min(max(base/8, 4*time.Millisecond), 16*time.Millisecond)
}
