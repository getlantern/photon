package recovery

import (
	"errors"
	"math"
	"time"
)

// NewReno is the RFC 9002 baseline congestion controller for one path. Flight
// accounting remains owned by Sent, including PTO packets and key retirement.
// Pacing, ECN validation, and persistent-congestion detection are separate.
type NewReno struct {
	window, threshold, datagram, credit uint64
	recoveryStart                       time.Time
	recovering                          bool
}

func NewCongestion(maxDatagram uint64) (*NewReno, error) {
	if maxDatagram < 1200 || maxDatagram > 65527 {
		return nil, errors.New("quic: invalid congestion datagram size")
	}
	c := new(NewReno)
	c.Reset(maxDatagram)
	return c, nil
}

// Reset returns the window to its initial value for maxDatagram, clamped to
// the sizes NewCongestion accepts.
func (c *NewReno) Reset(maxDatagram uint64) {
	maxDatagram = min(max(maxDatagram, 1200), 65527)
	*c = NewReno{window: min(10*maxDatagram, max(14720, 2*maxDatagram)), threshold: math.MaxUint64, datagram: maxDatagram}
}

// PacingRate is 0: NewReno is paced from its window.
func (c *NewReno) PacingRate() uint64 { return 0 }

// OnPacketSent does nothing: NewReno needs only ACK and loss events.
func (c *NewReno) OnPacketSent(SentEvent) {}

func (c *NewReno) Window() uint64    { return c.window }
func (c *NewReno) Threshold() uint64 { return c.threshold }
func (c *NewReno) CanSend(flight uint64, size int, inFlight, probe bool) bool {
	if size < 0 {
		return false
	}
	if !inFlight || probe {
		return true
	}
	return flight <= c.window && uint64(size) <= c.window-flight
}
func (c *NewReno) inRecovery(sent time.Time) bool {
	return c.recovering && !sent.After(c.recoveryStart)
}
func saturatingAdd(a, b uint64) uint64 {
	if b > math.MaxUint64-a {
		return math.MaxUint64
	}
	return a + b
}

// OnACK does not grow the window while application/flow-control limited, or
// for packets sent before the current recovery epoch. Accumulating ACKed bytes
// avoids rounding tiny packets down to zero growth in congestion avoidance.
func (c *NewReno) OnACK(e ACKEvent) {
	if e.Limited {
		return
	}
	for _, p := range e.Acked {
		if !p.InFlight || c.inRecovery(p.SentAt) {
			continue
		}
		if c.window < c.threshold {
			c.window = saturatingAdd(c.window, uint64(p.Bytes))
		} else {
			c.credit = saturatingAdd(c.credit, uint64(p.Bytes))
			for c.credit >= c.window {
				c.credit -= c.window
				c.window = saturatingAdd(c.window, c.datagram)
			}
		}
	}
}

// OnLoss reduces the window once per recovery epoch, never for ACK-only
// packets or dedicated MTU probes. PTO and key discard do not call this method.
func (c *NewReno) OnLoss(e LossEvent) {
	packets, now := e.Lost, e.Now
	var latest time.Time
	found := false
	for _, p := range packets {
		if p.InFlight && !p.MTUProbe && (!found || p.SentAt.After(latest)) {
			latest = p.SentAt
			found = true
		}
	}
	if !found {
		return
	}
	c.onCongestionEvent(latest, now)
}

// OnECN handles an increase in validated peer CE feedback for this path.
// sent is the send time of the largest acknowledged packet associated with
// that feedback, not the ACK receive time. The caller owns validation and
// counter/reordered-ACK suppression; raw unvalidated ACK counts must not call it.
// CE and loss share one recovery epoch, so neither can double-reduce the window.
func (c *NewReno) OnECN(sent, now time.Time) {
	c.onCongestionEvent(sent, now)
}

func (c *NewReno) onCongestionEvent(sent, now time.Time) {
	if c.inRecovery(sent) {
		return
	}
	c.recoveryStart, c.recovering = now, true
	c.threshold = c.window / 2
	c.window = max(c.threshold, 2*c.datagram)
	c.credit = 0
}

// OnPersistentCongestion collapses the window even within an existing recovery
// epoch, then ends that epoch so subsequent acknowledgments can grow again.
func (c *NewReno) OnPersistentCongestion() {
	c.recovering = false
	c.recoveryStart = time.Time{}
	c.window = 2 * c.datagram
	c.credit = 0
}
