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
	hystart                             hystart
}

func NewCongestion(maxDatagram uint64) (*NewReno, error) {
	if maxDatagram < 1200 || maxDatagram > 65527 {
		return nil, errors.New("quic: invalid congestion datagram size")
	}
	c := new(NewReno)
	c.Reset(maxDatagram)
	return c, nil
}

// initialWindowPackets is the initial congestion window, in datagrams, of
// both controllers. RFC 9002 Section 7.2 recommends (SHOULD) ten, capped at
// the larger of 14720 bytes or two datagrams; Chromium's QUIC, quic-go and
// Hysteria2 start with 32, and so does photon: on a long path ten cost about
// 1.7 more round trips to reach the same rate, which dominates short
// transfers. Both controllers pace from the start, so the larger window is
// spread out: NewReno's over about 0.8 of the smoothed RTT after a 12 KB
// burst, BBR Startup's over about a third of it.
const initialWindowPackets = 32

// InitialWindow is the initial congestion window for maxDatagram.
func InitialWindow(maxDatagram uint64) uint64 { return initialWindowPackets * maxDatagram }

// Reset returns the window to its initial value for maxDatagram, clamped to
// the sizes NewCongestion accepts.
func (c *NewReno) Reset(maxDatagram uint64) {
	maxDatagram = min(max(maxDatagram, 1200), 65527)
	*c = NewReno{window: InitialWindow(maxDatagram), threshold: math.MaxUint64, datagram: maxDatagram}
}

// PacingRate is 0: NewReno is paced from its window.
func (c *NewReno) PacingRate() uint64 { return 0 }

// ForcesAppLimited is false: NewReno never holds the sender back.
func (c *NewReno) ForcesAppLimited() bool { return false }

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
	if c.window < c.threshold && c.hystart.onACK(e) {
		// Conservative slow start ran its rounds: the path's queue is
		// filling, so grow as congestion avoidance from here.
		c.threshold = c.window
	}
	for _, p := range e.Acked {
		if !p.InFlight || c.inRecovery(p.SentAt) {
			continue
		}
		if c.window < c.threshold {
			grow := uint64(p.Bytes)
			if c.hystart.css {
				c.credit = saturatingAdd(c.credit, grow)
				grow = c.credit / cssGrowthDivisor
				c.credit -= grow * cssGrowthDivisor
			}
			c.window = saturatingAdd(c.window, grow)
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
	c.hystart.done, c.hystart.css = true, false
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
