package transport

import (
	"github.com/getlantern/photon/internal/recovery"
	"time"
)

func (c *Conn) hasQueuedData() bool {
	for i := range c.spaces {
		for _, item := range c.spaces[i].queue {
			if !item.acked {
				return true
			}
		}
	}
	return false
}

// CongestionStats exposes the current single-path window and flight in bytes.
func (c *Conn) CongestionStats() (window, flight uint64) {
	return c.congestion.Window(), c.recovery.BytesInFlight
}

func (c *Conn) onCongestionLoss(lost []recovery.Packet, now time.Time) {
	c.loss.lost(lost)
	c.congestion.OnLoss(lost, now)
	delay := 25 * time.Millisecond
	if c.peerParams != nil {
		delay = time.Duration(c.peerParams.MaxAckDelay) * time.Millisecond
	}
	if len(lost) > 0 && c.recovery.PersistentCongestion(delay) {
		c.congestion.OnPersistentCongestion()
		c.recovery.RTT.OnPersistentCongestion()
	}
}
