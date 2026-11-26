package transport

import (
	"github.com/getlantern/photon/internal/recovery"
	"time"
)

// hasQueuedData reports whether any queue holds an item a send pass could
// send. A NEW_CONNECTION_ID held while forwarding is blocked can't be sent
// yet (see selectPublishedQueue), so it doesn't count.
func (c *Conn) hasQueuedData() bool {
	for i := range c.spaces {
		for _, item := range c.spaces[i].queue {
			if c.sendable(item) {
				return true
			}
		}
	}
	return false
}

// sendable reports whether a queued item could be sent now, as
// selectPublishedQueue decides.
func (c *Conn) sendable(item *sendItem) bool {
	return !item.acked && !(c.forwardingBlocked && c.blocksCIDAdvertisement(item.frame))
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
