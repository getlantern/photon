package transport

import "github.com/getlantern/photon/internal/wire"

// Multiple streams can be waiting on the same connection grant. Keep one
// reliable signal until that grant changes or the last recorded waiter ends.
func (c *Conn) notifyDataBlocked(s *streamState) error {
	if !s.connectionBlocked {
		s.connectionBlocked = true
		c.streams.dataBlockedCount++
	}
	if c.streams.dataBlockedNotified || c.availableSendBytes() < 32 {
		return nil
	}
	if c.multipath != nil && c.confirmed {
		if _, ok := c.survivingPath(^uint64(0)); !ok {
			return nil
		}
	}
	if err := c.enqueue(applicationSpace, wire.DataBlockedFrame{Limit: c.streams.send.Limit()}, 32); err != nil {
		return err
	}
	c.streams.dataBlockedNotified = true
	return nil
}

func (c *Conn) retireDataBlocked() {
	if !c.streams.dataBlockedNotified {
		return
	}
	c.streams.dataBlockedNotified = false
	c.retireFrames(func(f wire.Frame) bool { _, ok := f.(wire.DataBlockedFrame); return ok })
}

func (c *Conn) releaseConnectionBlocked(s *streamState) {
	if !s.connectionBlocked {
		return
	}
	s.connectionBlocked = false
	c.streams.dataBlockedCount--
	if c.streams.dataBlockedCount == 0 {
		c.retireDataBlocked()
	}
}

func (c *Conn) updateConnectionSendLimit(limit uint64) error {
	before := c.streams.send.Limit()
	if err := c.streams.send.UpdateLimit(limit); err != nil {
		return err
	}
	if limit > before {
		c.retireDataBlocked()
		if c.streams.dataBlockedCount != 0 {
			for _, s := range c.streams.objects {
				s.connectionBlocked = false
			}
			c.streams.dataBlockedCount = 0
		}
	}
	return nil
}
