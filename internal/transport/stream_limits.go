package transport

import "github.com/getlantern/photon/internal/wire"

func peerStreamSendLimit(p wire.EarlyLimits, local, uni bool) uint64 {
	if uni {
		return p.InitialMaxStreamDataUni
	}
	if local {
		return p.InitialMaxStreamDataBidiRemote
	}
	return p.InitialMaxStreamDataBidiLocal
}

// applyPeerStreamLimits installs validated initial send credit separately from
// connection-specific peer parameters. For accepted 0-RTT, invoke with current
// values only at handshake completion: new credit must never enter early packets.
// A rejection must rebuild the stream set before installing potentially lower
// credit. This method never returns consumed credit or reduces MAX_* grants.
func (c *Conn) applyPeerStreamLimits(p wire.EarlyLimits) error {
	if !p.Supports(p) {
		return wire.ErrEarlyLimits
	}
	c.streams.peerLimits, c.streams.havePeerLimits = p, true
	c.updateConnectionSendLimit(p.InitialMaxData)
	c.updateSendStreamLimit(false, p.InitialMaxStreamsBidi)
	c.updateSendStreamLimit(true, p.InitialMaxStreamsUni)
	for id, s := range c.streams.objects {
		if s.send == nil {
			continue
		}
		local := (id&1 == 0) != c.config.Server
		c.updateStreamSendLimit(id, s, peerStreamSendLimit(p, local, id&2 != 0))
	}
	return nil
}

// Blocked notifications are advisory: local queue pressure must not close a
// connection merely because an application attempts to open another stream.
func (c *Conn) notifyStreamsBlocked(uni bool) error {
	d := 0
	if uni {
		d = 1
	}
	limit := c.streams.ids.SendLimit(uni)
	state := &c.streams.blockedLimits[d]
	if (state.notified && state.limit == limit) || c.availableSendBytes() < 32 {
		return nil
	}
	if c.multipath != nil && c.confirmed {
		if _, ok := c.survivingPath(^uint64(0)); !ok {
			return nil
		}
	}
	if err := c.enqueue(applicationSpace, wire.StreamsBlockedFrame{Unidirectional: uni, Limit: limit}, 32); err != nil {
		return err
	}
	state.limit, state.notified = limit, true
	return nil
}

func (c *Conn) updateSendStreamLimit(uni bool, limit uint64) error {
	before := c.streams.ids.SendLimit(uni)
	if err := c.streams.ids.UpdateSendLimit(uni, limit); err != nil {
		return err
	}
	if limit > before {
		c.retireFrames(func(frame wire.Frame) bool {
			blocked, ok := frame.(wire.StreamsBlockedFrame)
			return ok && blocked.Unidirectional == uni && blocked.Limit < limit
		})
	}
	return nil
}

func (c *Conn) notifyStreamDataBlocked(id uint64, s *streamState) error {
	limit := s.send.Limit()
	if (s.blockedNotified && s.blockedLimit == limit) || c.availableSendBytes() < 32 {
		return nil
	}
	if c.multipath != nil && c.confirmed {
		if _, ok := c.survivingPath(^uint64(0)); !ok {
			return nil
		}
	}
	if err := c.enqueue(applicationSpace, wire.StreamDataBlockedFrame{StreamID: id, Limit: limit}, 32); err != nil {
		return err
	}
	s.blockedLimit, s.blockedNotified = limit, true
	return nil
}
func (c *Conn) retireStreamDataBlocked(id uint64) {
	s := c.streams.objects[id]
	if s == nil || !s.blockedNotified {
		return
	}
	s.blockedNotified = false
	c.retireFrames(func(frame wire.Frame) bool {
		f, ok := frame.(wire.StreamDataBlockedFrame)
		return ok && f.StreamID == id
	})
}
func (c *Conn) updateStreamSendLimit(id uint64, s *streamState, limit uint64) error {
	before := s.send.Limit()
	if err := s.send.UpdateLimit(limit); err != nil {
		return err
	}
	if limit > before {
		c.retireStreamDataBlocked(id)
	}
	return nil
}
