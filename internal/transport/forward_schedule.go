package transport

import "github.com/getlantern/photon/internal/wire"

// refreshForwardPublication runs after path activation and before packet
// construction. A rejected alternative's reservations hold CID advertisements,
// not ordinary application traffic or the primary connection's lifetime.
// The path controller can inspect the publication error and remove that route.
func (e *ServerEndpoint) refreshForwardPublication(c *Conn) (bool, error) {
	if e.router.handoff == nil {
		c.forwardingBlocked = false
		return true, nil
	}
	ready, err := e.router.publishForwardCIDs(c, e.publicationWake)
	c.forwardingBlocked = !ready || err != nil
	return ready, err
}
func (c *Conn) blocksCIDAdvertisement(frame wire.Frame) bool {
	if !c.forwardingBlocked {
		return false
	}
	switch frame.(type) {
	case wire.NewConnectionIDFrame, wire.PathNewConnectionIDFrame:
		return true
	}
	return false
}

// selectPublishedQueue preserves held items and moves the first eligible item
// ahead of them. No bytes, packet numbers, sent flags or recovery state commit.
func (c *Conn) selectPublishedQueue(s *numberSpace) bool {
	if !c.forwardingBlocked {
		return len(s.queue) > 0
	}
	for i, item := range s.queue {
		if item.acked || c.blocksCIDAdvertisement(item.frame) {
			continue
		}
		if i > 0 {
			copy(s.queue[1:i+1], s.queue[:i])
			s.queue[0] = item
		}
		return true
	}
	return false
}
