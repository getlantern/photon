package transport

import (
	"errors"
	"time"
)

// handleForwardedDatagram authorizes only the connection bound to the source
// registration. A foreign socket never becomes a listener socket or a globally
// accepted local address. The normal engine still authenticates and validates
// the tuple before applying frames. Unroutable reset candidates can arrive via
// token hints; live tuple/token checks and authentication remain on this owner.
// The result suppresses further reset selection after authentication or reset receipt.
// Other unknown traffic has no admission or raw-delivery fallback on this path.
func (e *ServerEndpoint) handleForwardedDatagram(p receivedUDP, now time.Time) bool {
	h := e.router.handoff
	if e.closed || h == nil || p.route == nil || !p.forwarded || p.err != nil || (p.marking > ECNCE && p.marking != ECNUnknown) {
		return false
	}
	if len(p.data) == 0 {
		return false
	}
	tuple, err := canonicalPathTuple(p.local, p.peer)
	if err != nil || tuple.local.Addr().IsUnspecified() {
		return false
	}
	route := p.route
	if route.target != h || route.local != tuple.local || route.revoked.Load() {
		return false
	}
	h.mu.Lock()
	c := h.routes[route]
	closed := h.closed
	h.mu.Unlock()
	if closed || c == nil {
		return false
	}
	owner := e.networks[c]
	if owner == nil || c.closed || !c.confirmed || c.multipath == nil {
		return false
	}
	if p.resetFallback {
		// The source has already failed normal CID authentication. Its hint can
		// select this registration, but only live owner state authorizes receipt.
		if !c.resetTupleKnown(tuple, now) || !c.isStatelessReset(p.data) {
			return false
		}
	} else {
		selected, known := e.router.RouteNetwork(p.data, tuple.local, tuple.remote)
		if !known || selected != c {
			return false
		}
	}
	before := c.authenticatedPackets
	receiveErr := owner.receiveECN(p.data, tuple.local, tuple.remote, now, p.marking)
	_ = e.syncAt(c, now)
	return c.authenticatedPackets != before || errors.Is(receiveErr, ErrStatelessReset)
}
