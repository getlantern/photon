package transport

import (
	"net/netip"
	"time"
)

func (e *ServerEndpoint) receiveRoutedDatagram(c *Conn, data []byte, local, peer netip.AddrPort, now time.Time, marking ECN) error {
	before := c.authenticatedPackets
	var err error
	if owner := e.networks[c]; owner != nil {
		err = owner.receiveECN(data, local, peer, now, marking)
	} else {
		err = c.HandleDatagramECN(data, now, marking)
	}
	if c.authenticatedPackets != before {
		e.authenticatedDatagrams++
	}
	if syncErr := e.syncAt(c, now); syncErr != nil {
		return syncErr
	}
	return err
}

// receiveResetFallback runs only after normal CID routing failed to authenticate.
// It never changes the primary routing decision for an authenticated datagram.
// The endpoint owns every inspected connection; foreign-owner fallback remains
// a separate handoff problem. Ambiguous token matches do not choose an owner.
func (e *ServerEndpoint) receiveResetFallback(data []byte, local, peer netip.AddrPort, now time.Time, marking ECN, skip *Conn) (*Conn, error) {
	found, ambiguous := e.localResetMatch(data, local, peer, now, skip)
	if found == nil || ambiguous {
		return nil, nil
	}
	return found, e.receiveRoutedDatagram(found, data, local, peer, now, marking)
}

// localResetMatch is owner-only selection without receive side effects. The
// ambiguity bit distinguishes no match from multiple eligible connections.
func (e *ServerEndpoint) localResetMatch(data []byte, local, peer netip.AddrPort, now time.Time, skip *Conn) (*Conn, bool) {
	if len(data) < 21 {
		return nil, false
	}
	peer = canonicalPeer(peer)
	tuple, tupleErr := canonicalPathTuple(local, peer)
	var found *Conn
	consider := func(c *Conn) bool {
		route, registered := e.router.entries[c]
		if !registered || c == skip || c.closed || c.Draining() {
			return false
		}
		if owner := e.networks[c]; owner != nil {
			if tupleErr != nil {
				return false
			}
			if c.multipath != nil {
				if !c.resetTupleKnown(tuple, now) {
					return false
				}
			} else if tuple != owner.primary {
				return false
			}
		} else if route.peer != peer {
			return false
		}
		if !c.isStatelessReset(data) {
			return false
		}
		if found != nil {
			return true
		}
		found = c
		return false
	}
	if index := e.router.resetIndex; index != nil {
		for c := range index.candidates(data) {
			if consider(c) {
				return nil, true
			}
		}
	} else {
		for c := range e.router.entries {
			if consider(c) {
				return nil, true
			}
		}
	}
	return found, false
}
