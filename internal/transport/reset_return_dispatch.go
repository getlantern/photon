package transport

import "time"

// resetReturnSelection inspects only source-owned connections and immutable
// foreign hints. Ambiguity across either domain prevents a return reservation.
func (s *UDPServer) resetReturnSelection(p receivedUDP, now time.Time) resetReturnTarget {
	target, _ := s.resetSelection(p, now)
	return target
}

func (s *UDPServer) resetSelection(p receivedUDP, now time.Time) (resetReturnTarget, bool) {
	local, localAmbiguous := s.endpoint.localResetMatch(p.data, p.local, p.peer, now, nil)
	foreign, foreignAmbiguous := s.endpoint.router.foreignResetSelection(p.data, p.local, p.peer)
	if localAmbiguous || foreignAmbiguous || (local != nil && foreign != nil) {
		return resetReturnTarget{}, true
	}
	return resetReturnTarget{conn: local, route: foreign}, false
}

func (s *UDPServer) reserveResetReturn(p receivedUDP, first *handoffRoute, now time.Time) *resetReturnTicket {
	s.endpoint.router.enableResetIndex()
	target := s.resetReturnSelection(p, now)
	if target == (resetReturnTarget{}) || target.route == first {
		return nil
	}
	if s.resetReturns == nil {
		s.resetReturns = newResetReturnMailbox()
		s.returned = s.resetReturns.packets
	}
	return s.resetReturns.reserve(target)
}

// handleResetReturn consumes the original buffer without normal CID redispatch.
// The target must still be the unique eligible identity recorded before the
// first handoff. A final foreign reset handoff has no return ticket, so this
// sequence cannot recurse or fan out to additional owners.
func (s *UDPServer) handleResetReturn(result resetReturnResult, now time.Time) {
	p := result.packet
	release := true
	defer func() {
		if release {
			udpReadBuffers.Put(p.buffer)
		}
	}()
	if s.endpoint.closed || result.target == (resetReturnTarget{}) || s.resetReturnSelection(p, now) != result.target {
		return
	}
	if c := result.target.conn; c != nil {
		_ = s.endpoint.receiveRoutedDatagram(c, p.data, p.local, p.peer, now, p.marking)
		return
	}
	p.resetReturn = nil
	p.forwarded, p.resetFallback, p.route = true, true, result.target.route
	if p.route.target.offer(p) {
		release = false
	}
}
