package transport

import (
	"context"
	"errors"
	"sync"
)

// packetHandoff transfers datagram ownership between serialized socket owners.
// It never waits for queue space or receiver work, avoiding reciprocal owner waits.
// CID selection and local-address registration are separate responsibilities.
type packetHandoff struct {
	changed     chan struct{}
	mu          sync.Mutex
	closed      bool
	routes      map[*handoffRoute]*Conn
	connections map[*Conn]map[*handoffRoute]*forwardPublication
	packets     chan receivedUDP
}

func newPacketHandoff() *packetHandoff {
	return &packetHandoff{packets: make(chan receivedUDP, 64), changed: make(chan struct{}, 1)}
}

// offer transfers the buffer only on success. The caller must release or retain
// it on failure. Local reader errors cannot be forwarded as remote socket errors.
func (h *packetHandoff) offer(p receivedUDP) bool {
	if p.err != nil || p.buffer == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	select {
	case h.packets <- p:
		return true
	default:
		return false
	}
}

// close prevents late producers before draining queued buffers. The receiver
// calls it after leaving its receive loop; repeated calls are harmless. The
// channel stays open so no sender can panic on a close/send race.
func (h *packetHandoff) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for route := range h.routes {
		route.revoke()
	}
	h.routes = nil
	h.connections = nil
	for {
		select {
		case p := <-h.packets:
			if p.resetReturn != nil {
				p.resetReturn.cancel()
			}
			udpReadBuffers.Put(p.buffer)
		default:
			return
		}
	}
}

// handoffTarget installs an inbox on the receiving owner. The default runtime
// pays no inbox allocation cost until another transport needs this facility.
func (s *UDPServer) handoffTarget(ctx context.Context) (*packetHandoff, error) {
	var target *packetHandoff
	err := s.Do(ctx, func(*ServerEndpoint) error {
		if s.handoff == nil {
			s.handoff = newPacketHandoff()
			s.endpoint.router.handoff = s.handoff
			s.endpoint.publicationWake = s.handoff.changed
			s.publicationChanged = s.handoff.changed
			s.forwarded = s.handoff.packets
		}
		target = s.handoff
		return nil
	})
	return target, err
}

func (h *packetHandoff) subscribe(route *handoffRoute) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		route.revoked.Store(true)
		return false
	}
	if h.routes == nil {
		h.routes = make(map[*handoffRoute]*Conn)
	}
	h.routes[route] = nil
	return true
}
func (h *packetHandoff) unsubscribe(route *handoffRoute) {
	h.mu.Lock()
	defer h.mu.Unlock()
	route.revoke()
	if c := h.routes[route]; c != nil {
		delete(h.connections[c], route)
		if len(h.connections[c]) == 0 {
			delete(h.connections, c)
		}
	}
	delete(h.routes, route)
	h.signalPublication()
}

// bindConnection runs on the destination owner after checking router membership.
// The connection pointer is an identity only under this mutex; source owners
// never inspect connection state.
func (h *packetHandoff) bindConnection(route *handoffRoute, c *Conn) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	previous, ok := h.routes[route]
	if h.closed || !ok || route.revoked.Load() {
		return ErrEndpointClosed
	}
	if previous != nil && previous != c {
		return errors.New("quic: forwarding registration already bound")
	}
	if h.connections == nil {
		h.connections = make(map[*Conn]map[*handoffRoute]*forwardPublication)
	}
	if h.connections[c] == nil {
		h.connections[c] = make(map[*handoffRoute]*forwardPublication)
	}
	if h.connections[c][route] == nil {
		h.connections[c][route] = new(forwardPublication)
	}
	h.routes[route] = c
	h.signalPublication()
	return nil
}

func (h *packetHandoff) revokeConnection(c *Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for route := range h.connections[c] {
		route.revoke()
		delete(h.routes, route)
	}
	delete(h.connections, c)
}

// bindHandoffConnection must be called outside owner callbacks, on the receiving
// runtime. Router removal revokes the binding after closing/draining completes.
func (s *UDPServer) bindHandoffConnection(ctx context.Context, route *handoffRoute, c *Conn) error {
	return s.Do(ctx, func(e *ServerEndpoint) error {
		if route == nil || s.handoff == nil || route.target != s.handoff {
			return errors.New("quic: invalid forwarding destination")
		}
		if _, ok := e.router.entries[c]; !ok || c.closed {
			return ErrEndpointClosed
		}
		return s.handoff.bindConnection(route, c)
	})
}

func (h *packetHandoff) signalPublication() {
	select {
	case h.changed <- struct{}{}:
	default:
	}
}
