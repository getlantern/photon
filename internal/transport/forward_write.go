package transport

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"

	"github.com/getlantern/photon/internal/recovery"
)

var errForeignECNUnsupported = errors.New("quic: foreign socket cannot apply ECN marking")

type foreignWriteResult struct {
	n   int
	err error
}
type foreignWrite struct {
	route   *handoffRoute
	data    []byte
	peer    netip.AddrPort
	marking recovery.ECNMark
	result  chan foreignWriteResult
	notify  chan<- struct{}
}

func (request *foreignWrite) complete(n int, err error) {
	request.result <- foreignWriteResult{n, err}
	select {
	case request.notify <- struct{}{}:
	default:
	}
}

// packetWriter transfers copied wire bytes, never connection or recovery state.
// Only the socket owner consumes requests and closes the inbox after leaving its
// write loop. Success acknowledges an attempted socket write, not peer receipt.
type packetWriter struct {
	mu       sync.Mutex
	closed   bool
	router   *Router // immutable identity, not accessed by producers
	writes   chan *foreignWrite
	reserved map[*foreignWriteReservation]struct{}
	waiters  map[*handoffRoute]struct{}
}

func newPacketWriter(router *Router) *packetWriter {
	return &packetWriter{router: router, writes: make(chan *foreignWrite, 64)}
}
func (w *packetWriter) offer(route *handoffRoute, data []byte, peer netip.AddrPort, marking recovery.ECNMark, notify chan<- struct{}) (<-chan foreignWriteResult, error) {
	if route == nil || route.router != w.router || !peer.IsValid() || peer.Port() == 0 || len(data) == 0 || len(data) > 65535 || marking > recovery.ECT0 {
		return nil, errPathBinding
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || route.revoked.Load() {
		return nil, ErrEndpointClosed
	}
	w.pruneReservationsLocked()
	if len(w.writes)+len(w.reserved) >= cap(w.writes) {
		return nil, errOperationWouldBlock
	}
	request := &foreignWrite{route: route, data: append([]byte(nil), data...), peer: canonicalPeer(peer), marking: marking, result: make(chan foreignWriteResult, 1), notify: notify}
	w.writes <- request
	return request.result, nil
}
func (w *packetWriter) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	w.reserved = nil
	w.signalCapacityLocked()
	for {
		select {
		case request := <-w.writes:
			request.complete(0, ErrEndpointClosed)
		default:
			return
		}
	}
}
func (s *UDPServer) foreignWriteTarget(ctx context.Context) (*packetWriter, error) {
	var writer *packetWriter
	err := s.Do(ctx, func(e *ServerEndpoint) error {
		if s.foreignWriter == nil {
			s.foreignWriter = newPacketWriter(e.router)
			s.foreignWrites = s.foreignWriter.writes
		}
		writer = s.foreignWriter
		return nil
	})
	return writer, err
}

// writeForeignPacket executes only on the socket owner. Registration identity
// and live connection binding authorize the source socket. The requester must
// separately establish publication readiness and path validation before use.
// No ECN retry or mutation of the requester's connection state occurs here.
func (s *UDPServer) writeForeignPacket(request *foreignWrite) (int, error) {
	route := request.route
	if s.endpoint.closed || route == nil || route.target == nil || route.router != s.endpoint.router || route.revoked.Load() {
		return 0, ErrEndpointClosed
	}
	if _, ok := s.endpoint.router.forwardRegistrations[route]; !ok {
		return 0, ErrEndpointClosed
	}
	if !route.local.IsValid() || route.local.Addr().IsUnspecified() || route.local.Addr().Is4() != request.peer.Addr().Is4() {
		return 0, errPathBinding
	}
	route.target.mu.Lock()
	bound := !route.target.closed && route.target.routes[route] != nil
	route.target.mu.Unlock()
	if !bound {
		return 0, errPathBinding
	}
	connection := s.sockets[route.local]
	if connection == nil {
		return 0, errPathBinding
	}
	return writeForeignDatagram(connection, request.data, request.peer, request.marking)
}

// writeForeignDatagram performs exactly one write with the requested marking.
// Generic sockets report unsupported marking before writing any bytes. A caller
// may retry NotECT only after its owning engine rolls back ECN send accounting.
func writeForeignDatagram(connection net.PacketConn, data []byte, peer netip.AddrPort, marking recovery.ECNMark) (int, error) {
	if marking != recovery.NotECT {
		control, ok := connection.(OOBCapablePacketConn)
		if !ok {
			return 0, errForeignECNUnsupported
		}
		return writeECNDatagram(control, data, peer, marking)
	}
	if udp, ok := connection.(*net.UDPConn); ok {
		return udp.WriteToUDPAddrPort(data, peer)
	}
	return connection.WriteTo(data, net.UDPAddrFromAddrPort(peer))
}
