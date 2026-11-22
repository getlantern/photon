package transport

import (
	"context"
	"errors"
	"net"
	"net/netip"
)

// addSocket transfers a concrete bound socket into an address-aware actor.
// The socket set and endpoint address registry change on the owner goroutine.
// Rejected additions do not change socket ownership. Concurrent runtime close
// may close an accepted socket before this call returns. Retired addresses are
// not reused and the lifetime socket set is bounded at 64.
func (s *UDPServer) addSocket(ctx context.Context, socket *net.UDPConn) error {
	if socket == nil {
		return errors.New("quic: nil socket")
	}
	address := canonicalPeer(socket.LocalAddr().(*net.UDPAddr).AddrPort())
	if _, err := canonicalPathTuple(address, address); err != nil {
		return err
	}
	return s.Do(ctx, func(e *ServerEndpoint) error {
		if e.networks == nil {
			return errors.New("quic: socket registration requires address-aware owner")
		}
		s.socketMu.Lock()
		defer s.socketMu.Unlock()
		select {
		case <-s.stop:
			return ErrEndpointClosed
		default:
		}
		if s.readers.Load() == 0 {
			return ErrEndpointClosed
		}
		if s.sockets[address] != nil {
			return errors.New("quic: duplicate socket address")
		}
		if len(s.sockets) >= 64 {
			return errors.New("quic: socket limit reached")
		}
		if err := configureUDPSocket(socket); err != nil {
			return err
		}
		configureReceiveECN(socket)
		s.sockets[address] = socket
		e.locals[address] = true
		s.readers.Add(1)
		go s.readSocket(socket, address, s.packets)
		return nil
	})
}

var errSocketRemoved = errors.New("quic: local UDP socket removed")

// removeSocket closes a registered socket and abandons its paths on the owner
// goroutine. Removal is idempotent for retired addresses. Keep one local socket
// registered; Close is the operation for terminating the entire runtime. The
// closed socket remains in the lifetime registry until address reuse is supported.
func (s *UDPServer) removeSocket(ctx context.Context, address netip.AddrPort) error {
	address = canonicalPeer(address)
	return s.Do(ctx, func(e *ServerEndpoint) error {
		return s.removeSocketOnOwner(e, address)
	})
}

// removeSocketOnOwner requires the serialized endpoint owner.
func (s *UDPServer) removeSocketOnOwner(e *ServerEndpoint, address netip.AddrPort) error {
	if e.networks == nil {
		return errors.New("quic: socket removal requires address-aware owner")
	}
	s.socketMu.Lock()
	socket := s.sockets[address]
	s.socketMu.Unlock()
	if socket == nil {
		return errors.New("quic: unknown local socket")
	}
	if s.borrowedSocket != nil && address == s.primaryAddress {
		return errors.New("quic: cannot remove borrowed primary socket")
	}
	if !e.locals[address] {
		return nil
	}
	if len(e.locals) <= 1 {
		return errors.New("quic: use Close to remove the last socket")
	}
	err := socket.Close()
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	e.failLocalSocket(address, errSocketRemoved)
	return nil
}
