package transport

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"time"
)

// SupportsMultipath reports confirmed mutual negotiation. Owner goroutine only.
func (c *Conn) SupportsMultipath() bool { return c.confirmed && c.multipathNegotiated() }

// OpenPath owns a new socket and returns only after its path is validated.
// The caller supplies an unused ID and concrete local/remote addresses.
func (s *UDPServer) OpenPath(ctx context.Context, c *Conn, id uint64, local, remote netip.AddrPort) (netip.AddrPort, error) {
	if !local.IsValid() || local.Addr().IsUnspecified() || local.Addr().IsMulticast() {
		return netip.AddrPort{}, errPathBinding
	}
	if _, err := canonicalPathTuple(netip.AddrPortFrom(local.Addr(), 1), remote); err != nil {
		return netip.AddrPort{}, err
	}
	err := s.Wait(ctx, func(e *ServerEndpoint) (bool, error) {
		if _, ok := e.router.entries[c]; !ok || e.networks[c] == nil {
			return true, errConnectionOwner
		}
		if err := c.check(); err != nil {
			return true, err
		}
		if err := e.networks[c].activate(time.Now()); err != nil {
			return true, err
		}
		p := c.multipath
		if p == nil || c.config.Server || id <= p.highestInitiated || id > 3 || id > p.localMax || id > p.peerMax || p.abandoned[id] != nil {
			return true, errPathInitiation
		}
		state := p.paths[id]
		if state == nil {
			return false, nil
		}
		if state.application != nil {
			return true, errPathInitiation
		}
		if len(state.peer.active[state.peer.current].cid) == 0 {
			return false, nil
		}
		for seq := range state.local.active {
			if state.local.sent[seq] {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return netip.AddrPort{}, err
	}
	network := "udp6"
	if local.Addr().Unmap().Is4() {
		network = "udp4"
	}
	socket, err := net.ListenUDP(network, net.UDPAddrFromAddrPort(local))
	if err != nil {
		return netip.AddrPort{}, err
	}
	address := canonicalPeer(socket.LocalAddr().(*net.UDPAddr).AddrPort())
	registered, success := false, false
	defer func() {
		if !success {
			socket.Close()
			if registered {
				_ = s.removeSocket(context.Background(), address)
			}
		}
	}()
	if err = s.addSocket(ctx, socket); err != nil {
		return netip.AddrPort{}, err
	}
	registered = true
	if err = s.initiatePath(ctx, c, id, address, remote); err != nil {
		return netip.AddrPort{}, err
	}
	err = s.Wait(ctx, func(e *ServerEndpoint) (bool, error) {
		if _, ok := e.router.entries[c]; !ok {
			return true, errConnectionOwner
		}
		if err := c.check(); err != nil {
			return true, err
		}
		p := c.multipath
		if p == nil || p.abandoned[id] != nil {
			return true, errors.New("quic: path abandoned during validation")
		}
		state := p.paths[id]
		if state == nil || state.application == nil {
			return true, errPathInitiation
		}
		return state.application.validation.state == PathValidated, nil
	})
	if err != nil {
		return netip.AddrPort{}, err
	}
	success = true
	return address, nil
}

// ClosePathSocket retires a socket allocated by OpenPath.
func (s *UDPServer) ClosePathSocket(ctx context.Context, address netip.AddrPort) error {
	return s.removeSocket(ctx, address)
}
