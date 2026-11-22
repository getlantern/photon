package transport

import (
	"context"
	"errors"
	"net/netip"
)

// CloseOpenedPath serializes planned abandonment with socket retirement so no
// command can remove the validated alternative between the two operations.
func (s *UDPServer) CloseOpenedPath(ctx context.Context, c *Conn, id uint64, local netip.AddrPort) error {
	local = canonicalPeer(local)
	return s.Do(ctx, func(e *ServerEndpoint) error {
		return s.closeOpenedPathOnOwner(e, c, id, local)
	})
}

// CloseCurrentOpenedPath resolves the latest socket on the owner, including a
// retained address snapshot after abandonment. It cannot race a binding commit.
func (s *UDPServer) CloseCurrentOpenedPath(ctx context.Context, c *Conn, id uint64) error {
	return s.Do(ctx, func(e *ServerEndpoint) error {
		if _, ok := e.router.entries[c]; !ok || e.networks[c] == nil {
			return errConnectionOwner
		}
		local, _, ok := c.CurrentPathAddresses(id)
		if !ok {
			return errPathBinding
		}
		return s.closeOpenedPathOnOwner(e, c, id, local)
	})
}

func (s *UDPServer) closeOpenedPathOnOwner(e *ServerEndpoint, c *Conn, id uint64, local netip.AddrPort) error {
	if _, ok := e.router.entries[c]; !ok || e.networks[c] == nil {
		return errConnectionOwner
	}
	if c.config.Server || id == 0 || c.multipath == nil {
		return errPathInitiation
	}
	p := c.multipath
	state := p.paths[id]
	if state != nil {
		if state.application == nil || state.application.network == nil || state.application.network.local != local {
			return errPathBinding
		}
	} else if p.abandoned[id] == nil {
		return errPathInitiation
	}
	s.socketMu.Lock()
	socket := s.sockets[local]
	s.socketMu.Unlock()
	if socket == nil {
		return errPathBinding
	}
	if !e.locals[local] {
		return nil
	}
	if len(e.locals) <= 1 {
		return errors.New("quic: use Close to remove the last socket")
	}
	// Local request failures leave the socket and connection untouched.
	if err := c.abandonRequestedPath(id); err != nil {
		return err
	}
	return s.removeSocketOnOwner(e, local)
}
