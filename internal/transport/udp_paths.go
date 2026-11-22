package transport

import (
	"context"
	"errors"
	"net/netip"
	"time"
)

var errConnectionOwner = errors.New("quic: connection does not belong to this runtime")

// initiatePath validates runtime/socket ownership before allocating a path or
// queuing its first probe. An unknown local socket is a local request error,
// not a connection failure. The core then validates role, limits, CIDs and reuse.
func (s *UDPServer) initiatePath(ctx context.Context, c *Conn, id uint64, local, remote netip.AddrPort) error {
	tuple, err := canonicalPathTuple(local, remote)
	if err != nil {
		return err
	}
	return s.Do(ctx, func(e *ServerEndpoint) error {
		// Check identity in the owner's registry before reading any Conn fields;
		// a foreign connection may be concurrently owned by another goroutine.
		if _, ok := e.router.entries[c]; !ok || e.networks[c] == nil {
			return errConnectionOwner
		}
		if !e.locals[tuple.local] {
			return errPathBinding
		}
		return c.initiateMultipathPath(id, tuple.local, tuple.remote)
	})
}

// prepareLocalMigration checks runtime and registered socket ownership before
// the engine may reserve a CID or construct traffic from a new local address.
func (s *UDPServer) prepareLocalMigration(ctx context.Context, c *Conn, id uint64, local netip.AddrPort) error {
	local = canonicalPeer(local)
	return s.Do(ctx, func(e *ServerEndpoint) error {
		if _, ok := e.router.entries[c]; !ok || e.networks[c] == nil {
			return errConnectionOwner
		}
		if !e.locals[local] {
			return errPathBinding
		}
		_, err := c.prepareLocalMigration(id, local, time.Now())
		return err
	})
}
