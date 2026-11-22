package transport

import (
	"context"
	"errors"

	"github.com/getlantern/photon/internal/wire"
)

// abandonRequestedPath is a local policy operation. Rejection must leave the
// connection and queues untouched, unlike a fatal network-path failure.
func (c *Conn) abandonRequestedPath(id uint64) error {
	if err := c.check(); err != nil {
		return err
	}
	p := c.multipath
	if p == nil || !c.confirmed {
		return ErrHandshakeUnconfirmed
	}
	if p.abandoned[id] != nil {
		return nil
	}
	source := p.paths[id]
	if id > p.localMax || id > p.peerMax || source == nil || source.application == nil {
		return errors.New("quic: unknown active path")
	}
	if len(p.abandoned) >= maxAbandonedPaths {
		return errPathCIDCapacity
	}
	var dest uint64
	found := false
	for candidate, state := range p.paths {
		app := state.application
		if candidate == id || p.abandoned[candidate] != nil || app == nil || app.network == nil || len(state.peer.active[state.peer.current].cid) == 0 {
			continue
		}
		// Confirmation establishes the primary path. Additional paths require
		// their own completed reachability/MTU validation before a planned handoff.
		if candidate != 0 && app.validation.state != PathValidated {
			continue
		}
		if !found || candidate < dest {
			dest, found = candidate, true
		}
	}
	if !found {
		return errors.New("quic: abandoning path requires a validated alternative")
	}
	// Preflight above makes beginAbandon infallible after queue admission.
	if err := c.queueFrameOnPath(dest, wire.PathAbandonFrame{PathID: id}); err != nil {
		return err
	}
	if _, err := p.beginAbandon(id); err != nil {
		return c.fail(err)
	}
	c.evacuatePathTo(id, dest)
	return nil
}

// AbandonPath changes only this connection's path state; shared UDP sockets
// stay open for other connections and retained in-flight packets.
func (s *UDPServer) AbandonPath(ctx context.Context, c *Conn, id uint64) error {
	return s.Do(ctx, func(e *ServerEndpoint) error {
		if _, ok := e.router.entries[c]; !ok || e.networks[c] == nil {
			return errConnectionOwner
		}
		return c.abandonRequestedPath(id)
	})
}
