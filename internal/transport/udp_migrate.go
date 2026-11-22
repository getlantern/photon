package transport

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"time"
)

// Migrate opens a client socket and migrates the primary path. Completion wins
// a cancellation race once the owner has committed the new binding.
func (s *UDPServer) Migrate(ctx context.Context, c *Conn, local netip.AddrPort) error {
	return s.MigratePath(ctx, c, 0, local)
}

// MigratePath migrates an established client path without changing its ID.
func (s *UDPServer) MigratePath(ctx context.Context, c *Conn, id uint64, local netip.AddrPort) error {
	if !local.IsValid() || local.Addr().IsUnspecified() || local.Addr().IsMulticast() {
		return errPathBinding
	}
	var old netip.AddrPort
	if err := s.Do(ctx, func(e *ServerEndpoint) error {
		if _, ok := e.router.entries[c]; !ok || e.networks[c] == nil {
			return errConnectionOwner
		}
		if err := c.check(); err != nil {
			return err
		}
		if c.config.Server || !c.confirmed || c.multipath == nil || c.multipath.abandoned[id] != nil || c.multipath.paths[id] == nil {
			return errPathInitiation
		}
		app := c.multipath.paths[id].application
		if app == nil || app.network == nil || app.localMigration != nil {
			return errPathInitiation
		}
		return nil
	}); err != nil {
		return err
	}
	network := "udp6"
	if local.Addr().Unmap().Is4() {
		network = "udp4"
	}
	socket, err := net.ListenUDP(network, net.UDPAddrFromAddrPort(local))
	if err != nil {
		return err
	}
	address := canonicalPeer(socket.LocalAddr().(*net.UDPAddr).AddrPort())
	if err = s.addSocket(ctx, socket); err != nil {
		socket.Close()
		return err
	}
	var candidate *migrationCandidate
	err = s.Wait(ctx, func(e *ServerEndpoint) (bool, error) {
		if _, ok := e.router.entries[c]; !ok || e.networks[c] == nil {
			return true, errConnectionOwner
		}
		if !e.locals[address] {
			return true, errPathBinding
		}
		var prepareErr error
		candidate, prepareErr = c.prepareLocalMigration(id, address, time.Now())
		if prepareErr == nil {
			// Socket opening may overlap a completed migration. Capture the
			// predecessor in the same transaction that reserves this candidate.
			old = c.multipath.paths[id].application.network.local
		}
		if errors.Is(prepareErr, errPathCIDCapacity) {
			return false, nil
		}
		return true, prepareErr
	})
	if err == nil {
		err = s.Wait(ctx, func(e *ServerEndpoint) (bool, error) {
			if candidate.committed {
				return true, nil
			}
			if _, ok := e.router.entries[c]; !ok {
				return true, errConnectionOwner
			}
			if err := c.check(); err != nil {
				return true, err
			}
			if c.multipath == nil || c.multipath.abandoned[id] != nil || c.multipath.paths[id].application.localMigration != candidate || candidate.validation.state == PathValidationFailed {
				return true, errMigrationState
			}
			return false, nil
		})
	}
	// Finalize on the owner even when the caller canceled: never close a socket
	// concurrently with a commit or leave a canceled candidate eligible to commit.
	committed := false
	cleanupErr := s.Do(context.Background(), func(e *ServerEndpoint) error {
		committed = candidate != nil && candidate.committed
		if committed {
			s.removeUnusedMigrationSocket(e, old)
			return nil
		}
		if _, ok := e.router.entries[c]; ok && candidate != nil && c.multipath != nil && c.multipath.paths[id] != nil {
			app := c.multipath.paths[id].application
			if app != nil && app.localMigration == candidate {
				candidate.validation.state = PathValidationFailed
				candidate.responsePending = nil
				if cancelErr := c.cancelLocalMigration(id, candidate); cancelErr != nil && !errors.Is(cancelErr, ErrSendBuffer) {
					return cancelErr
				}
			}
		}
		return s.removeSocketOnOwner(e, address)
	})
	if committed {
		return nil
	}
	if cleanupErr != nil {
		socket.Close()
	}
	if err != nil {
		return err
	}
	return cleanupErr
}

// A primary socket may also back other connections or paths. Retire it only
// after checking all current bindings and pending migrations on this owner.
func (s *UDPServer) removeUnusedMigrationSocket(e *ServerEndpoint, local netip.AddrPort) {
	if !e.locals[local] || len(e.locals) <= 1 {
		return
	}
	for c, owner := range e.networks {
		if c.multipath == nil {
			if owner.primary.local == local {
				return
			}
			continue
		}
		if c.Closing() && c.closing.network != nil && c.closing.network.local == local {
			return
		}
		for id, state := range c.multipath.paths {
			if c.multipath.abandoned[id] != nil || state.application == nil {
				continue
			}
			app := state.application
			if app.network != nil && app.network.local == local {
				return
			}
			if app.localMigration != nil && app.localMigration.tuple.local == local {
				return
			}
		}
	}
	_ = s.removeSocketOnOwner(e, local)
}
