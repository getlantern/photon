package transport

import (
	"context"
	"errors"
	"net/netip"
	"sync/atomic"
	"time"
)

var ErrPathClosed = errors.New("quic: path closed")
var ErrPathNotValidated = errors.New("quic: path not yet validated")
var errActiveMigrationPath = errors.New("quic: cannot close the active migration path")

// MigrationPath associates a foreign socket with the primary path. Mutable
// state belongs to owner; addresses are a separately published immutable pair.
// Probe callers serialize through a context-aware gate, without holding an
// owner goroutine or preventing Close from canceling an in-progress probe.
type MigrationPath struct {
	owner     *UDPServer
	conn      *Conn
	route     *handoffRoute
	local     netip.AddrPort
	addresses atomic.Pointer[pathTuple]
	gate      chan struct{}
	candidate *migrationCandidate
	closed    bool
}

// AddMigrationPath associates an already-owned socket, without probing it or
// transferring socket ownership. Cross-owner setup calls occur outside callbacks.
func (s *UDPServer) AddMigrationPath(ctx context.Context, c *Conn, source *UDPServer) (*MigrationPath, error) {
	if source == nil || source == s {
		return nil, errPathBinding
	}
	check := func(e *ServerEndpoint) error {
		if _, ok := e.router.entries[c]; !ok || e.networks[c] == nil {
			return errConnectionOwner
		}
		if err := c.check(); err != nil {
			return err
		}
		if err := e.networks[c].activate(time.Now()); err != nil {
			return err
		}
		if c.config.Server || !c.confirmed || c.multipath == nil || c.multipath.paths[0] == nil || c.multipath.paths[0].application == nil {
			return errPathInitiation
		}
		if c.peerParams.DisableActiveMigration {
			return errPathInitiation
		}
		return nil
	}
	if err := s.Do(ctx, check); err != nil {
		return nil, err
	}
	h, err := s.handoffTarget(ctx)
	if err != nil {
		return nil, err
	}
	writer, err := source.foreignWriteTarget(ctx)
	if err != nil {
		return nil, err
	}
	route, err := source.registerHandoffRoute(ctx, h, nil)
	if err != nil {
		return nil, err
	}
	p := &MigrationPath{owner: s, conn: c, route: route, local: route.local, gate: make(chan struct{}, 1)}
	p.gate <- struct{}{}
	err = s.Do(ctx, func(e *ServerEndpoint) error {
		if err := check(e); err != nil {
			return err
		}
		network := c.multipath.paths[0].application.network
		if network == nil || network.local == route.local || network.remote.Addr().Is4() != route.local.Addr().Is4() {
			return errPathBinding
		}
		if err := h.bindConnection(route, c); err != nil {
			return err
		}
		if err := e.bindForeignWriter(c, writer, route); err != nil {
			h.unsubscribe(route)
			return err
		}
		p.addresses.Store(&pathTuple{route.local, network.remote})
		return nil
	})
	if err != nil {
		route.revoke()
		_ = source.closeHandoffRoute(context.Background(), route)
		return nil, err
	}
	return p, nil
}

func (p *MigrationPath) Addresses() (netip.AddrPort, netip.AddrPort) {
	tuple := p.addresses.Load()
	return tuple.local, tuple.remote
}
func (p *MigrationPath) available(e *ServerEndpoint) error {
	if p.closed {
		return ErrPathClosed
	}
	if _, ok := e.router.entries[p.conn]; !ok {
		return ErrPathClosed
	}
	if err := p.conn.check(); err != nil {
		return err
	}
	if p.candidate != nil && p.candidate.failure != nil {
		return p.candidate.failure
	}
	if p.route.revoked.Load() {
		return ErrPathClosed
	}
	if candidate := p.candidate; candidate != nil && !candidate.committed {
		state := p.conn.multipath.paths[0]
		if state == nil || state.application == nil || p.conn.multipath.abandoned[0] != nil || state.application.localMigration != candidate {
			return errMigrationState
		}
		if _, owned := state.peer.migrationID(candidate, candidate.peerSequence); !owned {
			return errMigrationState
		}
	}
	h := p.route.target
	h.mu.Lock()
	publication := h.connections[p.conn][p.route]
	var publicationErr error
	if publication != nil {
		publicationErr = publication.err
	}
	h.mu.Unlock()
	return publicationErr
}
func (p *MigrationPath) active() bool {
	c := p.conn
	if p.candidate == nil || !p.candidate.committed || p.route.revoked.Load() {
		return false
	}
	if c.multipath == nil || c.multipath.paths[0] == nil || c.multipath.paths[0].application == nil {
		return false
	}
	network := c.multipath.paths[0].application.network
	return network != nil && network.local == p.local
}

func (p *MigrationPath) Probe(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.owner.Done():
		return ErrPathClosed
	case <-p.gate:
	}
	defer func() { p.gate <- struct{}{} }()
	err := p.owner.Wait(ctx, func(e *ServerEndpoint) (bool, error) {
		if err := p.available(e); err != nil {
			return true, err
		}
		if p.candidate != nil {
			if p.candidate.committed {
				if p.active() {
					return true, nil
				}
				return true, ErrPathClosed
			}
			if p.candidate.validation.state == PathValidated {
				return true, nil
			}
			if p.candidate.validation.state == PathValidationFailed {
				return true, errMigrationState
			}
			return true, nil
		}
		if p.conn.pendingWrite != nil {
			return false, nil
		}
		// A canceled attempt can retain its slot until reliable CID retirement
		// fits in the send queue. Let normal output finish that cleanup first.
		if old := p.conn.multipath.paths[0].application.localMigration; old != nil && old.validation.state == PathValidationFailed {
			return false, nil
		}
		var err error
		p.candidate, err = p.conn.prepareLocalPathProbe(0, p.local, time.Now())
		if errors.Is(err, errPathCIDCapacity) {
			return false, nil
		}
		if err == nil {
			tuple := p.candidate.tuple
			p.addresses.Store(&tuple)
		}
		return true, err
	})
	if err == nil {
		err = p.owner.Wait(ctx, func(e *ServerEndpoint) (bool, error) {
			if err := p.available(e); err != nil {
				return true, err
			}
			candidate := p.candidate
			if candidate == nil {
				return true, errMigrationState
			}
			if candidate.committed {
				if p.active() {
					return true, nil
				}
				return true, ErrPathClosed
			}
			if candidate.validation.state == PathValidationFailed {
				return true, errMigrationState
			}
			return candidate.validation.state == PathValidated, nil
		})
	}
	// Resolve cancellation versus successful proof on the owner. A validated path
	// survives caller cancellation; otherwise release the candidate for later use.
	validated := false
	cleanupErr := p.owner.Do(context.Background(), func(e *ServerEndpoint) error {
		candidate := p.candidate
		if p.closed {
			return ErrPathClosed
		}
		if p.available(e) == nil && candidate != nil && ((!candidate.committed && candidate.validation.state == PathValidated) || (candidate.committed && p.active())) {
			validated = true
			return nil
		}
		if candidate != nil && !candidate.committed && p.conn.check() == nil && p.conn.multipath != nil {
			state := p.conn.multipath.paths[0]
			if state != nil && state.application != nil && state.application.localMigration == candidate {
				candidate.validation.state = PathValidationFailed
				candidate.responsePending = nil
				if cancelErr := p.conn.cancelLocalMigration(0, candidate); cancelErr != nil && !errors.Is(cancelErr, ErrSendBuffer) {
					return cancelErr
				}
			}
		}
		p.candidate = nil
		return nil
	})
	if validated {
		return nil
	}
	if err != nil {
		return migrationPathError(err)
	}
	return migrationPathError(cleanupErr)
}

func (p *MigrationPath) Switch() error {
	err := p.owner.Wait(context.Background(), func(e *ServerEndpoint) (bool, error) {
		if err := p.available(e); err != nil {
			return true, err
		}
		candidate := p.candidate
		if candidate == nil || candidate.validation.state != PathValidated {
			return true, ErrPathNotValidated
		}
		if candidate.committed {
			if p.active() {
				return true, nil
			}
			return true, ErrPathClosed
		}
		err := p.conn.commitLocalMigration(0, candidate)
		if errors.Is(err, errOperationWouldBlock) {
			return false, nil
		}
		return true, err
	})
	return migrationPathError(err)
}

func migrationPathError(err error) error {
	if errors.Is(err, ErrEndpointClosed) {
		return ErrPathClosed
	}
	return err
}

func (p *MigrationPath) Close() error {
	err := p.owner.Do(context.Background(), func(e *ServerEndpoint) error {
		if p.closed {
			return nil
		}
		if !p.conn.closed && p.active() {
			return errActiveMigrationPath
		}
		p.closed = true
		if p.conn.Draining() {
			return nil
		} // Retain CID routing until connection disposal.
		defer func() {
			if binding, ok := e.foreignWriters[p.conn][p.local]; ok && binding.route == p.route {
				e.removeForeignWriter(p.conn, p.local)
			}
		}()
		candidate := p.candidate
		if candidate != nil && !candidate.committed && p.conn.multipath != nil {
			state := p.conn.multipath.paths[0]
			if state != nil && state.application != nil && state.application.localMigration == candidate {
				candidate.validation.state = PathValidationFailed
				candidate.failure = ErrPathClosed
				candidate.responsePending = nil
				if err := p.conn.cancelLocalMigration(0, candidate); err != nil && !errors.Is(err, ErrSendBuffer) {
					return err
				}
			}
		}
		return nil
	})
	if errors.Is(err, ErrEndpointClosed) {
		return nil
	}
	return err
}
