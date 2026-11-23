package transport

import (
	"github.com/getlantern/photon/internal/wire"
	"net/netip"
	"time"
)

// prepareLocalMigration reserves a new local tuple and a fresh destination CID.
// The socket owner must first register the concrete local address. This operation
// sends no packet, moves no binding, and changes no shared packet/recovery state.
func (c *Conn) prepareLocalMigration(id uint64, local netip.AddrPort, now time.Time) (*migrationCandidate, error) {
	return c.prepareClientMigration(id, local, netip.AddrPort{}, false, now)
}

// prepareLocalPathProbe reserves a candidate for separate validation and switching.
// Only probing frames may be sent on it until the caller commits it explicitly.
// The established path remains eligible for ordinary application traffic.
func (c *Conn) prepareLocalPathProbe(id uint64, local netip.AddrPort, now time.Time) (*migrationCandidate, error) {
	candidate, err := c.prepareLocalMigration(id, local, now)
	if err != nil {
		return nil, err
	}
	candidate.explicitSwitch = true
	return candidate, nil
}

// preparePreferredMigration authorizes only a destination advertised in this
// connection's authenticated transport parameters. Socket selection stays with
// the owner; this operation does not send or switch the established binding.
func (c *Conn) preparePreferredMigration(local, remote netip.AddrPort, now time.Time) (*migrationCandidate, error) {
	return c.prepareClientMigration(0, local, remote, true, now)
}

func (c *Conn) preferredDestination(remote netip.AddrPort) bool {
	if c.peerParams == nil {
		return false
	}
	return matchesPreferredAddress(c.peerParams.PreferredAddress, remote)
}

func matchesPreferredAddress(p *wire.PreferredAddress, remote netip.AddrPort) bool {
	if p == nil || !remote.IsValid() {
		return false
	}
	remote = canonicalPeer(remote)
	v4 := netip.AddrFrom4(p.IPv4)
	v6 := netip.AddrFrom16(p.IPv6)
	return (p.IPv4Port != 0 && !v4.IsUnspecified() && remote == netip.AddrPortFrom(v4, p.IPv4Port)) ||
		(p.IPv6Port != 0 && !v6.IsUnspecified() && remote == netip.AddrPortFrom(v6, p.IPv6Port))
}

func (c *Conn) prepareClientMigration(id uint64, local, remote netip.AddrPort, preferred bool, now time.Time) (*migrationCandidate, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	if c.config.Server || !c.confirmed || c.peerParams == nil || c.multipath == nil {
		return nil, errPathInitiation
	}
	state := c.multipath.paths[id]
	if state == nil || state.application == nil || c.multipath.abandoned[id] != nil {
		return nil, errPathInitiation
	}
	app := state.application
	if app.network == nil || app.localMigration != nil || (app.migration != nil && app.migration.candidate != nil) || (id != 0 && app.validation.state != PathValidated) {
		return nil, errPathInitiation
	}
	if preferred {
		if id != 0 || !c.preferredDestination(remote) {
			return nil, errPathInitiation
		}
	} else {
		remote = app.network.remote
	}
	tuple, err := canonicalPathTuple(local, remote)
	if err != nil {
		return nil, err
	}
	if tuple == *app.network || (!preferred && tuple.local == app.network.local) || tuple.local.Addr().Is4() != tuple.remote.Addr().Is4() {
		return nil, errPathBinding
	}
	if tuple.local != app.network.local && c.peerParams.DisableActiveMigration && (!c.handshakeRemote.IsValid() || tuple.remote == c.handshakeRemote) {
		return nil, errPathInitiation
	}
	candidate, err := newUncreditedMigrationCandidate(tuple, now, c.migrationPTO(app))
	if err != nil {
		return nil, err
	}
	candidate.freshCID = true
	candidate.preferred = preferred
	candidate.peerSequence, candidate.hasPeerSequence = state.peer.reserveMigrationID(candidate)
	if !candidate.hasPeerSequence {
		return nil, errPathCIDCapacity
	}
	// Clients are not subject to the server's anti-amplification limit. Reachability
	// validation is still pending and must not be inferred from this budget flag.
	candidate.budget.Validate()
	app.localMigration = candidate
	return candidate, nil
}

// cancelLocalMigration commits cancellation only with any required reliable CID
// retirement. On queue pressure the pending operation remains available to retry.
func (c *Conn) cancelLocalMigration(id uint64, candidate *migrationCandidate) error {
	if err := c.check(); err != nil {
		return err
	}
	if c.multipath == nil {
		return errPathInitiation
	}
	state := c.multipath.paths[id]
	if state == nil || state.application == nil || candidate == nil || state.application.localMigration != candidate {
		return errMigrationState
	}
	app := state.application
	app.localMigration = nil
	var keep *migrationCandidate
	if app.migration != nil {
		keep = app.migration.candidate
	}
	if err := c.retireMigrationReservations(id, keep); err != nil {
		app.localMigration = candidate
		return err
	}
	return nil
}
