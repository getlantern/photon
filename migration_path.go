package quic

import (
	"context"
	"errors"

	"github.com/getlantern/photon/internal/transport"
)

// ErrPathClosed reports that a migration handle or its socket owner is closed.
var ErrPathClosed = transport.ErrPathClosed

// ErrPathNotValidated reports an attempt to switch before Probe succeeds.
var ErrPathNotValidated = transport.ErrPathNotValidated

// AddPath associates a Transport with this client's primary path without probing
// or switching. The Transport must have a concrete address of the current peer's
// address family, and the connection must already have a concrete local binding.
// Probe validates reachability; Switch later selects the socket.
// The Transport retains ownership of its socket. Currently only one uncommitted
// probe candidate per QUIC path is supported; other associations can remain idle.
func (c *Conn) AddPath(t *Transport) (*Path, error) {
	if c == nil || c.server == nil || c.raw == nil || t == nil {
		return nil, errors.New("quic: connection and Transport are required")
	}
	source, err := t.init(1024)
	if err != nil {
		return nil, err
	}
	migration, err := c.server.AddMigrationPath(context.Background(), c.raw, source)
	if err != nil {
		return nil, c.connectionError(err)
	}
	local, remote := migration.Addresses()
	return &Path{server: c.server, conn: c.raw, migration: migration, local: local, remote: remote}, nil
}

// Probe validates an AddPath migration handle without switching ordinary traffic.
// Concurrent calls on one handle serialize with context cancellation. Successful
// validation is reusable; canceling an unfinished probe releases its candidate.
// OpenPath performs validation itself and does not use this migration operation.
func (p *Path) Probe(ctx context.Context) error {
	if p.migration == nil {
		return errors.New("quic: Probe requires an AddPath migration handle")
	}
	return p.migration.Probe(ctx)
}

// Switch selects a successfully probed AddPath handle for primary-path traffic.
// It waits for an outstanding write to finish before committing the new binding.
func (p *Path) Switch() error {
	if p.migration == nil {
		return errors.New("quic: Switch requires an AddPath migration handle")
	}
	return p.migration.Switch()
}
