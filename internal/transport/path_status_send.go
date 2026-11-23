package transport

import (
	"context"
	"errors"

	"github.com/getlantern/photon/internal/wire"
)

// setPathBackup advertises a receive preference; the peer's own status remains
// separate and continues to govern our outgoing application scheduler.
func (c *Conn) setPathBackup(id uint64, backup bool) error {
	if err := c.check(); err != nil {
		return err
	}
	p := c.multipath
	if p == nil || !c.confirmed {
		return ErrHandshakeUnconfirmed
	}
	state := p.paths[id]
	if id > p.peerMax || state == nil || state.application == nil || p.abandoned[id] != nil {
		return errPathInitiation
	}
	if state.localStatusKnown && state.localBackup == backup {
		return nil
	}
	sequence := uint64(0)
	if state.localStatusKnown {
		if state.localStatusSequence == wire.MaxVarint {
			return errors.New("quic: path status sequence exhausted")
		}
		sequence = state.localStatusSequence + 1
	}
	if err := c.queuePathControl(id, wire.PathStatusFrame{PathID: id, Sequence: sequence, Backup: backup}); err != nil {
		return err
	}
	state.localStatusKnown, state.localStatusSequence, state.localBackup = true, sequence, backup
	// Only the most recent status needs recovery. Retire obsolete references in
	// every path's queued/sent ledger, preserving late-ACK accounting.
	c.retireFrames(func(f wire.Frame) bool {
		status, ok := f.(wire.PathStatusFrame)
		return ok && status.PathID == id && status.Sequence < sequence
	})
	return nil
}

// SetPathBackup serializes the status update with packet output and recovery.
func (s *UDPServer) SetPathBackup(ctx context.Context, c *Conn, id uint64, backup bool) error {
	return s.Do(ctx, func(e *ServerEndpoint) error {
		if _, ok := e.router.entries[c]; !ok || e.networks[c] == nil {
			return errConnectionOwner
		}
		return c.setPathBackup(id, backup)
	})
}
