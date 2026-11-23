package transport

import (
	"github.com/getlantern/photon/internal/wire"
	"time"
)

// receiveMigrationResponse runs only after authentication. A token validates
// the tuple on which its challenge was sent, not the response's arrival tuple.
func (c *Conn) receiveMigrationResponse(id uint64, frame wire.PathResponseFrame, now time.Time) error {
	state := c.multipath.paths[id]
	if state == nil || state.application == nil || c.multipath.abandoned[id] != nil {
		return nil
	}
	app := state.application
	if candidate := app.localMigration; candidate != nil {
		if _, owned := state.peer.migrationID(candidate, candidate.peerSequence); owned {
			candidate.responseProof(frame, now)
		}
	}
	if app.migration != nil && app.migration.candidate != nil {
		candidate := app.migration.candidate
		if candidate.responseProof(frame, now) {
			_, err := c.commitPeerRebinding(id, candidate)
			return err
		}
	}
	return nil
}
