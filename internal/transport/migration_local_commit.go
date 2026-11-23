package transport

import "github.com/getlantern/photon/internal/wire"

// commitLocalMigration runs on the socket owner after reachability validation.
// The owner must retain the registered candidate socket through this transition.
// The bounded candidate response queue transfers without application queue space.
func (c *Conn) commitLocalMigration(id uint64, candidate *migrationCandidate) error {
	if err := c.check(); err != nil {
		return err
	}
	if c.config.Server || !c.confirmed || c.multipath == nil {
		return errMigrationState
	}
	state := c.multipath.paths[id]
	if state == nil || state.application == nil || c.multipath.abandoned[id] != nil {
		return errMigrationState
	}
	app := state.application
	if candidate == nil || app.localMigration != candidate || candidate.validation.state != PathValidated || app.network == nil || app.space.discarded {
		return errMigrationState
	}
	if c.pendingWrite != nil {
		return errOperationWouldBlock
	}
	next := candidate.tuple
	if candidate.preferred {
		if id != 0 || !c.preferredDestination(next.remote) || next == *app.network {
			return errMigrationState
		}
	} else if next.local == app.network.local || next.remote != app.network.remote {
		return errMigrationState
	}
	if !candidate.hasPeerSequence {
		return errMigrationState
	}
	if !state.peer.commitMigrationID(candidate, candidate.peerSequence) {
		return errMigrationState
	}
	if id == 0 {
		c.peerCID = clone(state.peer.active[state.peer.current].cid)
	}
	app.resetMigrationRecovery()
	c.retirePathFrames(app.space, func(frame wire.Frame) bool {
		switch frame.(type) {
		case wire.PathChallengeFrame, wire.PathResponseFrame:
			return true
		}
		return false
	})
	app.network = &next
	*app.budget = candidate.budget
	*app.validation = pathValidation{state: PathValidated}
	app.resetMTU()
	app.controlResponses = candidate.responseTail
	candidate.responseTail = pathResponseQueue{}
	app.localMigration = nil
	app.migration = nil
	app.migrationResponse = candidate.responsePending
	candidate.responsePending = nil
	candidate.committed = true
	c.publishPathAddress(id, next)
	if id == 0 {
		c.publishRemoteAddress(next.remote)
	}
	return nil
}
