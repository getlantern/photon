package transport

import "github.com/getlantern/photon/internal/wire"

// commitPeerRebinding preserves packet-number and reliable-frame history. Port
// changes retain recovery estimates; IP changes reset the network recovery
// context under RFC 9000 section 9.4. Local-address changes use commitLocalMigration.
func (c *Conn) commitPeerRebinding(id uint64, candidate *migrationCandidate) (bool, error) {
	state := c.multipath.paths[id]
	if state == nil || state.application == nil || c.multipath.abandoned[id] != nil {
		return false, errMigrationState
	}
	app := state.application
	if app.network == nil || app.migration == nil || candidate == nil || candidate != app.migration.candidate || candidate.validation.state != PathValidated {
		return false, errMigrationState
	}
	if !app.migration.canCommit(candidate) {
		// Reachability proof can arrive before a non-probing nomination.
		// Keep validation without changing CIDs, routing, or recovery.
		return false, nil
	}
	old, next := *app.network, candidate.tuple
	ipChanged := old.remote.Addr() != next.remote.Addr()
	preferred := candidate.preferred && c.config.Server && id == 0 && matchesPreferredAddress(c.config.Parameters.PreferredAddress, next.local)
	if next.local.Addr().Is4() != next.remote.Addr().Is4() || (old.local != next.local && !preferred) || (ipChanged && !c.config.allowIPMigration && !preferred) {
		return false, nil
	}
	if candidate.freshCID {
		if !candidate.hasPeerSequence || !state.peer.commitMigrationID(candidate, candidate.peerSequence) {
			app.migration.candidate = nil
			return false, nil
		}
		if id == 0 {
			c.peerCID = clone(state.peer.active[state.peer.current].cid)
		}
	}
	if _, err := app.migration.commit(candidate); err != nil {
		return false, err
	}
	if ipChanged || old.local != next.local {
		app.resetMigrationRecovery()
	}
	app.network = &next
	c.publishPathAddress(id, next)
	if id == 0 {
		c.publishRemoteAddress(next.remote)
	}
	// Preserve path-zero aliases into the legacy Conn fields.
	*app.budget = candidate.budget
	*app.validation = pathValidation{state: PathValidated}
	app.resetMTU()
	app.controlResponses = candidate.responseTail
	candidate.responseTail = pathResponseQueue{}
	app.migrationResponse = candidate.responsePending
	candidate.responsePending = nil
	app.space.highestNonProbing = app.migration.highestNonProbing
	app.space.haveNonProbing = app.migration.haveNonProbing
	// Responses queued for the previous tuple must not follow the new binding.
	c.retirePathFrames(app.space, func(frame wire.Frame) bool {
		switch frame.(type) {
		case wire.PathChallengeFrame, wire.PathResponseFrame:
			return true
		}
		return false
	})
	return true, nil
}
