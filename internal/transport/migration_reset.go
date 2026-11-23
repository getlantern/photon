package transport

import "time"

// resetCandidate admits only a locally initiated attempt that has actually used
// its still-owned peer CID. A remote probing tuple does not gain reset authority.
func (state *pathCIDState) resetCandidate() *migrationCandidate {
	if state.application == nil || state.peer == nil {
		return nil
	}
	candidate := state.application.localMigration
	if candidate == nil || candidate.committed || !candidate.hasPeerSequence || (candidate.validation.state != PathValidating && candidate.validation.state != PathValidated) {
		return nil
	}
	peer, owned := state.peer.migrationID(candidate, candidate.peerSequence)
	if !owned || !peer.used || !peer.hasToken {
		return nil
	}
	return candidate
}

// resetTupleKnown is an owner-side eligibility check. A zero time is permitted
// for routing hints only; final receive checks always enforce candidate expiry.
func (c *Conn) resetTupleKnown(tuple pathTuple, now time.Time) bool {
	if c.multipath == nil {
		return false
	}
	for id, state := range c.multipath.paths {
		if c.multipath.abandoned[id] != nil {
			continue
		}
		app := state.application
		if app != nil && app.network != nil && *app.network == tuple {
			return true
		}
		if c.config.Server {
			continue
		}
		candidate := state.resetCandidate()
		if candidate != nil && candidate.tuple == tuple && (candidate.validation.state == PathValidated || now.IsZero() || now.Before(candidate.validation.expires)) {
			return true
		}
	}
	return false
}
