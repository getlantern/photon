package transport

import "errors"

// serviceLocalMigrations runs before output on the serialized socket owner.
// A dead candidate socket cancels the attempt; a validated live socket can
// become the established binding. Queue pressure leaves retryable ownership.
func (e *ServerEndpoint) serviceLocalMigrations(c *Conn) error {
	if c.config.Server || c.multipath == nil || c.Closing() || c.Draining() {
		return nil
	}
	for id, state := range c.multipath.paths {
		if state.application == nil || c.multipath.abandoned[id] != nil {
			continue
		}
		candidate := state.application.localMigration
		if candidate == nil {
			continue
		}
		var err error
		if !e.localSocketAvailable(c, candidate.tuple.local) {
			candidate.validation.state = PathValidationFailed
			candidate.responsePending = nil
			err = c.cancelLocalMigration(id, candidate)
		} else if candidate.validation.state == PathValidationFailed {
			err = c.cancelLocalMigration(id, candidate)
		} else if candidate.validation.state == PathValidated {
			// A peer retirement can revoke the reservation between proof and commit.
			if _, owned := state.peer.migrationID(candidate, candidate.peerSequence); !owned {
				candidate.validation.state = PathValidationFailed
				candidate.failure = errMigrationState
				candidate.responsePending = nil
				candidate.responseTail = pathResponseQueue{}
				err = c.cancelLocalMigration(id, candidate)
			} else if !candidate.explicitSwitch {
				err = c.commitLocalMigration(id, candidate)
			}
		}
		if err != nil && !errors.Is(err, ErrSendBuffer) && !errors.Is(err, errOperationWouldBlock) {
			return err
		}
	}
	return nil
}
