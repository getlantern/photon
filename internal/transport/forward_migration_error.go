package transport

import (
	"errors"
	"time"

	"github.com/getlantern/photon/internal/recovery"
)

type outboundMigrationProbe struct {
	pathID, number uint64
	app            *pathApplication
	sent           *recovery.Sent
	candidate      *migrationCandidate
	route          *handoffRoute
	idleBefore     idleActivity
	idleRevision   uint64
}

func (c *Conn) outboundMigration(id uint64, tuple pathTuple, route *handoffRoute, before idleActivity) *outboundMigrationProbe {
	if c.multipath == nil || c.Closing() {
		return nil
	}
	state := c.multipath.paths[id]
	if state == nil || state.application == nil {
		return nil
	}
	app := state.application
	candidate := app.localMigration
	if candidate == nil || candidate.committed || candidate.tuple != tuple || app.network == nil || app.network.local == tuple.local || app.space.nextPN == 0 {
		return nil
	}
	return &outboundMigrationProbe{pathID: id, number: app.space.nextPN - 1, app: app, sent: app.sent, candidate: candidate, route: route, idleBefore: before, idleRevision: c.idleRevision}
}

// rejectForeignMigration isolates an uncommitted alternative's write failure.
// Zero-byte rejection removes its latest probe from recovery before resuming the
// established path. Positive byte counts remain in recovery: they are not proof
// that no packet reached the peer. Never revoke a replacement registration or
// cancel a newer candidate because an old write completed late.
func (e *ServerEndpoint) rejectForeignMigration(out *Outbound, result foreignWriteResult, now time.Time) bool {
	if out == nil || out.migration == nil || out.Conn == nil {
		return false
	}
	c, probe := out.Conn, out.migration
	if probe.candidate.committed || c.multipath == nil {
		return false
	}
	state := c.multipath.paths[probe.pathID]
	current := state != nil && state.application == probe.app && probe.app.sent == probe.sent
	if current && result.n == 0 {
		if !probe.sent.RejectMigrationProbe(probe.number) {
			return false
		}
		c.restoreIdleActivity(probe.idleBefore, probe.idleRevision)
		c.armPathRecovery(probe.pathID, now)
	}
	candidate := probe.candidate
	candidate.failure = result.err
	candidate.validation.state = PathValidationFailed
	candidate.responsePending = nil
	candidate.responseTail = pathResponseQueue{}
	if current && probe.app.localMigration == candidate {
		if err := c.cancelLocalMigration(probe.pathID, candidate); err != nil && !errors.Is(err, ErrSendBuffer) {
			return false
		}
	}
	if binding, ok := e.foreignWriters[c][out.Local]; ok && binding.route == probe.route {
		e.removeForeignWriter(c, out.Local)
	}
	c.publishStatistics()
	return true
}
