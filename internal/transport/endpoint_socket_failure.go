package transport

import (
	"net/netip"

	"github.com/getlantern/photon/internal/wire"
)

// failLocalSocket runs on the serialized owner after a reader terminates. All
// affected paths are stopped before evacuation, so data cannot move from one
// failed path onto another path backed by the same failed socket.
func (e *ServerEndpoint) failLocalSocket(local netip.AddrPort, cause error) {
	local = canonicalPeer(local)
	if e.networks != nil && !e.locals[local] {
		return // An explicit removal may precede its reader's terminal event.
	}
	delete(e.locals, local)
	for _, c := range append([]*Conn(nil), e.conns...) {
		owner := e.networks[c]
		if owner == nil {
			c.fail(cause)
			e.Remove(c)
			continue
		}
		if c.multipath == nil {
			if owner.primary.local == local {
				c.fail(cause)
				e.Remove(c)
			}
			continue
		}
		if c.Closing() {
			if c.closing.network != nil && c.closing.network.local == local {
				e.Remove(c)
			}
			continue
		}
		if c.Draining() {
			continue
		}
		if err := e.serviceLocalMigrations(c); err != nil {
			c.fail(err)
			e.Remove(c)
			continue
		}
		var affected []uint64
		for id, state := range c.multipath.paths {
			app := state.application
			// Peer-initiated preferred migration can use a different local
			// socket from the established path. Cancel it before scheduling
			// can emit another probe through the failed socket. The normal
			// probe scheduler retires any used reservation, retrying on pressure.
			if app != nil && app.migration != nil && app.migration.candidate != nil && app.migration.candidate.tuple.local == local {
				app.migration.candidate = nil
			}
			if c.multipath.abandoned[id] == nil && app != nil && app.network != nil && app.network.local == local {
				affected = append(affected, id)
			}
		}
		if len(affected) == 0 {
			continue
		}
		failed := false
		for _, id := range affected {
			if _, err := c.multipath.stop(id); err != nil {
				failed = true
				break
			}
		}
		dest, ok := c.survivingPath(^uint64(0))
		if failed || !ok {
			c.fail(cause)
			e.Remove(c)
			continue
		}
		for _, id := range affected {
			if err := c.queueFrameOnPath(dest, wire.PathAbandonFrame{PathID: id}); err != nil {
				failed = true
				break
			}
			if _, err := c.multipath.beginAbandon(id); err != nil {
				failed = true
				break
			}
			if err := c.evacuatePath(id); err != nil {
				failed = true
				break
			}
		}
		if failed {
			c.fail(cause)
			e.Remove(c)
			continue
		}
		e.sync(c)
	}
}
