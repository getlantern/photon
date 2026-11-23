package transport

// applicationEnqueuePath distributes new stream/datagram frames over validated
// paths, preferring available paths to backups. Selection does not mutate state;
// enqueue advances the cursor only after admitting the item. Actual packet
// output still enforces each path's congestion, pacing and amplification limits.
// The packet scheduler can borrow queued data when another validated path is idle.
func (c *Conn) applicationEnqueuePath() (uint64, bool) {
	p := c.multipath
	for _, backup := range []bool{false, true} {
		var first, next uint64
		haveFirst, haveNext := false, false
		for id, state := range p.paths {
			app := state.application
			if app == nil || p.abandoned[id] != nil || state.backup != backup || len(state.peer.active[state.peer.current].cid) == 0 {
				continue
			}
			if app.mtuNeedsValidation || id != 0 && app.validation.state != PathValidated {
				continue
			}
			if !haveFirst || id < first {
				first, haveFirst = id, true
			}
			if p.haveEnqueued && id > p.lastEnqueued && (!haveNext || id < next) {
				next, haveNext = id, true
			}
		}
		if haveNext {
			return next, true
		}
		if haveFirst {
			return first, true
		}
	}
	return 0, false
}
