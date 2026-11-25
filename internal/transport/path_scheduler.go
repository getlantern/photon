package transport

import (
	"slices"
	"sort"
	"time"
)

// nextMultipathPacket services pending path responses before ordinary ACK/data
// selection. It prefers ACKs on their source path, but can send them on any
// surviving path with credit. Other
// output uses round robin within available/backup preference groups. This is
// an initial scheduling policy, not a throughput optimality claim. Idle validated
// paths can carry queued application frames from another path.
func (c *Conn) nextMultipathPacket(now time.Time) (uint64, []byte, error) {
	if c.pendingWrite != nil && !c.Closing() && !c.Draining() && !c.closed {
		return 0, nil, nil
	}
	if err := c.check(); err != nil {
		return 0, nil, err
	}
	if c.multipath == nil || !c.confirmed {
		return 0, nil, ErrHandshakeUnconfirmed
	}
	p := c.multipath
	if p.baseOnly {
		// Base mode owns exactly path zero: no path sorting, backup selection or
		// cross-path queue search is needed. Packet construction retains all gates.
		state := p.paths[0]
		if state == nil || state.application == nil || p.abandoned[0] != nil || len(state.peer.active[state.peer.current].cid) == 0 {
			return 0, nil, nil
		}
		packet, err := c.nextPacketOnPath(0, now)
		return 0, packet, err
	}
	// Negotiated path state is bounded at 64, including retained paths.
	var storage [64]uint64
	ids := storage[:0]
	for id, state := range p.paths {
		if state.application != nil {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	usable := func(id uint64) bool {
		state := p.paths[id]
		return p.abandoned[id] == nil && len(state.peer.active[state.peer.current].cid) > 0
	}
	// Service pending responses on their receive path before unrelated ACKs.
	// Blocked responses retain all send gates and do not stop other paths.
	for _, id := range ids {
		if !usable(id) || !p.paths[id].application.hasPathResponse() {
			continue
		}
		b, err := c.nextPacketOnPathMode(id, id, now, false)
		if err != nil {
			return 0, nil, err
		}
		if b != nil {
			return id, b, nil
		}
	}
	// Retained abandoned receive spaces remain eligible ACK sources.
	for _, source := range ids {
		p.paths[source].application.space.expireACKDelay(now)
		if !p.paths[source].application.space.ackPending {
			continue
		}
		if usable(source) {
			b, err := c.nextPacketOnPathACK(source, source, now)
			if err != nil {
				return 0, nil, err
			}
			if b != nil {
				return source, b, nil
			}
		}
		for _, dest := range ids {
			if dest == source || !usable(dest) {
				continue
			}
			b, err := c.nextPacketOnPathACK(dest, source, now)
			if err != nil {
				return 0, nil, err
			}
			if b != nil {
				return dest, b, nil
			}
		}
	}
	start := 0
	if p.haveScheduled {
		start = sort.Search(len(ids), func(i int) bool { return ids[i] > p.lastScheduled })
	}
	for _, backup := range []bool{false, true} {
		for offset := 0; offset < len(ids); offset++ {
			id := ids[(start+offset)%len(ids)]
			if !usable(id) || p.paths[id].backup != backup {
				continue
			}
			b, err := c.nextPacketOnPathMode(id, id, now, false)
			if err == nil && b == nil {
				b, err = c.nextSharedPathData(id, ids, now)
			}
			if err != nil {
				return 0, nil, err
			}
			if b != nil {
				p.lastScheduled = id
				p.haveScheduled = true
				return id, b, nil
			}
		}
	}
	// Discovery is background traffic. Try ordinary output (including borrowing
	// on backup paths) everywhere before committing any larger padded probe.
	for _, backup := range []bool{false, true} {
		for offset := 0; offset < len(ids); offset++ {
			id := ids[(start+offset)%len(ids)]
			if !usable(id) || p.paths[id].backup != backup || p.paths[id].application.mtu == nil {
				continue
			}
			b, err := c.nextPacketOnPathMode(id, id, now, true)
			if err != nil {
				return 0, nil, err
			}
			if b != nil {
				p.lastScheduled, p.haveScheduled = id, true
				return id, b, nil
			}
		}
	}
	return 0, nil, nil
}
