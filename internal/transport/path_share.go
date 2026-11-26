package transport

import (
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// nextSharedPathData lets an idle validated path carry queued application data
// from another path. Borrowing is transactional: a blocked packet attempt leaves
// ownership at the source (which may now contain split children). Control/probe frames retain their path affinity.
// The same sendItem retains memory charges and any earlier sent-packet references.
func (c *Conn) nextSharedPathData(dest uint64, ids []uint64, now time.Time) ([]byte, error) {
	p := c.multipath
	target := p.paths[dest].application
	// An ACK owed, or the path's own work, goes first.
	if !c.mayBorrow(dest) || target.space.ackPending || len(target.space.queue) != 0 {
		return nil, nil
	}
	for _, source := range ids {
		if !c.mayLend(source, dest) {
			continue
		}
		state := p.paths[source]
		queue := state.application.space
		preferred := c.prioritizedStreamIndex(queue)
		for offset := 0; offset < len(queue.queue); offset++ {
			index := offset
			if preferred >= 0 {
				if offset == 0 {
					index = preferred
				} else if offset <= preferred {
					index = offset - 1
				}
			}
			item := queue.queue[index]
			if !c.borrowable(item, dest) {
				continue
			}
			// Split in the source queue before borrowing. Otherwise a blocked
			// destination could discard the new tail during rollback.
			if frame, ok := item.frame.(wire.StreamFrame); ok && len(frame.Data) > target.streamPayloadLimit() {
				if err := c.splitQueuedStream(queue, index, portableStreamPayload); err != nil {
					return nil, err
				}
				item = queue.queue[index]
			}
			target.space.queue = append(target.space.queue, item)
			packet, err := c.nextPacketOnPath(dest, now)
			if err != nil || packet == nil {
				// The item stays owned by its original queue until a packet is committed.
				clear(target.space.queue)
				target.space.queue = target.space.queue[:0]
				return packet, err
			}
			copy(queue.queue[index:], queue.queue[index+1:])
			queue.queue[len(queue.queue)-1] = nil
			queue.queue = queue.queue[:len(queue.queue)-1]
			return packet, nil
		}
	}
	return nil, nil
}

// mayBorrow reports whether path dest is eligible to carry another path's
// queued data: usable, validated (path zero excepted), and with no probe or
// MTU validation of its own pending. nextSharedPathData also waits while dest
// owes an ACK or has its own queue to send.
func (c *Conn) mayBorrow(dest uint64) bool {
	p := c.multipath
	state := p.paths[dest]
	if state == nil || state.application == nil || p.abandoned[dest] != nil || len(state.peer.active[state.peer.current].cid) == 0 {
		return false
	}
	target := state.application
	return !target.mtuNeedsValidation && !target.probePending && (dest == 0 || target.validation.state == PathValidated)
}

// mayLend reports whether path source's queue may lend to path dest.
func (c *Conn) mayLend(source, dest uint64) bool {
	state := c.multipath.paths[source]
	return source != dest && state != nil && state.application != nil && c.multipath.abandoned[source] == nil && !state.application.probePending
}

// borrowable reports whether item, queued on another path, can be carried by
// path dest: unacknowledged STREAM data, or a DATAGRAM that fits dest. Control
// and probe frames keep their path; a DATAGRAM too large for dest stays on its
// source rather than being dropped on the smaller path.
func (c *Conn) borrowable(item *sendItem, dest uint64) bool {
	if item.acked {
		return false
	}
	switch f := item.frame.(type) {
	case wire.StreamFrame:
		return true
	case wire.DatagramFrame:
		state := c.multipath.paths[dest]
		peer := state.peer
		return datagramFitsPath(f, state.application.packetPayloadLimit(), len(peer.active[peer.current].cid))
	}
	return false
}
