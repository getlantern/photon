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
	if target.mtuNeedsValidation || target.space.ackPending || target.probePending || len(target.space.queue) != 0 || ((dest != 0 || target.mtuNeedsValidation) && target.validation.state != PathValidated) {
		return nil, nil
	}
	for _, source := range ids {
		state := p.paths[source]
		if source == dest || p.abandoned[source] != nil || state.application.probePending {
			continue
		}
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
			if item.acked {
				continue
			}
			switch item.frame.(type) {
			case wire.StreamFrame, wire.DatagramFrame:
			default:
				continue
			}
			if frame, ok := item.frame.(wire.DatagramFrame); ok {
				peer := p.paths[dest].peer
				if !datagramFitsPath(frame, target.packetPayloadLimit(), len(peer.active[peer.current].cid)) {
					// Keep it on the source rather than dropping it on a smaller
					// idle path. Borrow rollback must not resurrect a dropped item.
					continue
				}
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
