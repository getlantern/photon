package transport

import "github.com/getlantern/photon/internal/wire"

type streamPriority struct {
	urgency     uint8
	incremental bool
}

type streamPriorityCursor struct {
	id    uint64
	valid bool
}

// SetStreamPriority changes queued and future STREAM scheduling without changing
// flow-control credit, delivery ownership, or already sealed packets.
func (c *Conn) SetStreamPriority(id uint64, urgency int8, incremental bool) {
	if s := c.streams.objects[id]; s != nil && s.send != nil {
		s.priority = streamPriority{uint8(max(int8(0), min(int8(7), urgency))), incremental}
	}
}

func (c *Conn) streamPriority(id uint64) streamPriority {
	if s := c.streams.objects[id]; s != nil {
		return s.priority
	}
	return streamPriority{3, true}
}

// maxQueuedFrames bounds the application frames waiting to be sent before
// new stream data waits too. Choosing each packet's stream scans the queue,
// so its length is the per-packet cost: about 5 ns a frame, so 1024 cost
// about 5 µs. A write of one byte is one frame, so without the bound tiny
// writes could queue tens of thousands. Priority orders only what is
// queued, so the bound is no tighter than the 1 MiB of stream data, in
// frames of about a packet each, that the send buffer has queued.
const maxQueuedFrames = 1024

// checkRetirements fails with CONNECTION_ID_LIMIT_ERROR if queueing adding
// more RETIRE_CONNECTION_IDs on path would leave more than twice our
// active_connection_id_limit waiting to be sent, the bound RFC 9000 Section
// 5.1.2 allows: a peer retiring every earlier ID with each new one, and not
// acknowledging, would otherwise queue one per frame it sends. peerIDs
// already answers each sequence number once. retiresQueued counts those
// queued on each path, less those sent; frames that leave a queue some other
// way leave it high, so only the path's queue, counted exactly, can fail
// the connection, and recounting corrects it.
func (c *Conn) checkRetirements(path uint64, adding int, frameType uint64) error {
	limit := 2 * int(max(c.config.Parameters.ActiveConnectionIDLimit, 2))
	if adding == 0 || c.retiresQueued[path]+adding <= limit {
		return nil
	}
	queue := c.spaces[applicationSpace].queue
	if c.multipath != nil {
		state := c.multipath.paths[path]
		if state == nil || state.application == nil {
			return nil
		}
		queue = state.application.space.queue
	}
	waiting := 0
	for _, item := range queue {
		if _, ok := retirementPath(item.frame); ok && !item.acked {
			waiting++
		}
	}
	if c.retiresQueued == nil {
		c.retiresQueued = map[uint64]int{}
	}
	c.retiresQueued[path] = waiting
	if waiting+adding > limit {
		return &TransportError{Code: 9, FrameType: frameType, Reason: "too many connection IDs awaiting retirement"}
	}
	return nil
}

// retirementPath reports the path a RETIRE_CONNECTION_ID frame retires on.
func retirementPath(f wire.Frame) (uint64, bool) {
	switch f := f.(type) {
	case wire.RetireConnectionIDFrame:
		return 0, true
	case wire.PathRetireConnectionIDFrame:
		return f.PathID, true
	}
	return 0, false
}

// noteRetireQueued and noteRetireSent keep retiresQueued.
func (c *Conn) noteRetireQueued(path uint64, n int) {
	if c.retiresQueued == nil {
		c.retiresQueued = map[uint64]int{}
	}
	c.retiresQueued[path] += n
}
func (c *Conn) noteRetireSent(f wire.Frame) {
	if path, ok := retirementPath(f); ok && c.retiresQueued[path] > 0 {
		c.retiresQueued[path]--
	}
}

// queuedFrames is how many application frames wait to be sent, on every
// path.
func (c *Conn) queuedFrames() int {
	if c.multipath == nil {
		return len(c.spaces[applicationSpace].queue)
	}
	n := 0
	for _, state := range c.multipath.paths {
		if state.application != nil {
			n += len(state.application.space.queue)
		}
	}
	return n
}

// prioritizeStreams moves one selected item to the queue head. Non-STREAM slots
// retain their positions, preserving control and DATAGRAM service. Within STREAM work, retransmitted
// bytes precede fresh bytes, then urgency determines precedence. Incremental
// streams share turns with the lowest-ID non-incremental stream at that urgency.
// Selection does not advance a cursor: congestion, pacing, validation or budget
// failure must not consume a scheduling turn.
func (c *Conn) prioritizeStreams(s *numberSpace) {
	if len(s.queue) < 2 {
		return
	}
	if _, ok := s.queue[0].frame.(wire.StreamFrame); !ok {
		return
	}
	if index := c.prioritizedStreamIndex(s); index >= 0 {
		moveQueueHead(s, index)
	}
}

func (c *Conn) prioritizedStreamIndex(s *numberSpace) int {
	bestClass := 16
	var cursor streamPriorityCursor
	first, after, nonIncremental := -1, -1, -1
	var firstID, afterID, nonIncrementalID uint64
	consider := func(index int, id uint64) {
		if first < 0 || id < firstID {
			first, firstID = index, id
		}
		if cursor.valid && id > cursor.id && (after < 0 || id < afterID) {
			after, afterID = index, id
		}
	}
	for i, item := range s.queue {
		if item.acked {
			continue
		}
		frame, ok := item.frame.(wire.StreamFrame)
		if !ok {
			continue
		}
		priority := c.streamPriority(frame.StreamID)
		class := int(priority.urgency)
		if !item.transmitted {
			class += 8
		}
		if class > bestClass {
			continue
		}
		if class < bestClass {
			bestClass = class
			cursor = c.streams.priorityCursor[class]
			first, after, nonIncremental = -1, -1, -1
		}
		if priority.incremental {
			consider(i, frame.StreamID)
		} else if nonIncremental < 0 || frame.StreamID < nonIncrementalID {
			nonIncremental, nonIncrementalID = i, frame.StreamID
		}
	}
	if nonIncremental >= 0 {
		consider(nonIncremental, nonIncrementalID)
	}
	if after >= 0 {
		first = after
	}
	return first
}

func moveQueueHead(s *numberSpace, index int) {
	if index == 0 {
		return
	}
	item := s.queue[index]
	for i := 0; i <= index; i++ {
		if _, ok := s.queue[i].frame.(wire.StreamFrame); ok {
			item, s.queue[i] = s.queue[i], item
		}
	}
}

func (c *Conn) commitStreamPriority(item *sendItem) {
	if frame, ok := item.frame.(wire.StreamFrame); ok {
		class := int(c.streamPriority(frame.StreamID).urgency)
		if !item.transmitted {
			class += 8
		}
		c.streams.priorityCursor[class] = streamPriorityCursor{frame.StreamID, true}
	}
	item.transmitted = true
}
