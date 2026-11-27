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
