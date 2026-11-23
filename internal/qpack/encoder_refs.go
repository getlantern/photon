package qpack

import "errors"

var (
	ErrEncoderLimit    = errors.New("qpack: encoder tracking limit exceeded")
	ErrDecoderFeedback = errors.New("qpack: invalid decoder feedback")
	ErrEntryProtected  = errors.New("qpack: dynamic entry is not evictable")
)

type trackedSection struct {
	required uint64
	refs     []uint64
}

// EncoderReferences tracks outstanding dynamic references, not field contents.
// The owner serializes operations with its encoder table and wire scheduling.
// RecordInsertion/RecordEviction must mirror actual table changes. Track must
// succeed before a field section is committed to the wire. Stream cancellation
// lifecycle is owned by HTTP/3; this tracker keeps no lifetime stream tombstones.
type EncoderReferences struct {
	inserted, known, dropped         uint64
	maxBlocked, maxSections, maxRefs int
	sections, refs                   int
	streams                          map[uint64][]trackedSection
	protection                       map[uint64]int
}

func NewEncoderReferences(maxBlocked, maxSections, maxRefs int) (*EncoderReferences, error) {
	if maxBlocked < 0 || maxSections < 0 || maxRefs < 0 {
		return nil, ErrEncoderLimit
	}
	return &EncoderReferences{maxBlocked: maxBlocked, maxSections: maxSections, maxRefs: maxRefs, streams: make(map[uint64][]trackedSection), protection: make(map[uint64]int)}, nil
}
func (e *EncoderReferences) InsertCount() uint64        { return e.inserted }
func (e *EncoderReferences) KnownReceivedCount() uint64 { return e.known }
func (e *EncoderReferences) OutstandingSections() int   { return e.sections }
func (e *EncoderReferences) RecordInsertion() error {
	if e.inserted == MaxInteger {
		return ErrIntegerOverflow
	}
	e.inserted++
	return nil
}
func (e *EncoderReferences) streamBlocked(id uint64) bool {
	for _, s := range e.streams[id] {
		if s.required > e.known {
			return true
		}
	}
	return false
}
func (e *EncoderReferences) BlockedStreams() int {
	count := 0
	for id := range e.streams {
		if e.streamBlocked(id) {
			count++
		}
	}
	return count
}
func (e *EncoderReferences) CanEvict(id uint64) bool {
	return id >= e.dropped && id < e.known && e.protection[id] == 0
}

// RecordEviction enforces oldest-first eviction and receipt/reference safety.
func (e *EncoderReferences) RecordEviction(id uint64) error {
	if id != e.dropped || !e.CanEvict(id) {
		return ErrEntryProtected
	}
	e.dropped++
	return nil
}

// Track records one section's absolute dynamic references, preserving FIFO ACK
// order within a stream. Static-only sections require no ACK and are not stored.
// Repeated references are allowed and charged against maxRefs. Input is copied;
// all failures preserve state. Return value is the exact Required Insert Count.
func (e *EncoderReferences) Track(streamID uint64, refs []uint64) (uint64, error) {
	if streamID > MaxInteger {
		return 0, ErrIntegerOverflow
	}
	if len(refs) == 0 {
		return 0, nil
	}
	if e.sections == e.maxSections || len(refs) > e.maxRefs-e.refs {
		return 0, ErrEncoderLimit
	}
	required := uint64(0)
	for _, id := range refs {
		if id < e.dropped || id >= e.inserted {
			return 0, ErrTableIndex
		}
		if id+1 > required {
			required = id + 1
		}
	}
	if required > e.known && !e.streamBlocked(streamID) && e.BlockedStreams() >= e.maxBlocked {
		return 0, ErrBlockedStreams
	}
	copied := append([]uint64(nil), refs...)
	e.streams[streamID] = append(e.streams[streamID], trackedSection{required: required, refs: copied})
	for _, id := range copied {
		e.protection[id]++
	}
	e.sections++
	e.refs += len(copied)
	return required, nil
}
func (e *EncoderReferences) release(s trackedSection) {
	for _, id := range s.refs {
		e.protection[id]--
		if e.protection[id] == 0 {
			delete(e.protection, id)
		}
	}
	e.sections--
	e.refs -= len(s.refs)
}

// ApplyFeedback validates before mutation. Invalid peer feedback must become
// QPACK_DECODER_STREAM_ERROR at the endpoint. Cancellation does not acknowledge
// insertions, and increments do not release references held by field sections.
func (e *EncoderReferences) ApplyFeedback(v DecoderInstruction) error {
	if v.Value > MaxInteger {
		return ErrDecoderFeedback
	}
	switch v.Type {
	case InsertCountIncrement:
		if v.Value == 0 || v.Value > e.inserted-e.known {
			return ErrDecoderFeedback
		}
		e.known += v.Value
	case SectionAcknowledgment:
		queue := e.streams[v.Value]
		if len(queue) == 0 {
			return ErrDecoderFeedback
		}
		section := queue[0]
		if section.required > e.known {
			e.known = section.required
		}
		e.release(section)
		copy(queue, queue[1:])
		queue[len(queue)-1] = trackedSection{}
		if len(queue) == 1 {
			delete(e.streams, v.Value)
		} else {
			e.streams[v.Value] = queue[:len(queue)-1]
		}
	case StreamCancellation:
		for _, section := range e.streams[v.Value] {
			e.release(section)
		}
		delete(e.streams, v.Value)
	default:
		return ErrDecoderFeedback
	}
	return nil
}
