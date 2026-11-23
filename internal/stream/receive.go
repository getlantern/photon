// Package stream implements stream state independently of packets and sockets.
// Callers serialize access, including streams sharing connection flow credit.
package stream

import (
	"errors"
	"io"
	"math/bits"

	"github.com/getlantern/photon/internal/wire"
)

var (
	ErrFlowControl   = errors.New("quic: receive flow control limit exceeded")
	ErrFinalSize     = errors.New("quic: inconsistent stream final size")
	ErrDataConflict  = errors.New("quic: conflicting stream data")
	ErrWouldBlock    = errors.New("quic: stream has no contiguous data available")
	ErrReceiveWindow = errors.New("quic: invalid receive window")
)

type ResetError struct{ Code uint64 }

func (e *ResetError) Error() string { return "quic: stream reset by peer" }

// Flow accounts the sum of each stream's highest received offset, including
// final sizes from RESET_STREAM. Reading never rolls back Charged; consumption
// can instead be used by the connection to advertise a larger MAX_DATA.
type Flow struct{ limit, charged, consumed uint64 }

func NewFlow(limit uint64) (*Flow, error) {
	if limit > wire.MaxVarint {
		return nil, ErrFlowControl
	}
	return &Flow{limit: limit}, nil
}
func (f *Flow) Limit() uint64    { return f.limit }
func (f *Flow) Charged() uint64  { return f.charged }
func (f *Flow) Consumed() uint64 { return f.consumed }
func (f *Flow) SetLimit(limit uint64) error {
	if limit < f.limit || limit > wire.MaxVarint {
		return ErrFlowControl
	}
	f.limit = limit
	return nil
}

// Receive grows a circular buffer to cover the span of retained bytes.
// Advertised capacity and allocated storage are independent; an initial small
// contiguous span needs at most 4 KiB of payload storage with a large window.
// Idle streams and empty terminal streams need no payload storage.
// Advertised stream credit cannot extend
// beyond the consumed offset plus its capacity, so any flow-control-compliant
// fragment fits without allocating memory proportional to a peer's offset.
// Stream IDs, directionality, and stream-count limits belong to the manager.
type Receive struct {
	buffered                      uint64
	capacity                      int
	flow                          *Flow
	data                          []byte
	present                       []uint64
	next, highest, limit, final   uint64
	finalKnown, eofRead           bool
	reset                         *ResetError
	reliable                      uint64
	partialReset, resetSuppressed bool
}

func NewReceive(flow *Flow, capacity int, limit uint64) (*Receive, error) {
	if flow == nil || capacity < 1 || capacity > 16<<20 || limit > uint64(capacity) {
		return nil, ErrReceiveWindow
	}
	return &Receive{flow: flow, capacity: capacity, limit: limit}, nil
}
func (r *Receive) Offset() uint64            { return r.next }
func (r *Receive) Limit() uint64             { return r.limit }
func (r *Receive) Highest() uint64           { return r.highest }
func (r *Receive) FinalSize() (uint64, bool) { return r.final, r.finalKnown }
func (r *Receive) Complete() bool {
	if r.reset != nil {
		return r.Drained() || r.buffered == r.reliable-r.next
	}
	return r.finalKnown && r.buffered == r.final-r.next
}
func (r *Receive) SetLimit(limit uint64) error {
	if limit < r.limit || limit > wire.MaxVarint || limit-r.next > uint64(r.capacity) {
		return ErrReceiveWindow
	}
	r.limit = limit
	return nil
}

// Grow raises the receive window capacity without granting additional credit
// or allocating payload storage.
// The owner must separately advertise and install a new limit. Unread bytes,
// gaps, final size and shared flow accounting are preserved. Storage stays lazy
// when there are no retained bytes. Shrinking is not supported.
func (r *Receive) Grow(capacity int) error {
	if capacity < r.capacity || capacity > 16<<20 {
		return ErrReceiveWindow
	}
	if capacity == r.capacity {
		return nil
	}
	r.capacity = capacity
	if r.buffered == 0 {
		r.data, r.present = nil, nil
	}
	return nil
}

// ensureStorage covers the incoming span measured from the consumed offset.
// Existing bytes already fit the old ring; expansion remaps only occupied bits.
// A sparse far-ahead fragment can still require the entire advertised window.
func (r *Receive) ensureStorage(span int) {
	oldSize := len(r.data)
	if span <= oldSize {
		return
	}
	size := min(r.capacity, max(4096, oldSize))
	for size < span {
		size = min(r.capacity, size*2)
	}
	data := make([]byte, size)
	present := make([]uint64, (size+63)/64)
	if r.buffered != 0 {
		start := int(r.next % uint64(oldSize))
		for word, occupied := range r.present {
			for occupied != 0 {
				index := word*64 + bits.TrailingZeros64(occupied)
				distance := (index - start + oldSize) % oldSize
				next := int((r.next + uint64(distance)) % uint64(size))
				data[next] = r.data[index]
				present[next/64] |= uint64(1) << uint(next%64)
				occupied &= occupied - 1
			}
		}
	}
	r.data, r.present = data, present
}

func (r *Receive) validate(end uint64, final bool) error {
	if r.finalKnown && (end > r.final || (final && end != r.final)) {
		return ErrFinalSize
	}
	if final && end < r.highest {
		return ErrFinalSize
	}
	if end > r.limit {
		return ErrFlowControl
	}
	delta := uint64(0)
	if end > r.highest {
		delta = end - r.highest
	}
	if delta > r.flow.limit-r.flow.charged {
		return ErrFlowControl
	}
	return nil
}
func (r *Receive) commit(end uint64, final bool) {
	if end > r.highest {
		r.flow.charged += end - r.highest
		r.highest = end
	}
	if final {
		r.final, r.finalKnown = end, true
	}
}

// Push copies new bytes only after validating the entire frame. Duplicate data
// below the consumed offset is ignored; buffered overlaps must agree.
func (r *Receive) Push(offset uint64, data []byte, fin bool) error {
	if offset > wire.MaxVarint || uint64(len(data)) > wire.MaxVarint-offset {
		return ErrFlowControl
	}
	end := offset + uint64(len(data))
	if err := r.validate(end, fin); err != nil {
		return err
	}
	if r.Drained() || end <= r.next {
		r.commit(end, fin)
		return nil
	}
	if offset < r.next {
		data = data[r.next-offset:]
		offset = r.next
	}
	if r.reset != nil {
		if offset >= r.reliable {
			r.commit(end, fin)
			return nil
		}
		if end > r.reliable {
			data = data[:r.reliable-offset]
		}
	}
	// Empty frames can advance accounting without storage. Trimming for a
	// partial reset above ensures discarded suffix bytes do not drive allocation.
	if len(data) == 0 {
		r.commit(end, fin)
		return nil
	}
	r.ensureStorage(int(offset + uint64(len(data)) - r.next))
	ringSize := len(r.data)
	// Inspect only occupied positions, but validate every overlap before
	// committing either flow accounting or new bytes.
	index := int(offset % uint64(ringSize))
	for pos := 0; pos < len(data); {
		shift := index % 64
		n := min(len(data)-pos, ringSize-index, 64-shift)
		mask := receiveMask(shift, n)
		occupied := r.present[index/64] & mask
		for occupied != 0 {
			bit := bits.TrailingZeros64(occupied)
			if r.data[index+bit-shift] != data[pos+bit-shift] {
				return ErrDataConflict
			}
			occupied &= occupied - 1
		}
		pos, index = pos+n, index+n
		if index == ringSize {
			index = 0
		}
	}
	r.commit(end, fin)
	index = int(offset % uint64(ringSize))
	for pos := 0; pos < len(data); {
		n := min(len(data)-pos, ringSize-index, 64-index%64)
		mask := receiveMask(index%64, n)
		r.buffered += uint64(bits.OnesCount64(mask &^ r.present[index/64]))
		copy(r.data[index:index+n], data[pos:pos+n])
		r.present[index/64] |= mask
		pos, index = pos+n, index+n
		if index == ringSize {
			index = 0
		}
	}

	return nil
}

// Reset charges the final size and discards the delivery obligation. Ordinary
// resets retain the first code; once ResetAt is used, code changes are invalid.
func (r *Receive) Reset(final, code uint64) error {
	return r.resetAt(final, 0, code, false)
}

// ResetAt guarantees delivery through reliable before surfacing the reset.
// Reordered larger reliable sizes cannot increase a previous obligation.
func (r *Receive) ResetAt(final, reliable, code uint64) error {
	if reliable > final {
		return wire.ErrFrame
	}
	return r.resetAt(final, reliable, code, true)
}

func (r *Receive) resetAt(final, reliable, code uint64, partial bool) error {
	if final > wire.MaxVarint || code > wire.MaxVarint {
		return ErrFinalSize
	}
	if err := r.validate(final, true); err != nil {
		return err
	}
	if r.reset != nil && (partial || r.partialReset) && code != r.reset.Code {
		return ErrStreamState
	}
	r.commit(final, true)
	r.partialReset = r.partialReset || partial
	if r.reset == nil {
		r.reset = &ResetError{Code: code}
		r.reliable = reliable
		r.resetSuppressed = r.eofRead
	} else {
		r.reliable = min(r.reliable, reliable)
	}
	if r.next >= r.reliable {
		r.finishReset()
		return nil
	}
	// Drop bytes beyond the current obligation. Iterate occupied bitmap bits,
	// bounded by the receive window, never the peer's absolute stream offset.
	if r.buffered == 0 {
		return nil
	}
	ringSize := len(r.data)
	start := int(r.next % uint64(ringSize))
	for word, occupied := range r.present {
		for occupied != 0 {
			bit := bits.TrailingZeros64(occupied)
			index := word*64 + bit
			distance := (index - start + ringSize) % ringSize
			if r.next+uint64(distance) >= r.reliable {
				r.present[word] &^= uint64(1) << uint(bit)
				r.buffered--
			}
			occupied &= occupied - 1
		}
	}
	return nil
}

func (r *Receive) finishReset() {
	r.flow.consumed += r.final - r.next
	r.next = r.final
	r.data, r.present = nil, nil
	r.buffered = 0
}

// Read returns ErrWouldBlock when a packet gap prevents progress; a socket/API
// adapter will turn this into waiting and support cancellation/deadlines.
func (r *Receive) Read(dst []byte) (int, error) {
	if r.reset != nil && !r.resetSuppressed && r.next >= r.reliable {
		return 0, r.reset
	}
	if len(dst) == 0 {
		return 0, nil
	}
	n := 0
	ringSize := len(r.data)
	index := 0
	if r.buffered != 0 {
		index = int(r.next % uint64(ringSize))
	}
	for n < len(dst) && r.buffered > 0 {
		shift := index % 64
		contiguous := bits.TrailingZeros64(^(r.present[index/64] >> uint(shift)))
		count := min(contiguous, 64-shift, ringSize-index, len(dst)-n)
		if count == 0 {
			break
		}
		copy(dst[n:n+count], r.data[index:index+count])
		r.present[index/64] &^= receiveMask(shift, count)
		index += count
		if index == ringSize {
			index = 0
		}
		r.buffered -= uint64(count)
		r.next += uint64(count)
		n += count
	}

	r.flow.consumed += uint64(n)
	if r.reset != nil && !r.resetSuppressed && r.next >= r.reliable {
		r.finishReset()
		return n, r.reset
	}
	if r.finalKnown && r.next == r.final {
		r.eofRead = true
		r.data, r.present = nil, nil
		return n, io.EOF
	}
	if n > 0 {
		return n, nil
	}
	return 0, ErrWouldBlock
}

func (r *Receive) Drained() bool { return r.eofRead || (r.reset != nil && r.next >= r.reliable) }

// DiscardComplete releases unread terminal data after application cancellation.
func (r *Receive) DiscardComplete() error {
	if !r.Complete() {
		return ErrWouldBlock
	}
	r.flow.consumed += r.final - r.next
	r.next = r.final
	r.buffered = 0
	r.eofRead = true
	r.data, r.present = nil, nil
	return nil
}

// receiveMask covers a nonempty span contained within one bitmap word.
func receiveMask(shift, n int) uint64 {
	return (^uint64(0) >> uint(64-n)) << uint(shift)
}

// TerminalError reports the completed receive outcome without consuming data.
// A partial reset can be complete (prefix buffered) without being drained.
func (r *Receive) TerminalError() error {
	if !r.Complete() {
		return ErrWouldBlock
	}
	if r.reset != nil && !r.resetSuppressed {
		return r.reset
	}
	return io.EOF
}

// PartialResetInfo exposes immutable validation metadata for recent retirement.
func (r *Receive) PartialResetInfo() (final, code uint64, ok bool) {
	if !r.partialReset || r.reset == nil {
		return 0, 0, false
	}
	return r.final, r.reset.Code, true
}
