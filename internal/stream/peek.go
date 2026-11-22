package stream

import (
	"io"
	"math/bits"
)

// Peek fills dst without consuming data or changing terminal state. A short
// result is returned only at EOF or the reliable boundary of a partial reset.
// Incomplete requests leave dst unchanged and return ErrWouldBlock.
func (r *Receive) Peek(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if r.reset != nil && !r.resetSuppressed && r.next >= r.reliable {
		return 0, r.reset
	}
	n := 0
	ringSize := len(r.data)
	index := 0
	if r.buffered != 0 {
		index = int(r.next % uint64(ringSize))
	}
	remaining := min(uint64(len(dst)), r.buffered)
	for remaining > 0 {
		shift := index % 64
		contiguous := bits.TrailingZeros64(^(r.present[index/64] >> uint(shift)))
		count := min(contiguous, 64-shift, ringSize-index, int(remaining))
		if count == 0 {
			break
		}
		n += count
		remaining -= uint64(count)
		index += count
		if index == ringSize {
			index = 0
		}
	}
	var err error
	if n < len(dst) {
		switch {
		case r.reset != nil && !r.resetSuppressed && r.next+uint64(n) == r.reliable:
			err = r.reset
		case r.finalKnown && r.next+uint64(n) == r.final:
			err = io.EOF
		default:
			return 0, ErrWouldBlock
		}
	}
	if n > 0 {
		start := int(r.next % uint64(ringSize))
		first := min(n, ringSize-start)
		copy(dst[:first], r.data[start:start+first])
		copy(dst[first:n], r.data[:n-first])
	}
	return n, err
}
