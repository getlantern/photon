// Package handshake connects QUIC CRYPTO streams to Go's TLS 1.3 implementation.
package handshake

import (
	"errors"

	"github.com/getlantern/photon/internal/wire"
)

var (
	ErrCryptoBuffer   = errors.New("quic: CRYPTO reassembly window exceeded")
	ErrCryptoConflict = errors.New("quic: conflicting CRYPTO data")
)

// reassembler uses a bounded circular window. Memory is independent of offsets,
// duplication, fragmentation, or attacker-controlled gaps. Delivered bytes are
// discarded; retransmissions entirely before next are ignored.
type reassembler struct {
	data    []byte
	present []bool
	next    uint64
}

func newReassembler(limit int) *reassembler {
	return &reassembler{data: make([]byte, limit), present: make([]bool, limit)}
}

func (r *reassembler) push(offset uint64, b []byte) error {
	if offset > wire.MaxVarint || uint64(len(b)) > wire.MaxVarint-offset {
		return wire.ErrVarintRange
	}
	if len(b) == 0 {
		return nil
	}
	end := offset + uint64(len(b))
	if end <= r.next {
		return nil
	}
	if end-r.next > uint64(len(r.data)) {
		return ErrCryptoBuffer
	}
	if offset < r.next {
		b = b[r.next-offset:]
		offset = r.next
	}
	// Check for contradictions before changing any buffered state.
	for i, v := range b {
		j := (offset + uint64(i)) % uint64(len(r.data))
		if r.present[j] && r.data[j] != v {
			return ErrCryptoConflict
		}
	}
	for i, v := range b {
		j := (offset + uint64(i)) % uint64(len(r.data))
		r.data[j], r.present[j] = v, true
	}
	return nil
}

func (r *reassembler) pop() []byte {
	var out []byte
	for len(out) < len(r.data) {
		i := r.next % uint64(len(r.data))
		if !r.present[i] {
			break
		}
		out = append(out, r.data[i])
		r.present[i] = false
		r.next++
	}
	return out
}
