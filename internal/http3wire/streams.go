package http3wire

import (
	"io"
	"sync"

	"github.com/getlantern/photon/internal/wire"
)

const (
	StreamCreationError uint64 = 0x103
	UniControl          uint64 = 0
	UniPush             uint64 = 1
	UniQPACKEncoder     uint64 = 2
	UniQPACKDecoder     uint64 = 3
)

// ReadUniType consumes only the stream-type prefix, accepting nonminimal
// encodings. EOF, partial EOF and transport resets before a complete type are
// deliberately not HTTP/3 connection errors (RFC 9114 section 6.2). The caller
// must tolerate those closures; cancellation/deadline policy is its own.
func ReadUniType(source io.Reader) (uint64, error) { return readInteger(source) }

// UniRegistry owns the three incoming critical-stream identities for one peer.
// Its memory does not grow with peer stream IDs or unknown stream types. It is
// safe for concurrent stream-prefix readers. The owner must call Register once
// per accepted stream after decoding its type, close the connection on returned
// protocol errors, and route known types to the appropriate stream consumer.
// Push-ID validation and unknown-stream discard/STOP_SENDING belong to the owner.
type UniRegistry struct {
	mu           sync.Mutex
	peerIsServer bool
	ids          [3]uint64
	seen         [3]bool
}

func NewUniRegistry(peerIsServer bool) *UniRegistry { return &UniRegistry{peerIsServer: peerIsServer} }
func criticalIndex(typ uint64) (int, bool) {
	switch typ {
	case UniControl:
		return 0, true
	case UniQPACKEncoder:
		return 1, true
	case UniQPACKDecoder:
		return 2, true
	}
	return 0, false
}

// Register returns known=false for unknown/reserved stream types, which must
// be discarded or aborted without turning their type into a connection error.
func (r *UniRegistry) Register(id, typ uint64) (known bool, err error) {
	expected := uint64(2)
	if r.peerIsServer {
		expected = 3
	}
	if id > wire.MaxVarint || id%4 != expected || typ > wire.MaxVarint {
		return false, &Error{StreamCreationError, "invalid incoming unidirectional stream"}
	}
	if typ == UniPush {
		if !r.peerIsServer {
			return false, &Error{StreamCreationError, "client initiated push stream"}
		}
		return true, nil
	}
	index, critical := criticalIndex(typ)
	if !critical {
		return false, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.seen[index] {
		return false, &Error{StreamCreationError, "duplicate critical stream"}
	}
	// Detect caller misuse rather than assigning two types to the same stream.
	for i, seen := range r.seen {
		if seen && r.ids[i] == id {
			return false, &Error{StreamCreationError, "stream already classified"}
		}
	}
	r.ids[index], r.seen[index] = id, true
	return true, nil
}

// Closed must be called on confirmed FIN or RESET_STREAM, not on a local read
// timeout/cancellation. Critical ownership is retained for the connection's
// lifetime, so closure cannot make a replacement stream legal.
func (r *UniRegistry) Closed(id uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, seen := range r.seen {
		if seen && r.ids[i] == id {
			return &Error{ClosedCriticalStream, "critical stream closed"}
		}
	}
	return nil
}
