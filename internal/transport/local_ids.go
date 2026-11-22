package transport

import (
	"bytes"
	"crypto/rand"
	"errors"

	"github.com/getlantern/photon/internal/wire"
)

// localIDs owns IDs issued to the peer. Router.Sync reflects this active set;
// the connection engine also checks it directly.
type localIDs struct {
	generate func() ([]byte, error)
	resetKey *ResetKey
	// Authenticated established-tuple use, bounded by the active CID set.
	observed map[uint64]pathTuple
	active   map[uint64]peerID
	sent     map[uint64]bool
	highest  uint64
	issued   bool
	used     map[string]struct{}
	length   int
}

func newLocalIDs(initial []byte) *localIDs {
	return &localIDs{active: map[uint64]peerID{0: {cid: clone(initial)}}, length: len(initial), issued: true, sent: map[uint64]bool{0: true}, used: map[string]struct{}{string(initial): {}}}
}

// newPathLocalIDs shares the connection's lifetime uniqueness registry. The
// caller serializes issuance across paths. Each path starts at sequence zero;
// path creation itself neither issues nor marks a CID as sent.
func newPathLocalIDs(initial *localIDs) (*localIDs, error) {
	if initial == nil || initial.length < 1 || initial.length > 20 || initial.used == nil {
		return nil, errors.New("quic: multipath requires a nonzero connection ID")
	}
	return &localIDs{active: make(map[uint64]peerID), sent: make(map[uint64]bool), length: initial.length, used: initial.used, resetKey: initial.resetKey, generate: initial.generate}, nil
}

func (l *localIDs) issue(peerLimit uint64) ([]wire.NewConnectionIDFrame, error) {
	if l.length == 0 {
		return nil, nil
	}
	// Four is a local issuance policy, not a receive-side restriction. The peer
	// may advertise more capacity without forcing us to allocate that many IDs.
	target := min(peerLimit, 4)
	var frames []wire.NewConnectionIDFrame
	for uint64(len(l.active)) < target {
		// Bound lifetime uniqueness history. Issuance is optional; preserve
		// existing active IDs rather than reusing a retired ID or growing
		// memory without limit. A 1-byte CID has only 256 possible values.
		maxIDs := 4096
		if l.length == 1 {
			maxIDs = 256
		}
		if len(l.used) >= maxIDs {
			return frames, nil
		}
		if l.issued && l.highest == wire.MaxVarint {
			return nil, errors.New("quic: connection ID sequence exhausted")
		}
		var id peerID
		id.hasToken = true
		unique := false
		for tries := 0; tries < 16; tries++ {
			var err error
			id.cid, err = generateLocalCID(l.length, l.generate)
			if err != nil {
				return nil, err
			}
			if _, used := l.used[string(id.cid)]; !used {
				unique = true
				break
			}
		}
		if !unique {
			if l.generate != nil {
				return nil, errors.New("quic: generator repeated previously issued CID")
			}
			return frames, nil // Keep active IDs if the bounded uniqueness search is exhausted.
		}
		if l.resetKey != nil {
			id.token = l.resetKey.token(id.cid)
		} else if _, err := rand.Read(id.token[:]); err != nil {
			return nil, err
		}
		l.used[string(id.cid)] = struct{}{}
		if l.issued {
			l.highest++
		}
		l.issued = true
		l.active[l.highest] = id
		frames = append(frames, wire.NewConnectionIDFrame{Sequence: l.highest, ConnectionID: clone(id.cid), StatelessResetToken: id.token})
	}
	return frames, nil
}
func (l *localIDs) retire(sequence uint64, packetDCID []byte) (bool, error) {
	return l.retireWithType(sequence, packetDCID, 0x19)
}

// retirePath operates on the path state selected by the caller from f.PathID.
func (l *localIDs) retirePath(f wire.PathRetireConnectionIDFrame, packetDCID []byte) (bool, error) {
	return l.retireWithType(f.Sequence, packetDCID, f.Type())
}

func (l *localIDs) retireWithType(sequence uint64, packetDCID []byte, frameType uint64) (bool, error) {
	bad := func(reason string) (bool, error) {
		return false, &TransportError{Code: 0xa, FrameType: frameType, Reason: reason}
	}
	if l.length == 0 {
		return bad("retirement with zero-length connection ID")
	}
	if !l.issued || sequence > l.highest {
		return bad("retirement of unissued connection ID")
	}
	id, ok := l.active[sequence]
	if !ok {
		return false, nil
	}
	if bytes.Equal(id.cid, packetDCID) {
		return bad("retirement of packet destination connection ID")
	}
	if !l.sent[sequence] {
		return bad("retirement of unsent connection ID")
	}
	delete(l.sent, sequence)
	delete(l.active, sequence)
	delete(l.observed, sequence)
	return true, nil
}

func (l *localIDs) accept(cid []byte) bool {
	for seq, id := range l.active {
		if l.sent[seq] && bytes.Equal(id.cid, cid) {
			return true
		}
	}
	return false
}
func (l *localIDs) markSent(sequence uint64) {
	if _, ok := l.active[sequence]; ok {
		l.sent[sequence] = true
	}
}

// noteReceived records authenticated use only after established-tuple admission.
func (l *localIDs) noteReceived(cid []byte, tuple pathTuple) {
	for seq, id := range l.active {
		if l.sent[seq] && bytes.Equal(id.cid, cid) {
			if l.observed == nil {
				l.observed = make(map[uint64]pathTuple)
			}
			l.observed[seq] = tuple
			return
		}
	}
}

func (l *localIDs) receivedOn(cid []byte, tuple pathTuple) bool {
	for seq, id := range l.active {
		if l.sent[seq] && bytes.Equal(id.cid, cid) {
			observed, ok := l.observed[seq]
			return ok && observed == tuple
		}
	}
	return false
}

// noteAuthenticated retains pre-handoff CID evidence without inventing a tuple.
// Initial packets must not call this: their protection keys are publicly derived.
func (l *localIDs) noteAuthenticated(cid []byte) {
	for seq, id := range l.active {
		if l.sent[seq] && bytes.Equal(id.cid, cid) {
			if l.observed == nil {
				l.observed = make(map[uint64]pathTuple)
			}
			if _, exists := l.observed[seq]; !exists {
				l.observed[seq] = pathTuple{}
			}
			return
		}
	}
}

// bindAuthenticated assigns only previously authenticated evidence to the
// owner's fixed handshake tuple. Issued-but-unused CIDs gain no evidence.
func (l *localIDs) bindAuthenticated(tuple pathTuple) {
	for seq, observed := range l.observed {
		if observed == (pathTuple{}) {
			if _, active := l.active[seq]; active && l.sent[seq] {
				l.observed[seq] = tuple
			}
		}
	}
}

// reservePreferred installs the server's transport-parameter CID as sequence 1
// before any ordinary issuance. The owner must reserve this CID in its routers
// before sending the TLS flight and keep the advertised address serviceable.
// It has the same issued/sent status as the initial handshake CID.
func (l *localIDs) reservePreferred(address *wire.PreferredAddress) error {
	if address == nil {
		return nil
	}
	if l == nil || !l.issued || l.highest != 0 || len(l.active) != 1 || l.length == 0 || l.length > 20 || len(address.ConnectionID) != l.length {
		return errors.New("quic: preferred CID requires fresh fixed-length handshake IDs")
	}
	if _, exists := l.used[string(address.ConnectionID)]; exists {
		return errors.New("quic: preferred CID reuses an issued connection ID")
	}
	l.active[1] = peerID{cid: clone(address.ConnectionID), token: address.StatelessResetToken, hasToken: true}
	l.sent[1] = true
	l.used[string(address.ConnectionID)] = struct{}{}
	l.highest = 1
	return nil
}
