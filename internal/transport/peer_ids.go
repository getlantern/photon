package transport

import (
	"bytes"
	"errors"
	"maps"
	"slices"
	"sort"

	"github.com/getlantern/photon/internal/wire"
)

type peerID struct {
	cid      []byte
	token    [16]byte
	hasToken bool
	used     bool
	// Reserved CIDs must never be selected for the established tuple.
	reservation *migrationCandidate
}

// peerIDs retains only active IDs. IDs below retireBefore can never become active
// again; a retired ID's NEW_CONNECTION_ID elicits RETIRE_CONNECTION_ID once
// (RFC 9000 Section 19.15), as answered records.
type peerIDs struct {
	active                       map[uint64]peerID
	retired                      map[uint64]struct{}
	current, retireBefore, limit uint64
	// answered holds the latest maxAnswered sequence numbers retired, oldest
	// first: a repeat or stale NEW_CONNECTION_ID for one retires nothing
	// again, whether its RETIRE is waiting, sent or lost and requeued. One
	// older than these elicits another, which is harmless.
	answered []uint64
}

const maxAnswered = 256

func newPeerIDs(cid []byte, limit uint64) *peerIDs {
	return &peerIDs{active: map[uint64]peerID{0: {cid: clone(cid)}}, limit: limit}
}

// newPathPeerIDs starts an additional path without inventing a handshake CID.
func newPathPeerIDs(limit uint64) *peerIDs {
	return &peerIDs{active: make(map[uint64]peerID), limit: limit}
}

func (p *peerIDs) add(f wire.NewConnectionIDFrame) ([]uint64, error) {
	return p.addWithType(f, 0x18)
}

// addPath assumes the caller has checked path limits, selected the matching
// path state, and checked CID uniqueness across paths.
func (p *peerIDs) addPath(f wire.PathNewConnectionIDFrame) ([]uint64, error) {
	return p.addWithType(f.NewConnectionIDFrame, f.Type())
}

func (p *peerIDs) addWithType(f wire.NewConnectionIDFrame, frameType uint64) ([]uint64, error) {
	violation := func(reason string) ([]uint64, error) {
		return nil, &TransportError{Code: 0xa, FrameType: frameType, Reason: reason}
	}
	if current, ok := p.active[p.current]; ok && len(current.cid) == 0 {
		return violation("NEW_CONNECTION_ID with zero-length destination CID")
	}
	if len(f.ConnectionID) < 1 || len(f.ConnectionID) > 20 || f.Sequence > wire.MaxVarint || f.RetirePriorTo > f.Sequence {
		return nil, &TransportError{Code: 7, FrameType: frameType, Reason: "invalid NEW_CONNECTION_ID"}
	}
	for seq, id := range p.active {
		if seq == f.Sequence {
			if !bytes.Equal(id.cid, f.ConnectionID) || (id.hasToken && id.token != f.StatelessResetToken) {
				return violation("connection ID sequence reused with different data")
			}
		} else if bytes.Equal(id.cid, f.ConnectionID) {
			return violation("connection ID reused with different sequence")
		}
	}
	floor := max(p.retireBefore, f.RetirePriorTo)
	next := make(map[uint64]peerID, len(p.active)+1)
	var retire []uint64
	for seq, id := range p.active {
		if seq < floor {
			retire = append(retire, seq)
		} else {
			next[seq] = id
		}
	}
	_, selectivelyRetired := p.retired[f.Sequence]
	if f.Sequence < floor || selectivelyRetired {
		if !slices.Contains(p.answered, f.Sequence) {
			retire = append(retire, f.Sequence)
		}
	} else {
		next[f.Sequence] = peerID{cid: clone(f.ConnectionID), token: f.StatelessResetToken, hasToken: true, used: next[f.Sequence].used, reservation: next[f.Sequence].reservation}
	}
	if uint64(len(next)) > p.limit {
		return nil, &TransportError{Code: 9, FrameType: frameType, Reason: "active connection ID limit exceeded"}
	}
	current := p.current
	if id, ok := next[current]; !ok || id.reservation != nil {
		current = wire.MaxVarint + 1 // No ordinary CID if every remaining ID is reserved.
		for seq, id := range next {
			if id.reservation == nil {
				current = min(current, seq)
			}
		}
	}
	retained := maps.Clone(p.retired)
	for seq := range retained {
		if seq < floor {
			delete(retained, seq)
		}
	}
	p.retired = retained
	p.active, p.current, p.retireBefore = next, current, floor
	sort.Slice(retire, func(i, j int) bool { return retire[i] < retire[j] })
	p.answer(retire)
	return retire, nil
}

// answer records sequences retired, keeping the latest maxAnswered.
func (p *peerIDs) answer(sequences []uint64) {
	if len(sequences) == 0 {
		return
	}
	answered := append(slices.Clone(p.answered), sequences...)
	if extra := len(answered) - maxAnswered; extra > 0 {
		answered = answered[extra:]
	}
	p.answered = answered
}

// nextUnused selects deterministically without changing CID ownership.
func (p *peerIDs) nextUnused() (uint64, bool) {
	var next uint64
	found := false
	for seq, id := range p.active {
		if seq > p.current && !id.used && id.reservation == nil && (!found || seq < next) {
			next, found = seq, true
		}
	}
	return next, found
}

// rotate switches to the next higher unused sequence and retires earlier IDs.
// The monotonic floor prevents retransmissions from reactivating retired IDs.
func (p *peerIDs) rotate() ([]uint64, error) {
	next, found := p.nextUnused()
	if !found {
		return nil, errors.New("quic: no unused peer connection ID")
	}
	var retired []uint64
	for seq := range p.active {
		if seq < next {
			retired = append(retired, seq)
			delete(p.active, seq)
		}
	}
	p.current = next
	p.retireBefore = max(p.retireBefore, next)
	sort.Slice(retired, func(i, j int) bool { return retired[i] < retired[j] })
	p.answer(retired)
	return retired, nil
}

// retireReserved removes only a spent reservation. The sparse tombstone prevents
// delayed NEW_CONNECTION_ID frames from reviving it while lower IDs remain live.
// The caller must commit its reliable retirement frame in the same transaction.
func (p *peerIDs) retireReserved(candidate *migrationCandidate, seq uint64) error {
	id, ok := p.migrationID(candidate, seq)
	if !ok || !id.used || seq == p.current {
		return errMigrationState
	}
	if len(p.retired) >= maxPeerCIDHistory {
		return errPathCIDCapacity
	}
	retired := maps.Clone(p.retired)
	if retired == nil {
		retired = make(map[uint64]struct{})
	}
	retired[seq] = struct{}{}
	p.retired = retired
	delete(p.active, seq)
	p.answer([]uint64{seq})
	return nil
}
