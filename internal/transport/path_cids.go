package transport

import (
	"github.com/getlantern/photon/internal/recovery"

	"bytes"
	"errors"

	"github.com/getlantern/photon/internal/wire"
)

var errPathCIDCapacity = errors.New("quic: path CID state capacity exhausted")

const maxPeerCIDHistory = 4096

type pathCIDState struct {
	application         *pathApplication
	local               *localIDs
	peer                *peerIDs
	statusKnown         bool
	statusSequence      uint64
	backup              bool
	localStatusKnown    bool
	localStatusSequence uint64
	localBackup         bool
}
type pathCIDBinding struct{ path, sequence uint64 }

// pathCIDs owns CID state for negotiated multipath, serialized by the connection
// owner. Creating CID state does not initiate or validate a network path. The
// engine must queue abandonment frames and manage packet numbers and recovery.
// All CID mutations must go through this table once it owns the connection.
// Capacity errors are local resource failures, not peer protocol violations.
type pathCIDs struct {
	// baseOnly uses path-zero ownership without negotiating multipath wire frames.
	baseOnly bool
	// congestionControl selects new paths' congestion controllers.
	congestionControl recovery.Algorithm
	// highestInitiated is client-local; receive order may differ across paths.
	highestInitiated                  uint64
	lastScheduled                     uint64
	lastEnqueued                      uint64
	haveEnqueued                      bool
	haveScheduled                     bool
	paths                             map[uint64]*pathCIDState
	localTemplate                     *localIDs
	abandoned                         map[uint64]*pathAbandonment
	localMax, peerMax, initialPeerMax uint64
	localLimit, peerLimit             uint64
	capacity                          int
	// Retained bindings detect reuse even after rotation. Both maps have the
	// same bounded cardinality and are never sized from a peer's path ID.
	peerHistory map[pathCIDBinding]peerID
	peerByCID   map[string]pathCIDBinding
}

func newPathCIDs(localCID, peerCID []byte, localMax, peerMax, localLimit, peerLimit uint64, capacity int) (*pathCIDs, error) {
	if len(localCID) < 1 || len(localCID) > 20 || len(peerCID) < 1 || len(peerCID) > 20 || localMax > 1<<32-1 || peerMax > 1<<32-1 || localLimit < 2 || localLimit > wire.MaxVarint || peerLimit < 2 || peerLimit > wire.MaxVarint || capacity < 1 || capacity > 64 {
		return nil, errors.New("quic: invalid multipath CID configuration")
	}
	local, peer := newLocalIDs(localCID), newPeerIDs(peerCID, localLimit)
	binding := pathCIDBinding{}
	return &pathCIDs{localTemplate: &localIDs{length: local.length, used: local.used}, abandoned: make(map[uint64]*pathAbandonment), paths: map[uint64]*pathCIDState{0: {local: local, peer: peer}}, localMax: localMax, peerMax: peerMax, initialPeerMax: peerMax, localLimit: localLimit, peerLimit: peerLimit, capacity: capacity, peerHistory: map[pathCIDBinding]peerID{binding: peer.active[0]}, peerByCID: map[string]pathCIDBinding{string(peerCID): binding}}, nil
}

func pathCIDViolation(typ uint64, reason string) error {
	return &TransportError{Code: 0xa, FrameType: typ, Reason: reason}
}

// prepare returns new, uncommitted state so invalid frames cannot consume slots.
func (p *pathCIDs) prepare(id uint64) (*pathCIDState, error) {
	if p.abandoned[id] != nil {
		return nil, errPathAbandoned
	}
	if s := p.paths[id]; s != nil {
		return s, nil
	}
	if len(p.paths) >= p.capacity {
		return nil, errPathCIDCapacity
	}
	local, err := newPathLocalIDs(p.localTemplate)
	if err != nil {
		return nil, err
	}
	return &pathCIDState{local: local, peer: newPathPeerIDs(p.localLimit)}, nil
}

// receiveNew accepts both base NEW_CONNECTION_ID (path zero) and its multipath
// counterpart. Retirements are returned with explicit path IDs for the sender.
func (p *pathCIDs) receiveNew(frame wire.Frame) ([]wire.PathRetireConnectionIDFrame, error) {
	var id uint64
	var f wire.NewConnectionIDFrame
	switch v := frame.(type) {
	case wire.NewConnectionIDFrame:
		f = v
	case wire.PathNewConnectionIDFrame:
		id = v.PathID
		f = v.NewConnectionIDFrame
	default:
		return nil, errors.New("quic: expected new connection ID frame")
	}
	typ := frame.Type()
	if id > p.localMax {
		return nil, pathCIDViolation(typ, "connection ID path exceeds advertised maximum")
	}
	if len(f.ConnectionID) < 1 || len(f.ConnectionID) > 20 || f.Sequence > wire.MaxVarint || f.RetirePriorTo > f.Sequence {
		return nil, &TransportError{Code: 7, FrameType: typ, Reason: "invalid connection ID frame"}
	}
	if p.abandoned[id] != nil {
		return nil, nil
	}
	binding := pathCIDBinding{id, f.Sequence}
	key := string(f.ConnectionID)
	if prior, ok := p.peerByCID[key]; ok && prior != binding {
		return nil, pathCIDViolation(typ, "connection ID reused across sequences or paths")
	}
	old, known := p.peerHistory[binding]
	if known && (!bytes.Equal(old.cid, f.ConnectionID) || (old.hasToken && old.token != f.StatelessResetToken)) {
		return nil, pathCIDViolation(typ, "connection ID binding changed")
	}
	if !known && len(p.peerHistory) >= maxPeerCIDHistory {
		return nil, errPathCIDCapacity
	}
	s, err := p.prepare(id)
	if err != nil {
		return nil, err
	}
	retired, err := s.peer.addWithType(f, typ)
	if err != nil {
		return nil, err
	}
	p.paths[id] = s
	p.peerHistory[binding] = peerID{cid: clone(f.ConnectionID), token: f.StatelessResetToken, hasToken: true}
	p.peerByCID[key] = binding
	out := make([]wire.PathRetireConnectionIDFrame, 0, len(retired))
	for _, seq := range retired {
		out = append(out, wire.PathRetireConnectionIDFrame{PathID: id, Sequence: seq})
	}
	return out, nil
}

func (p *pathCIDs) issue(id uint64) ([]wire.PathNewConnectionIDFrame, error) {
	if id > min(p.localMax, p.peerMax) {
		return nil, errors.New("quic: path exceeds negotiated CID issuance limits")
	}
	s, err := p.prepare(id)
	if err != nil {
		return nil, err
	}
	frames, err := s.local.issue(p.peerLimit)
	if len(frames) > 0 {
		p.paths[id] = s
	}
	out := make([]wire.PathNewConnectionIDFrame, 0, len(frames))
	for _, f := range frames {
		out = append(out, wire.PathNewConnectionIDFrame{PathID: id, NewConnectionIDFrame: f})
	}
	return out, err
}

func (p *pathCIDs) markSent(id, sequence uint64) {
	if p.abandoned[id] != nil {
		return
	}
	if s := p.paths[id]; s != nil {
		s.local.markSent(sequence)
	}
}

func (p *pathCIDs) receiveRetire(frame wire.Frame, packetDCID []byte) (bool, error) {
	var id, seq uint64
	switch f := frame.(type) {
	case wire.RetireConnectionIDFrame:
		seq = f.Sequence
	case wire.PathRetireConnectionIDFrame:
		id = f.PathID
		seq = f.Sequence
	default:
		return false, errors.New("quic: expected retire connection ID frame")
	}
	if id > p.localMax {
		return false, pathCIDViolation(frame.Type(), "retirement path exceeds advertised maximum")
	}
	if p.abandoned[id] != nil {
		return false, nil
	}
	s := p.paths[id]
	if s == nil {
		return false, pathCIDViolation(frame.Type(), "retirement for unissued path CID")
	}
	return s.local.retireWithType(seq, packetDCID, frame.Type())
}

// lookupLocal returns a path only for a sent, active destination CID. The caller
// must authenticate the packet using this path's nonce/packet-number context.
func (p *pathCIDs) lookupLocal(cid []byte) (uint64, bool) {
	for id, s := range p.paths {
		if s.local.accept(cid) {
			return id, true
		}
	}
	return 0, false
}

func (p *pathCIDs) updatePeerMaximum(f wire.MaxPathIDFrame) error {
	if f.Maximum > 1<<32-1 || f.Maximum < p.initialPeerMax {
		return pathCIDViolation(f.Type(), "invalid maximum path ID")
	}
	p.peerMax = max(p.peerMax, f.Maximum)
	return nil
}
