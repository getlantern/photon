package transport

import (
	"errors"
	"sort"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

var errPathAbandoned = errors.New("quic: path ID has been abandoned")

const maxAbandonedPaths = 4096

type pathAbandonment struct {
	sent, received bool
	retainUntil    time.Time
}

// stop retires peer CIDs immediately, including their stateless reset tokens.
// Local CIDs remain routable for packets already in flight. Tombstones survive
// resource reclamation so a path ID can never acquire fresh packet numbers.
func (p *pathCIDs) stop(id uint64) (*pathAbandonment, error) {
	if a := p.abandoned[id]; a != nil {
		return a, nil
	}
	if len(p.abandoned) >= maxAbandonedPaths {
		return nil, errPathCIDCapacity
	}
	a := new(pathAbandonment)
	p.abandoned[id] = a
	if s := p.paths[id]; s != nil {
		clear(s.peer.active)
	}
	return a, nil
}

// beginAbandon is called when the owner commits a PATH_ABANDON to its reliable
// send queue. It returns true only for the first local abandonment. Queue loss
// recovery must retransmit that frame on a surviving path. Closing the final
// usable path is a connection-level decision, not a CID-table operation.
func (p *pathCIDs) beginAbandon(id uint64) (bool, error) {
	if id > p.localMax || (id > p.peerMax && (p.abandoned[id] == nil || !p.abandoned[id].received)) {
		return false, errors.New("quic: abandonment exceeds negotiated path limits")
	}
	a, err := p.stop(id)
	if err != nil {
		return false, err
	}
	first := !a.sent
	a.sent = true
	return first, nil
}

// receiveAbandon returns whether a corresponding local abandonment still needs
// to be queued. Duplicate frames do not extend the retention deadline. The
// caller supplies the path's PTO and keeps packet-number/recovery state until
// collectAbandoned reports the path ready for disposal.
func (p *pathCIDs) receiveAbandon(f wire.PathAbandonFrame, now time.Time, pto time.Duration) (bool, error) {
	if err := p.validateAbandon(f, now, pto); err != nil {
		return false, err
	}
	a, err := p.stop(f.PathID)
	if err != nil {
		return false, err
	}
	if !a.received {
		a.received = true
		a.retainUntil = now.Add(3 * pto)
	}
	return !a.sent, nil
}

func (p *pathCIDs) validateAbandon(f wire.PathAbandonFrame, now time.Time, pto time.Duration) error {
	if f.PathID > p.localMax {
		return pathCIDViolation(f.Type(), "abandoned path exceeds advertised maximum")
	}
	if f.ErrorCode > wire.MaxVarint {
		return &TransportError{Code: 7, FrameType: f.Type(), Reason: "invalid path abandonment error code"}
	}
	if now.IsZero() || pto <= 0 || pto > time.Duration(1<<63-1)/3 {
		return errors.New("quic: invalid path retention timer")
	}
	return nil
}

// collectAbandoned removes retained CID resources after both sides abandon and
// three PTOs elapse from reception. Returned results identify remaining lost
// transmissions so the engine can recover their reliable data on another path.
// Tombstones and uniqueness history remain bounded for the connection lifetime.
type collectedPath struct {
	ID       uint64
	Lost     []recovery.Packet
	Reliable []*sendItem
}

func (p *pathCIDs) collectAbandoned(now time.Time) []collectedPath {
	var disposed []collectedPath
	for id := range p.paths {
		a := p.abandoned[id]
		if a == nil || !a.sent || !a.received || now.Before(a.retainUntil) {
			continue
		}
		result := collectedPath{ID: id}
		if app := p.paths[id].application; app != nil {
			result.Lost = app.sent.Abandon(recovery.Application)
			result.Reliable = detachItems(app.space)
			app.timer.DiscardKeys()
		}
		delete(p.paths, id)
		disposed = append(disposed, result)
	}
	sort.Slice(disposed, func(i, j int) bool { return disposed[i].ID < disposed[j].ID })
	return disposed
}

func (p *pathCIDs) receiveStatus(f wire.PathStatusFrame) (bool, error) {
	if f.PathID > p.localMax {
		return false, pathCIDViolation(f.Type(), "status path exceeds advertised maximum")
	}
	if f.Sequence > wire.MaxVarint {
		return false, &TransportError{Code: 7, FrameType: f.Type(), Reason: "invalid path status sequence"}
	}
	if p.abandoned[f.PathID] != nil {
		return false, nil
	}
	s, err := p.prepare(f.PathID)
	if err != nil {
		return false, err
	}
	if s.statusKnown && f.Sequence <= s.statusSequence {
		return false, nil
	}
	s.statusKnown = true
	s.statusSequence = f.Sequence
	s.backup = f.Backup
	p.paths[f.PathID] = s
	return true, nil
}
