package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestPathAbandonmentRetention(t *testing.T) {
	for _, peerFirst := range []bool{false, true} {
		p := testPathCIDs(t, 2)
		frames, err := p.issue(1)
		if err != nil {
			t.Fatal(err)
		}
		p.markSent(1, frames[0].Sequence)
		if _, err = p.receiveNew(pathNew(1, 0, 0, "pathone0")); err != nil {
			t.Fatal(err)
		}
		now := time.Unix(100, 0)
		pto := time.Second
		if !peerFirst {
			if first, err := p.beginAbandon(1); err != nil || !first {
				t.Fatal(first, err)
			}
			if len(p.collectAbandoned(now.Add(time.Hour))) != 0 {
				t.Fatal("collected without peer abandonment")
			}
		}
		reply, err := p.receiveAbandon(wire.PathAbandonFrame{PathID: 1}, now, pto)
		if err != nil || reply != peerFirst {
			t.Fatal(reply, err)
		}
		if len(p.paths[1].peer.active) != 0 {
			t.Fatal("peer IDs or reset tokens remain active")
		}
		if len(p.collectAbandoned(now.Add(2*pto))) != 0 {
			t.Fatal("collected before retention deadline")
		}
		if path, ok := p.lookupLocal(frames[0].ConnectionID); !ok || path != 1 {
			t.Fatal("in-flight packet CID lost")
		}
		if peerFirst {
			if len(p.collectAbandoned(now.Add(4*pto))) != 0 {
				t.Fatal("collected before queuing response")
			}
			if first, err := p.beginAbandon(1); err != nil || !first {
				t.Fatal(first, err)
			}
		}
		if reply, err = p.receiveAbandon(wire.PathAbandonFrame{PathID: 1}, now.Add(2*pto), pto); err != nil || reply {
			t.Fatal("duplicate response request", err)
		}
		if got := p.collectAbandoned(now.Add(3 * pto)); len(got) != 1 || got[0].ID != 1 {
			t.Fatal("duplicate extended retention", got)
		}
		if _, ok := p.lookupLocal(frames[0].ConnectionID); ok {
			t.Fatal("disposed path still routable")
		}
		if len(p.collectAbandoned(now.Add(time.Hour))) != 0 {
			t.Fatal("disposed twice")
		}
		if _, err := p.issue(1); !errors.Is(err, errPathAbandoned) {
			t.Fatal("path ID reused", err)
		}
		if _, err := p.receiveNew(pathNew(1, 5, 0, "latecid0")); err != nil || p.paths[1] != nil {
			t.Fatal("late frame revived path", err)
		}
		if changed, err := p.receiveRetire(wire.PathRetireConnectionIDFrame{PathID: 1}, nil); err != nil || changed {
			t.Fatal("late retirement", err)
		}
		if changed, err := p.receiveStatus(wire.PathStatusFrame{PathID: 1, Sequence: 99}); err != nil || changed {
			t.Fatal("late status", err)
		}
		if _, err := p.issue(2); err != nil {
			t.Fatal("slot not reclaimed", err)
		}
	}
}

func TestPathZeroAbandonment(t *testing.T) {
	p := testPathCIDs(t, 2)
	now := time.Unix(100, 0)
	p.beginAbandon(0)
	p.receiveAbandon(wire.PathAbandonFrame{}, now, time.Second)
	p.collectAbandoned(now.Add(3 * time.Second))
	if p.paths[0] != nil {
		t.Fatal("path zero retained")
	}
	frames, err := p.issue(1)
	if err != nil || len(frames) != 2 {
		t.Fatal("new path depends on path zero state", err)
	}
	for _, f := range frames {
		if string(f.ConnectionID) == "local000" {
			t.Fatal("retired path zero CID reused")
		}
	}
}

func TestPathStatusOrdering(t *testing.T) {
	p := testPathCIDs(t, 3)
	for _, id := range []uint64{1, 2} {
		if changed, err := p.receiveStatus(wire.PathStatusFrame{PathID: id, Backup: true}); err != nil || !changed {
			t.Fatal(changed, err)
		}
	}
	if changed, err := p.receiveStatus(wire.PathStatusFrame{PathID: 1, Sequence: 2}); err != nil || !changed {
		t.Fatal(changed, err)
	}
	for _, seq := range []uint64{0, 1, 2} {
		if changed, err := p.receiveStatus(wire.PathStatusFrame{PathID: 1, Sequence: seq, Backup: true}); err != nil || changed {
			t.Fatal("stale status applied", err)
		}
	}
	if p.paths[1].backup || !p.paths[2].backup {
		t.Fatal("status sequence crossed paths")
	}
	if len(p.paths[1].local.active) != 0 || len(p.paths[1].peer.active) != 0 {
		t.Fatal("status issued a CID")
	}
	_, err := p.receiveStatus(wire.PathStatusFrame{PathID: 11})
	requirePathError(t, err, 0xa, wire.FramePathStatusAvailable)
	_, err = p.receiveStatus(wire.PathStatusFrame{PathID: 3})
	if !errors.Is(err, errPathCIDCapacity) {
		t.Fatal(err)
	}
}

func TestPathAbandonmentBounds(t *testing.T) {
	p := testPathCIDs(t, 2)
	now := time.Unix(100, 0)
	_, err := p.receiveAbandon(wire.PathAbandonFrame{PathID: 11}, now, time.Second)
	requirePathError(t, err, 0xa, wire.FramePathAbandon)
	for _, pto := range []time.Duration{0, -1, time.Duration(1<<63 - 1)} {
		if _, err := p.receiveAbandon(wire.PathAbandonFrame{PathID: 1}, now, pto); err == nil {
			t.Fatal("invalid retention timer")
		}
	}
	if len(p.abandoned) != 0 {
		t.Fatal("invalid frame consumed tombstone")
	}
	// Abandoning a never-opened path does not allocate a CID slot.
	if reply, err := p.receiveAbandon(wire.PathAbandonFrame{PathID: 1}, now, time.Second); err != nil || !reply || len(p.paths) != 1 {
		t.Fatal(reply, err)
	}
	p.peerMax = 0
	if _, err := p.beginAbandon(1); err != nil {
		t.Fatal("cannot acknowledge peer abandonment", err)
	}
	for len(p.abandoned) < maxAbandonedPaths {
		p.abandoned[uint64(len(p.abandoned)+20)] = new(pathAbandonment)
	}
	if _, err := p.beginAbandon(0); !errors.Is(err, errPathCIDCapacity) {
		t.Fatal("missing tombstone bound", err)
	}
	if len(p.paths[0].peer.active) != 1 {
		t.Fatal("failed abandonment retired CIDs")
	}
}
