package transport

import (
	"errors"
	"fmt"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func testPathCIDs(t *testing.T, capacity int) *pathCIDs {
	t.Helper()
	p, err := newPathCIDs([]byte("local000"), []byte("remote00"), 10, 10, 2, 2, capacity)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func pathNew(id, seq, prior uint64, cid string) wire.PathNewConnectionIDFrame {
	return wire.PathNewConnectionIDFrame{PathID: id, NewConnectionIDFrame: wire.NewConnectionIDFrame{Sequence: seq, RetirePriorTo: prior, ConnectionID: []byte(cid), StatelessResetToken: [16]byte{byte(seq)}}}
}
func requirePathError(t *testing.T, err error, code, typ uint64) {
	t.Helper()
	var te *TransportError
	if !errors.As(err, &te) || te.Code != code || te.FrameType != typ {
		t.Fatalf("expected code %x frame %x: %v", code, typ, err)
	}
}

func TestPathCIDTablePeerBindings(t *testing.T) {
	p := testPathCIDs(t, 4)
	// Equal sequence numbers on different paths are legal; equal CIDs are not.
	for _, f := range []wire.PathNewConnectionIDFrame{pathNew(1, 0, 0, "pathone0"), pathNew(2, 0, 0, "pathtwo0")} {
		if _, err := p.receiveNew(f); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []wire.PathNewConnectionIDFrame{pathNew(2, 1, 0, "pathone0"), pathNew(1, 1, 0, "remote00"), pathNew(1, 0, 0, "modified")} {
		_, err := p.receiveNew(f)
		requirePathError(t, err, 0xa, f.Type())
	}
	f := pathNew(1, 0, 0, "pathone0")
	f.StatelessResetToken[0] = 99
	_, err := p.receiveNew(f)
	requirePathError(t, err, 0xa, f.Type())
	retired, err := p.receiveNew(pathNew(1, 1, 1, "pathone1"))
	if err != nil || len(retired) != 1 || retired[0].PathID != 1 || retired[0].Sequence != 0 {
		t.Fatal(retired, err)
	}
	if p.paths[2].peer.current != 0 || len(p.paths[2].peer.active) != 1 {
		t.Fatal("retirement crossed paths")
	}
	// Retired CID history continues preventing reassignment to another path.
	_, err = p.receiveNew(pathNew(3, 0, 0, "pathone0"))
	requirePathError(t, err, 0xa, wire.FramePathNewConnectionID)
	if _, ok := p.paths[3]; ok {
		t.Fatal("failed frame allocated path")
	}
	retired, err = p.receiveNew(pathNew(1, 0, 0, "pathone0"))
	if err != nil || len(retired) != 1 || retired[0].Sequence != 0 || len(p.paths[1].peer.active) != 1 {
		t.Fatal("retired duplicate revived", retired, err)
	}
	// Base and multipath frame forms share path zero's binding space.
	base := pathNew(0, 1, 0, "remote01").NewConnectionIDFrame
	if _, err = p.receiveNew(base); err != nil {
		t.Fatal(err)
	}
	if _, err = p.receiveNew(wire.PathNewConnectionIDFrame{NewConnectionIDFrame: base}); err != nil {
		t.Fatal(err)
	}
	base.ConnectionID = []byte("changed0")
	_, err = p.receiveNew(base)
	requirePathError(t, err, 0xa, 0x18)
	if len(p.peerHistory) != 5 || len(p.peerByCID) != 5 {
		t.Fatal("failed/duplicate frames grew history")
	}
}

func TestPathCIDTableRoutingAndRetirement(t *testing.T) {
	p := testPathCIDs(t, 3)
	seen := map[string]bool{"local000": true}
	for _, id := range []uint64{1, 2} {
		frames, err := p.issue(id)
		if err != nil || len(frames) != 2 {
			t.Fatal(frames, err)
		}
		for seq, f := range frames {
			if f.PathID != id || f.Sequence != uint64(seq) || seen[string(f.ConnectionID)] {
				t.Fatal("wrong binding or CID reuse")
			}
			seen[string(f.ConnectionID)] = true
			if _, ok := p.lookupLocal(f.ConnectionID); ok {
				t.Fatal("unsent CID routable")
			}
			p.markSent(id, f.Sequence)
			if path, ok := p.lookupLocal(f.ConnectionID); !ok || path != id {
				t.Fatal("wrong routing path")
			}
		}
		retire := wire.PathRetireConnectionIDFrame{PathID: id, Sequence: 0}
		_, err = p.receiveRetire(retire, frames[0].ConnectionID)
		requirePathError(t, err, 0xa, retire.Type())
		// Packet can arrive on a different path; only its actual DCID is forbidden.
		if changed, err := p.receiveRetire(retire, []byte("local000")); err != nil || !changed {
			t.Fatal(changed, err)
		}
		if _, ok := p.lookupLocal(frames[0].ConnectionID); ok {
			t.Fatal("retired CID routed")
		}
		if changed, err := p.receiveRetire(retire, []byte("local000")); err != nil || changed {
			t.Fatal("duplicate retirement", err)
		}
		next, err := p.issue(id)
		if err != nil || len(next) != 1 || next[0].Sequence != 2 {
			t.Fatal(next, err)
		}
	}
	if id, ok := p.lookupLocal([]byte("local000")); !ok || id != 0 {
		t.Fatal("path zero lost")
	}
	if changed, err := p.receiveRetire(wire.RetireConnectionIDFrame{}, []byte("otherCID")); err != nil || !changed {
		t.Fatal(changed, err)
	}
	if _, ok := p.lookupLocal([]byte("local000")); ok {
		t.Fatal("base retirement not applied to path zero")
	}
}

func TestPathCIDTableBounds(t *testing.T) {
	p := testPathCIDs(t, 2)
	_, err := p.receiveNew(pathNew(11, 0, 0, "toolarge"))
	requirePathError(t, err, 0xa, wire.FramePathNewConnectionID)
	_, err = p.receiveRetire(wire.PathRetireConnectionIDFrame{PathID: 11}, nil)
	requirePathError(t, err, 0xa, wire.FramePathRetireConnectionID)
	_, err = p.receiveRetire(wire.PathRetireConnectionIDFrame{PathID: 1}, nil)
	requirePathError(t, err, 0xa, wire.FramePathRetireConnectionID)
	if len(p.paths) != 1 {
		t.Fatal("invalid frames consumed capacity")
	}
	if _, err = p.receiveNew(pathNew(1, 0, 0, "pathone0")); err != nil {
		t.Fatal(err)
	}
	if _, err = p.receiveNew(pathNew(2, 0, 0, "pathtwo0")); !errors.Is(err, errPathCIDCapacity) {
		t.Fatal("missing path bound", err)
	}
	if _, err = p.issue(2); !errors.Is(err, errPathCIDCapacity) {
		t.Fatal("issuance bypassed path bound", err)
	}
	if _, err = p.receiveNew(pathNew(1, 1, 0, "pathone1")); err != nil {
		t.Fatal(err)
	}
	_, err = p.receiveNew(pathNew(1, 2, 0, "pathone2"))
	requirePathError(t, err, 9, wire.FramePathNewConnectionID)
	if len(p.peerHistory) != 3 || len(p.paths) != 2 {
		t.Fatal("failed frame changed table")
	}
	for len(p.peerHistory) < maxPeerCIDHistory {
		n := uint64(len(p.peerHistory))
		cid := fmt.Sprintf("saved%d", n)
		b := pathCIDBinding{9, n}
		p.peerHistory[b] = peerID{cid: []byte(cid)}
		p.peerByCID[cid] = b
	}
	if _, err = p.receiveNew(pathNew(1, 2, 2, "pathone2")); !errors.Is(err, errPathCIDCapacity) {
		t.Fatal("missing history bound", err)
	}
	if p.paths[1].peer.retireBefore != 0 {
		t.Fatal("capacity error changed retirement floor")
	}
	if _, err = p.receiveNew(pathNew(1, 1, 0, "pathone1")); err != nil {
		t.Fatal("duplicate rejected at capacity", err)
	}
}

func TestPathCIDMaximumUpdate(t *testing.T) {
	p := testPathCIDs(t, 4)
	for _, v := range []uint64{9, 1 << 32} {
		requirePathError(t, p.updatePeerMaximum(wire.MaxPathIDFrame{Maximum: v}), 0xa, wire.FrameMaxPathID)
	}
	if err := p.updatePeerMaximum(wire.MaxPathIDFrame{Maximum: 20}); err != nil {
		t.Fatal(err)
	}
	if err := p.updatePeerMaximum(wire.MaxPathIDFrame{Maximum: 10}); err != nil || p.peerMax != 20 {
		t.Fatal("reordered limit", err)
	}
	if _, err := p.issue(11); err == nil {
		t.Fatal("ignored local maximum")
	}
	// Large wire maxima do not preallocate corresponding path state.
	if err := p.updatePeerMaximum(wire.MaxPathIDFrame{Maximum: 1<<32 - 1}); err != nil || len(p.paths) != 1 {
		t.Fatal(err)
	}
}

func FuzzPathCIDTable(f *testing.F) {
	f.Add([]byte{1, 0, 0, 1, 2, 0, 0, 2, 1, 1, 1, 3})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 4096 {
			return
		}
		p, err := newPathCIDs([]byte("local000"), []byte("remote00"), 7, 7, 2, 2, 4)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i+3 < len(b); i += 4 {
			_, _ = p.receiveNew(pathNew(uint64(b[i]), uint64(b[i+1]), uint64(b[i+2]), string([]byte{b[i+3]})))
			if len(p.paths) > 4 || len(p.peerHistory) > maxPeerCIDHistory || len(p.peerByCID) != len(p.peerHistory) {
				t.Fatal("table bound violated")
			}
			active := map[string]pathCIDBinding{}
			for id, s := range p.paths {
				if id > 7 || len(s.peer.active) > 2 {
					t.Fatal("path limit violated")
				}
				for seq, cid := range s.peer.active {
					if seq < s.peer.retireBefore {
						t.Fatal("retired CID active")
					}
					binding := pathCIDBinding{id, seq}
					key := string(cid.cid)
					if previous, exists := active[key]; exists && previous != binding {
						t.Fatal("cross-path CID collision")
					}
					active[key] = binding
					if got, ok := p.peerByCID[key]; !ok || got != binding {
						t.Fatal("active binding missing from history")
					}
				}
			}
		}
	})
}
