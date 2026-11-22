package transport

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestPathPeerIDs(t *testing.T) {
	paths := []*peerIDs{newPathPeerIDs(2), newPathPeerIDs(2)}
	for i, p := range paths {
		f := wire.PathNewConnectionIDFrame{PathID: uint64(i + 1), NewConnectionIDFrame: wire.NewConnectionIDFrame{ConnectionID: []byte{byte(i + 1)}, StatelessResetToken: [16]byte{byte(i + 1)}}}
		if retire, err := p.addPath(f); err != nil || len(retire) != 0 || p.current != 0 || len(p.active) != 1 {
			t.Fatalf("first sequence zero: %v %v", retire, err)
		}
		if _, err := p.addPath(f); err != nil {
			t.Fatal("duplicate", err)
		}
		f.ConnectionID[0] ^= 0xff
		if p.active[0].cid[0] != byte(i+1) {
			t.Fatal("CID borrowed from frame")
		}
	}
	f := wire.PathNewConnectionIDFrame{PathID: 1, NewConnectionIDFrame: wire.NewConnectionIDFrame{Sequence: 1, ConnectionID: []byte{3}}}
	if _, err := paths[0].addPath(f); err != nil {
		t.Fatal(err)
	}
	if retired, err := paths[0].rotate(); err != nil || !reflect.DeepEqual(retired, []uint64{0}) {
		t.Fatal(retired, err)
	}
	if paths[1].current != 0 || len(paths[1].active) != 1 || paths[1].retireBefore != 0 {
		t.Fatal("rotation affected another path")
	}
	// A first arriving frame need not have sequence zero.
	p := newPathPeerIDs(2)
	f.NewConnectionIDFrame = wire.NewConnectionIDFrame{Sequence: 7, RetirePriorTo: 5, ConnectionID: []byte{7}}
	if _, err := p.addPath(f); err != nil || p.current != 7 {
		t.Fatal("out-of-order first CID", err)
	}
	f.NewConnectionIDFrame = wire.NewConnectionIDFrame{Sequence: 0, ConnectionID: []byte{1}}
	if retired, err := p.addPath(f); err != nil || !reflect.DeepEqual(retired, []uint64{0}) || len(p.active) != 1 {
		t.Fatal("retired CID revived", retired, err)
	}
	f.NewConnectionIDFrame = wire.NewConnectionIDFrame{Sequence: 8, ConnectionID: []byte{8}}
	if _, err := p.addPath(f); err != nil {
		t.Fatal(err)
	}
	f.NewConnectionIDFrame = wire.NewConnectionIDFrame{Sequence: 9, ConnectionID: []byte{9}}
	_, err := p.addPath(f)
	var te *TransportError
	if !errors.As(err, &te) || te.Code != 9 || te.FrameType != wire.FramePathNewConnectionID || len(p.active) != 2 {
		t.Fatal("path CID limit", err)
	}
	f.NewConnectionIDFrame = wire.NewConnectionIDFrame{Sequence: 8, ConnectionID: []byte{99}}
	_, err = p.addPath(f)
	if !errors.As(err, &te) || te.Code != 0xa || te.FrameType != wire.FramePathNewConnectionID || p.active[8].cid[0] != 8 {
		t.Fatal("conflicting duplicate", err)
	}
}

func TestPathLocalIDs(t *testing.T) {
	initial := newLocalIDs([]byte("initial0"))
	one, err := newPathLocalIDs(initial)
	if err != nil {
		t.Fatal(err)
	}
	two, err := newPathLocalIDs(initial)
	if err != nil {
		t.Fatal(err)
	}
	for i, l := range []*localIDs{one, two} {
		_, err := l.retirePath(wire.PathRetireConnectionIDFrame{PathID: uint64(i + 1)}, []byte("othercid"))
		var te *TransportError
		if !errors.As(err, &te) || te.Code != 0xa || te.FrameType != wire.FramePathRetireConnectionID {
			t.Fatal("unissued sequence zero", err)
		}
		frames, err := l.issue(2)
		if err != nil || len(frames) != 2 || frames[0].Sequence != 0 || frames[1].Sequence != 1 {
			t.Fatal("path sequence origin", frames, err)
		}
		if l.accept(frames[0].ConnectionID) {
			t.Fatal("unsent CID accepted")
		}
		if _, err := l.retirePath(wire.PathRetireConnectionIDFrame{}, frames[1].ConnectionID); err == nil {
			t.Fatal("unsent CID retired")
		}
		l.markSent(0)
		l.markSent(1)
		if _, err := l.retirePath(wire.PathRetireConnectionIDFrame{}, frames[0].ConnectionID); err == nil {
			t.Fatal("packet destination retired")
		}
		if changed, err := l.retirePath(wire.PathRetireConnectionIDFrame{}, frames[1].ConnectionID); err != nil || !changed {
			t.Fatal(changed, err)
		}
		if l.accept(frames[0].ConnectionID) {
			t.Fatal("retired CID routed")
		}
		if _, ok := initial.used[string(frames[0].ConnectionID)]; !ok {
			t.Fatal("retired CID lost from shared history")
		}
		if _, ok := two.used[string(frames[0].ConnectionID)]; !ok {
			t.Fatal("uniqueness registry not shared")
		}
		next, err := l.issue(2)
		if err != nil || len(next) != 1 || next[0].Sequence != 2 {
			t.Fatal("per-path sequence advance", next, err)
		}
		before := bytes.Clone(l.active[2].cid)
		next[0].ConnectionID[0] ^= 1
		if !bytes.Equal(before, l.active[2].cid) {
			t.Fatal("issued frame aliases state")
		}
	}
	if initial.highest != 0 || len(initial.active) != 1 || !initial.accept([]byte("initial0")) {
		t.Fatal("additional paths changed path zero")
	}
	if len(initial.used) != 7 {
		t.Fatalf("shared lifetime history: %d", len(initial.used))
	}
	// The lifetime allocation bound is connection-wide, not multiplied per path.
	for len(initial.used) < 4096 {
		n := len(initial.used)
		initial.used[string([]byte{0, byte(n >> 8), byte(n)})] = struct{}{}
	}
	one.retire(1, []byte("othercid"))
	if frames, err := one.issue(4); err != nil || len(frames) != 0 {
		t.Fatal("shared history cap ignored", err)
	}
	for _, invalid := range []*localIDs{nil, newLocalIDs(nil)} {
		if _, err := newPathLocalIDs(invalid); err == nil {
			t.Fatal("accepted zero-length path CID setup")
		}
	}
}

func FuzzPathPeerIDs(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 7, 5, 0, 2, 1, 1, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 4096 {
			return
		}
		paths := []*peerIDs{newPathPeerIDs(2), newPathPeerIDs(2)}
		for i := 0; i+2 < len(b); i += 3 {
			n := int(b[i] & 1)
			p := paths[n]
			other := paths[1-n]
			otherCurrent, otherFloor, otherSize := other.current, other.retireBefore, len(other.active)
			seq := uint64(b[i+1])
			floor := p.retireBefore
			_, _ = p.addPath(wire.PathNewConnectionIDFrame{PathID: uint64(n + 1), NewConnectionIDFrame: wire.NewConnectionIDFrame{Sequence: seq, RetirePriorTo: uint64(b[i+2]), ConnectionID: []byte{byte(n + 1), byte(seq)}}})
			if len(p.active) > 2 || p.retireBefore < floor {
				t.Fatal("CID bound or floor violated")
			}
			if len(p.active) > 0 {
				if _, ok := p.active[p.current]; !ok {
					t.Fatal("missing current CID")
				}
			}
			for s := range p.active {
				if s < p.retireBefore {
					t.Fatal("retired CID revived")
				}
			}
			if other.current != otherCurrent || other.retireBefore != otherFloor || len(other.active) != otherSize {
				t.Fatal("changed another path")
			}
		}
	})
}
