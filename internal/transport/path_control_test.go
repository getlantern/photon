package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestEncryptedPathCIDControl(t *testing.T) {
	c, keys, _ := pathDecodeFixture(t, wire.Version2)
	now := time.Unix(101, 0)
	pn := uint64(100)
	deliver := func(f wire.Frame) {
		t.Helper()
		payload, err := f.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = c.handleMultipathPacket(sealPathPayload(t, keys, c.config.LocalCID, 0, pn, payload), now); err != nil {
			t.Fatal(err)
		}
		pn++
	}
	deliver(pathNew(1, 0, 0, "remote10"))
	deliver(pathNew(1, 1, 1, "remote11"))
	queue := c.spaces[applicationSpace].queue
	response, ok := queue[len(queue)-1].frame.(wire.PathRetireConnectionIDFrame)
	if !ok || response.PathID != 1 || response.Sequence != 0 {
		t.Fatal("incorrect retirement response")
	}
	if c.multipath.paths[1].peer.current != 1 || c.peerIDs.current != 0 {
		t.Fatal("peer CID retirement crossed paths")
	}
	// Retire local path-one CID using a packet addressed to path zero.
	deliver(wire.PathRetireConnectionIDFrame{PathID: 1, Sequence: 0})
	queue = c.spaces[applicationSpace].queue
	replacement, ok := queue[len(queue)-1].frame.(wire.PathNewConnectionIDFrame)
	if !ok || replacement.PathID != 1 || replacement.Sequence != 2 {
		t.Fatal("incorrect replacement CID")
	}
	if _, ok := c.multipath.lookupLocal(replacement.ConnectionID); ok {
		t.Fatal("queued unsent replacement routed")
	}
	before := len(queue)
	deliver(wire.PathRetireConnectionIDFrame{PathID: 1, Sequence: 0})
	if len(c.spaces[applicationSpace].queue) != before {
		t.Fatal("duplicate retirement generated replacement")
	}
}

func TestEncryptedPathStatusAndLimits(t *testing.T) {
	c, keys, _ := pathDecodeFixture(t, wire.Version1)
	now := time.Unix(101, 0)
	frames := []wire.Frame{
		wire.PathStatusFrame{PathID: 1, Sequence: 2, Backup: true},
		wire.PathStatusFrame{PathID: 1, Sequence: 1},
		wire.MaxPathIDFrame{Maximum: 5},
		wire.MaxPathIDFrame{Maximum: 3},
		wire.PathsBlockedFrame{Maximum: 3},
		wire.PathCIDsBlockedFrame{PathID: 2, NextSequence: 0},
	}
	for i, f := range frames {
		b, err := f.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = c.handleMultipathPacket(sealPathPayload(t, keys, c.config.LocalCID, 0, uint64(100+i), b), now); err != nil {
			t.Fatal(err)
		}
	}
	if !c.multipath.paths[1].backup || c.multipath.paths[1].statusSequence != 2 || c.multipath.peerMax != 5 {
		t.Fatal("control ordering")
	}
	if c.multipath.paths[2] != nil {
		t.Fatal("informational frame allocated path")
	}
}

func TestEncryptedPathControlViolations(t *testing.T) {
	for _, f := range []wire.Frame{
		wire.PathsBlockedFrame{Maximum: 4},
		wire.PathCIDsBlockedFrame{PathID: 4},
		wire.PathCIDsBlockedFrame{PathID: 2, NextSequence: 1},
		wire.MaxPathIDFrame{Maximum: 2},
		wire.PathStatusFrame{PathID: 4},
	} {
		c, keys, _ := pathDecodeFixture(t, wire.Version1)
		b, err := f.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		err = c.handleMultipathPacket(sealPathPayload(t, keys, c.config.LocalCID, 0, 100, b), time.Unix(101, 0))
		requirePathError(t, err, 0xa, f.Type())
	}
}

func TestPathControlResponseSelectionAndBound(t *testing.T) {
	c, _, _ := pathDecodeFixture(t, wire.Version1)
	// The preferred path lacks a peer CID; queue on usable path zero.
	f := wire.PathsBlockedFrame{Maximum: 3}
	base := len(c.spaces[applicationSpace].queue)
	if err := c.queuePathControl(1, f); err != nil {
		t.Fatal(err)
	}
	if len(c.spaces[applicationSpace].queue) != base+1 || len(c.multipath.paths[1].application.space.queue) != 0 {
		t.Fatal("selected unusable path")
	}
	c.queuedBytes = maxSendBytes
	if err := c.queuePathControl(0, f); err != ErrSendBuffer || len(c.spaces[applicationSpace].queue) != base+1 {
		t.Fatal("queue capacity changed output", err)
	}
}
