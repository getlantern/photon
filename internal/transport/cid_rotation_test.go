package transport

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestPeerRotationSkipsUsedCID(t *testing.T) {
	p := newPeerIDs([]byte("initial"), 4)
	for seq := uint64(1); seq <= 3; seq++ {
		if _, err := p.add(wire.NewConnectionIDFrame{Sequence: seq, ConnectionID: []byte{byte(seq)}}); err != nil {
			t.Fatal(err)
		}
	}
	used := p.active[1]
	used.used = true
	p.active[1] = used
	retired, err := p.rotate()
	if err != nil || p.current != 2 || !reflect.DeepEqual(retired, []uint64{0, 1}) {
		t.Fatal(retired, err, p.current)
	}
	used = p.active[3]
	used.used = true
	p.active[3] = used
	if _, ok := p.nextUnused(); ok {
		t.Fatal("used CID offered as fresh")
	}
	if _, err := p.rotate(); err == nil || p.current != 2 || len(p.active) != 2 || p.retireBefore != 2 {
		t.Fatal("failed rotation mutated CID state")
	}
}

func TestCIDRotationQueueAdmissionAtomic(t *testing.T) {
	for _, multipath := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "multipath"}[multipath], func(t *testing.T) {
			var c *Conn
			if multipath {
				c, _ = sendingPathPair(t, wire.Version1)
			} else {
				client, server := testPair(t, wire.Version1)
				drivePair(t, client, server, false, false)
				c = client
			}
			if !c.CanRotatePeerID() {
				t.Fatal("missing spare CID")
			}
			manager := c.peerIDs
			oldCID := clone(c.peerCID)
			oldCurrent, oldFloor, oldCount := manager.current, manager.retireBefore, len(manager.active)
			next, _ := manager.nextUnused()
			retirementCount := 0
			for seq := range manager.active {
				if seq < next {
					retirementCount++
				}
			}
			savedBytes := c.queuedBytes
			c.queuedBytes = maxSendBytes - 16*retirementCount + 1
			beforeQueue := len(c.spaces[applicationSpace].queue)
			if err := c.RotatePeerID(); !errors.Is(err, ErrSendBuffer) {
				t.Fatal(err)
			}
			if c.Err() != nil || c.peerIDs != manager || manager.current != oldCurrent || manager.retireBefore != oldFloor || len(manager.active) != oldCount || !bytes.Equal(c.peerCID, oldCID) || len(c.spaces[applicationSpace].queue) != beforeQueue || c.queuedBytes != maxSendBytes-16*retirementCount+1 {
				t.Fatal("rejected rotation changed state or closed connection")
			}
			c.queuedBytes = maxSendBytes - 16*retirementCount
			if err := c.RotatePeerID(); err != nil {
				t.Fatal(err)
			}
			if c.peerIDs != manager || manager.current != next || c.queuedBytes != maxSendBytes || len(c.spaces[applicationSpace].queue) != beforeQueue+retirementCount {
				t.Fatal("rotation did not commit complete batch")
			}
			if multipath && c.multipath.paths[0].peer != manager {
				t.Fatal("rotation broke path-zero alias")
			}
			for _, item := range c.spaces[applicationSpace].queue[beforeQueue:] {
				frame, ok := item.frame.(wire.RetireConnectionIDFrame)
				if !ok || frame.Sequence >= next || !item.queued || item.bytes != 16 {
					t.Fatal("invalid retirement batch")
				}
			}
			c.queuedBytes = savedBytes + 16*retirementCount
			c.Close()
			if c.CanRotatePeerID() {
				t.Fatal("closed connection offers rotation")
			}
		})
	}
}
