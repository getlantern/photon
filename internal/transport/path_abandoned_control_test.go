package transport

import (
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestDisposedPathControlIgnored(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			c, keys, _ := pathDecodeFixture(t, version)
			now := time.Unix(102, 0)
			if err := c.abandonFailedPath(1); err != nil {
				t.Fatal(err)
			}
			if _, err := c.multipath.receiveAbandon(wire.PathAbandonFrame{PathID: 1}, now, time.Millisecond); err != nil {
				t.Fatal(err)
			}
			now = now.Add(3 * time.Millisecond)
			if err := c.expireMultipathPaths(now); err != nil {
				t.Fatal(err)
			}
			if c.multipath.paths[1] != nil || c.multipath.abandoned[1] == nil {
				t.Fatal("path not disposed with tombstone")
			}
			retainedHistory := len(c.multipath.peerHistory)
			retainedCIDHistory := len(c.multipath.peerByCID)
			queued, charge := len(c.spaces[applicationSpace].queue), c.queuedBytes
			frames := []wire.Frame{
				wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 1000, Largest: 1000}}}},
				wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 1000, Largest: 1000}}, ECN: &[3]uint64{1, 2, 3}}},
				wire.PathRetireConnectionIDFrame{PathID: 1, Sequence: wire.MaxVarint},
				pathNew(1, 100, 0, "disposed"),
				wire.PathStatusFrame{PathID: 1, Sequence: wire.MaxVarint, Backup: true},
				wire.PathStatusFrame{PathID: 1, Sequence: wire.MaxVarint},
				wire.PathCIDsBlockedFrame{PathID: 1, NextSequence: wire.MaxVarint},
				wire.PathAbandonFrame{PathID: 1},
			}
			for i, frame := range frames {
				payload, err := frame.Append(nil)
				if err != nil {
					t.Fatal(err)
				}
				packet := sealPathPayload(t, keys, c.config.LocalCID, 0, uint64(100+i), payload)
				if err := c.handleMultipathPacket(packet, now); err != nil {
					t.Fatalf("frame %x: %v", frame.Type(), err)
				}
				if c.multipath.paths[1] != nil || len(c.multipath.peerHistory) != retainedHistory || len(c.multipath.peerByCID) != retainedCIDHistory || len(c.spaces[applicationSpace].queue) != queued || c.queuedBytes != charge {
					t.Fatalf("frame %x resurrected state or output", frame.Type())
				}
			}
		})
	}
}

func TestMultipathControlLimitsBothVersions(t *testing.T) {
	frames := []wire.Frame{
		wire.MaxPathIDFrame{Maximum: 1 << 32},
		wire.MaxPathIDFrame{Maximum: 2},
		wire.PathAckFrame{PathID: 4, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}},
		wire.PathAckFrame{PathID: 4, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}, ECN: &[3]uint64{1, 2, 3}}},
		wire.PathAbandonFrame{PathID: 4},
		wire.PathStatusFrame{PathID: 4},
		wire.PathStatusFrame{PathID: 4, Backup: true},
		pathNew(4, 0, 0, "beyond00"),
		wire.PathRetireConnectionIDFrame{PathID: 4},
		wire.PathsBlockedFrame{Maximum: 4},
		wire.PathCIDsBlockedFrame{PathID: 4},
	}
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, frame := range frames {
			t.Run(fmt.Sprintf("%v/%x", version, frame.Type()), func(t *testing.T) {
				c, keys, _ := pathDecodeFixture(t, version)
				payload, err := frame.Append(nil)
				if err != nil {
					t.Fatal(err)
				}
				err = c.handleMultipathPacket(sealPathPayload(t, keys, c.config.LocalCID, 0, 100, payload), time.Unix(102, 0))
				requirePathError(t, err, 0xa, frame.Type())
			})
		}
	}
}
