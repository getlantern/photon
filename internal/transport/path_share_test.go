package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestSharedPathDataBlockedAndCommitted(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		c, _ := sendingPathPair(t, version)
		source := c.multipath.paths[0].application
		dest := c.multipath.paths[1].application
		// Keep a path-affine probe ahead of the stream; only the stream may move.
		if err := c.queueFrameOnPath(0, wire.PathResponseFrame{}); err != nil {
			t.Fatal(err)
		}
		if err := c.queueFrameOnPath(0, wire.StreamFrame{StreamID: 0, Data: []byte("shared")}); err != nil {
			t.Fatal(err)
		}
		item := source.space.queue[len(source.space.queue)-1]
		before, queued := c.queuedBytes, len(source.space.queue)
		now := time.Unix(101, 0)
		checkBlocked := func() {
			t.Helper()
			if b, err := c.nextSharedPathData(1, []uint64{0, 1}, now); err != nil || b != nil {
				t.Fatal(err)
			}
			if len(source.space.queue) != queued || source.space.queue[queued-1] != item || len(dest.space.queue) != 0 || c.queuedBytes != before || !item.queued {
				t.Fatal("blocked borrowing changed ownership")
			}
		}
		checkBlocked() // unvalidated
		dest.validation.state = PathValidated
		checkBlocked() // no amplification credit
		dest.budget.Validate()
		pn := dest.space.nextPN
		if b, err := c.nextSharedPathData(1, []uint64{0, 1}, now); err != nil || b == nil {
			t.Fatal(err)
		}
		if len(source.space.queue) != queued-1 || dest.space.sent[pn] != item || item.queued || c.queuedBytes != before {
			t.Fatal("committed borrowing lost ownership/charge")
		}
		// Borrowed reliable data is completed once by its destination ACK.
		ack := wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: pn, Largest: pn}}}}
		if _, err := c.multipath.receiveReliableACK(ack, now.Add(time.Millisecond), 0, 25*time.Millisecond, true, func(item *sendItem) error { c.queuedBytes -= item.bytes; return nil }); err != nil {
			t.Fatal(err)
		}
		if !item.acked || c.queuedBytes != before-item.bytes {
			t.Fatal("ACK failed to complete borrowed data")
		}
	}
}

func TestSchedulerUsesIdleValidatedPath(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	dest := c.multipath.paths[1].application
	dest.validation.state = PathValidated
	dest.budget.Validate()
	if err := c.queueFrameOnPath(0, wire.StreamFrame{StreamID: 0, Data: []byte("busy source")}); err != nil {
		t.Fatal(err)
	}
	c.budget = Amplification{} // primary cannot transmit its backlog
	path, b, err := c.nextMultipathPacket(time.Unix(101, 0))
	if err != nil || b == nil || path != 1 {
		t.Fatal("idle path failed to carry backlog", path, err)
	}
}
