package transport

import (
	"bytes"
	"github.com/getlantern/photon/internal/wire"
	"testing"
	"time"
)

func TestCandidateCIDReplayDoesNotQueueRetirements(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		_, c := sendingPathPair(t, v)
		a := c.multipath.paths[1].application
		peer := c.multipath.paths[1].peer
		old, ok := peer.active[0]
		if !ok {
			t.Fatal("missing initial CID")
		}
		next, ok := peer.active[1]
		if !ok {
			t.Fatal("missing replacement CID")
		}
		now := time.Unix(101, 0)
		frame := func(seq, floor uint64, id peerID) wire.Frame {
			return wire.PathNewConnectionIDFrame{PathID: 1, NewConnectionIDFrame: wire.NewConnectionIDFrame{Sequence: seq, RetirePriorTo: floor, ConnectionID: id.cid, StatelessResetToken: id.token}}
		}
		if err := c.receiveMigrationCIDControl(&pathPacket{pathID: 1, number: 100, frames: []wire.Frame{frame(1, 1, next)}}, now); err != nil {
			t.Fatal(err)
		}
		payload := []byte("withheld control packet data")
		packet := &pathPacket{pathID: 1, number: 101, receipt: NewPacket, frames: []wire.Frame{frame(0, 0, old), wire.StreamFrame{StreamID: 0, Data: payload, Fin: true}}}
		before := c.queuedBytes
		if err := c.receiveMigrationCIDControl(packet, now); err != nil {
			t.Fatal(err)
		}
		charge, count := c.queuedBytes, len(a.space.queue)
		if charge <= before {
			t.Fatal("obsolete CID did not elicit retirement")
		}
		for i := 0; i < 20; i++ {
			if err := c.receiveMigrationCIDControl(packet, now); err != nil {
				t.Fatal(err)
			}
		}
		if c.queuedBytes != charge || len(a.space.queue) != count || a.space.received.Expected() != 0 || a.space.ackPending {
			t.Fatal("candidate replay changed queues/admission")
		}
		// After commit the packet can admit its application data without repeating
		// the connection-wide control side effect already applied during validation.
		if err := c.dispatchMultipathPacket(packet, now); err != nil {
			t.Fatal(err)
		}
		if c.queuedBytes != charge || len(a.space.queue) != count {
			t.Fatal("admission repeated CID control")
		}
		got := make([]byte, len(payload))
		if n, err := c.ReadStream(0, got); n != len(payload) || !bytes.Equal(got, payload) {
			t.Fatal("withheld data", n, err)
		}
		for pn := uint64(102); pn < 302; pn += 2 {
			p := *packet
			p.number = pn
			if err := c.receiveMigrationCIDControl(&p, now); err != nil {
				t.Fatal(err)
			}
		}
		if len(a.candidateCIDControls.ranges) != 64 {
			t.Fatal("unbounded CID history")
		}
		charge = c.queuedBytes
		if err := c.receiveMigrationCIDControl(packet, now); err != nil || c.queuedBytes != charge {
			t.Fatal("forgotten CID packet replayed", err)
		}

	}
}
