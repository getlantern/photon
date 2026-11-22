package transport

import (
	"io"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestFailedPathEvacuatesAndRetainsLateACK(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	app := client.multipath.paths[1].application
	app.budget.Validate()
	if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	if b, err := client.nextPacketOnPath(1, now); err != nil || b == nil {
		t.Fatal(err)
	}
	original := app.space.sent[0]
	if err := client.queueFrameOnPath(1, wire.PathResponseFrame{}); err != nil {
		t.Fatal(err)
	}
	if err := client.validateMultipathPath(1); err != nil {
		t.Fatal(err)
	}
	// Exercise expiry of a previously sent challenge, with an unsent retry.
	app.validation.expires = now.Add(time.Second)
	if deadline := client.multipathPathsDeadline(); deadline != app.validation.expires {
		t.Fatal("validation deadline", deadline)
	}
	if err := client.expireMultipathPaths(app.validation.expires); err != nil {
		t.Fatal(err)
	}
	if client.multipath.abandoned[1] == nil || len(app.space.queue) != 0 || !original.queued || original.acked {
		t.Fatal("failed path did not transfer data")
	}
	if app.space.sent[0] != original {
		t.Fatal("lost late ACK mapping")
	}
	count := 0
	for _, item := range client.spaces[applicationSpace].queue {
		if item == original {
			count++
		}
		switch item.frame.(type) {
		case wire.PathChallengeFrame, wire.PathResponseFrame:
			t.Fatal("moved path-affine probe")
		}
	}
	if count != 1 {
		t.Fatal("logical item moved more than once", count)
	}
	before := client.queuedBytes
	ack := wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}}
	_, err := client.multipath.receiveReliableACK(ack, now.Add(time.Second), 0, 25*time.Millisecond, true, func(item *sendItem) error { client.queuedBytes -= item.bytes; return nil })
	if err != nil || !original.acked || client.queuedBytes != before-original.bytes {
		t.Fatal("late ACK completion", err)
	}
	if err := client.expireMultipathPaths(now.Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if client.queuedBytes != before-original.bytes {
		t.Fatal("double completion")
	}
}

func TestAbandonedPathCollectionPreservesTransferredItem(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version2)
	now := time.Unix(101, 0)
	app := client.multipath.paths[1].application
	if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	original := app.space.queue[0]
	if err := client.abandonFailedPath(1); err != nil {
		t.Fatal(err)
	}
	if _, err := client.multipath.receiveAbandon(wire.PathAbandonFrame{PathID: 1}, now, time.Second); err != nil {
		t.Fatal(err)
	}
	deadline := now.Add(3 * time.Second)
	if got := client.multipathPathsDeadline(); got != deadline {
		t.Fatal("retention deadline", got)
	}
	if err := client.expireMultipathPaths(deadline.Add(-time.Nanosecond)); err != nil {
		t.Fatal(err)
	}
	if client.multipath.paths[1] == nil {
		t.Fatal("early disposal")
	}
	if err := client.expireMultipathPaths(deadline); err != nil {
		t.Fatal(err)
	}
	if client.multipath.paths[1] != nil || client.multipath.abandoned[1] == nil {
		t.Fatal("disposal/tombstone")
	}
	count := 0
	for _, item := range client.spaces[applicationSpace].queue {
		if item == original {
			count++
		}
	}
	if count != 1 || !original.queued || original.acked {
		t.Fatal("disposed transferred data")
	}
}

func TestFailedPathStreamDeliveredOnSurvivor(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, version)
		now := time.Unix(101, 0)
		if err := client.queueFrameOnPath(1, wire.StreamFrame{StreamID: 0, Data: []byte("survived"), Fin: true}); err != nil {
			t.Fatal(err)
		}
		if err := client.abandonFailedPath(1); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 8; i++ {
			id, b, err := client.nextMultipathPacket(now.Add(time.Duration(i) * time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			if id != 0 {
				t.Fatal("used failed path")
			}
			if err := server.handleMultipathPacket(b, now); err != nil {
				t.Fatal(err)
			}
		}
		id, err := server.AcceptStream(false)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]byte, 16)
		n, err := server.ReadStream(id, out)
		if err != io.EOF || string(out[:n]) != "survived" {
			t.Fatal(n, err, string(out[:n]))
		}
	}
}
