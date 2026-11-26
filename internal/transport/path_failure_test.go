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

// drainSurvivor sends everything path zero will emit and reports how many
// packets left. Any output on another path is a test failure.
func drainSurvivor(t *testing.T, c *Conn, now time.Time) int {
	t.Helper()
	for sent := 0; sent < 16; sent++ {
		id, b, err := c.nextMultipathPacket(now)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			return sent
		}
		if id != 0 {
			t.Fatal("used abandoned path")
		}
	}
	t.Fatal("survivor did not quiesce")
	return 0
}

func queuedCopies(c *Conn, item *sendItem) int {
	count := 0
	for _, state := range c.multipath.paths {
		if state.application == nil {
			continue
		}
		for _, queued := range state.application.space.queue {
			if queued == item {
				count++
			}
		}
	}
	return count
}

func TestEvacuatedItemNotRequeuedByUnrelatedTimer(t *testing.T) {
	client, server := sendingPathPair(t, wire.Version1)
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
	if err := client.abandonFailedPath(1); err != nil {
		t.Fatal(err)
	}
	if drainSurvivor(t, client, now) == 0 || original.queued {
		t.Fatal("evacuated copy not retransmitted")
	}
	survivor := client.multipath.paths[0].application.space
	copyPN, found := uint64(0), false
	for pn, item := range survivor.sent {
		if item == original {
			copyPN, found = pn, true
		}
	}
	if !found || app.space.sent[0] != original {
		t.Fatal("missing survivor or late-ACK mapping")
	}
	// A timer with no loss or PTO decision must not retransmit the copy again,
	// and neither may a repeated PATH_ABANDON from the peer.
	if err := client.HandleTimeout(now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if original.queued || queuedCopies(client, original) != 0 {
		t.Fatal("timer requeued an outstanding evacuated copy")
	}
	if err := client.evacuatePath(1); err != nil {
		t.Fatal(err)
	}
	if original.queued || queuedCopies(client, original) != 0 || drainSurvivor(t, client, now.Add(time.Millisecond)) != 0 {
		t.Fatal("repeated evacuation requeued an outstanding copy")
	}
	// Genuine loss of the copy on the survivor still retransmits it, once.
	for i := 0; i < 3; i++ {
		if err := client.queueFrameOnPath(0, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
	}
	if drainSurvivor(t, client, now.Add(2*time.Millisecond)) != 3 {
		t.Fatal("survivor did not send followers")
	}
	keys := server.writeKeys(applicationSpace)
	payload, _ := ackFor(sentNumbers(survivor, copyPN+1)...).Append(nil)
	if err := client.handleMultipathPacket(sealPathPayload(t, keys, client.config.LocalCID, 0, 100, payload), now.Add(3*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if !original.queued || original.acked || queuedCopies(client, original) != 1 {
		t.Fatal("lost survivor copy not queued exactly once")
	}
	if err := client.HandleTimeout(now.Add(4 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if queuedCopies(client, original) != 1 {
		t.Fatal("timer duplicated a queued item")
	}
	// A late ACK for the abandoned path's transmission still completes it.
	before := client.queuedBytes
	payload, _ = (wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}}).Append(nil)
	if err := client.handleMultipathPacket(sealPathPayload(t, keys, client.config.LocalCID, 0, 101, payload), now.Add(5*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if !original.acked || client.queuedBytes != before-original.bytes || len(app.space.sent) != 0 {
		t.Fatal("late ACK on abandoned path did not complete the item")
	}
}

func TestLateLossOnAbandonedPathDoesNotRequeueEvacuatedItem(t *testing.T) {
	client, server := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	app := client.multipath.paths[1].application
	app.budget.Validate()
	for i := 0; i < 4; i++ {
		if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
		if b, err := client.nextPacketOnPath(1, now); err != nil || b == nil {
			t.Fatal(err)
		}
	}
	original, newest := app.space.sent[0], app.space.largestSent
	if err := client.abandonFailedPath(1); err != nil {
		t.Fatal(err)
	}
	if drainSurvivor(t, client, now) == 0 || original.queued {
		t.Fatal("evacuated copy not retransmitted")
	}
	// The peer acknowledges only the newest old-path packet. Packet-threshold
	// loss of the older ones says nothing about their copies on the survivor.
	payload, _ := (wire.PathAckFrame{PathID: 1, AckFrame: ackFor(newest)}).Append(nil)
	if err := client.handleMultipathPacket(sealPathPayload(t, server.writeKeys(applicationSpace), client.config.LocalCID, 0, 100, payload), now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := client.HandleTimeout(now.Add(2 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if original.acked || original.queued || queuedCopies(client, original) != 0 {
		t.Fatal("old-path loss requeued an outstanding evacuated copy")
	}
}
