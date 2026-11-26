package transport

import (
	"bytes"
	"errors"
	"testing"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestSendBufferLateACKAndLoss(t *testing.T) {
	c := &Conn{}
	payload := bytes.Repeat([]byte{0x41}, 1140)
	item := &sendItem{frame: wire.StreamFrame{Data: payload}, queued: true}
	s := &numberSpace{sent: map[uint64]*sendItem{0: item, 1: item}, queue: []*sendItem{item}}
	complete := func(item *sendItem) error { c.recycleStreamItem(item); return nil }
	refused := errors.New("completion refused")
	if err := acknowledgeFrame(s, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}, func(*sendItem) error { return refused }); !errors.Is(err, refused) || len(c.sendBuffers) != 0 || item.acked {
		t.Fatal("failed completion released storage", err)
	}
	if err := acknowledgeFrame(s, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 1, Largest: 1}}}, complete); err != nil {
		t.Fatal(err)
	}
	if len(c.sendBuffers) != 1 || !item.acked || item.frame != nil {
		t.Fatal("completed payload still owned by old item")
	}
	reused := c.takeSendBuffer(1140)
	if cap(reused) != cap(payload) || &reused[:cap(reused)][0] != &payload[0] {
		t.Fatal("buffer not reused")
	}
	reused = append(reused, bytes.Repeat([]byte{0x42}, 1140)...)
	destination := &numberSpace{}
	queueLostItems(s, destination, []recovery.Packet{{Number: 0}, {Number: 1}})
	if len(destination.queue) != 0 {
		t.Fatal("retransmitted recycled bytes")
	}
	if err := acknowledgeFrame(s, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 1}}}, complete); err != nil || len(c.sendBuffers) != 0 {
		t.Fatal("late ACK released active reused buffer", err)
	}
	pruneAcknowledged(s)
	if len(s.sent) != 0 {
		t.Fatal("stale packet mappings survived")
	}
}

func TestSplitSendBufferNotRecycled(t *testing.T) {
	c := &Conn{}
	frame := wire.StreamFrame{Data: make([]byte, 1400)}
	charge := len(frame.Data) + 32 + streamSplitReservation(len(frame.Data))
	if err := c.enqueue(applicationSpace, frame, charge); err != nil {
		t.Fatal(err)
	}
	s := &c.spaces[applicationSpace]
	root := s.queue[0]
	if err := c.splitQueuedStream(s, 0, 700); err != nil {
		t.Fatal(err)
	}
	for _, item := range s.queue {
		if err := acknowledgeSendItem(item, func(root *sendItem) error { c.recycleStreamItem(root); return nil }); err != nil {
			t.Fatal(err)
		}
		if len(c.sendBuffers) != 0 {
			t.Fatal("split payload recycled")
		}
	}
	if !root.acked {
		t.Fatal("split write not completed")
	}
}

func TestSendBufferCacheBoundAndRelease(t *testing.T) {
	c := &Conn{}
	for i := 0; i < maxCachedSendBuffers+10; i++ {
		c.putSendBuffer(bytes.Repeat([]byte{0x41}, maxCachedSendBufferSize))
	}
	c.putSendBuffer(make([]byte, maxCachedSendBufferSize+1))
	if len(c.sendBuffers) != maxCachedSendBufferBytes/maxCachedSendBufferSize || c.cachedSendBufferBytes != maxCachedSendBufferBytes {
		t.Fatal("cache bound")
	}
	if c.takeSendBuffer(0) != nil || c.takeSendBuffer(maxCachedSendBufferSize+1) != nil {
		t.Fatal("invalid size used cache")
	}
	// Removing a buffer returns its capacity to the cache budget. A smaller
	// replacement must not let a larger buffer overcommit that budget.
	c.takeSendBuffer(1)
	c.putSendBuffer(make([]byte, 512))
	before := c.cachedSendBufferBytes
	c.putSendBuffer(make([]byte, maxCachedSendBufferSize))
	if c.cachedSendBufferBytes != before {
		t.Fatal("capacity overcommit")
	}
	c = &Conn{}
	for i := 0; i < maxCachedSendBuffers+10; i++ {
		c.putSendBuffer(make([]byte, 64))
	}
	if len(c.sendBuffers) != maxCachedSendBuffers || c.cachedSendBufferBytes != maxCachedSendBuffers*64 {
		t.Fatal("entry bound")
	}
	// Cache selection preserves buffers too small for a larger write.
	c = &Conn{}
	small := make([]byte, 16)
	c.putSendBuffer(small)
	if c.takeSendBuffer(17) != nil || len(c.sendBuffers) != 1 {
		t.Fatal("undersized buffer consumed")
	}
}

func TestSendBufferCleanup(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, _ := testPair(t, version, streamParameters())
		storage := bytes.Repeat([]byte{0x42}, 2048)
		client.putSendBuffer(storage)
		client.releaseTransportBuffers()
		if client.sendBuffers != nil || client.cachedSendBufferBytes != 0 || !bytes.Equal(storage, make([]byte, len(storage))) {
			t.Fatal("cleanup retained send plaintext")
		}
	}
}

// acknowledgeFrame acknowledges every packet in s's ledger that ack covers.
// Unlike recovery, which reports a packet newly acknowledged only once, it
// reports a covered entry again on every call, so these unit tests can retry
// a completion they refused; production fails the connection instead. Tests
// of the ledger contract itself use a real recovery.Sent.
func acknowledgeFrame(s *numberSpace, ack wire.AckFrame, complete func(*sendItem) error) error {
	var acked []recovery.Packet
	for pn := range s.sent {
		if _, untracked := s.untracked[pn]; !untracked && ack.Acknowledges(pn) {
			acked = append(acked, recovery.Packet{Number: pn})
		}
	}
	return acknowledgeItems(s, ack, acked, complete)
}
