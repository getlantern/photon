package transport

import (
	"errors"
	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
	"testing"
)

func splitItemFixture(t *testing.T) (*Conn, *numberSpace, *sendItem) {
	t.Helper()
	root := &sendItem{frame: wire.StreamFrame{StreamID: 0, Data: make([]byte, 2500), Fin: true}, queued: true, bytes: 2532}
	c := &Conn{queuedBytes: root.bytes}
	s := &numberSpace{queue: []*sendItem{root}, sent: map[uint64]*sendItem{0: root}}
	if err := c.splitQueuedStream(s, 0, 1140); err != nil {
		t.Fatal(err)
	}
	if err := c.splitQueuedStream(s, 1, 1140); err != nil {
		t.Fatal(err)
	}
	for i, item := range s.queue {
		s.sent[uint64(i+1)] = item
	}
	return c, s, root
}

func TestSplitSendItemsCompleteOnce(t *testing.T) {
	for _, original := range []bool{false, true} {
		c, s, root := splitItemFixture(t)
		calls := 0
		complete := func(item *sendItem) error {
			if item != root {
				t.Fatal("fragment released as logical item")
			}
			calls++
			c.queuedBytes -= item.bytes
			return nil
		}
		ack := func(n uint64) {
			t.Helper()
			if err := acknowledgeFrame(s, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: n, Largest: n}}}, complete); err != nil {
				t.Fatal(err)
			}
		}
		ack(3)
		ack(1)
		ack(1)
		if calls != 0 || root.acked || c.queuedBytes != root.bytes {
			t.Fatal("partial delivery released charge")
		}
		if original {
			ack(0)
		} else {
			ack(2)
		}
		ack(0)
		ack(2)
		ack(3)
		pruneAcknowledged(s)
		if calls != 1 || c.queuedBytes != 0 || !root.acked || len(s.sent) != 0 {
			t.Fatal("shared completion accounting")
		}
		for _, item := range s.queue {
			if !item.acked {
				t.Fatal("queued copy not retired")
			}
		}
	}
}

func TestSplitSendItemsCancelAndAdmission(t *testing.T) {
	c, s, root := splitItemFixture(t)
	// ACK one child, then cancel through another; later original ACK is inert.
	if err := acknowledgeFrame(s, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 1, Largest: 1}}}, func(*sendItem) error { t.Fatal("early completion"); return nil }); err != nil {
		t.Fatal(err)
	}
	c.retirePathFrames(s, func(f wire.Frame) bool { _, ok := f.(wire.StreamFrame); return ok })
	if c.queuedBytes != 0 || len(s.queue) != 0 || len(s.sent) != 0 || !root.acked {
		t.Fatal("cancel double-charge or leak")
	}
	c, s, root = splitItemFixture(t)
	c.queuedBytes = maxSendBytes
	item := s.queue[0]
	members := len(root.delivery.items)
	charge := root.bytes
	if err := c.splitQueuedStream(s, 0, 500); !errors.Is(err, ErrSendBuffer) || s.queue[0] != item || !item.queued || root.bytes != charge || len(root.delivery.items) != members {
		t.Fatal("failed split changed ownership")
	}
}

func TestSplitSendCompletionFailureAtomic(t *testing.T) {
	_, s, root := splitItemFixture(t)
	fail := errors.New("failed callback")
	ack := wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}
	if err := acknowledgeFrame(s, ack, func(*sendItem) error { return fail }); !errors.Is(err, fail) || root.acked || root.delivery.ledger.completed {
		t.Fatal("failed completion committed")
	}
	if err := acknowledgeFrame(s, ack, func(*sendItem) error { return nil }); err != nil || !root.acked {
		t.Fatal("retry failed", err)
	}
}

func TestSplitSendItemStreamCompletionBarrier(t *testing.T) {
	c, peer := testPair(t, wire.Version1, streamParameters())
	drivePair(t, c, peer, false, false)
	id, err := c.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := c.WriteStream(id, make([]byte, 1000), true); err != nil || n != 1000 {
		t.Fatal(err)
	}
	s := &c.spaces[applicationSpace]
	index := len(s.queue) - 1
	root := s.queue[index]
	if err := c.splitQueuedStream(s, index, 500); err != nil {
		t.Fatal(err)
	}
	left, right := s.queue[index], s.queue[index+1]
	s.sent[100], s.sent[101] = left, right
	complete := func(item *sendItem) error {
		if item != root {
			t.Fatal("wrong logical stream item")
		}
		if err := c.ackStreamFrame(item.frame); err != nil {
			return err
		}
		c.queuedBytes -= item.bytes
		return nil
	}
	ack := func(n uint64) {
		t.Helper()
		if err := acknowledgeFrame(s, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: n, Largest: n}}}, complete); err != nil {
			t.Fatal(err)
		}
	}
	ack(101)
	if c.StreamAcknowledged(id) == nil || c.streams.objects[id].outstandingData != 1 {
		t.Fatal("FIN fragment prematurely completed stream")
	}
	ack(100)
	if err := c.StreamAcknowledged(id); err != nil || c.streams.objects[id].outstandingData != 0 {
		t.Fatal("all fragments did not complete stream", err)
	}
}

func TestSplitRecoverySelectsLeaves(t *testing.T) {
	_, s, root := splitItemFixture(t)
	leaves := append([]*sendItem(nil), s.queue...)
	for _, part := range leaves {
		part.queued = false
	}
	s.queue = nil
	dest := &numberSpace{}
	loss := []recovery.Packet{{Number: 0}, {Number: 0}, {Number: 1}}
	queueLostItems(s, dest, loss)
	if len(dest.queue) != 3 {
		t.Fatal("duplicate or parent retransmission", len(dest.queue))
	}
	for i, part := range dest.queue {
		if part != leaves[i] || part == root {
			t.Fatal("wrong fragment order")
		}
	}
	// ACK the older tail transmission after it was split: both descendants
	// must become inert even though the first fragment is still unacknowledged.
	tail := root.delivery.branches[root][1]
	if err := acknowledgeSendItem(tail, func(*sendItem) error { t.Fatal("tail completed whole item"); return nil }); err != nil {
		t.Fatal(err)
	}
	if root.acked || leaves[0].acked || !leaves[1].acked || !leaves[2].acked {
		t.Fatal("subtree delivery not propagated")
	}
	for _, part := range leaves {
		part.queued = false
	}
	dest.queue = nil
	queueLostItems(s, dest, loss)
	if len(dest.queue) != 1 || dest.queue[0] != leaves[0] {
		t.Fatal("acknowledged children retransmitted")
	}
	detached := detachItems(s)
	if len(detached) != 1 || detached[0] != leaves[0] || !leaves[0].queued {
		t.Fatal("detach resurrected parent or lost other-path queue ownership")
	}
}

func TestSplitPTOSelectsLeaf(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	app := c.multipath.paths[1].application
	app.validation.state = PathValidated
	app.budget.Validate()
	owner, s, root := splitItemFixture(t)
	c.queuedBytes += owner.queuedBytes
	app.space = s
	for _, part := range s.queue {
		part.queued = false
	}
	s.queue = nil
	if err := c.queuePathProbe(1); err != nil {
		t.Fatal(err)
	}
	if len(s.queue) != 1 || s.queue[0] == root {
		t.Fatal("PTO did not choose a fragment")
	}
	f, ok := s.queue[0].frame.(wire.StreamFrame)
	if !ok || len(f.Data) > portableStreamPayload {
		t.Fatal("PTO selected oversized parent or PING")
	}
}

func TestSplitPathEvacuationKeepsLateACK(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version2)
	owner, s, root := splitItemFixture(t)
	c.queuedBytes += owner.queuedBytes
	c.multipath.paths[1].application.space = s
	dest := c.multipath.paths[0].application.space
	before := len(dest.queue)
	if err := c.evacuatePath(1); err != nil {
		t.Fatal(err)
	}
	if err := c.evacuatePath(1); err != nil {
		t.Fatal(err)
	}
	if len(dest.queue) != before+3 || s.sent[0] != root {
		t.Fatal("evacuation lost parent ACK mapping or duplicated fragments")
	}
	for _, item := range dest.queue[before:] {
		if item == root || item.delivery == nil {
			t.Fatal("evacuation queued parent")
		}
	}
	if err := acknowledgeFrame(s, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}, func(item *sendItem) error { c.queuedBytes -= item.bytes; return nil }); err != nil {
		t.Fatal(err)
	}
	for _, item := range dest.queue[before:] {
		if !item.acked {
			t.Fatal("late parent ACK failed to retire destination copy")
		}
	}
}
