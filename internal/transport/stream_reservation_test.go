package transport

import (
	"errors"
	"github.com/getlantern/photon/internal/wire"
	"testing"
)

func TestReservedSplitWorksAtFullQueue(t *testing.T) {
	for _, size := range []int{1141, 2500, 65527} {
		c := &Conn{}
		frame := wire.StreamFrame{StreamID: 0, Data: make([]byte, size), Fin: true}
		charge := size + 32 + streamSplitReservation(size)
		if err := c.enqueue(applicationSpace, frame, charge); err != nil {
			t.Fatal(err)
		}
		s := &c.spaces[applicationSpace]
		root := s.queue[0]
		if root.delivery == nil {
			t.Fatal("missing reservation")
		}
		c.queuedBytes = maxSendBytes // Other admitted data occupies all remaining credit.
		for i := 0; i < len(s.queue); i++ {
			f := s.queue[i].frame.(wire.StreamFrame)
			if len(f.Data) > portableStreamPayload {
				if err := c.splitQueuedStream(s, i, portableStreamPayload); err != nil {
					t.Fatal("reserved split blocked", size, err)
				}
			}
		}
		if c.queuedBytes != maxSendBytes || root.bytes != charge || root.delivery.reserved != 0 || len(root.delivery.items) > maxStreamDeliveryItems {
			t.Fatal("split consumed extra capacity or leaked reservation")
		}
		total := 0
		for i, item := range s.queue {
			f := item.frame.(wire.StreamFrame)
			total += len(f.Data)
			if f.Fin != (i == len(s.queue)-1) {
				t.Fatal("FIN placement")
			}
		}
		if total != size {
			t.Fatal("split lost data")
		}
		c.retirePathFrames(s, func(wire.Frame) bool { return true })
		if c.queuedBytes != maxSendBytes-charge {
			t.Fatal("cancellation did not refund full reservation")
		}
	}
}

func TestLargeWriteAdmissionDoesNotConsumeCreditWhenBlocked(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	a := c.multipath.paths[1].application
	a.validation.state = PathValidated
	c.multipath.paths[0].backup = true
	m, _ := newMTUSearch(3000)
	m.confirmed = 3000
	a.mtu = &m
	id, err := c.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	c.queuedBytes = maxSendBytes - 100
	if n, err := c.WriteStream(id, make([]byte, 2500), true); err == nil || n != 0 || c.streams.objects[id].send.Offset() != 0 || c.streams.objects[id].finQueued {
		t.Fatal("blocked admission committed credit/FIN", n, err)
	}
	c.queuedBytes = maxSendBytes - (portableStreamPayload + 32)
	n, err := c.WriteStream(id, make([]byte, 2500), true)
	if err != nil || n != portableStreamPayload || c.streams.objects[id].finQueued || c.queuedBytes != maxSendBytes {
		t.Fatal("portable fallback admission", n, err)
	}
	if a.space.queue[0].delivery != nil {
		t.Fatal("portable write allocated split ledger")
	}
}

func TestLargeStreamPayloadIncludesLengthWidth(t *testing.T) {
	for _, limit := range []int{1200, 3000, 16443, 16444, 16445, 65527} {
		a := newPathApplication()
		m, _ := newMTUSearch(limit)
		m.confirmed = limit
		a.mtu = &m
		payload := a.streamPayloadLimit()
		frame := wire.StreamFrame{StreamID: wire.MaxVarint, Offset: wire.MaxVarint - uint64(payload), Data: make([]byte, payload)}
		encoded, err := frame.Append(nil)
		if err != nil || len(encoded)+41 > limit {
			t.Fatal("STREAM length width exceeds packet budget", limit, len(encoded)+41, err)
		}
	}
}

func TestUnsplitReservationDoesNotMaterializeLedger(t *testing.T) {
	frame := wire.StreamFrame{StreamID: 0, Data: make([]byte, 1400), Fin: true}
	charge := len(frame.Data) + 32 + streamSplitReservation(len(frame.Data))
	for _, cancel := range []bool{false, true} {
		item := &sendItem{frame: frame, bytes: charge}
		if err := reserveStreamItem(item, frame); err != nil {
			t.Fatal(err)
		}
		if item.delivery.ledger != nil || item.delivery.branches != nil || item.delivery.items != nil {
			t.Fatal("eager split metadata")
		}
		refunded := 0
		if cancel {
			refunded = retireSendItem(item)
		} else {
			if err := acknowledgeSendItem(item, func(root *sendItem) error { refunded += root.bytes; return nil }); err != nil {
				t.Fatal(err)
			}
		}
		if !item.acked || refunded != charge || retireSendItem(item) != 0 {
			t.Fatal("unsplit charge release")
		}
	}
}

func TestDeferredReservationFailureThenLateOriginalACK(t *testing.T) {
	for _, size := range []int{1141, 2500, 65527} {
		c := &Conn{}
		frame := wire.StreamFrame{StreamID: 0, Data: make([]byte, size), Fin: true}
		charge := size + 32 + streamSplitReservation(size)
		if err := c.enqueue(applicationSpace, frame, charge); err != nil {
			t.Fatal(err)
		}
		s := &c.spaces[applicationSpace]
		original := s.queue[0]
		descriptor := original.delivery
		before := *descriptor
		failure := errors.New("completion refused")
		if err := acknowledgeSendItem(original, func(*sendItem) error { return failure }); !errors.Is(err, failure) {
			t.Fatal("callback error lost", err)
		}
		if original.acked || original.delivery != descriptor || descriptor.ledger != nil || c.queuedBytes != charge {
			t.Fatal("failed unsplit ACK mutated state")
		}
		// Invalid split requests must not publish partially allocated metadata.
		for _, index := range []int{-1, 0, 1} {
			if err := c.splitQueuedStream(s, index, 0); err == nil {
				t.Fatal("invalid split accepted")
			}
			if len(s.queue) != 1 || s.queue[0] != original || !original.queued || original.delivery != descriptor || descriptor.reserved != before.reserved || descriptor.ledger != nil || descriptor.branches != nil || descriptor.items != nil || c.queuedBytes != charge {
				t.Fatal("failed split mutated reservation")
			}
		}
		c.queuedBytes = maxSendBytes
		for i := 0; i < len(s.queue); i++ {
			if len(s.queue[i].frame.(wire.StreamFrame).Data) > portableStreamPayload {
				if err := c.splitQueuedStream(s, i, portableStreamPayload); err != nil {
					t.Fatal("fallback exhausted reservation", err)
				}
			}
		}
		if original.delivery == descriptor || original.delivery.ledger == nil || c.queuedBytes != maxSendBytes {
			t.Fatal("deferred materialization")
		}
		completed := 0
		if err := acknowledgeSendItem(original, func(item *sendItem) error {
			if item != original {
				t.Fatal("wrong logical owner")
			}
			c.queuedBytes -= item.bytes
			completed++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		for _, part := range s.queue {
			if !part.acked || retireSendItem(part) != 0 {
				t.Fatal("late original ACK left live fragment")
			}
		}
		if completed != 1 || c.queuedBytes != maxSendBytes-charge || retireSendItem(original) != 0 {
			t.Fatal("late ACK charge release")
		}
	}
}
