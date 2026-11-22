package transport

import (
	"bytes"
	"errors"
	"testing"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestRetainStreamItemPrefixAcrossPaths(t *testing.T) {
	for _, oldACK := range []bool{false, true} {
		c, initial, root := splitItemFixture(t)
		c.spaces[applicationSpace] = *initial
		base := &c.spaces[applicationSpace]
		second := &numberSpace{sent: make(map[uint64]*sendItem)}
		c.multipath = &pathCIDs{paths: map[uint64]*pathCIDState{
			0: {application: &pathApplication{space: base}},
			1: {application: &pathApplication{space: second}},
		}}
		original := root.frame.(wire.StreamFrame)
		original.Data[1199] = 42
		calls := 0
		complete := func(item *sendItem) error {
			calls++
			if item != root {
				t.Fatal("fragment completed")
			}
			c.queuedBytes -= item.bytes
			return nil
		}
		if err := c.retainStreamItemPrefix(root, 1200, complete); err != nil {
			t.Fatal(err)
		}
		if calls != 0 || len(base.queue) != 2 {
			t.Fatal("trimmed queue", calls, len(base.queue))
		}
		tail := base.queue[1]
		f := tail.frame.(wire.StreamFrame)
		if f.Offset != 1140 || len(f.Data) != 60 || f.Fin || f.Data[59] != 42 {
			t.Fatal("prefix frame", f)
		}
		if !bytes.Equal(root.frame.(wire.StreamFrame).Data, original.Data) || !root.frame.(wire.StreamFrame).Fin {
			t.Fatal("old packet changed")
		}
		// Simulate queue consumption, then loss of the old full packet on path zero.
		for _, item := range base.queue {
			item.queued = false
		}
		base.queue = nil
		queueLostItems(base, second, []recovery.Packet{{Number: 0}})
		if len(second.queue) != 2 || second.queue[1] != tail {
			t.Fatal("loss resurrected abandoned suffix")
		}
		queueLostItems(base, second, []recovery.Packet{{Number: 2}, {Number: 3}})
		if len(second.queue) != 2 {
			t.Fatal("duplicate loss queued copies")
		}
		for i, item := range second.queue {
			second.sent[uint64(10+i)] = item
		}
		ack := wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 10, Largest: 11}}}
		if oldACK {
			if err := acknowledgeItems(base, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}, complete); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := acknowledgeItems(second, ack, complete); err != nil {
				t.Fatal(err)
			}
		}
		if err := acknowledgeItems(second, ack, complete); err != nil {
			t.Fatal(err)
		}
		if err := acknowledgeItems(base, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 3}}}, complete); err != nil {
			t.Fatal(err)
		}
		if calls != 1 || c.queuedBytes != 0 {
			t.Fatal("ownership released twice", calls, c.queuedBytes)
		}
		for _, item := range root.delivery.items {
			if !item.acked {
				t.Fatal("unretired branch")
			}
		}
	}
}

func TestRetainStreamItemPrefixReductionAndFailure(t *testing.T) {
	c, initial, root := splitItemFixture(t)
	c.spaces[applicationSpace] = *initial
	s := &c.spaces[applicationSpace]
	calls := 0
	complete := func(item *sendItem) error { calls++; c.queuedBytes -= item.bytes; return nil }
	if err := acknowledgeItems(s, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 1, Largest: 1}}}, complete); err != nil {
		t.Fatal(err)
	}
	if err := c.retainStreamItemPrefix(root, 1200, complete); err != nil {
		t.Fatal(err)
	}
	beforeBytes, beforeItems, beforeRequired := c.queuedBytes, len(root.delivery.items), root.delivery.ledger.required
	failure := errors.New("completion failed")
	if err := c.retainStreamItemPrefix(root, 1140, func(*sendItem) error { return failure }); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if c.queuedBytes != beforeBytes || len(root.delivery.items) != beforeItems || root.delivery.ledger.required != beforeRequired || len(s.queue) != 1 || root.acked {
		t.Fatal("failed reduction mutated ownership")
	}
	if err := c.retainStreamItemPrefix(root, 1201, complete); !errors.Is(err, errStreamDelivery) {
		t.Fatal("increased prefix", err)
	}
	if err := c.retainStreamItemPrefix(root, 1140, complete); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || c.queuedBytes != 0 || len(s.queue) != 0 {
		t.Fatal("covered reduction not retired", calls, c.queuedBytes, len(s.queue))
	}
}

func TestRetainStreamItemPrefixAdmission(t *testing.T) {
	frame := wire.StreamFrame{StreamID: 0, Data: make([]byte, 100), Fin: true}
	c := &Conn{}
	if err := c.enqueue(applicationSpace, frame, 132); err != nil {
		t.Fatal(err)
	}
	s := &c.spaces[applicationSpace]
	root := s.queue[0]
	before := root.delivery
	c.queuedBytes = maxSendBytes
	if err := c.retainStreamItemPrefix(root, 50, func(*sendItem) error { t.Fatal("premature completion"); return nil }); !errors.Is(err, ErrSendBuffer) {
		t.Fatal(err)
	}
	if root.delivery != before || (root.delivery != nil && root.delivery.ledger != nil) || !root.queued || root.bytes != 132 || c.queuedBytes != maxSendBytes || len(s.queue) != 1 {
		t.Fatal("failed reservation mutated item")
	}
	// A zero-prefix cancellation needs no metadata even with a full send queue.
	if err := c.retainStreamItemPrefix(root, 0, func(item *sendItem) error { c.queuedBytes -= item.bytes; return nil }); err != nil {
		t.Fatal(err)
	}
	if c.queuedBytes != maxSendBytes-132 || !root.acked || len(s.queue) != 0 {
		t.Fatal("zero prefix retained charge")
	}
}

func TestRetainStreamItemPrefixMaterializesAndSplits(t *testing.T) {
	c := &Conn{}
	frame := wire.StreamFrame{StreamID: 0, Data: make([]byte, 2500), Fin: true}
	charge := len(frame.Data) + 32 + streamSplitReservation(len(frame.Data))
	if err := c.enqueue(applicationSpace, frame, charge); err != nil {
		t.Fatal(err)
	}
	s := &c.spaces[applicationSpace]
	root := s.queue[0]
	calls := 0
	complete := func(item *sendItem) error { calls++; c.queuedBytes -= item.bytes; return nil }
	if err := c.retainStreamItemPrefix(root, 2000, complete); err != nil {
		t.Fatal(err)
	}
	if len(s.queue) != 1 || root.delivery.ledger == nil {
		t.Fatal("missing trimmed leaf")
	}
	trimmed := s.queue[0]
	s.sent = make(map[uint64]*sendItem)
	s.sent[0] = root
	s.sent[1] = trimmed
	if err := c.splitQueuedStream(s, 0, 1140); err != nil {
		t.Fatal(err)
	}
	if len(s.queue) != 2 {
		t.Fatal("MTU split")
	}
	for i, item := range s.queue {
		s.sent[uint64(2+i)] = item
	}
	if err := c.retainStreamItemPrefix(root, 1500, complete); err != nil {
		t.Fatal(err)
	}
	if len(s.queue) != 2 || len(s.queue[1].frame.(wire.StreamFrame).Data) != 360 {
		t.Fatal("reduction after MTU split")
	}
	if err := acknowledgeItems(s, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 1, Largest: 1}}}, complete); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || c.queuedBytes != 0 {
		t.Fatal("old trimmed ACK did not cover newer prefix")
	}
}

func TestRetainStreamItemPrefixBoundsHistory(t *testing.T) {
	c := &Conn{}
	if err := c.enqueue(applicationSpace, wire.StreamFrame{Data: make([]byte, 1000), Fin: true}, 1032); err != nil {
		t.Fatal(err)
	}
	s := &c.spaces[applicationSpace]
	root := s.queue[0]
	calls := 0
	complete := func(item *sendItem) error { calls++; c.queuedBytes -= item.bytes; return nil }
	for i := 1; i < maxStreamDeliveryItems; i++ {
		if err := c.retainStreamItemPrefix(root, uint64(1000-i), complete); err != nil {
			t.Fatal(i, err)
		}
		if len(s.queue) != 1 || len(root.delivery.items) != i+1 {
			t.Fatal("unbounded queue or lost history", i)
		}
	}
	beforeBytes, beforeRequired := c.queuedBytes, root.delivery.ledger.required
	beforeLeaf := s.queue[0]
	if err := c.retainStreamItemPrefix(root, uint64(1000-maxStreamDeliveryItems), complete); !errors.Is(err, ErrSendBuffer) {
		t.Fatal(err)
	}
	if c.queuedBytes != beforeBytes || root.delivery.ledger.required != beforeRequired || s.queue[0] != beforeLeaf || calls != 0 {
		t.Fatal("history exhaustion changed state")
	}
	if err := c.retainStreamItemPrefix(root, 0, complete); err != nil {
		t.Fatal(err)
	}
	if c.queuedBytes != 0 || calls != 1 || len(s.queue) != 0 {
		t.Fatal("bounded history could not retire")
	}
}

func FuzzRetainStreamItemPrefix(f *testing.F) {
	f.Add([]byte{64, 0, 32, 0, 1, 16, 0, 2, 0, 0, 3, 0, 0})
	f.Add([]byte{128, 1, 64, 0, 0, 100, 0, 4, 0, 0, 2, 0, 0, 3, 1, 0})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) == 0 {
			return
		}
		size := int(input[0]) + 1
		c := &Conn{}
		if err := c.enqueue(applicationSpace, wire.StreamFrame{Data: make([]byte, size), Fin: true}, size+32); err != nil {
			t.Fatal(err)
		}
		s := &c.spaces[applicationSpace]
		root := s.queue[0]
		// Packet zero is an earlier full transmission now queued for retransmission.
		s.sent = map[uint64]*sendItem{0: root}
		nextPN := uint64(1)
		required, partial, finSeen, calls := size, false, false, 0
		covered := make([]bool, size)
		complete := func(item *sendItem) error { calls++; c.queuedBytes -= item.bytes; return nil }
		for i := 1; i+2 < len(input) && i < 301; i += 3 {
			switch input[i] % 5 {
			case 0:
				next := int(input[i+1]) % (required + 1)
				err := c.retainStreamItemPrefix(root, uint64(next), complete)
				if err == nil {
					required, partial = next, true
				} else if !errors.Is(err, ErrSendBuffer) {
					t.Fatal(err)
				}
			case 1:
				if len(s.queue) > 0 {
					index := int(input[i+1]) % len(s.queue)
					item := s.queue[index]
					if !item.acked {
						frame := item.frame.(wire.StreamFrame)
						if len(frame.Data) > 1 {
							err := c.splitQueuedStream(s, index, 1+int(input[i+2])%(len(frame.Data)-1))
							if err != nil && !errors.Is(err, ErrSendBuffer) {
								t.Fatal(err)
							}
						}
					}
				}
			case 2:
				if len(s.queue) > 0 {
					item := s.queue[0]
					s.queue = s.queue[1:]
					if !item.acked {
						item.queued = false
						s.sent[nextPN] = item
						nextPN++
					}
				}
			case 3:
				pn := uint64(input[i+1]) % nextPN
				if item := s.sent[pn]; item != nil && !item.acked {
					frame := item.frame.(wire.StreamFrame)
					for j := int(frame.Offset); j < int(frame.Offset)+len(frame.Data); j++ {
						covered[j] = true
					}
					finSeen = finSeen || frame.Fin
					if err := acknowledgeItems(s, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: pn, Largest: pn}}}, complete); err != nil {
						t.Fatal(err)
					}
				}
			case 4:
				queueLostItems(s, s, []recovery.Packet{{Number: uint64(input[i+1]) % nextPN}})
			}
			want := partial || finSeen
			for _, v := range covered[:required] {
				want = want && v
			}
			if root.acked != want || calls > 1 || want && calls != 1 || c.queuedBytes < 0 || want && c.queuedBytes != 0 {
				t.Fatal("completion/charge model mismatch", required, calls, c.queuedBytes)
			}
			for _, item := range s.queue {
				if item.acked {
					continue
				}
				frame := item.frame.(wire.StreamFrame)
				if !item.queued || partial && (frame.Fin || frame.Offset+uint64(len(frame.Data)) > uint64(required)) {
					t.Fatal("invalid retransmission leaf")
				}
			}
		}
	})
}

func TestRetainStreamItemPrefixUsesSplitReservation(t *testing.T) {
	c := &Conn{}
	frame := wire.StreamFrame{Data: make([]byte, 2500), Fin: true}
	charge := len(frame.Data) + 32 + streamSplitReservation(len(frame.Data))
	if err := c.enqueue(applicationSpace, frame, charge); err != nil {
		t.Fatal(err)
	}
	s := &c.spaces[applicationSpace]
	root := s.queue[0]
	c.queuedBytes = maxSendBytes
	complete := func(item *sendItem) error { c.queuedBytes -= item.bytes; return nil }
	if err := c.retainStreamItemPrefix(root, 1200, complete); err != nil {
		t.Fatal("reserved trim", err)
	}
	if err := c.splitQueuedStream(s, 0, 1140); err != nil {
		t.Fatal("reserved split", err)
	}
	if err := c.retainStreamItemPrefix(root, 1150, complete); err != nil {
		t.Fatal("reserved reduction", err)
	}
	if c.queuedBytes != maxSendBytes || root.bytes != charge || root.delivery.reserved != 0 {
		t.Fatal("partial trim mischarged reservation")
	}
	if err := c.retainStreamItemPrefix(root, 0, complete); err != nil {
		t.Fatal(err)
	}
	if c.queuedBytes != maxSendBytes-charge || len(s.queue) != 0 {
		t.Fatal("reserved charge not released")
	}
}
