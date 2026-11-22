package transport

import (
	"errors"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func prefixPlanFixture(t *testing.T, tailSize int) (*Conn, *sendItem, *sendItem) {
	t.Helper()
	c := &Conn{}
	if err := c.enqueue(applicationSpace, wire.StreamFrame{StreamID: 0, Data: make([]byte, 100)}, 132); err != nil {
		t.Fatal(err)
	}
	charge := tailSize + 32 + streamSplitReservation(tailSize)
	if err := c.enqueue(applicationSpace, wire.StreamFrame{StreamID: 0, Offset: 100, Data: make([]byte, tailSize), Fin: true}, charge); err != nil {
		t.Fatal(err)
	}
	s := &c.spaces[applicationSpace]
	head, tail := s.queue[0], s.queue[1]
	s.sent = map[uint64]*sendItem{0: head, 1: tail}
	return c, head, tail
}

func TestStreamPrefixPlanAdmissionIsAtomic(t *testing.T) {
	c, head, tail := prefixPlanFixture(t, 100)
	c.queuedBytes = maxSendBytes
	headGroup, tailGroup := head.delivery, tail.delivery
	if _, err := c.prepareStreamPrefix(0, 50, 32); !errors.Is(err, ErrSendBuffer) {
		t.Fatal(err)
	}
	if c.queuedBytes != maxSendBytes || head.delivery != headGroup || tail.delivery != tailGroup || head.acked || tail.acked || !head.queued || !tail.queued || len(c.spaces[applicationSpace].queue) != 2 {
		t.Fatal("failed stream preflight mutated items")
	}
	// Even a plan with no payload obligations must reserve the reset frame.
	if _, err := c.prepareStreamPrefix(4, 0, 32); !errors.Is(err, ErrSendBuffer) {
		t.Fatal("missing reset reservation", err)
	}
	for _, charge := range []int{-1, maxSendBytes + 1} {
		if _, err := c.prepareStreamPrefix(0, 0, charge); !errors.Is(err, errStreamDelivery) {
			t.Fatal(err)
		}
	}
}

func TestStreamPrefixPlanFundsPrefixFromAbandonedTail(t *testing.T) {
	c, head, tail := prefixPlanFixture(t, 9000)
	base := &c.spaces[applicationSpace]
	other := &numberSpace{queue: []*sendItem{tail}, sent: map[uint64]*sendItem{10: head, 11: tail}}
	base.queue = base.queue[:1]
	c.multipath = &pathCIDs{paths: map[uint64]*pathCIDState{0: {application: &pathApplication{space: base}}, 1: {application: &pathApplication{space: other}}}}
	c.queuedBytes = maxSendBytes
	beforeHead := head.bytes
	tailCharge := tail.bytes
	plan, err := c.prepareStreamPrefix(0, 50, 32)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.items) != 2 || c.queuedBytes != maxSendBytes || head.delivery != nil || tail.acked {
		t.Fatal("preflight mutation or duplicate roots")
	}
	completed := plan.commit(c)
	if len(completed) != 1 || completed[0] != tail || !tail.acked || head.acked || len(base.queue) != 1 || len(other.queue) != 0 {
		t.Fatal("stream commit", len(completed))
	}
	if base.sent[1] != nil || other.sent[11] != nil || base.sent[0] != head || other.sent[10] != head {
		t.Fatal("retired mappings retained or live ACK evidence lost")
	}
	additional := head.bytes - beforeHead
	if additional <= 0 || c.queuedBytes != maxSendBytes-tailCharge+additional || c.queuedBytes > maxSendBytes-32 {
		t.Fatal("net admission accounting")
	}
	f := base.queue[0].frame.(wire.StreamFrame)
	if len(f.Data) != 50 || f.Fin {
		t.Fatal("prefix wire image")
	}
	before := c.queuedBytes
	if len(plan.commit(c)) != 0 || c.queuedBytes != before {
		t.Fatal("repeated commit")
	}
	if err := c.enqueue(applicationSpace, wire.ResetStreamAtFrame{StreamID: 0, FinalSize: 9100, ReliableSize: 50}, 32); err != nil {
		t.Fatal("reserved reset could not enqueue", err)
	}
	if c.queuedBytes != before+32 {
		t.Fatal("reset charge")
	}
}

func TestStreamPrefixPlanUnchangedFullItems(t *testing.T) {
	c, head, tail := prefixPlanFixture(t, 100)
	if err := c.enqueue(applicationSpace, wire.StreamFrame{StreamID: 4, Data: []byte("other")}, 37); err != nil {
		t.Fatal(err)
	}
	unrelated := c.spaces[applicationSpace].queue[2]
	plan, err := c.prepareStreamPrefix(0, 100, 32)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.items) != 1 || plan.items[0].root != tail {
		t.Fatal("whole prefix item unnecessarily materialized")
	}
	completed := plan.commit(c)
	if len(completed) != 1 || completed[0] != tail || head.delivery != nil || head.acked || unrelated.acked {
		t.Fatal("unrelated/full prefix item changed")
	}
	if c.queuedBytes != 132+37 || len(c.spaces[applicationSpace].queue) != 2 {
		t.Fatal("retained prefix accounting")
	}
}

func TestStreamPrefixPlanInvalidReductionIsAtomic(t *testing.T) {
	c, head, tail := prefixPlanFixture(t, 100)
	// Stage an existing partial item so a larger boundary is invalid.
	if err := c.retainStreamItemPrefix(head, 50, func(*sendItem) error { return nil }); err != nil {
		t.Fatal(err)
	}
	before := c.queuedBytes
	first := c.spaces[applicationSpace].queue[0]
	if _, err := c.prepareStreamPrefix(0, 60, 32); !errors.Is(err, errStreamDelivery) {
		t.Fatal(err)
	}
	if c.queuedBytes != before || head.delivery.ledger.required != 50 || c.spaces[applicationSpace].queue[0] != first || tail.acked || !tail.queued {
		t.Fatal("invalid stream reduction partially committed")
	}
}
