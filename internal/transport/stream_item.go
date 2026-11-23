package transport

import "github.com/getlantern/photon/internal/wire"

// The packet builder splits oversized STREAM items into portable fragments.
// Charges include coverage and retained nodes. Larger writes reserve portable
// fallback capacity before committing flow-control credit.
const maxStreamDeliveryItems = 128
const streamDeliveryItemCharge = 64

type streamItemDelivery struct {
	reserved int // charged metadata capacity, materialized only when splitting
	branches map[*sendItem][2]*sendItem
	root     *sendItem
	ledger   *streamDelivery
	items    []*sendItem
}

func (c *Conn) splitQueuedStream(s *numberSpace, index, maxPayload int) error {
	if index < 0 || index >= len(s.queue) {
		return errStreamDelivery
	}
	item := s.queue[index]
	f, ok := item.frame.(wire.StreamFrame)
	if !ok || item.acked || !item.queued {
		return errStreamDelivery
	}
	head, tail, split, err := splitStreamFrame(f, maxPayload)
	if err != nil || !split {
		return err
	}
	group := item.delivery
	extra := 2 * streamDeliveryItemCharge
	if group == nil || group.ledger == nil {
		ledger, err := newStreamDelivery(f)
		if err != nil {
			return err
		}
		// Reserve the bounded pointer table as well as the ledger and group.
		extra += len(ledger.covered)*8 + maxStreamDeliveryItems*8 + 128 + maxStreamDeliveryItems*32
		reserved := 0
		if group != nil {
			reserved = group.reserved
		}
		group = &streamItemDelivery{reserved: reserved, branches: make(map[*sendItem][2]*sendItem, maxStreamDeliveryItems/2), root: item, ledger: ledger, items: make([]*sendItem, 0, maxStreamDeliveryItems)}
		group.items = append(group.items, item)
	}
	if _, exists := group.branches[item]; exists {
		return errStreamDelivery
	}
	reserved := min(group.reserved, extra)
	additional := extra - reserved
	if len(group.items)+2 > maxStreamDeliveryItems || additional > c.availableSendBytes() {
		return ErrSendBuffer
	}
	left := &sendItem{frame: head, queued: true, transmitted: item.transmitted, delivery: group}
	right := &sendItem{frame: tail, queued: true, transmitted: item.transmitted, delivery: group}
	item.delivery = group
	item.queued = false
	group.items = append(group.items, left, right)
	group.branches[item] = [2]*sendItem{left, right}
	group.reserved -= reserved
	group.root.bytes += additional
	c.queuedBytes += additional
	s.queue = append(s.queue, nil)
	copy(s.queue[index+2:], s.queue[index+1:len(s.queue)-1])
	s.queue[index], s.queue[index+1] = left, right
	return nil
}

func acknowledgeSendItem(item *sendItem, complete func(*sendItem) error) error {
	if item.delivery == nil || item.delivery.ledger == nil {
		if err := complete(item); err != nil {
			return err
		}
		item.acked = true
		return nil
	}
	group := item.delivery
	f := item.frame.(wire.StreamFrame)
	done, err := group.ledger.acknowledge(f.Offset, len(f.Data), f.Fin, func() error { return complete(group.root) })
	if err != nil {
		return err
	}
	markSendSubtreeAcknowledged(item)
	if done {
		for _, part := range group.items {
			part.acked = true
		}
	}
	return nil
}

// retireSendItem cancels shared ownership once without claiming delivery.
func retireSendItem(item *sendItem) int {
	if item.acked {
		return 0
	}
	if group := item.delivery; group != nil && group.ledger != nil {
		for _, part := range group.items {
			part.acked = true
		}
		return group.root.bytes
	}
	item.acked = true
	return item.bytes
}

// visitSendLeaves retains old packet mappings but selects only current unacknowledged
// fragments for retransmission. A true visitor result terminates traversal.
func visitSendLeaves(item *sendItem, visit func(*sendItem) bool) bool {
	if item == nil || item.acked {
		return false
	}
	if item.delivery != nil {
		if children, ok := item.delivery.branches[item]; ok {
			return visitSendLeaves(children[0], visit) || visitSendLeaves(children[1], visit)
		}
	}
	return visit(item)
}

func markSendSubtreeAcknowledged(item *sendItem) {
	if item == nil {
		return
	}
	item.acked = true
	if item.delivery != nil {
		if children, ok := item.delivery.branches[item]; ok {
			markSendSubtreeAcknowledged(children[0])
			markSendSubtreeAcknowledged(children[1])
		}
	}
}

// streamSplitReservation bounds all splits at portable prefix boundaries for
// one maximum-size UDP payload. The fixed charge matches splitQueuedStream's
// ledger, pointer table and branch-map reservation.
func streamSplitReservation(n int) int {
	if n <= portableStreamPayload {
		return 0
	}
	return ((n+63)/64)*8 + maxStreamDeliveryItems*8 + 128 + maxStreamDeliveryItems*32 + 2*((n+portableStreamPayload-1)/portableStreamPayload-1)*streamDeliveryItemCharge
}

// Charge full fallback capacity at admission, but materialize maps, coverage
// and fragment tables only if a smaller path actually requires splitting.
func reserveStreamItem(item *sendItem, frame wire.StreamFrame) error {
	if !validStreamDelivery(frame) {
		return errStreamDelivery
	}
	item.delivery = &streamItemDelivery{root: item, reserved: streamSplitReservation(len(frame.Data))}
	return nil
}
