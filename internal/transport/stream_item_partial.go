package transport

import "github.com/getlantern/photon/internal/wire"

type streamPrefixReplacement struct{ old, fresh *sendItem }
type streamItemPrefixPlan struct {
	root                        *sendItem
	group                       *streamItemDelivery
	next                        streamDelivery
	materialize, done           bool
	additional, usedReservation int
	changes                     []streamPrefixReplacement
}

// prepareStreamItemPrefix allocates bounded staging state without modifying
// packet mappings, shared ledgers, queues, or charges. The owner must not process
// other events between preparation and commit.
func prepareStreamItemPrefix(root *sendItem, end uint64) (*streamItemPrefixPlan, error) {
	if root == nil || end > wire.MaxVarint {
		return nil, errStreamDelivery
	}
	if root.delivery != nil && root.delivery.root != nil {
		root = root.delivery.root
	}
	if root.acked {
		return nil, nil
	}
	frame, ok := root.frame.(wire.StreamFrame)
	if !ok || !validStreamDelivery(frame) {
		return nil, errStreamDelivery
	}
	group := root.delivery
	// Whole data-only items before the boundary need no new ledger or wire
	// image. Their ordinary logical ACK remains valid prefix-delivery evidence.
	if !frame.Fin && frame.Offset+uint64(len(frame.Data)) <= end && (group == nil || group.ledger == nil || !group.ledger.partial) {
		return nil, nil
	}
	materialize := group == nil || group.ledger == nil
	var ledger *streamDelivery
	if materialize {
		var err error
		ledger, err = newStreamDelivery(frame)
		if err != nil {
			return nil, err
		}
	} else {
		ledger = group.ledger
	}
	next := *ledger
	done, err := next.retainPrefix(end, func() error { return nil })
	if err != nil {
		return nil, err
	}
	plan := &streamItemPrefixPlan{root: root, group: group, next: next, materialize: materialize, done: done}
	if done {
		return plan, nil
	}
	items := []*sendItem{root}
	if !materialize {
		items = group.items
	}
	plan.changes = make([]streamPrefixReplacement, 0, len(items))
	newItems := 0
	for _, item := range items {
		if item.acked {
			continue
		}
		if !materialize {
			if _, branch := group.branches[item]; branch {
				continue
			}
		}
		f := item.frame.(wire.StreamFrame)
		if f.Offset >= end {
			plan.changes = append(plan.changes, streamPrefixReplacement{old: item})
			continue
		}
		n := int(min(uint64(len(f.Data)), end-f.Offset))
		if n == len(f.Data) && !f.Fin {
			continue
		}
		f.Data, f.Fin, f.OmitLength = f.Data[:n], false, false
		plan.changes = append(plan.changes, streamPrefixReplacement{old: item, fresh: &sendItem{frame: f, queued: item.queued, transmitted: item.transmitted}})
		newItems++
	}
	if len(items)+newItems > maxStreamDeliveryItems {
		return nil, ErrSendBuffer
	}
	extra := newItems * streamDeliveryItemCharge
	reserved := 0
	if group != nil {
		reserved = group.reserved
	}
	if materialize {
		extra += len(ledger.covered)*8 + maxStreamDeliveryItems*8 + 128 + maxStreamDeliveryItems*32
	}
	plan.usedReservation = min(reserved, extra)
	plan.additional = extra - plan.usedReservation
	if materialize {
		plan.group = &streamItemDelivery{root: root, ledger: ledger, reserved: reserved, branches: make(map[*sendItem][2]*sendItem, maxStreamDeliveryItems/2), items: make([]*sendItem, 1, maxStreamDeliveryItems)}
		plan.group.items[0] = root
	}
	return plan, nil
}

// commit changes one prepared item. The caller releases the original charge for
// done items; this method charges only added metadata. No completion callback or
// recoverable admission failure occurs here.
func (p *streamItemPrefixPlan) commit(c *Conn) {
	if p.done {
		if !p.materialize {
			*p.group.ledger = p.next
			for _, item := range p.group.items {
				item.acked, item.queued = true, false
			}
		}
		p.root.acked, p.root.queued = true, false
	} else {
		if p.materialize {
			p.root.delivery = p.group
		}
		*p.group.ledger = p.next
		p.group.reserved -= p.usedReservation
		p.root.bytes += p.additional
		c.queuedBytes += p.additional
		for _, change := range p.changes {
			change.old.queued = false
			if change.fresh == nil {
				change.old.acked = true
				continue
			}
			change.fresh.delivery = p.group
			p.group.items = append(p.group.items, change.fresh)
			p.group.branches[change.old] = [2]*sendItem{change.fresh, nil}
		}
	}
}

// retainStreamItemPrefix is the single-item adapter. complete releases the
// charge for a satisfied reduced obligation, not evidence of full data/FIN ACK.
func (c *Conn) retainStreamItemPrefix(root *sendItem, end uint64, complete func(*sendItem) error) error {
	if complete == nil {
		return errStreamDelivery
	}
	plan, err := prepareStreamItemPrefix(root, end)
	if err != nil || plan == nil {
		return err
	}
	if plan.additional > c.availableSendBytes() {
		return ErrSendBuffer
	}
	if plan.done {
		if err := complete(plan.root); err != nil {
			return err
		}
	}
	plan.commit(c)
	c.rewritePartialStreamQueues()
	return nil
}

// Replace queued old wire images in every packet-number space that can own
// application retransmissions. Live in-flight mappings remain untouched; retired
// mappings are pruned so released payload cannot accumulate. A base
// space aliased by path zero may be visited twice; rewriting is idempotent.
func (c *Conn) rewritePartialStreamQueues() {
	rewrite := func(s *numberSpace) {
		queue := s.queue[:0]
		for _, item := range s.queue {
			if item.acked {
				continue
			}
			// Trimming produces at most one live leaf for each old queued
			// leaf, so compaction cannot overwrite an unread queue entry.
			visitSendLeaves(item, func(leaf *sendItem) bool { queue = append(queue, leaf); return false })
		}
		clear(s.queue[len(queue):])
		s.queue = queue
		pruneAcknowledged(s)
	}
	rewrite(&c.spaces[applicationSpace])
	if c.multipath != nil {
		for _, state := range c.multipath.paths {
			if state.application != nil {
				rewrite(state.application.space)
			}
		}
	}
}
