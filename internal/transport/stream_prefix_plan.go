package transport

import "github.com/getlantern/photon/internal/wire"

// streamPrefixPlan is a serialized-owner transaction. Preparing it is read-only
// with respect to live state; commit must immediately follow any remaining
// stream/reset-frame validation, with no intervening ACK, write or path event.
// The caller must enqueue the preflighted reset frame before yielding ownership.
type streamPrefixPlan struct {
	items      []*streamItemPrefixPlan
	superseded []*sendItem
	committed  bool
}

func (c *Conn) prepareStreamPrefix(id, end uint64, resetCharge int) (*streamPrefixPlan, error) {
	if id > wire.MaxVarint || end > wire.MaxVarint || resetCharge < 0 || resetCharge > maxSendBytes {
		return nil, errStreamDelivery
	}
	roots := make(map[*sendItem]bool)
	controls := make(map[*sendItem]bool)
	collect := func(s *numberSpace) {
		add := func(item *sendItem) {
			if item.acked {
				return
			}
			if f, ok := item.frame.(wire.ResetStreamAtFrame); ok && f.StreamID == id {
				controls[item] = true
				return
			}
			f, ok := item.frame.(wire.StreamFrame)
			if !ok || f.StreamID != id {
				return
			}
			root := item
			if item.delivery != nil && item.delivery.root != nil {
				root = item.delivery.root
			}
			if !root.acked {
				roots[root] = true
			}
		}
		for _, item := range s.queue {
			add(item)
		}
		for _, item := range s.sent {
			add(item)
		}
	}
	collect(&c.spaces[applicationSpace])
	if c.multipath != nil {
		for _, path := range c.multipath.paths {
			if path.application != nil {
				collect(path.application.space)
			}
		}
	}
	plan := &streamPrefixPlan{items: make([]*streamItemPrefixPlan, 0, len(roots))}
	additional, released := resetCharge, 0
	for item := range controls {
		plan.superseded = append(plan.superseded, item)
		released += item.bytes
	}
	for root := range roots {
		item, err := prepareStreamItemPrefix(root, end)
		if err != nil {
			return nil, err
		}
		if item == nil {
			continue
		}
		plan.items = append(plan.items, item)
		additional += item.additional
		if item.done {
			released += root.bytes
		}
	}
	if released > c.queuedBytes || additional > c.availableSendBytes()+released {
		return nil, ErrSendBuffer
	}
	return plan, nil
}

// commit releases satisfied obligations first so the preflighted net budget
// also bounds peak committed charges. Returned items identify reduced obligations
// for stream-level accounting, not full STREAM/FIN delivery. No user callback
// runs until the entire transaction has committed. Repeated commit is a no-op.
func (p *streamPrefixPlan) commit(c *Conn) []*sendItem {
	if p.committed {
		return nil
	}
	completed := make([]*sendItem, 0, len(p.items))
	for _, item := range p.superseded {
		c.queuedBytes -= retireSendItem(item)
	}
	for _, item := range p.items {
		if item.done {
			c.queuedBytes -= item.root.bytes
			item.commit(c)
			completed = append(completed, item.root)
		}
	}
	for _, item := range p.items {
		if !item.done {
			item.commit(c)
		}
	}
	c.rewritePartialStreamQueues()
	p.committed = true
	return completed
}
