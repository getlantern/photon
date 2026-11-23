package recovery

import (
	"time"

	"github.com/getlantern/photon/internal/wire"
)

type transmissionID struct {
	space  Space
	number uint64
}
type congestionRecord struct {
	prev, next         *congestionRecord
	id                 transmissionID
	sent               time.Time
	eliciting, sampled bool
	state              uint8  // 0 unresolved, 1 lost, 2 acknowledged or discarded
	bytes              uint16 // validated packet size; retained for late loss corrections
}

// congestionOrder embeds ordering links in each record to avoid a second
// allocation per transmission. Records belong to exactly one history.
type congestionOrder struct {
	front, back *congestionRecord
	length      int
}

func (o *congestionOrder) Front() *congestionRecord { return o.front }
func (o *congestionOrder) Len() int                 { return o.length }
func (p *congestionRecord) Next() *congestionRecord { return p.next }
func (o *congestionOrder) PushBack(p *congestionRecord) *congestionRecord {
	p.prev = o.back
	if o.back != nil {
		o.back.next = p
	} else {
		o.front = p
	}
	o.back = p
	o.length++
	return p
}
func (o *congestionOrder) Remove(p *congestionRecord) {
	if p.prev != nil {
		p.prev.next = p.next
	} else {
		o.front = p.next
	}
	if p.next != nil {
		p.next.prev = p.prev
	} else {
		o.back = p.prev
	}
	p.prev, p.next = nil, nil
	o.length--
}

// congestionHistory preserves send ordering across packet-number spaces and
// loss callbacks. Late ACKs can split a previously lost interval. Its bounded
// horizon can conservatively omit an interval under extreme history pressure;
// forgotten packets never become evidence of persistent congestion.
type congestionHistory struct {
	order    congestionOrder
	index    map[transmissionID]*congestionRecord
	capacity int
}

func (h *congestionHistory) sent(space Space, p Packet, sampled bool) {
	if h.index == nil {
		h.index = make(map[transmissionID]*congestionRecord)
	}
	for h.order.Len() >= h.capacity {
		h.removeFront()
	}
	id := transmissionID{space, p.Number}
	h.index[id] = h.order.PushBack(&congestionRecord{id: id, sent: p.SentAt, eliciting: p.AckEliciting && !p.MTUProbe, sampled: sampled, bytes: uint16(p.Bytes)})
}
func (h *congestionHistory) removeFront() {
	e := h.order.Front()
	delete(h.index, e.id)
	h.order.Remove(e)
}
func (h *congestionHistory) trimAcknowledged() {
	for h.order.Front() != nil && h.order.Front().state == 2 {
		h.removeFront()
	}
}
func (h *congestionHistory) ack(space Space, f wire.AckFrame) (recoveredPackets, recoveredBytes uint64) {
	defer h.trimAcknowledged()
	mark := func(e *congestionRecord) {
		if e == nil {
			return
		}
		p := e
		if p.state == 1 {
			recoveredPackets++
			recoveredBytes += uint64(p.bytes)
		}
		p.state = 2
	}
	if useACKLookup(f, len(h.index)) {
		for _, span := range f.Ranges {
			for pn := span.Smallest; pn <= span.Largest; pn++ {
				mark(h.index[transmissionID{space, pn}])
			}
		}
	} else {
		for id, e := range h.index {
			if id.space == space && f.Acknowledges(id.number) {
				mark(e)
			}
		}
	}
	return recoveredPackets, recoveredBytes
}
func (h *congestionHistory) discard(space Space) {
	defer h.trimAcknowledged()
	for id, e := range h.index {
		if id.space == space {
			p := e
			p.state = 2
		}
	}
}
func (h *congestionHistory) lost(space Space, packets []Packet) {
	for _, p := range packets {
		if e := h.index[transmissionID{space, p.Number}]; e != nil {
			v := e
			v.state = 1
		}
	}
}
func (h *congestionHistory) detect(duration time.Duration) bool {
	var first, previous time.Time
	var through *congestionRecord
	for e := h.order.Front(); e != nil; e = e.Next() {
		p := e
		// Unresolved packets and local clock regressions are conservative barriers.
		if p.state != 1 || !p.sampled || (!previous.IsZero() && p.sent.Before(previous)) {
			first = time.Time{}
		}
		previous = p.sent
		if p.state != 1 || !p.sampled || !p.eliciting {
			continue
		}
		if first.IsZero() {
			first = p.sent
			continue
		}
		if p.sent.Sub(first) > duration {
			through = e
		}
	}
	if through == nil {
		return false
	}
	// A reported interval cannot repeatedly collapse a recovering window.
	for {
		last := h.order.Front() == through
		h.removeFront()
		if last {
			break
		}
	}
	return true
}

// PersistentCongestion consumes evidence accumulated by ACK/loss processing.
// Max ACK delay applies regardless of packet-number space or confirmation;
// PTO exponential backoff is deliberately excluded (RFC 9002 section 7.6.1).
func (r *Sent) PersistentCongestion(maxACKDelay time.Duration) bool {
	duration := multiply(r.RTT.PTO(true, true, maxACKDelay, 0), 3)
	return r.history.detect(duration)
}
