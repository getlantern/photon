package transport

import (
	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

// acknowledgeItems runs completion once per logical item, including late ACKs
// for transmissions already declared lost. Retransmissions share item pointers,
// even across paths. The owner validates ACK-of-unsent before calling this.
func acknowledgeItems(source *numberSpace, ack wire.AckFrame, complete func(*sendItem) error) error {
	for pn, item := range source.sent {
		if ack.Acknowledges(pn) && !item.acked {
			if err := acknowledgeSendItem(item, complete); err != nil {
				return err
			}
		}
	}
	return nil
}

func pruneAcknowledged(s *numberSpace) {
	for pn, item := range s.sent {
		if item.acked {
			delete(s.sent, pn)
		}
	}
}

// queueLostItems permits retransmission on a different path while preserving
// the old packet mapping for late ACKs. A shared item is queued at most once.
func queueLostItems(source, destination *numberSpace, lost []recovery.Packet) {
	for _, packet := range lost {
		visitSendLeaves(source.sent[packet.Number], func(item *sendItem) bool {
			if !item.queued {
				item.queued = true
				destination.queue = append(destination.queue, item)
			}
			return false
		})
	}
}

// detachItems transfers reliable data out of a disposed path. Include queued
// items and already-declared-lost transmissions, which need not remain in the
// recovery packet ledger. An item queued on another path keeps its queue flag.
func detachItems(s *numberSpace) []*sendItem {
	seen := make(map[*sendItem]bool)
	out := make([]*sendItem, 0, len(s.queue))
	add := func(item *sendItem) {
		if !item.acked && !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	for _, item := range s.queue {
		item.queued = false
		visitSendLeaves(item, func(part *sendItem) bool { add(part); return false })
	}
	for _, item := range s.sent {
		visitSendLeaves(item, func(part *sendItem) bool { add(part); return false })
	}
	clear(s.sent)
	clear(s.queue)
	s.queue = nil
	return out
}
