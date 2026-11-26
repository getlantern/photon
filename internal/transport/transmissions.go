package transport

import (
	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

// acknowledgeItems runs completion once per logical item, including late ACKs
// for transmissions already declared lost. Retransmissions share item pointers,
// even across paths. The owner validates ACK-of-unsent before calling this.
// acked is recovery's newly acknowledged packets for ack; with source's
// untracked packets, they are the only entries ack can complete, so the cost
// is theirs, not the whole ledger's. Completed entries leave the ledger, and
// so do untracked ones whose items another packet completed.
func acknowledgeItems(source *numberSpace, ack wire.AckFrame, acked []recovery.Packet, complete func(*sendItem) error) error {
	// An entry leaves only once its item is complete: a refused completion
	// keeps it for the next ACK.
	for _, p := range acked {
		item, ok := source.sent[p.Number]
		if !ok {
			continue
		}
		if !item.acked {
			if err := acknowledgeSendItem(item, complete); err != nil {
				return err
			}
		}
		delete(source.sent, p.Number)
	}
	for pn := range source.untracked {
		item, ok := source.sent[pn]
		switch {
		case !ok:
			delete(source.untracked, pn)
			continue
		case !item.acked && ack.Acknowledges(pn):
			if err := acknowledgeSendItem(item, complete); err != nil {
				return err
			}
		}
		if item.acked {
			delete(source.sent, pn)
			delete(source.untracked, pn)
		}
	}
	return nil
}

func pruneAcknowledged(s *numberSpace) {
	for pn, item := range s.sent {
		if item.acked {
			delete(s.sent, pn)
			delete(s.untracked, pn)
		}
	}
}

// queueLostItems permits retransmission on a different path while preserving
// the old packet mapping for late ACKs. A shared item is queued at most once.
func queueLostItems(source, destination *numberSpace, lost []recovery.Packet) {
	for _, packet := range lost {
		if _, ok := source.sent[packet.Number]; ok {
			if source.untracked == nil {
				source.untracked = make(map[uint64]struct{})
			}
			source.untracked[packet.Number] = struct{}{}
		}
		visitSendLeaves(source.sent[packet.Number], func(item *sendItem) bool {
			// Another path's ACK may already have completed a shared item.
			if !item.queued && !item.acked {
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

// reclaimSent reports whether s's packet ledger has room for another packet
// carrying frames. At capacity it first drops the packets tracked no longer
// reports, declared lost: they stay only so a late ACK can still mark their
// frames delivered, and dropping one costs at most a duplicate
// retransmission. A ledger still full holds only packets in flight, which
// ACKs or loss detection will release, so the caller waits rather than
// failing the connection.
func reclaimSent(s *numberSpace, tracked func(pn uint64) bool) bool {
	if len(s.sent) < maxSentPackets {
		return true
	}
	for pn := range s.sent {
		if !tracked(pn) {
			delete(s.sent, pn)
			delete(s.untracked, pn)
		}
	}
	return len(s.sent) < maxSentPackets
}
