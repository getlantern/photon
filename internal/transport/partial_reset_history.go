package transport

import (
	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

// Keep validation metadata for recent partial resets without retaining payload
// or growing with connection lifetime. Evicted IDs remain in the merged closed
// ranges, so late input cannot recreate a stream or consume credit again.
const partialResetHistoryCapacity = 1024

type partialResetRecord struct{ final, code uint64 }
type partialResetHistory struct {
	records map[uint64]partialResetRecord
	order   []uint64
	next    int
}

func (h *partialResetHistory) add(id, final, code uint64) {
	if h.records == nil {
		h.records = make(map[uint64]partialResetRecord)
	}
	if _, exists := h.records[id]; exists {
		return
	}
	if len(h.order) < partialResetHistoryCapacity {
		h.order = append(h.order, id)
	} else {
		delete(h.records, h.order[h.next])
		h.order[h.next] = id
		h.next = (h.next + 1) % partialResetHistoryCapacity
	}
	h.records[id] = partialResetRecord{final, code}
}

func (h *partialResetHistory) validate(id uint64, frame wire.Frame) error {
	if h == nil {
		return nil
	}
	record, ok := h.records[id]
	if !ok {
		return nil
	}
	var final, code uint64
	switch f := frame.(type) {
	case wire.StreamFrame:
		if f.Offset > record.final || uint64(len(f.Data)) > record.final-f.Offset || (f.Fin && f.Offset+uint64(len(f.Data)) != record.final) {
			return stream.ErrFinalSize
		}
		return nil
	case wire.ResetStreamFrame:
		final, code = f.FinalSize, f.ErrorCode
	case wire.ResetStreamAtFrame:
		final, code = f.FinalSize, f.ErrorCode
	default:
		return nil
	}
	if final != record.final {
		return stream.ErrFinalSize
	}
	if code != record.code {
		return stream.ErrStreamState
	}
	return nil
}
