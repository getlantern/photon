package transport

import (
	"errors"
	"github.com/getlantern/photon/internal/wire"
	"math/bits"
)

var errStreamDelivery = errors.New("quic: invalid stream delivery range")

// streamDelivery is a bounded delivery ledger for one original STREAM item.
// Splits and overlapping retransmissions share its byte/FIN evidence. The
// packet owner must validate ACKs and map them to the transmitted frame first.
// Integration with send-item memory ownership is deliberately separate.
type streamDelivery struct {
	offset                                 uint64
	length                                 int  // original ACK validation bound
	required, remaining                    int  // required prefix and its outstanding byte count
	needsFIN, finACKed, completed, partial bool // partial removes the FIN obligation
	covered                                []uint64
}

func validStreamDelivery(f wire.StreamFrame) bool {
	return f.StreamID <= wire.MaxVarint && f.Offset <= wire.MaxVarint && len(f.Data) <= maximumUDPPayload && uint64(len(f.Data)) <= wire.MaxVarint-f.Offset && (len(f.Data) > 0 || f.Fin)
}

func newStreamDelivery(f wire.StreamFrame) (*streamDelivery, error) {
	if !validStreamDelivery(f) {
		return nil, errStreamDelivery
	}
	return &streamDelivery{offset: f.Offset, length: len(f.Data), required: len(f.Data), remaining: len(f.Data), needsFIN: f.Fin, covered: make([]uint64, (len(f.Data)+63)/64)}, nil
}

// acknowledge commits evidence only after a successful final completion
// callback. A failed callback leaves this ACK unapplied, allowing an owner to
// fail or retry without falsely reporting delivery or releasing charges twice.
func (d *streamDelivery) acknowledge(offset uint64, length int, fin bool, complete func() error) (bool, error) {
	if complete == nil || length < 0 || offset < d.offset || offset-d.offset > uint64(d.length) {
		return false, errStreamDelivery
	}
	start := int(offset - d.offset)
	if length > d.length-start || fin && (!d.needsFIN || start+length != d.length) {
		return false, errStreamDelivery
	}
	if d.completed {
		return false, nil
	}
	end := start + length
	requiredEnd := min(end, d.required)
	added := 0
	for word := start / 64; word < (requiredEnd+63)/64 && start < requiredEnd; word++ {
		mask := deliveryMask(word, start, requiredEnd)
		added += bits.OnesCount64(mask &^ d.covered[word])
	}
	done := added == d.remaining && (!d.needsFIN || d.partial || d.finACKed || fin)
	if done {
		if err := complete(); err != nil {
			return false, err
		}
	}
	for word := start / 64; word < (end+63)/64; word++ {
		d.covered[word] |= deliveryMask(word, start, end)
	}
	d.remaining -= added
	d.finACKed = d.finACKed || fin
	d.completed = done
	return done, nil
}

func deliveryMask(word, start, end int) uint64 {
	lo := max(start-word*64, 0)
	hi := min(end-word*64, 64)
	return (^uint64(0) << lo) & (^uint64(0) >> (64 - hi))
}

// splitStreamFrame partitions immutable owned data without copying it. FIN
// remains on the tail, and explicit lengths allow either piece to be combined
// with other frames later. Queue admission and shared memory accounting belong
// to the caller; this helper itself commits no transport state.
func splitStreamFrame(f wire.StreamFrame, maxPayload int) (wire.StreamFrame, wire.StreamFrame, bool, error) {
	if maxPayload < 1 || f.StreamID > wire.MaxVarint || f.Offset > wire.MaxVarint || uint64(len(f.Data)) > wire.MaxVarint-f.Offset {
		return wire.StreamFrame{}, wire.StreamFrame{}, false, errStreamDelivery
	}
	if len(f.Data) <= maxPayload {
		return f, wire.StreamFrame{}, false, nil
	}
	head, tail := f, f
	head.Data, head.Fin, head.OmitLength = f.Data[:maxPayload], false, false
	tail.Data, tail.Offset, tail.OmitLength = f.Data[maxPayload:], f.Offset+uint64(maxPayload), false
	return head, tail, true, nil
}

// retainPrefix removes the suffix and FIN delivery obligations while preserving
// the original bounds used to validate late ACKs. It does not claim that removed
// bytes were delivered. Subsequent reductions can complete an already-covered
// prefix. As with acknowledge, callback failure leaves all ledger state intact.
// Packet owners must distinguish this completion from full data-plus-FIN ACK.
func (d *streamDelivery) retainPrefix(end uint64, complete func() error) (bool, error) {
	if complete == nil || end > wire.MaxVarint {
		return false, errStreamDelivery
	}
	required := 0
	if end > d.offset {
		required = int(min(end-d.offset, uint64(d.length)))
	}
	if d.partial && required > d.required {
		return false, errStreamDelivery
	}
	if d.completed {
		return false, nil
	}
	remaining := required
	for word := 0; word < (required+63)/64; word++ {
		remaining -= bits.OnesCount64(d.covered[word] & deliveryMask(word, 0, required))
	}
	done := remaining == 0
	if done {
		if err := complete(); err != nil {
			return false, err
		}
	}
	d.required, d.remaining, d.partial, d.completed = required, remaining, true, done
	return done, nil
}
