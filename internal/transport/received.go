// Package transport contains connection state independent of sockets and clocks.
// Callers serialize state transitions and supply timestamps explicitly.
package transport

import (
	"errors"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

type Receipt uint8

const (
	NewPacket Receipt = iota
	DuplicatePacket
	ForgottenPacket
)

// Received tracks one packet number space using bounded descending ACK ranges.
// Once an old range is evicted, packets below the retained floor are discarded,
// never processed anew. Otherwise a replay could deliver forgotten frames twice.
// A distinct instance is needed for each Initial/Handshake/application space.
type Received struct {
	ranges    []wire.AckRange
	maxRanges int
	floor     uint64
	largestAt time.Time
	ecn       [3]uint64
	haveECN   bool
}

func NewReceived(maxRanges int) (*Received, error) {
	if maxRanges < 1 || maxRanges > 4096 {
		return nil, errors.New("quic: ACK range capacity must be 1..4096")
	}
	return &Received{maxRanges: maxRanges, ranges: make([]wire.AckRange, 0, maxRanges+1)}, nil
}

// Record is called for an authenticated, valid packet. Duplicate and forgotten
// packets must not have their frames processed again. ACK scheduling is separate:
// ACK-only input does not trigger ACK-only output, while retransmitted
// ack-eliciting packets can trigger a replacement for a lost ACK.
func (r *Received) Record(pn uint64, now time.Time) (Receipt, error) {
	return r.RecordECN(pn, now, ECNUnknown)
}

// ECN is the IP header's two-bit congestion codepoint. Unknown means the socket
// did not supply metadata; it is distinct from an observed Not-ECT value.
type ECN uint8

const (
	ECNNotECT  ECN = 0
	ECNECT1    ECN = 1
	ECNECT0    ECN = 2
	ECNCE      ECN = 3
	ECNUnknown ECN = 255
)

// RecordECN records one authenticated, valid QUIC packet with its enclosing IP
// marking. Coalesced packets each call this operation in their own number space.
// Only newly processed packets count, including non-ack-eliciting packets.
func (r *Received) RecordECN(pn uint64, now time.Time, marking ECN) (Receipt, error) {
	if marking > ECNCE && marking != ECNUnknown {
		return ForgottenPacket, errors.New("quic: invalid ECN codepoint")
	}
	receipt, err := r.record(pn, now)
	if err != nil || receipt != NewPacket {
		return receipt, err
	}
	if marking == ECNUnknown {
		return receipt, nil
	}
	r.haveECN = true
	index := -1
	switch marking {
	case ECNECT0:
		index = 0
	case ECNECT1:
		index = 1
	case ECNCE:
		index = 2
	}
	if index >= 0 {
		if r.ecn[index] == wire.MaxVarint {
			return receipt, errors.New("quic: ECN counter exhausted")
		}
		r.ecn[index]++
	}
	return receipt, nil
}

// Largest returns the largest packet number recorded, if any.
func (r *Received) Largest() (uint64, bool) {
	if len(r.ranges) == 0 {
		return 0, false
	}
	return r.ranges[0].Largest, true
}

func (r *Received) record(pn uint64, now time.Time) (Receipt, error) {
	if pn > wire.MaxVarint {
		return ForgottenPacket, wire.ErrPacketNumber
	}
	if pn < r.floor {
		return ForgottenPacket, nil
	}
	pos := len(r.ranges)
	for i, a := range r.ranges {
		if pn >= a.Smallest && pn <= a.Largest {
			return DuplicatePacket, nil
		}
		if pn > a.Largest {
			pos = i
			break
		}
	}
	if len(r.ranges) == 0 || pn > r.ranges[0].Largest {
		r.largestAt = now
	}
	r.ranges = append(r.ranges, wire.AckRange{})
	copy(r.ranges[pos+1:], r.ranges[pos:])
	r.ranges[pos] = wire.AckRange{Smallest: pn, Largest: pn}
	// Merge adjacency in place. Bounds are at most 2^62-1, so +1 is safe.
	kept := 0
	for _, a := range r.ranges {
		if kept > 0 && r.ranges[kept-1].Smallest == a.Largest+1 {
			r.ranges[kept-1].Smallest = a.Smallest
		} else {
			r.ranges[kept] = a
			kept++
		}
	}
	r.ranges = r.ranges[:kept]
	if len(r.ranges) > r.maxRanges {
		r.ranges = r.ranges[:r.maxRanges]
		r.floor = r.ranges[len(r.ranges)-1].Smallest
	}
	if pn < r.floor {
		return ForgottenPacket, nil
	}
	return NewPacket, nil
}

func (r *Received) Expected() uint64 {
	if len(r.ranges) == 0 {
		return 0
	}
	return r.ranges[0].Largest + 1
}

// Ack snapshots ranges so future receives do not alter an already queued ACK.
// Delay measures time since the largest packet, not the most recent arrival.
// The connection uses zero delay for Initial/Handshake packets by passing the
// largest packet's arrival time, or overwrites Delay with zero.
func (r *Received) Ack(now time.Time, exponent uint8) (wire.AckFrame, error) {
	ack, err := r.ackView(now, exponent)
	if err != nil {
		return ack, err
	}
	ack.Ranges = append([]wire.AckRange(nil), ack.Ranges...)
	if ack.ECN != nil {
		counts := *ack.ECN
		ack.ECN = &counts
	}
	return ack, nil
}

// ackView borrows range and ECN storage for immediate serialized packet encoding.
// It must not be retained, queued or used across a receive-state mutation.
func (r *Received) ackView(now time.Time, exponent uint8) (wire.AckFrame, error) {
	if exponent > 20 {
		return wire.AckFrame{}, errors.New("quic: invalid ACK delay exponent")
	}
	if len(r.ranges) == 0 {
		return wire.AckFrame{}, errors.New("quic: no packets to acknowledge")
	}
	delay := now.Sub(r.largestAt)
	if delay < 0 {
		delay = 0
	}
	ack := wire.AckFrame{Delay: uint64(delay/time.Microsecond) >> exponent, Ranges: r.ranges}
	if r.haveECN {
		ack.ECN = &r.ecn
	}
	return ack, nil
}
