package stream

import (
	"errors"

	"github.com/getlantern/photon/internal/wire"
)

var (
	ErrStreamState = errors.New("quic: frame references an invalid stream direction or unopened local stream")
	ErrStreamLimit = errors.New("quic: stream count limit exceeded")
)

const MaxStreams = uint64(1) << 60

// IDs tracks cumulative stream opening and acceptance in constant memory.
// Implicitly opened lower IDs are represented by counters, not eagerly
// allocated stream objects. Closed IDs stay logically opened; object lifetime
// and final-size retention are managed separately by the connection.
type IDs struct {
	localBit                          uint64
	localOpened, peerOpened, accepted [2]uint64
	receiveLimit, sendLimit           [2]uint64
}

func NewIDs(server bool, receiveBidi, receiveUni, sendBidi, sendUni uint64) (*IDs, error) {
	if max(receiveBidi, receiveUni, sendBidi, sendUni) > MaxStreams {
		return nil, ErrStreamLimit
	}
	bit := uint64(0)
	if server {
		bit = 1
	}
	return &IDs{localBit: bit, receiveLimit: [2]uint64{receiveBidi, receiveUni}, sendLimit: [2]uint64{sendBidi, sendUni}}, nil
}
func direction(uni bool) int {
	if uni {
		return 1
	}
	return 0
}

// NextOpen inspects the next local ID without consuming it. The caller must
// serialize inspection, resource allocation, and Open.
func (s *IDs) NextOpen(uni bool) (uint64, error) {
	d := direction(uni)
	if s.localOpened[d] >= s.sendLimit[d] {
		return 0, ErrWouldBlock
	}
	return s.localOpened[d]*4 + s.localBit + uint64(d)*2, nil
}
func (s *IDs) Open(uni bool) (uint64, error) {
	id, err := s.NextOpen(uni)
	if err != nil {
		return 0, err
	}
	s.localOpened[direction(uni)]++
	return id, nil
}
func (s *IDs) UpdateSendLimit(uni bool, limit uint64) error {
	if limit > MaxStreams {
		return ErrStreamLimit
	}
	d := direction(uni)
	s.sendLimit[d] = max(s.sendLimit[d], limit)
	return nil
}
func (s *IDs) UpdateReceiveLimit(uni bool, limit uint64) error {
	d := direction(uni)
	if limit > MaxStreams || limit < s.receiveLimit[d] {
		return ErrStreamLimit
	}
	s.receiveLimit[d] = limit
	return nil
}

// Observe validates a peer frame referencing a stream. receiveSide is true
// for STREAM, RESET_STREAM, or STREAM_DATA_BLOCKED; false for STOP_SENDING or
// MAX_STREAM_DATA. Valid peer references implicitly open preceding streams.
func (s *IDs) Observe(id uint64, receiveSide bool) error {
	if id > wire.MaxVarint {
		return ErrStreamState
	}
	local := id&1 == s.localBit
	d := int((id >> 1) & 1)
	if d == 1 && (receiveSide == local) {
		return ErrStreamState
	}
	count := id/4 + 1
	if local {
		if count > s.localOpened[d] {
			return ErrStreamState
		}
		return nil
	}
	if count > s.receiveLimit[d] {
		return ErrStreamLimit
	}
	s.peerOpened[d] = max(s.peerOpened[d], count)
	return nil
}
func (s *IDs) Pending(uni bool) uint64 { d := direction(uni); return s.peerOpened[d] - s.accepted[d] }

// NextAccept inspects the next pending peer stream without consuming it.
func (s *IDs) NextAccept(uni bool) (uint64, error) {
	d := direction(uni)
	if s.accepted[d] == s.peerOpened[d] {
		return 0, ErrWouldBlock
	}
	return s.accepted[d]*4 + (s.localBit ^ 1) + uint64(d)*2, nil
}
func (s *IDs) Accept(uni bool) (uint64, error) {
	id, err := s.NextAccept(uni)
	if err != nil {
		return 0, err
	}
	s.accepted[direction(uni)]++
	return id, nil
}

func (s *IDs) ReceiveLimit(uni bool) uint64 { return s.receiveLimit[direction(uni)] }

func (s *IDs) SendLimit(uni bool) uint64 { return s.sendLimit[direction(uni)] }
