package stream

import "github.com/getlantern/photon/internal/wire"

// ResetAt closes writing and commits delivery through reliable. Only committed
// bytes may be included. Repeated calls preserve the first error code and can
// reduce, but never increase, the reliable size. The caller must reliably queue
// the returned frame and retain outstanding prefix data until acknowledgment.
func (s *Send) ResetAt(code, reliable uint64) (wire.ResetStreamAtFrame, error) {
	frame, err := s.PrepareResetAt(code, reliable)
	if err != nil {
		return wire.ResetStreamAtFrame{}, err
	}
	if s.resetAt == nil {
		s.resetAt = &frame
	} else if reliable < s.resetAt.ReliableSize {
		s.resetAt.ReliableSize = reliable
		s.resetAtAcked = false
	}
	return frame, nil
}

// PrepareResetAt validates without changing state so a packet owner can
// preflight every affected send item and control-frame charge before committing.
func (s *Send) PrepareResetAt(code, reliable uint64) (wire.ResetStreamAtFrame, error) {
	if code > wire.MaxVarint || reliable > wire.MaxVarint {
		return wire.ResetStreamAtFrame{}, wire.ErrVarintRange
	}
	if s.complete || s.reset != nil {
		return wire.ResetStreamAtFrame{}, ErrSendClosed
	}
	if reliable > s.offset {
		return wire.ResetStreamAtFrame{}, ErrFinalSize
	}
	if s.resetAt != nil {
		if reliable > s.resetAt.ReliableSize {
			return wire.ResetStreamAtFrame{}, ErrStreamState
		}
		code = s.resetAt.ErrorCode
	}
	return wire.ResetStreamAtFrame{StreamID: s.id, ErrorCode: code, FinalSize: s.offset, ReliableSize: reliable}, nil
}

// AcknowledgePrefix reports the contiguous acknowledged prefix, not the highest
// acknowledged offset. The caller owns range tracking and packet validation.
// It may report progress before or after ResetAt. Credit is not charged again.
func (s *Send) AcknowledgePrefix(offset uint64) error {
	if offset > s.offset {
		return ErrFinalSize
	}
	s.prefixAcked = max(s.prefixAcked, offset)
	s.maybeCompletePartialReset()
	return nil
}

// AcknowledgeResetAt identifies the exact reset information acknowledged. An
// older, larger reliable-size ACK cannot retire a newer smaller reset: the peer
// must learn that the sender no longer promises delivery through the old size.
// A late partial-reset ACK cannot acknowledge a subsequent ordinary reset.
func (s *Send) AcknowledgeResetAt(frame wire.ResetStreamAtFrame) error {
	if frame.StreamID != s.id || frame.FinalSize != s.offset || frame.ReliableSize > frame.FinalSize {
		return ErrStreamState
	}
	if s.reset != nil {
		if frame.ErrorCode != s.reset.ErrorCode {
			return ErrStreamState
		}
		return nil
	}
	if s.resetAt == nil || frame.ErrorCode != s.resetAt.ErrorCode || frame.ReliableSize < s.resetAt.ReliableSize {
		return ErrStreamState
	}
	if frame.ReliableSize == s.resetAt.ReliableSize {
		s.resetAtAcked = true
	}
	s.maybeCompletePartialReset()
	return nil
}

func (s *Send) maybeCompletePartialReset() {
	if s.resetAt != nil && s.resetAtAcked && s.prefixAcked >= s.resetAt.ReliableSize {
		s.complete = true
	}
}

// RetainForRetransmission returns the still-required portion of a committed
// STREAM frame. It borrows the original owned payload and changes neither the
// input frame nor send credit. Partial resets remove FIN and bytes beyond the
// current boundary; ordinary resets and completed streams retain no data.
func (s *Send) RetainForRetransmission(frame wire.StreamFrame) (wire.StreamFrame, bool, error) {
	if frame.StreamID != s.id || frame.Offset > s.offset || uint64(len(frame.Data)) > s.offset-frame.Offset {
		return wire.StreamFrame{}, false, ErrStreamState
	}
	if s.complete || s.reset != nil {
		return wire.StreamFrame{}, false, nil
	}
	if s.resetAt == nil {
		return frame, true, nil
	}
	if frame.Offset >= s.resetAt.ReliableSize {
		return wire.StreamFrame{}, false, nil
	}
	frame.Data = frame.Data[:min(uint64(len(frame.Data)), s.resetAt.ReliableSize-frame.Offset)]
	frame.Fin = false
	return frame, len(frame.Data) != 0, nil
}
