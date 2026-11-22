package stream

import (
	"errors"

	"github.com/getlantern/photon/internal/wire"
)

var ErrSendClosed = errors.New("quic: stream send side closed")

// SendFlow accounts unique bytes committed to reliable output across streams.
// Retransmission and RESET_STREAM never return or consume additional credit.
type SendFlow struct{ limit, used uint64 }

func NewSendFlow(limit uint64) (*SendFlow, error) {
	if limit > wire.MaxVarint {
		return nil, ErrFlowControl
	}
	return &SendFlow{limit: limit}, nil
}
func (f *SendFlow) Used() uint64  { return f.used }
func (f *SendFlow) Limit() uint64 { return f.limit }
func (f *SendFlow) UpdateLimit(limit uint64) error {
	if limit > wire.MaxVarint {
		return ErrFlowControl
	}
	f.limit = max(f.limit, limit)
	return nil
}

type Send struct {
	id, offset, limit uint64
	flow              *SendFlow
	fin, complete     bool
	reset             *wire.ResetStreamFrame
	resetAt           *wire.ResetStreamAtFrame
	prefixAcked       uint64
	resetAtAcked      bool
}

func NewSend(id, limit uint64, flow *SendFlow) (*Send, error) {
	if id > wire.MaxVarint || limit > wire.MaxVarint || flow == nil {
		return nil, ErrFlowControl
	}
	return &Send{id: id, limit: limit, flow: flow}, nil
}
func (s *Send) Offset() uint64 { return s.offset }
func (s *Send) Limit() uint64  { return s.limit }
func (s *Send) UpdateLimit(limit uint64) error {
	if limit > wire.MaxVarint {
		return ErrFlowControl
	}
	s.limit = max(s.limit, limit)
	return nil
}

// Write commits up to maxPayload bytes to reliable output and returns an owned
// frame. The caller must reserve queue capacity before calling and retain the
// frame until ACK or reset; retransmit that frame without calling Write again.
// Partial progress is returned without error; FIN is emitted only if all input
// was accepted. Empty FIN remains legal even with no remaining flow credit.
func (s *Send) Write(data []byte, fin bool, maxPayload int) (wire.StreamFrame, int, error) {
	return s.WriteBuffer(data, fin, maxPayload, nil)
}

// WriteBuffer transfers storage ownership on success. The caller must not reuse
// storage until the returned frame and all retransmissions are retired. On error
// storage is unchanged and remains with the caller.
func (s *Send) WriteBuffer(data []byte, fin bool, maxPayload int, storage []byte) (wire.StreamFrame, int, error) {
	if s.fin || s.reset != nil || s.resetAt != nil || s.complete {
		return wire.StreamFrame{}, 0, ErrSendClosed
	}
	if maxPayload < 1 {
		return wire.StreamFrame{}, 0, ErrWouldBlock
	}
	n := min(uint64(len(data)), uint64(maxPayload), s.limit-s.offset, s.flow.limit-s.flow.used)
	if n == 0 && len(data) > 0 {
		return wire.StreamFrame{}, 0, ErrWouldBlock
	}
	if n == 0 && !fin {
		return wire.StreamFrame{}, 0, ErrWouldBlock
	}
	frame := wire.StreamFrame{StreamID: s.id, Offset: s.offset, Data: append(storage[:0], data[:n]...), Fin: fin && n == uint64(len(data))}
	s.offset += n
	s.flow.used += n
	s.fin = frame.Fin
	return frame, int(n), nil
}

// Reset commits a final size and preserves the first reset code. The caller
// retires queued STREAM data and reliably transmits the returned reset frame.
func (s *Send) Reset(code uint64) (wire.ResetStreamFrame, error) {
	if code > wire.MaxVarint {
		return wire.ResetStreamFrame{}, wire.ErrVarintRange
	}
	if s.complete {
		return wire.ResetStreamFrame{}, ErrSendClosed
	}
	if s.resetAt != nil {
		// An ordinary reset removes the prefix obligation without changing the
		// already committed application error or final size.
		s.reset = &wire.ResetStreamFrame{StreamID: s.id, ErrorCode: s.resetAt.ErrorCode, FinalSize: s.offset}
		s.resetAt = nil
		s.resetAtAcked = false
	}
	if s.reset == nil {
		s.reset = &wire.ResetStreamFrame{StreamID: s.id, ErrorCode: code, FinalSize: s.offset}
	}
	return *s.reset, nil
}

// AcknowledgeData requires acknowledgment of every committed byte AND FIN.
// AcknowledgeReset requires acknowledgment of the generated RESET_STREAM.
// The connection owns packet validation and delivery tracking; a FIN-only ACK
// is insufficient to call AcknowledgeData when earlier data remains outstanding.
func (s *Send) AcknowledgeData() {
	if s.fin && s.reset == nil {
		s.complete = true
	}
}
func (s *Send) AcknowledgeReset() {
	if s.reset != nil {
		s.complete = true
	}
}
func (s *Send) Complete() bool { return s.complete }
