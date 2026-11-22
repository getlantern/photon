package wire

// Frame is a single frame. Parsed variable-sized byte fields borrow the packet.
// Append validates before writing to dst. State-dependent checks (stream
// ownership, flow control, CID sequences, handshake state) belong to the engine.
type Frame interface {
	Type() uint64
	Append([]byte) ([]byte, error)
}

func (CryptoFrame) Type() uint64 { return 6 }
func (f AckFrame) Type() uint64 {
	if f.ECN != nil {
		return 3
	}
	return 2
}
func (f StreamFrame) Type() uint64 {
	v := uint64(8)
	if f.Fin {
		v |= 1
	}
	if !f.OmitLength {
		v |= 2
	}
	if f.Offset != 0 {
		v |= 4
	}
	return v
}

func appendFields(dst []byte, typ uint64, values ...uint64) ([]byte, error) {
	for _, v := range values {
		if v > MaxVarint {
			return dst, ErrFrame
		}
	}
	dst, _ = AppendVarint(dst, typ)
	for _, v := range values {
		dst, _ = AppendVarint(dst, v)
	}
	return dst, nil
}

type PaddingFrame struct{}

func (PaddingFrame) Type() uint64                    { return 0 }
func (PaddingFrame) Append(b []byte) ([]byte, error) { return append(b, 0), nil }

type PingFrame struct{}

func (PingFrame) Type() uint64                    { return 1 }
func (PingFrame) Append(b []byte) ([]byte, error) { return append(b, 1), nil }

type HandshakeDoneFrame struct{}

func (HandshakeDoneFrame) Type() uint64                    { return 0x1e }
func (HandshakeDoneFrame) Append(b []byte) ([]byte, error) { return append(b, 0x1e), nil }

type ResetStreamFrame struct{ StreamID, ErrorCode, FinalSize uint64 }

func (ResetStreamFrame) Type() uint64 { return 4 }
func (f ResetStreamFrame) Append(b []byte) ([]byte, error) {
	return appendFields(b, f.Type(), f.StreamID, f.ErrorCode, f.FinalSize)
}

type StopSendingFrame struct{ StreamID, ErrorCode uint64 }

func (StopSendingFrame) Type() uint64 { return 5 }
func (f StopSendingFrame) Append(b []byte) ([]byte, error) {
	return appendFields(b, f.Type(), f.StreamID, f.ErrorCode)
}

type NewTokenFrame struct{ Token []byte }

func (NewTokenFrame) Type() uint64 { return 7 }
func (f NewTokenFrame) Append(b []byte) ([]byte, error) {
	if len(f.Token) == 0 || uint64(len(f.Token)) > MaxVarint {
		return b, ErrFrame
	}
	b, _ = appendFields(b, f.Type(), uint64(len(f.Token)))
	return append(b, f.Token...), nil
}

type MaxDataFrame struct{ Maximum uint64 }

func (MaxDataFrame) Type() uint64                      { return 0x10 }
func (f MaxDataFrame) Append(b []byte) ([]byte, error) { return appendFields(b, f.Type(), f.Maximum) }

type MaxStreamDataFrame struct{ StreamID, Maximum uint64 }

func (MaxStreamDataFrame) Type() uint64 { return 0x11 }
func (f MaxStreamDataFrame) Append(b []byte) ([]byte, error) {
	return appendFields(b, f.Type(), f.StreamID, f.Maximum)
}

type MaxStreamsFrame struct {
	Unidirectional bool
	Maximum        uint64
}

func (f MaxStreamsFrame) Type() uint64 {
	if f.Unidirectional {
		return 0x13
	}
	return 0x12
}
func (f MaxStreamsFrame) Append(b []byte) ([]byte, error) {
	if f.Maximum > 1<<60 {
		return b, ErrFrame
	}
	return appendFields(b, f.Type(), f.Maximum)
}

type DataBlockedFrame struct{ Limit uint64 }

func (DataBlockedFrame) Type() uint64                      { return 0x14 }
func (f DataBlockedFrame) Append(b []byte) ([]byte, error) { return appendFields(b, f.Type(), f.Limit) }

type StreamDataBlockedFrame struct{ StreamID, Limit uint64 }

func (StreamDataBlockedFrame) Type() uint64 { return 0x15 }
func (f StreamDataBlockedFrame) Append(b []byte) ([]byte, error) {
	return appendFields(b, f.Type(), f.StreamID, f.Limit)
}

type StreamsBlockedFrame struct {
	Unidirectional bool
	Limit          uint64
}

func (f StreamsBlockedFrame) Type() uint64 {
	if f.Unidirectional {
		return 0x17
	}
	return 0x16
}
func (f StreamsBlockedFrame) Append(b []byte) ([]byte, error) {
	if f.Limit > 1<<60 {
		return b, ErrFrame
	}
	return appendFields(b, f.Type(), f.Limit)
}

type NewConnectionIDFrame struct {
	Sequence, RetirePriorTo uint64
	ConnectionID            []byte
	StatelessResetToken     [16]byte
}

func (NewConnectionIDFrame) Type() uint64 { return 0x18 }
func (f NewConnectionIDFrame) Append(b []byte) ([]byte, error) {
	return f.appendPrefix(b, f.Type())
}

func (f NewConnectionIDFrame) appendPrefix(b []byte, typ uint64, prefix ...uint64) ([]byte, error) {
	for _, v := range prefix {
		if v > MaxVarint {
			return b, ErrFrame
		}
	}
	if f.Sequence > MaxVarint || f.RetirePriorTo > f.Sequence || len(f.ConnectionID) < 1 || len(f.ConnectionID) > 20 {
		return b, ErrFrame
	}
	b, _ = appendFields(b, typ, prefix...)
	b, _ = AppendVarint(b, f.Sequence)
	b, _ = AppendVarint(b, f.RetirePriorTo)
	b = append(b, byte(len(f.ConnectionID)))
	b = append(b, f.ConnectionID...)
	return append(b, f.StatelessResetToken[:]...), nil
}

type RetireConnectionIDFrame struct{ Sequence uint64 }

func (RetireConnectionIDFrame) Type() uint64 { return 0x19 }
func (f RetireConnectionIDFrame) Append(b []byte) ([]byte, error) {
	return appendFields(b, f.Type(), f.Sequence)
}

type PathChallengeFrame struct{ Data [8]byte }

func (PathChallengeFrame) Type() uint64 { return 0x1a }
func (f PathChallengeFrame) Append(b []byte) ([]byte, error) {
	return append(append(b, 0x1a), f.Data[:]...), nil
}

type PathResponseFrame struct{ Data [8]byte }

func (PathResponseFrame) Type() uint64 { return 0x1b }
func (f PathResponseFrame) Append(b []byte) ([]byte, error) {
	return append(append(b, 0x1b), f.Data[:]...), nil
}

type ConnectionCloseFrame struct {
	Application bool
	ErrorCode   uint64
	// TriggerFrame is omitted for application closes and must then be zero.
	TriggerFrame uint64
	// Reason is not required to be valid UTF-8 by RFC 9000.
	Reason []byte
}

func (f ConnectionCloseFrame) Type() uint64 {
	if f.Application {
		return 0x1d
	}
	return 0x1c
}
func (f ConnectionCloseFrame) Append(b []byte) ([]byte, error) {
	if f.ErrorCode > MaxVarint || f.TriggerFrame > MaxVarint || uint64(len(f.Reason)) > MaxVarint || (f.Application && f.TriggerFrame != 0) {
		return b, ErrFrame
	}
	b, _ = appendFields(b, f.Type(), f.ErrorCode)
	if !f.Application {
		b, _ = AppendVarint(b, f.TriggerFrame)
	}
	b, _ = AppendVarint(b, uint64(len(f.Reason)))
	return append(b, f.Reason...), nil
}

// DatagramFrame implements RFC 9221 framing only. Negotiation, size limits, and
// application delivery are enforced by the connection. DATAGRAM is not reliable.
type DatagramFrame struct {
	Data       []byte
	OmitLength bool
}

func (f DatagramFrame) Type() uint64 {
	if f.OmitLength {
		return 0x30
	}
	return 0x31
}
func (f DatagramFrame) Append(b []byte) ([]byte, error) {
	if uint64(len(f.Data)) > MaxVarint {
		return b, ErrFrame
	}
	b = append(b, byte(f.Type()))
	if !f.OmitLength {
		b, _ = AppendVarint(b, uint64(len(f.Data)))
	}
	return append(b, f.Data...), nil
}
