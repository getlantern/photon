package wire

import (
	"errors"
	"fmt"
	"io"
)

// FrameError preserves the offending type for CONNECTION_CLOSE. Every parser
// error is a frame encoding error; Unwrap also exposes truncation when present.
type FrameError struct {
	FrameType uint64
	Cause     error
}

func (e *FrameError) Error() string   { return fmt.Sprintf("quic: frame 0x%x: %v", e.FrameType, e.Cause) }
func (e *FrameError) Unwrap() []error { return []error{ErrFrame, e.Cause} }

// cursor latches errors so no later read can slice beyond the packet.
type cursor struct {
	b   []byte
	i   int
	err error
}

func (c *cursor) integer() uint64 {
	if c.err != nil {
		return 0
	}
	v, n, err := ConsumeVarint(c.b[c.i:])
	if err != nil {
		c.err = err
		return 0
	}
	c.i += n
	return v
}
func (c *cursor) take(n uint64) []byte {
	if c.err != nil {
		return nil
	}
	if n > uint64(len(c.b)-c.i) {
		c.err = io.ErrUnexpectedEOF
		return nil
	}
	b := c.b[c.i : c.i+int(n)]
	c.i += int(n)
	return b
}
func (c *cursor) byte() byte {
	b := c.take(1)
	if len(b) == 0 {
		return 0
	}
	return b[0]
}

// ConsumeFrame consumes one frame. Errors consume nothing. Unknown types cannot
// be skipped: QUIC frame encodings are not self-describing. Parsing must happen
// only after packet authentication. Packet-context checks are separate below.
func ConsumeFrame(b []byte) (Frame, int, error) {
	typ, n, err := ConsumeVarint(b)
	if err != nil {
		return nil, 0, &FrameError{Cause: err}
	}
	fail := func(err error) (Frame, int, error) { return nil, 0, &FrameError{FrameType: typ, Cause: err} }
	if n != VarintLen(typ) {
		return fail(errors.New("nonminimal frame type"))
	}
	c := cursor{b: b, i: n}
	var f Frame
	switch typ {
	case 0:
		f = PaddingFrame{}
	case 1:
		f = PingFrame{}
	case 2, 3:
		ack, used, err := ConsumeAckFrame(b)
		if err != nil {
			return fail(err)
		}
		return ack, used, nil
	case FramePathAck, FramePathAckECN:
		pathID := c.integer()
		if c.err != nil {
			return fail(c.err)
		}
		ack, used, err := consumeAckBody(b, c.i, typ == FramePathAckECN)
		if err != nil {
			return fail(err)
		}
		return PathAckFrame{PathID: pathID, AckFrame: ack}, used, nil
	case FramePathAbandon:
		f = PathAbandonFrame{PathID: c.integer(), ErrorCode: c.integer()}
	case FramePathStatusBackup, FramePathStatusAvailable:
		f = PathStatusFrame{PathID: c.integer(), Sequence: c.integer(), Backup: typ == FramePathStatusBackup}
	case FramePathRetireConnectionID:
		f = PathRetireConnectionIDFrame{PathID: c.integer(), Sequence: c.integer()}
	case FrameMaxPathID:
		f = MaxPathIDFrame{Maximum: c.integer()}
	case FramePathsBlocked:
		f = PathsBlockedFrame{Maximum: c.integer()}
	case FramePathCIDsBlocked:
		f = PathCIDsBlockedFrame{PathID: c.integer(), NextSequence: c.integer()}
	case FrameResetStreamAt:
		reset := ResetStreamAtFrame{StreamID: c.integer(), ErrorCode: c.integer(), FinalSize: c.integer(), ReliableSize: c.integer()}
		if c.err == nil && reset.ReliableSize > reset.FinalSize {
			return fail(errors.New("reliable size exceeds final size"))
		}
		f = reset
	case 4:
		f = ResetStreamFrame{StreamID: c.integer(), ErrorCode: c.integer(), FinalSize: c.integer()}
	case 5:
		f = StopSendingFrame{StreamID: c.integer(), ErrorCode: c.integer()}
	case 6:
		crypto, used, err := ConsumeCryptoFrame(b)
		if err != nil {
			return fail(err)
		}
		return crypto, used, nil
	case 7:
		length := c.integer()
		if c.err == nil && length == 0 {
			return fail(errors.New("empty NEW_TOKEN"))
		}
		f = NewTokenFrame{Token: c.take(length)}
	case 8, 9, 10, 11, 12, 13, 14, 15:
		stream, used, err := ConsumeStreamFrame(b)
		if err != nil {
			return fail(err)
		}
		return stream, used, nil
	case 0x10:
		f = MaxDataFrame{Maximum: c.integer()}
	case 0x11:
		f = MaxStreamDataFrame{StreamID: c.integer(), Maximum: c.integer()}
	case 0x12, 0x13:
		maximum := c.integer()
		if maximum > 1<<60 {
			return fail(errors.New("stream limit exceeds 2^60"))
		}
		f = MaxStreamsFrame{Unidirectional: typ == 0x13, Maximum: maximum}
	case 0x14:
		f = DataBlockedFrame{Limit: c.integer()}
	case 0x15:
		f = StreamDataBlockedFrame{StreamID: c.integer(), Limit: c.integer()}
	case 0x16, 0x17:
		limit := c.integer()
		if limit > 1<<60 {
			return fail(errors.New("stream limit exceeds 2^60"))
		}
		f = StreamsBlockedFrame{Unidirectional: typ == 0x17, Limit: limit}
	case 0x18, FramePathNewConnectionID:
		var pathID uint64
		if typ == FramePathNewConnectionID {
			pathID = c.integer()
		}
		cid := NewConnectionIDFrame{Sequence: c.integer(), RetirePriorTo: c.integer()}
		size := c.byte()
		if c.err == nil && (size < 1 || size > 20 || cid.RetirePriorTo > cid.Sequence) {
			return fail(errors.New("invalid NEW_CONNECTION_ID"))
		}
		cid.ConnectionID = c.take(uint64(size))
		copy(cid.StatelessResetToken[:], c.take(16))
		f = cid
		if typ == FramePathNewConnectionID {
			f = PathNewConnectionIDFrame{PathID: pathID, NewConnectionIDFrame: cid}
		}
	case 0x19:
		f = RetireConnectionIDFrame{Sequence: c.integer()}
	case 0x1a:
		p := PathChallengeFrame{}
		copy(p.Data[:], c.take(8))
		f = p
	case 0x1b:
		p := PathResponseFrame{}
		copy(p.Data[:], c.take(8))
		f = p
	case 0x1c, 0x1d:
		close := ConnectionCloseFrame{Application: typ == 0x1d, ErrorCode: c.integer()}
		if !close.Application {
			close.TriggerFrame = c.integer()
		}
		length := c.integer()
		close.Reason = c.take(length)
		f = close
	case 0x1e:
		f = HandshakeDoneFrame{}
	case 0x30, 0x31:
		d := DatagramFrame{OmitLength: typ == 0x30}
		length := uint64(len(b) - c.i)
		if !d.OmitLength {
			length = c.integer()
		}
		d.Data = c.take(length)
		f = d
	default:
		return fail(errors.New("unknown frame type"))
	}
	if c.err != nil {
		return fail(c.err)
	}
	return f, c.i, nil
}

var ErrFrameUnexpected = errors.New("quic: frame not permitted in packet context")

// FrameContext describes reception, not transmission. MaxDatagramFrameSize is
// the LOCAL advertised maximum, not the peer's sending capability.
type FrameContext struct {
	// ResetStreamAtEnabled means this receiver advertised support. In 0-RTT,
	// this must also respect the remembered server capability and acceptance.
	ResetStreamAtEnabled bool
	PacketType           PacketType
	SenderIsServer       bool
	MaxDatagramFrameSize uint64
	// MultipathNegotiated must mean both endpoints advertised the extension.
	MultipathNegotiated bool
}

// ValidateFrame checks RFC 9000 packet types and server-only frames. RFC 9221
// limits include the type and length fields, so encodedLen is the number of
// bytes actually consumed, not a re-encoding's possibly shorter size.
func ValidateFrame(f Frame, c FrameContext, encodedLen int) error {
	typ := f.Type()
	if c.PacketType == Retry || c.PacketType > OneRTT || (c.SenderIsServer && c.PacketType == ZeroRTT) {
		return ErrFrameUnexpected
	}
	if multipathFrameType(typ) {
		if !c.MultipathNegotiated || c.PacketType != OneRTT {
			return ErrFrameUnexpected
		}
		return nil
	}
	if typ == FrameResetStreamAt {
		if !c.ResetStreamAtEnabled || (c.PacketType != ZeroRTT && c.PacketType != OneRTT) {
			return ErrFrameUnexpected
		}
		return nil
	}
	known := typ <= 0x1e || typ == 0x30 || typ == 0x31
	if !known {
		return ErrFrameUnexpected
	}
	if c.PacketType == Initial || c.PacketType == Handshake {
		if typ != 0 && typ != 1 && typ != 2 && typ != 3 && typ != 6 && typ != 0x1c {
			return ErrFrameUnexpected
		}
	}
	if c.PacketType == ZeroRTT {
		switch typ {
		case 2, 3, 6, 7, 0x19, 0x1b, 0x1e:
			return ErrFrameUnexpected
		}
	}
	if !c.SenderIsServer && (typ == 7 || typ == 0x1e) {
		return ErrFrameUnexpected
	}
	if typ == 0x30 || typ == 0x31 {
		if c.MaxDatagramFrameSize == 0 || encodedLen < 1 || uint64(encodedLen) > c.MaxDatagramFrameSize {
			return ErrFrameUnexpected
		}
	}
	return nil
}

func AckEliciting(f Frame) bool {
	switch f.Type() {
	case 0, 2, 3, FramePathAck, FramePathAckECN, 0x1c, 0x1d:
		return false
	}
	return true
}
func ProbingFrame(f Frame) bool {
	switch f.Type() {
	case 0, 0x18, 0x1a, 0x1b:
		return true
	}
	return false
}

// InFlightFrame follows RFC 9002: an ack-eliciting frame or PADDING makes a
// packet count toward bytes in flight. CONNECTION_CLOSE alone does not.
func InFlightFrame(f Frame) bool { return f.Type() == 0 || AckEliciting(f) }
