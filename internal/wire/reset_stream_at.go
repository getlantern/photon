package wire

// FrameResetStreamAt is draft-ietf-quic-reliable-stream-reset-11's assigned type.
const FrameResetStreamAt uint64 = 0x24

// ResetStreamAtFrame terminates a stream after reliable delivery of at least
// ReliableSize bytes. FinalSize still determines flow-control accounting.
// Stream state and negotiated support are checked separately from encoding.
type ResetStreamAtFrame struct {
	StreamID, ErrorCode, FinalSize, ReliableSize uint64
}

func (ResetStreamAtFrame) Type() uint64 { return FrameResetStreamAt }
func (f ResetStreamAtFrame) Append(dst []byte) ([]byte, error) {
	if f.ReliableSize > f.FinalSize {
		return dst, ErrFrame
	}
	return appendFields(dst, f.Type(), f.StreamID, f.ErrorCode, f.FinalSize, f.ReliableSize)
}
