package transport

// StreamNum is the one-based ordinal within a stream initiator/direction class.
// Stream IDs in the same class are separated by four.
type StreamNum int64

// StreamType selects the direction class when reconstructing a stream ID.
type StreamType uint8

const (
	StreamTypeUni StreamType = iota
	StreamTypeBidi
)

// Perspective selects the endpoint that initiated the stream.
type Perspective int

const (
	PerspectiveServer Perspective = 1
	PerspectiveClient Perspective = 2
)

// StreamNum returns the one-based count through this stream in its class.
// Valid QUIC stream IDs are nonnegative 62-bit integers. Signed division is
// retained for compatibility when applications convert other integers to StreamID.
func (id StreamID) StreamNum() StreamNum {
	return 1 + StreamNum(int64(id)/4)
}

// StreamID reconstructs an ID for the chosen initiator and direction. Zero is
// the empty stream count and maps to the invalid-ID sentinel -1.
func (number StreamNum) StreamID(direction StreamType, initiator Perspective) StreamID {
	if number == 0 {
		return -1
	}
	base := StreamID(number-1) * 4
	if initiator != PerspectiveClient && initiator != PerspectiveServer {
		return base
	}
	switch direction {
	case StreamTypeUni:
		base += 2
	case StreamTypeBidi:
	default:
		return base
	}
	if initiator == PerspectiveServer {
		base++
	}
	return base
}
