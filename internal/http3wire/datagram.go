package http3wire

import (
	"github.com/getlantern/photon/internal/wire"
	"io"
)

const DatagramError uint64 = 0x33
const DatagramCapsule uint64 = 0

// ParseDatagram decodes RFC 9297's quarter stream ID. The payload aliases data.
// Request existence, receive-side lifetime and extension semantics are checked
// by the connection layer, not by this stateless codec.
func ParseDatagram(data []byte) (streamID uint64, payload []byte, err error) {
	quarter, n, err := wire.ConsumeVarint(data)
	if err != nil || quarter > wire.MaxVarint/4 {
		return 0, nil, &Error{DatagramError, "invalid quarter stream ID"}
	}
	return quarter * 4, data[n:], nil
}

// AppendDatagram accepts client-initiated bidirectional IDs only. Invalid IDs
// leave dst unchanged. Negotiation and stream send state belong to the caller.
func AppendDatagram(dst []byte, streamID uint64, payload []byte) ([]byte, error) {
	if streamID > wire.MaxVarint || streamID%4 != 0 {
		return dst, &Error{DatagramError, "invalid request stream ID"}
	}
	dst, _ = wire.AppendVarint(dst, streamID/4)
	return append(dst, payload...), nil
}

// NewCapsuleReader parses RFC 9297 capsules from an already unframed HTTP data
// stream. Unlike HTTP/3 frames, every type is permitted. Callers consume or skip
// unknown types using Read/io.Copy before Next; lengths do not drive allocation.
// Truncation is H3_MESSAGE_ERROR (request scope), not a connection frame error.
// This does not negotiate Capsule-Protocol or authorize a CONNECT data stream.
func NewCapsuleReader(source io.Reader) *Reader { return &Reader{source: source, capsule: true} }

// AppendCapsuleHeader emits the type and length; payload follows separately.
// The DATAGRAM capsule payload has no quarter-stream-ID prefix.
func AppendCapsuleHeader(dst []byte, typ, length uint64) ([]byte, error) {
	return AppendHeader(dst, Header{typ, length})
}
