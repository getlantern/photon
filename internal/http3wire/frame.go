// Package http3wire implements HTTP/3 framing primitives from RFC 9114.
// Frame placement and control-stream parsing are included. Message ordering,
// QPACK and connection lifecycle belong to the unfinished HTTP/3 endpoint.
package http3wire

import (
	"errors"
	"fmt"
	"io"

	"github.com/getlantern/photon/internal/wire"
)

const (
	Data                  uint64 = 0
	Headers               uint64 = 1
	CancelPush            uint64 = 3
	Settings              uint64 = 4
	PushPromise           uint64 = 5
	Goaway                uint64 = 7
	MaxPushID             uint64 = 13
	PriorityUpdateRequest uint64 = 0xf0700
	PriorityUpdatePush    uint64 = 0xf0701
	FrameUnexpected       uint64 = 0x105
	FrameError            uint64 = 0x106
	ExcessiveLoad         uint64 = 0x107
	SettingsError         uint64 = 0x109
)

type Error struct {
	Code   uint64
	Reason string
}

func (e *Error) Error() string { return fmt.Sprintf("http3: 0x%x: %s", e.Code, e.Reason) }

var ErrPayloadPending = errors.New("http3: consume frame payload before reading next header")

type Header struct{ Type, Length uint64 }

// Reader reads one stream, without buffering proportional to a frame length.
// Next selects a payload, exposed through Read; consume it fully before Next.
// It is not safe for concurrent use. Stream deadlines/cancellation are supplied
// by the underlying reader. Unknown types are exposed for the caller to skip.
type Reader struct {
	integerScratch [8]byte
	capsule        bool
	source         io.Reader
	remaining      uint64
	err            error
}

func NewReader(source io.Reader) *Reader { return &Reader{source: source} }

func (r *Reader) Next() (Header, error) {
	if r.err != nil {
		return Header{}, r.err
	}
	if r.remaining != 0 {
		return Header{}, ErrPayloadPending
	}
	typ, err := readIntegerScratch(r.source, &r.integerScratch)
	if err != nil {
		return Header{}, r.headerError(err, true)
	}
	length, err := readIntegerScratch(r.source, &r.integerScratch)
	if err != nil {
		return Header{}, r.headerError(err, false)
	}
	// RFC 9114 section 7.2.8 reserves HTTP/2-only frame types as errors.
	switch typ {
	case 2, 6, 8, 9:
		if !r.capsule {
			r.err = &Error{FrameUnexpected, "HTTP/2-only frame type"}
			return Header{}, r.err
		}
	}
	r.remaining = length
	return Header{typ, length}, nil
}
func (r *Reader) headerError(err error, between bool) error {
	if (!between && errors.Is(err, io.EOF)) || errors.Is(err, io.ErrUnexpectedEOF) {
		err = r.truncated("header")
	}
	r.err = err
	return err
}
func (r *Reader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if uint64(len(p)) > r.remaining {
		p = p[:int(r.remaining)]
	}
	n, err := r.source.Read(p)
	r.remaining -= uint64(n)
	if errors.Is(err, io.EOF) {
		if r.remaining != 0 {
			err = r.truncated("payload")
		} else {
			err = nil
		}
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = r.truncated("payload")
	}
	if err != nil {
		r.err = err
	}
	return n, err
}

func readInteger(source io.Reader) (uint64, error) {
	var buf [8]byte
	return readIntegerScratch(source, &buf)
}
func readIntegerScratch(source io.Reader, buf *[8]byte) (uint64, error) {
	if _, err := io.ReadFull(source, buf[:1]); err != nil {
		return 0, err
	}
	size := 1 << (buf[0] >> 6)
	if _, err := io.ReadFull(source, buf[1:size]); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return 0, err
	}
	value, _, err := wire.ConsumeVarint(buf[:size])
	return value, err
}

// AppendHeader is transactional for out-of-range integers. It does not enforce
// placement or extension policy, which depend on the endpoint and stream type.
func AppendHeader(dst []byte, h Header) ([]byte, error) {
	if h.Type > wire.MaxVarint || h.Length > wire.MaxVarint {
		return dst, wire.ErrVarintRange
	}
	dst, _ = wire.AppendVarint(dst, h.Type)
	dst, _ = wire.AppendVarint(dst, h.Length)
	return dst, nil
}

// ParseID parses CANCEL_PUSH, GOAWAY or MAX_PUSH_ID payloads. Role and ID-space
// checks are performed by the endpoint. Extra bytes are not permitted.
func ParseID(payload []byte) (uint64, error) {
	id, n, err := wire.ConsumeVarint(payload)
	if err != nil || n != len(payload) {
		return 0, &Error{FrameError, "expected exactly one identifier"}
	}
	return id, nil
}

func (r *Reader) truncated(part string) error {
	if r.capsule {
		return &Error{0x10e, "truncated capsule " + part}
	}
	return &Error{FrameError, "truncated frame " + part}
}
