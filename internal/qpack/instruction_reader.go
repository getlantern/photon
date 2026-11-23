package qpack

import (
	"errors"
	"io"
)

var ErrInstructionLimit = errors.New("qpack: encoded instruction exceeds limit")

// EncoderInstructionReader frames encoder instructions without changing table
// state. The source starts after the unidirectional stream type. Reads never
// consume the next instruction. String bodies are read once at their advertised
// length, bounded before allocation; fragmented bodies are not repeatedly parsed.
// It is serialized by its owner; cancellation/deadlines belong to source.
type EncoderInstructionReader struct {
	source io.Reader
	limit  int
	buf    []byte
	err    error
}

func NewEncoderInstructionReader(source io.Reader, maxEncodedBytes int) (*EncoderInstructionReader, error) {
	if source == nil || maxEncodedBytes < 1 {
		return nil, ErrDecoderConfig
	}
	return &EncoderInstructionReader{source: source, limit: maxEncodedBytes}, nil
}

// Next returns encoded bytes valid until the next call. Pass them to
// Decoder.ConsumeEncoder for semantic checks and table updates. Errors are sticky;
// io.EOF means closure at a boundary, io.ErrUnexpectedEOF truncation. Both require
// critical-stream handling by the endpoint, not normal completion. Other source
// errors (including resets and deadlines) are preserved for endpoint policy.
func (r *EncoderInstructionReader) Next() ([]byte, error) {
	if r.err != nil {
		return nil, r.err
	}
	r.buf = r.buf[:0]
	first, err := r.octet()
	if err == nil {
		switch {
		case first&0x80 != 0:
			_, err = r.integer(first, 6)
			if err == nil {
				err = r.stringBody(7)
			}
		case first&0x40 != 0:
			var size uint64
			size, err = r.integer(first, 5)
			if err == nil {
				err = r.body(size)
			}
			if err == nil {
				err = r.stringBody(7)
			}
		default:
			_, err = r.integer(first, 5)
		}
	}
	if err != nil {
		if errors.Is(err, io.EOF) && len(r.buf) > 0 {
			err = io.ErrUnexpectedEOF
		}
		r.err = err
		r.buf = nil // Release partial bodies on terminal failure.
		return nil, err
	}
	return r.buf, nil
}
func (r *EncoderInstructionReader) octet() (byte, error) {
	if len(r.buf) == r.limit {
		return 0, ErrInstructionLimit
	}
	var b [1]byte
	_, err := io.ReadFull(r.source, b[:])
	if err != nil {
		return 0, err
	}
	if len(r.buf) == cap(r.buf) {
		next := cap(r.buf) * 2
		if next < 16 {
			next = 16
		}
		if next > r.limit {
			next = r.limit
		}
		buf := make([]byte, len(r.buf), next)
		copy(buf, r.buf)
		r.buf = buf
	}
	r.buf = append(r.buf, b[0])
	return b[0], nil
}
func (r *EncoderInstructionReader) integer(first byte, prefix uint8) (uint64, error) {
	start := len(r.buf) - 1
	mask := byte((1 << prefix) - 1)
	if first&mask != mask {
		return uint64(first & mask), nil
	}
	for len(r.buf)-start < MaxIntegerBytes {
		b, err := r.octet()
		if err != nil {
			return 0, err
		}
		if b&128 == 0 {
			value, _, err := ConsumeInteger(r.buf[start:], prefix)
			return value, err
		}
	}
	return 0, ErrIntegerLength
}
func (r *EncoderInstructionReader) stringBody(prefix uint8) error {
	first, err := r.octet()
	if err != nil {
		return err
	}
	size, err := r.integer(first, prefix)
	if err != nil {
		return err
	}
	return r.body(size)
}
func (r *EncoderInstructionReader) body(size uint64) error {
	if size > uint64(r.limit-len(r.buf)) {
		return ErrInstructionLimit
	}
	start := len(r.buf)
	// Allocate only within the configured encoded instruction cap.
	end := start + int(size)
	if end > cap(r.buf) {
		buf := make([]byte, end)
		copy(buf, r.buf)
		r.buf = buf
	} else {
		r.buf = r.buf[:end]
	}
	_, err := io.ReadFull(r.source, r.buf[start:end])
	return err
}
