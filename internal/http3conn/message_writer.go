package http3conn

import (
	"fmt"
	"io"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

// MessageWriter sends one message direction. Calls must be serialized, including
// all other writes on the same transport stream. The shared FieldEncoder may
// serve other streams concurrently. Every error is terminal for this writer;
// the owner must reset/close as appropriate, including disposing of outstanding
// QPACK references through the connection lifecycle. No write is retried.
// The owner supplies interruptible stream writes/deadlines. Close sends FIN only
// after successful message validation; it does not close the connection.
type MessageWriter struct {
	stream         io.WriteCloser
	encoder        *FieldEncoder
	id             uint64
	message        *Message
	permitsTrailer func(string) bool
	closed         bool
	err            error
}

func NewMessageWriter(stream io.WriteCloser, encoder *FieldEncoder, id uint64, config MessageConfig, permitsTrailer func(string) bool) (*MessageWriter, error) {
	if stream == nil || encoder == nil || id > qpack.MaxInteger {
		return nil, fmt.Errorf("http3: invalid message writer configuration")
	}
	message, err := NewMessage(config)
	if err != nil {
		return nil, err
	}
	return &MessageWriter{stream: stream, encoder: encoder, id: id, message: message, permitsTrailer: permitsTrailer}, nil
}

// Headers sends initial/informational fields or trailers according to message
// state. Outgoing trailers require an explicit field-definition permission policy.
func (w *MessageWriter) Headers(fields []qpack.DecodedField) error {
	if err := w.writable(); err != nil {
		return err
	}
	kind, control, err := w.message.Headers(fields)
	if err != nil {
		return w.fail(err)
	}
	if kind == TrailerSection {
		if err := ValidateOutgoingTrailers(fields, w.permitsTrailer); err != nil {
			return w.fail(err)
		}
	}
	if w.message.config.Kind == ResponseFields && w.message.config.RequestMethod == "CONNECT" && control.Status >= 200 && control.Status < 300 {
		for _, field := range fields {
			if field.Name == "content-length" {
				return w.fail(&MessageError{"content-length prohibited on successful CONNECT response"})
			}
		}
	}
	if w.message.config.Kind == RequestFields && kind != TrailerSection {
		fields = canonicalEarlyData(fields)
	}
	fields, err = outgoingContentLength(fields)
	if err != nil {
		return w.fail(err)
	}
	block, err := w.encoder.Encode(w.id, fields)
	if err != nil {
		return w.fail(err)
	}
	if err = w.header(h3.Headers, len(block)); err != nil {
		return err
	}
	_, err = w.write(block)
	return err
}

// Write sends one DATA frame, returning the number of payload bytes written.
// Body length and placement are checked before any bytes of this frame are sent.
func (w *MessageWriter) Write(p []byte) (int, error) {
	if err := w.writable(); err != nil {
		return 0, err
	}
	if err := w.message.Data(uint64(len(p))); err != nil {
		return 0, w.fail(err)
	}
	if err := w.header(h3.Data, len(p)); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	return w.write(p)
}
func (w *MessageWriter) Close() error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return nil
	}
	if err := w.message.Finish(); err != nil {
		return w.fail(err)
	}
	if err := w.stream.Close(); err != nil {
		return w.fail(err)
	}
	w.closed = true
	return nil
}
func (w *MessageWriter) EstablishTunnel() error {
	if err := w.writable(); err != nil {
		return err
	}
	if err := w.message.EstablishTunnel(); err != nil {
		return w.fail(err)
	}
	return nil
}
func (w *MessageWriter) writable() error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return io.ErrClosedPipe
	}
	return nil
}
func (w *MessageWriter) header(typ uint64, n int) error {
	b, err := h3.AppendHeader(nil, h3.Header{Type: typ, Length: uint64(n)})
	if err != nil {
		return w.fail(err)
	}
	_, err = w.write(b)
	return err
}
func (w *MessageWriter) write(b []byte) (int, error) {
	n, err := w.stream.Write(b)
	if n < 0 || n > len(b) {
		n = 0
		err = io.ErrShortWrite
	} else if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return n, w.fail(err)
	}
	return n, nil
}
func (w *MessageWriter) fail(err error) error { w.err = err; return err }

// Receive-side validation permits equivalent repeated/list Content-Length values.
// Forwarding must emit a single decimal value, without mutating caller fields or
// losing a NeverIndex instruction supplied on any of the equivalent occurrences.
func outgoingContentLength(fields []qpack.DecodedField) ([]qpack.DecodedField, error) {
	length, err := ParseContentLength(fields)
	if err != nil {
		return nil, err
	}
	if length == "" {
		return fields, nil
	}
	count, first := 0, -1
	never := false
	for i, field := range fields {
		if field.Name == "content-length" {
			count++
			if first < 0 {
				first = i
			}
			never = never || field.NeverIndex
		}
	}
	if count == 1 && fields[first].Value == length {
		return fields, nil
	}
	result := make([]qpack.DecodedField, 0, len(fields)-count+1)
	for i, field := range fields {
		if field.Name == "content-length" {
			if i != first {
				continue
			}
			field.Value = length
			field.NeverIndex = never
		}
		result = append(result, field)
	}
	return result, nil
}
