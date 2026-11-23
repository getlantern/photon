package http3conn

import (
	"errors"
	"fmt"
	"io"

	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

// MessageWriter sends one message direction. Calls must be serialized, including
// all other writes on the same transport stream. The shared FieldEncoder may
// serve other streams concurrently. The owner supplies interruptible stream
// writes/deadlines. Close sends FIN only after successful message validation;
// it does not close the connection. No write is retried.
//
// Some outcomes leave the writer usable and the call repeatable:
//   - an expired stream deadline in Write: the next Write or TryWrite first
//     finishes the interrupted DATA frame;
//   - an expired stream deadline in TryWrite or Close: nothing was queued;
//   - quic.ErrWouldBlock from TryWrite: nothing was queued;
//   - Headers or Close while a DATA frame is unfinished (errUnfinishedData).
//
// Every other error is cached and terminal for this writer: a failed stream
// write, failed message validation, and any failure, an expired deadline
// included, while a HEADERS frame is being written, whose block is tied to
// QPACK state already committed. The owner must then reset/close as
// appropriate, including disposing of outstanding QPACK references through the
// connection lifecycle.
type MessageWriter struct {
	stream         io.WriteCloser
	encoder        *FieldEncoder
	id             uint64
	message        *Message
	permitsTrailer func(string) bool
	frame          []byte // unwritten header bytes of the DATA frame a deadline interrupted
	payload        uint64 // payload bytes that frame still needs
	closed         bool
	err            error
}

var errUnfinishedData = errors.New("http3: DATA frame interrupted by a deadline is unfinished")

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
	if w.unfinished() {
		return errUnfinishedData
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
	frame, err := h3.AppendHeader(nil, h3.Header{Type: h3.Headers, Length: uint64(len(block))})
	if err != nil {
		return w.fail(err)
	}
	// The block is tied to QPACK state already committed, so a partial HEADERS
	// frame cannot be abandoned or re-encoded: any failure here is terminal.
	if _, err = w.write(frame); err == nil {
		_, err = w.write(block)
	}
	if err != nil {
		return w.fail(err)
	}
	return nil
}

// Write sends one DATA frame, returning the number of payload bytes written.
// Body length and placement are checked before any bytes of this frame are sent.
// After a deadline interrupted a frame, p first supplies that frame's remaining
// payload and only the rest starts a new one.
func (w *MessageWriter) Write(p []byte) (int, error) {
	if err := w.writable(); err != nil {
		return 0, err
	}
	done := 0
	if w.unfinished() {
		n, err := w.resume(p)
		if err != nil || n == len(p) {
			return n, err
		}
		done, p = n, p[n:]
	}
	if err := w.message.Data(uint64(len(p))); err != nil {
		return done, w.fail(err)
	}
	frame, err := h3.AppendHeader(nil, h3.Header{Type: h3.Data, Length: uint64(len(p))})
	if err != nil {
		return done, w.fail(err)
	}
	w.frame, w.payload = frame, uint64(len(p))
	n, err := w.resume(p)
	return done + n, err
}
func (w *MessageWriter) unfinished() bool { return len(w.frame) > 0 || w.payload > 0 }
func (w *MessageWriter) resume(p []byte) (int, error) {
	if len(w.frame) > 0 {
		n, err := w.write(w.frame)
		w.frame = w.frame[n:]
		if err != nil {
			return 0, err
		}
	}
	if uint64(len(p)) > w.payload {
		p = p[:w.payload]
	}
	if len(p) == 0 {
		return 0, nil
	}
	n, err := w.write(p)
	w.payload -= uint64(n)
	return n, err
}

// TryWrite queues p as one DATA frame, behind the rest of an interrupted frame,
// only if the stream accepts all of it at once. On quic.ErrWouldBlock nothing
// was queued and no state changed.
func (w *MessageWriter) TryWrite(p []byte) error {
	if err := w.writable(); err != nil {
		return err
	}
	stream, ok := w.stream.(interface{ TryWriteAll([]byte) error })
	if !ok {
		return fmt.Errorf("http3: stream cannot queue without blocking")
	}
	completed := min(uint64(len(p)), w.payload)
	rest := p[completed:]
	data := make([]byte, 0, len(w.frame)+16+len(p))
	data = append(append(data, w.frame...), p[:completed]...)
	var saved BodyLength
	if len(rest) > 0 {
		if w.message.body != nil {
			saved = *w.message.body
		}
		if err := w.message.Data(uint64(len(rest))); err != nil {
			return w.fail(err)
		}
		var err error
		if data, err = h3.AppendHeader(data, h3.Header{Type: h3.Data, Length: uint64(len(rest))}); err != nil {
			return w.fail(err)
		}
		data = append(data, rest...)
	}
	if len(data) == 0 {
		return nil
	}
	if err := stream.TryWriteAll(data); err != nil {
		// Neither a full stream nor an expired deadline queued anything.
		if !errors.Is(err, quic.ErrWouldBlock) && !h3.IsDeadline(err) {
			return w.fail(err)
		}
		if len(rest) > 0 {
			*w.message.body = saved
		}
		return err
	}
	w.frame, w.payload = nil, w.payload-completed
	return nil
}
func (w *MessageWriter) Close() error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return nil
	}
	if w.unfinished() {
		return errUnfinishedData
	}
	if err := w.message.Finish(); err != nil {
		return w.fail(err)
	}
	if err := w.stream.Close(); err != nil {
		// A deadline that expired before FIN was queued sent nothing.
		if h3.IsDeadline(err) {
			return err
		}
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
func (w *MessageWriter) write(b []byte) (int, error) {
	n, err := w.stream.Write(b)
	if n < 0 || n > len(b) {
		n = 0
		err = io.ErrShortWrite
	} else if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	if err != nil && !h3.IsDeadline(err) {
		return n, w.fail(err)
	}
	return n, err
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
