package http3conn

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
)

// RawStream hands both directions of one request stream to an application
// while the request's MessageReader and MessageWriter keep the DATA framing.
// One Read and one Write may run concurrently. The application must finish
// both directions: its request slot, QPACK state and datagram registration are
// held until it has, or until the connection ends.
//
// Peer resets are returned, not acted on. A protocol violation still closes the
// connection and a malformed message still resets the request.
type RawStream struct {
	stream    ClientExchangeStream // through the datagram binding when there is one
	quic      *quic.Stream
	control   streamControl   // quic, except under test
	datagrams *DatagramStream // nil without datagram semantics
	task      *requestTask
	mapError  handlerErrorMap

	readMu sync.Mutex
	body   io.Reader // nil until a message body is available
	noBody error

	writeMu  sync.Mutex
	writer   *MessageWriter // nil while this side has sent no header
	noWriter error

	received  chan struct{} // closed, by the task and once, when receiving is over
	completed func()        // reports a validated end of message, before received closes

	canceledRead atomic.Pointer[quic.StreamError] // set by the first CancelRead

	fieldsMu     sync.Mutex
	fields       *FieldStream // the receive direction's QPACK waits, once it has any
	readDeadline time.Time
}

// streamControl is the part of a QUIC stream that HTTP/3 framing passes through.
type streamControl interface {
	SetDeadline(time.Time) error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	SetPriority(urgency int8, incremental bool)
}

func newRawStream(stream ClientExchangeStream, underlying *quic.Stream, task *requestTask, mapError handlerErrorMap) *RawStream {
	s := &RawStream{stream: stream, quic: underlying, task: task, mapError: mapError, received: make(chan struct{})}
	if underlying != nil {
		s.control = underlying
	}
	return s
}

// attachFields extends the read deadline and CancelRead to header and trailer
// sections waiting on the QPACK dynamic table.
func (s *RawStream) attachFields(fields *FieldStream) {
	s.fieldsMu.Lock()
	defer s.fieldsMu.Unlock()
	s.fields = fields
	fields.SetReadDeadline(s.readDeadline)
}
func (s *RawStream) setReadDeadline(t time.Time) {
	s.fieldsMu.Lock()
	defer s.fieldsMu.Unlock()
	s.readDeadline = t
	if s.fields != nil {
		s.fields.SetReadDeadline(t)
	}
}

// quicStreamOf finds the QUIC stream beneath datagram and endpoint adapters.
func quicStreamOf(stream any) *quic.Stream {
	for {
		switch s := stream.(type) {
		case *quic.Stream:
			return s
		case *DatagramStream:
			stream = s.DatagramTransportStream
		case interface{ QUICStream() *quic.Stream }:
			return s.QUICStream()
		default:
			return nil
		}
	}
}

// Read returns DATA payload. Trailers are delivered to the message's owner and
// unknown frames are skipped. After an expired read deadline it can be retried.
func (s *RawStream) Read(p []byte) (n int, err error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	if s.body == nil {
		return 0, s.mapError.apply(s.noBody)
	}
	n, err = s.body.Read(p)
	return n, s.readEnded(err)
}

// readEnded applies a raw stream's rules to the result of a read of its body,
// and returns the error the application sees. Only a protocol violation or a
// malformed message fails the request; any other terminal result, a peer reset
// among them, ends receiving and nothing else. An expired deadline ends nothing.
func (s *RawStream) readEnded(err error) error {
	if err == io.EOF && s.completed != nil {
		s.completed()
	}
	s.task.readOutcome(err, false)
	return s.mapError.apply(s.ended(err))
}

// Write sends p in one DATA frame. After an expired write deadline, the next
// Write or TryWriteAll first supplies the rest of the interrupted frame.
func (s *RawStream) Write(p []byte) (n int, err error) {
	defer func() { err = s.mapError.apply(s.ended(err)) }()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writer == nil {
		return 0, s.noWriter
	}
	if len(p) == 0 {
		return 0, nil
	}
	return s.writer.Write(p)
}

// TryWriteAll queues p as one DATA frame if the stream takes all of it at once,
// and otherwise returns quic.ErrWouldBlock having queued nothing. It does not
// wait for a Write in progress.
func (s *RawStream) TryWriteAll(p []byte) (err error) {
	defer func() { err = s.mapError.apply(s.ended(err)) }()
	if !s.writeMu.TryLock() {
		return quic.ErrWouldBlock
	}
	defer s.writeMu.Unlock()
	if s.writer == nil {
		return s.noWriter
	}
	return s.writer.TryWrite(p)
}

// Close ends the send direction with FIN. It waits for a Write in progress and
// fails while a DATA frame interrupted by a deadline is unfinished; CancelWrite
// abandons such a stream. Receiving is unaffected. A peer that already stopped
// this direction with H3_NO_ERROR, as a server may once it has responded, has
// ended it just as well: that is not an error.
func (s *RawStream) Close() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var err error
	if s.writer == nil {
		err = s.stream.Close()
	} else {
		err = s.writer.Close()
	}
	if normalUploadStop(err) {
		return nil
	}
	return s.mapError.apply(s.ended(err))
}

// CancelRead aborts receiving, as quic.Stream.CancelRead does, and unblocks Read.
func (s *RawStream) CancelRead(code quic.StreamErrorCode) {
	s.fieldsMu.Lock()
	fields := s.fields
	s.fieldsMu.Unlock()
	// A section waiting for the dynamic table reads nothing from the stream, so
	// resetting the stream would not wake it. Abandoning its QPACK work does,
	// and is due anyway. Nothing can be waiting once receiving has ended.
	select {
	case <-s.received:
	default:
		s.canceledRead.CompareAndSwap(nil, &quic.StreamError{StreamID: s.stream.StreamID(), ErrorCode: code})
		if fields != nil {
			_ = fields.Cancel()
		}
	}
	s.stream.CancelRead(code)
	s.receiveEnded()
}

// CancelWrite aborts sending, as quic.Stream.CancelWrite does, and unblocks Write.
func (s *RawStream) CancelWrite(code quic.StreamErrorCode) { s.stream.CancelWrite(code) }

// Context is the QUIC stream's send context: done once sending has ended.
func (s *RawStream) Context() context.Context { return s.stream.Context() }
func (s *RawStream) StreamID() quic.StreamID  { return s.stream.StreamID() }

func (s *RawStream) SetDeadline(t time.Time) error {
	s.setReadDeadline(t)
	return s.control.SetDeadline(t)
}
func (s *RawStream) SetReadDeadline(t time.Time) error {
	s.setReadDeadline(t)
	return s.control.SetReadDeadline(t)
}
func (s *RawStream) SetWriteDeadline(t time.Time) error { return s.control.SetWriteDeadline(t) }

// SetPriority sets the QUIC stream's scheduling priority.
func (s *RawStream) SetPriority(urgency int8, incremental bool) {
	s.control.SetPriority(urgency, incremental)
}

// QUICStream bypasses HTTP/3 framing and this stream's bookkeeping.
func (s *RawStream) QUICStream() *quic.Stream { return s.quic }

// SendDatagram queues an HTTP datagram for this request. It waits for queue
// space until the send direction or the connection ends.
func (s *RawStream) SendDatagram(payload []byte) error {
	if s.datagrams == nil {
		return s.mapError.apply(ErrHTTPDatagramsNotNegotiated)
	}
	return s.mapError.apply(s.datagrams.SendDatagram(context.Background(), payload))
}

// ReceiveDatagram waits for an HTTP datagram sent on this request.
func (s *RawStream) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	if s.datagrams == nil {
		return nil, s.mapError.apply(ErrHTTPDatagramsNotNegotiated)
	}
	payload, err := s.datagrams.ReceiveDatagram(ctx)
	return payload, s.mapError.apply(err)
}

// ended prefers the reason the request ended (a GOAWAY rejection, the
// connection's close, its owner) to the stream reset that reason caused.
func (s *RawStream) ended(err error) error {
	if err == nil || err == io.EOF || h3.IsDeadline(err) || errors.Is(err, quic.ErrWouldBlock) {
		return err
	}
	var protocol *h3.Error
	var message *MessageError
	if errors.As(err, &protocol) || errors.As(err, &message) {
		return err
	}
	// CancelRead ended a wait that the QUIC stream's own error could not.
	if canceled := s.canceledRead.Load(); canceled != nil && errors.Is(err, ErrFieldSectionCanceled) {
		return canceled
	}
	if cause := context.Cause(s.task.ctx); cause != nil && cause != context.Canceled {
		return cause
	}
	return err
}

func (s *RawStream) receiveEnded() { s.task.receiveEnded() }

// wait returns once the application has finished both directions, or ctx ends.
// The send context alone covers sending. Receiving needs the application's own
// signal: the receive context ends when FIN arrives, before buffered data is read.
func (s *RawStream) wait(ctx context.Context) {
	select {
	case <-s.stream.Context().Done():
	case <-ctx.Done():
		return
	}
	select {
	case <-s.received:
	case <-ctx.Done():
	}
}
