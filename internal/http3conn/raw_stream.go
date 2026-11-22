package http3conn

import (
	"context"
	"errors"
	"io"
	"sync"
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
	datagrams *DatagramStream // nil without datagram semantics
	task      *requestTask
	mapError  handlerErrorMap

	readMu sync.Mutex
	body   io.Reader // nil until a message body is available
	noBody error

	writeMu  sync.Mutex
	writer   *MessageWriter // nil while this side has sent no header
	noWriter error

	received     chan struct{} // closed when the application is done receiving
	receivedOnce sync.Once
}

func newRawStream(stream ClientExchangeStream, underlying *quic.Stream, task *requestTask, mapError handlerErrorMap) *RawStream {
	return &RawStream{stream: stream, quic: underlying, task: task, mapError: mapError, received: make(chan struct{})}
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
	defer func() { err = s.mapError.apply(s.ended(err)) }()
	s.readMu.Lock()
	defer s.readMu.Unlock()
	if s.body == nil {
		return 0, s.noBody
	}
	n, err = s.body.Read(p)
	if err != nil && !h3.IsDeadline(err) {
		var protocol *h3.Error
		var message *MessageError
		if errors.As(err, &protocol) || errors.As(err, &message) {
			s.task.fail(err, 0x10e)
		}
		s.receiveEnded()
	}
	return n, err
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
// abandons such a stream. Receiving is unaffected.
func (s *RawStream) Close() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writer == nil {
		return s.mapError.apply(s.ended(s.stream.Close()))
	}
	return s.mapError.apply(s.ended(s.writer.Close()))
}

// CancelRead aborts receiving, as quic.Stream.CancelRead does, and unblocks Read.
func (s *RawStream) CancelRead(code quic.StreamErrorCode) {
	s.stream.CancelRead(code)
	s.receiveEnded()
}

// CancelWrite aborts sending, as quic.Stream.CancelWrite does, and unblocks Write.
func (s *RawStream) CancelWrite(code quic.StreamErrorCode) { s.stream.CancelWrite(code) }

// Context is the QUIC stream's send context: done once sending has ended.
func (s *RawStream) Context() context.Context { return s.stream.Context() }
func (s *RawStream) StreamID() quic.StreamID  { return s.stream.StreamID() }

func (s *RawStream) SetDeadline(t time.Time) error      { return s.quic.SetDeadline(t) }
func (s *RawStream) SetReadDeadline(t time.Time) error  { return s.quic.SetReadDeadline(t) }
func (s *RawStream) SetWriteDeadline(t time.Time) error { return s.quic.SetWriteDeadline(t) }

// SetPriority sets the QUIC stream's scheduling priority.
func (s *RawStream) SetPriority(urgency int8, incremental bool) {
	s.quic.SetPriority(urgency, incremental)
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
	if cause := context.Cause(s.task.ctx); cause != nil && cause != context.Canceled {
		return cause
	}
	return err
}

func (s *RawStream) receiveEnded() { s.receivedOnce.Do(func() { close(s.received) }) }

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
