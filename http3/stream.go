package http3

import (
	"context"
	"io"
	"time"

	quic "github.com/getlantern/photon"
)

// FrameType is the type of an HTTP/3 frame.
type FrameType uint64

// StreamType is the type of a unidirectional HTTP/3 stream.
type StreamType uint64

// HTTPStreamer lets a handler take over its HTTP/3 request stream. The
// http.ResponseWriter passed to handlers implements it. The caller then owns
// the stream beyond the handler and must finish both directions: Close or
// CancelWrite, and Read to EOF or CancelRead. A Write or Close that fails
// does not finish sending. Until it has, the stream keeps
// its Server.MaxConcurrentRequests slot and its request context stays live.
//
// If WriteHeader set a final status before HTTPStream, the response header is
// sent. Otherwise no response is sent and the Stream cannot be written.
// HTTPStream fails once response body bytes were written, and a failed header
// write fails the request. Afterwards WriteHeader panics and Write returns
// http.ErrHijacked. Repeated calls return the same Stream.
type HTTPStreamer interface {
	HTTPStream() (*Stream, error)
}

type datagramStream interface {
	io.ReadWriteCloser
	CancelRead(quic.StreamErrorCode)
	CancelWrite(quic.StreamErrorCode)
	StreamID() quic.StreamID
	Context() context.Context
	SetDeadline(time.Time) error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	SetPriority(urgency int8, incremental bool)
	TryWriteAll([]byte) error
	SendDatagram(b []byte) error
	ReceiveDatagram(ctx context.Context) ([]byte, error)

	QUICStream() *quic.Stream
}

// A Stream is an HTTP/3 request stream taken over with HTTPStreamer. Reads and
// writes carry the payload of DATA frames. One Read and one Write may run
// concurrently. Stream and connection errors are returned as *Error.
//
// Close ends sending with FIN and leaves receiving open; CancelRead and
// CancelWrite abort one direction as the quic.Stream methods do. Context is
// done once sending has ended. An expired deadline (os.ErrDeadlineExceeded)
// leaves the stream usable: the interrupted Read or Write can be repeated, and
// Close fails while a DATA frame is unfinished. SetPriority sets the QUIC
// stream's scheduling priority. QUICStream bypasses HTTP/3 framing.
type Stream struct {
	datagramStream
}

// Read reads DATA payload. Request trailers populate Request.Trailer.
func (s *Stream) Read(b []byte) (int, error) { return s.datagramStream.Read(b) }

// Write sends b in one DATA frame. It fails if HTTPStream was called before
// the response header was set. An empty b sends nothing, but still returns the
// error of a send direction that has ended, as TryWriteAll does.
func (s *Stream) Write(b []byte) (int, error) { return s.datagramStream.Write(b) }

// TryWriteAll writes b in a DATA frame if the entire frame can be queued
// immediately. Otherwise it returns quic.ErrWouldBlock and queues nothing,
// unless sending has ended, which it reports instead.
func (s *Stream) TryWriteAll(b []byte) error { return s.datagramStream.TryWriteAll(b) }

// StreamID returns the ID of the underlying QUIC stream.
func (s *Stream) StreamID() quic.StreamID { return s.datagramStream.StreamID() }

// SendDatagram sends an HTTP datagram (RFC 9297) on this request. It requires
// a request routed through Server.DatagramHandlers and datagram support
// negotiated with the peer; otherwise it returns ErrHTTPDatagramsNotNegotiated.
func (s *Stream) SendDatagram(b []byte) error { return s.datagramStream.SendDatagram(b) }

// ReceiveDatagram waits for an HTTP datagram sent on this request, under the
// same conditions as SendDatagram. It returns ErrDatagramRequestClosed once
// the receive direction has ended.
func (s *Stream) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	return s.datagramStream.ReceiveDatagram(ctx)
}
