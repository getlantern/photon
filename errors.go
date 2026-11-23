package quic

import (
	"context"
	"errors"

	engineStream "github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/transport"
)

// StreamLimitReachedError reports that OpenStream or OpenUniStream exhausted
// the peer's stream-count credit. The synchronous open methods wait for credit
// instead. This error does not close the connection.
type StreamLimitReachedError struct{}

func (StreamLimitReachedError) Error() string { return "too many open streams" }

func (StreamLimitReachedError) Is(target error) bool {
	_, ok := target.(*StreamLimitReachedError)
	return ok
}

// Unwrap preserves matching against the engine's historical would-block cause.
func (StreamLimitReachedError) Unwrap() error { return engineStream.ErrWouldBlock }

// IdleTimeoutError indicates that the connection closed after inactivity.
// It implements net.Error and unwraps to net.ErrClosed.
type IdleTimeoutError = transport.IdleTimeoutError

// HandshakeTimeoutError indicates that the absolute handshake deadline expired.
// It implements net.Error and unwraps to net.ErrClosed.
type HandshakeTimeoutError = transport.HandshakeTimeoutError

// Preserve the per-connection terminal cause after the socket owner has exited.
// Called only outside the owner, after an adapter operation returns.
func (c *Conn) connectionError(err error) error {
	if errors.Is(err, transport.ErrEndpointClosed) {
		if cause := context.Cause(c.ctx); cause != nil {
			return cause
		}
	}
	return err
}

// TransportError preserves a local or remote QUIC transport failure and its cause.
type TransportError = transport.ConnectionTransportError

// TransportErrorCode identifies a QUIC transport failure.
type TransportErrorCode = transport.TransportErrorCode

const (
	NoError TransportErrorCode = iota
	InternalError
	ConnectionRefused
	FlowControlError
	StreamLimitError
	StreamStateError
	FinalSizeError
	FrameEncodingError
	TransportParameterError
	ConnectionIDLimitError
	ProtocolViolation
	InvalidToken
	ApplicationErrorErrorCode
	CryptoBufferExceeded
	KeyUpdateError
	AEADLimitReached
	NoViablePathError
)

// StatelessResetError indicates that a received packet matched an active peer
// stateless reset token. It implements net.Error and unwraps to net.ErrClosed.
type StatelessResetError = transport.StatelessResetError

// DatagramTooLargeError is returned by SendDatagram when the payload exceeds
// the peer's frame limit or the selected path's current packet budget.
// MaxDatagramPayloadSize is a snapshot; later path changes can lower it.
type DatagramTooLargeError = transport.DatagramTooLargeError

// ErrWriteLimitReached reports that a WriteWithLimit callback restricted a write.
// The returned byte count identifies the prefix already committed to the stream.
var ErrWriteLimitReached = transport.ErrWriteLimitReached

// ErrStreamClosed is returned by Write, WriteWithLimit and TryWriteAll after
// Close on the same stream, and is the cause of the stream's Context after a
// normal Close.
var ErrStreamClosed = engineStream.ErrSendClosed

// ErrWouldBlock reports that TryWriteAll cannot immediately admit the whole slice.
var ErrWouldBlock = errors.New("operation would block")
