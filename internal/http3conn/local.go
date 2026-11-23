// Package http3conn contains connection lifecycle components for the unfinished
// HTTP/3 endpoint. It is not a net/http client or server implementation.
package http3conn

import (
	"context"
	"errors"
	"io"
	"time"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/http3wire"
)

const InternalError uint64 = 0x102

// SendStream omits Close: local critical streams must remain open for the
// connection lifetime. Their sole writers are assigned by the connection owner.
// SetWriteDeadline must interrupt an in-progress Write (as our QUIC streams do).
type SendStream interface {
	io.Writer
	SetWriteDeadline(time.Time) error
}
type Transport interface {
	OpenUni(context.Context) (SendStream, error)
	Abort(uint64, string) error
}

// QUICTransport adapts the independent public transport to lifecycle operations.
type QUICTransport struct{ Conn *quic.Conn }

func (t QUICTransport) OpenUni(ctx context.Context) (SendStream, error) {
	return t.Conn.OpenUniStreamSync(ctx)
}
func (t QUICTransport) Abort(code uint64, reason string) error {
	return t.Conn.CloseWithError(quic.ApplicationErrorCode(code), reason)
}

type LocalStreams struct{ Control, Encoder, Decoder SendStream }

// OpenLocal creates all three critical streams, writes their type prefixes and
// the control SETTINGS frame, and transfers writer ownership to the caller.
// The connection owner calls this exactly once. Invalid local settings and an
// already canceled context fail before touching the transport. Any failure once
// setup starts aborts the connection, except 0-RTT rejection which invalidates
// the entire early generation. Otherwise partially opened critical streams cannot
// be independently reset/closed and replaced. No FIN is sent on success.
func OpenLocal(ctx context.Context, transport Transport, settings []http3wire.Setting) (*LocalStreams, error) {
	if transport == nil {
		return nil, errors.New("http3: nil transport")
	}
	control, err := http3wire.AppendSettings([]byte{0}, settings)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, context.Cause(ctx)
	}
	streams := &LocalStreams{}
	slots := []*SendStream{&streams.Control, &streams.Encoder, &streams.Decoder}
	payloads := [][]byte{control, {2}, {3}}
	for i, slot := range slots {
		stream, err := transport.OpenUni(ctx)
		if err != nil {
			if ctx.Err() != nil {
				err = context.Cause(ctx)
			}
			if !errors.Is(err, quic.Err0RTTRejected) {
				transport.Abort(http3wire.StreamCreationError, "cannot open local critical stream")
			}
			return nil, err
		}
		// Control and QPACK must make progress even while urgency-zero request
		// streams have data queued, or header dependencies can stall delivery.
		if scheduler, ok := stream.(interface{ SetPriority(int8, bool) }); ok {
			scheduler.SetPriority(0, true)
		}
		*slot = stream
		if err = writeSetup(ctx, stream, payloads[i]); err != nil {
			if !errors.Is(err, quic.Err0RTTRejected) {
				transport.Abort(InternalError, "cannot initialize local critical stream")
			}
			return nil, err
		}
	}
	return streams, nil
}
func writeSetup(ctx context.Context, stream SendStream, data []byte) error {
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	// A deadline interrupts a flow-control-blocked Write even for a cancel-only
	// context. Join an already-running callback before clearing the setup deadline.
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); stream.SetWriteDeadline(time.Now()) })
	n, err := stream.Write(data)
	if !stop() {
		<-done
	}
	clearErr := stream.SetWriteDeadline(time.Time{})
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return clearErr
}

// ReceiveDatagram adapts QUIC payloads for the session-owned HTTP dispatcher.
func (t QUICTransport) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	return t.Conn.ReceiveDatagram(ctx)
}

func (t QUICTransport) SendDatagramContext(ctx context.Context, data []byte) error {
	return t.Conn.SendDatagramContext(ctx, data)
}
