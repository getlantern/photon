package http3conn

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/http3wire"
)

type IncomingTransport interface {
	AcceptUni(context.Context) (ReceiveStream, error)
	Abort(uint64, string) error
}

func (t QUICTransport) AcceptUni(ctx context.Context) (ReceiveStream, error) {
	return t.Conn.AcceptUniStream(ctx)
}

// StreamHandler consumes a known stream after its type prefix. Calls for
// different streams are concurrent. A handler must honor ctx when not reading,
// must not replace stream deadlines, and must not launch unjoined work. It returns
// HTTP/3 errors for protocol violations; returning nil on a critical stream is
// treated as closure. Push handlers own push-ID and message validation.
type StreamHandler func(context.Context, uint64, ReceiveStream) error

// RunIncoming accepts and dispatches incoming unidirectional streams once per
// connection. At most maxActive streams (including incomplete prefixes) are owned
// concurrently; exhausting that budget closes with H3_EXCESSIVE_LOAD. The budget
// must allow the three critical streams. Unknown types are stopped by Classify.
// First failure cancels and joins all workers. Parent cancellation stops the loop
// without selecting a connection error; the caller owns connection shutdown.
func RunIncoming(parent context.Context, transport IncomingTransport, peerIsServer bool, maxActive int, handler StreamHandler) error {
	if transport == nil || handler == nil || maxActive < 3 {
		return errors.New("http3: invalid incoming dispatcher configuration")
	}
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(context.Canceled)
	incoming := NewIncoming(peerIsServer)
	slots := make(chan struct{}, maxActive)
	var workers sync.WaitGroup
	var failed sync.Once
	fail := func(err error) {
		failed.Do(func() {
			if ctx.Err() != nil {
				return
			}
			// A connection close waking a critical reader is not an HTTP/3
			// critical-stream violation. Preserve its authenticated peer code.
			var peerClose *quic.PeerCloseError
			if errors.As(err, &peerClose) || errors.Is(err, quic.Err0RTTRejected) {
				cancel(err)
				return
			}
			code := InternalError
			var protocol *http3wire.Error
			if errors.As(err, &protocol) {
				code = protocol.Code
			}
			cancel(err)
			transport.Abort(code, "incoming HTTP/3 stream failed")
		})
	}
	for {
		stream, err := transport.AcceptUni(ctx)
		if err != nil {
			cancel(err)
			break
		}
		if ctx.Err() != nil {
			break
		}
		select {
		case slots <- struct{}{}:
		default:
			fail(&http3wire.Error{Code: http3wire.ExcessiveLoad, Reason: "incoming stream worker limit"})
			// The just-accepted stream has no worker. The connection abort releases it.
			break
		}
		if ctx.Err() != nil {
			break
		}
		workers.Add(1)
		go func(stream ReceiveStream) {
			defer workers.Done()
			defer func() { <-slots }()
			typ, known, err := incoming.Classify(ctx, stream)
			if err != nil {
				if ctx.Err() == nil {
					fail(err)
				}
				return
			}
			if !known || ctx.Err() != nil {
				return
			}
			done := make(chan struct{})
			stop := context.AfterFunc(ctx, func() { defer close(done); stream.SetReadDeadline(time.Now()) })
			err = handler(ctx, typ, stream)
			if !stop() {
				<-done
			}
			stream.SetReadDeadline(time.Time{})
			if ctx.Err() != nil {
				return
			}
			if err == nil && typ != http3wire.UniPush {
				err = io.EOF
			}
			err = incoming.ReadError(stream.StreamID(), err)
			if typ == http3wire.UniPush && peerStreamClosed(err) {
				return
			}
			if err != nil {
				fail(err)
			}
		}(stream)
	}
	workers.Wait()
	return context.Cause(ctx)
}
