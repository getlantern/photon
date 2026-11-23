package http3conn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

// BodyConfig connects a streaming body to endpoint policy. Trailers runs on the
// reading goroutine; nil discards incoming trailers. Push must consume and validate
// an entire PUSH_PROMISE payload; nil rejects pushes. Close runs exactly once and
// must interrupt source I/O, cancel abandoned QPACK work, and release ownership.
// Its complete argument is true only after clean, semantically valid message FIN.
// Close may run concurrently with Read and must not mutate MessageReader directly.
type BodyConfig struct {
	Trailers func([]qpack.DecodedField) error
	Push     func(context.Context, MessageEvent, io.Reader) error
	Close    func(complete bool) error
}

// MessageBody exposes DATA across frames as io.ReadCloser. Construct immediately
// after reading final/initial HEADERS, then transfer exclusive reader ownership.
// One Read may run at a time; Close is concurrent-safe and interrupts QPACK waits.
// The endpoint's Close callback/deadlines must interrupt transport reads. Callers
// must Close even after EOF or error. This adapter does not choose reset codes,
// merge trailers into headers, perform retries, or close the QUIC connection.
// A Read that hits an expired stream deadline can be repeated.
type MessageBody struct {
	reader           *MessageReader
	ctx              context.Context
	cancel           context.CancelFunc
	config           BodyConfig
	data, skipping   bool
	err              error // reading goroutine only
	complete, closed atomic.Bool
	closeOnce        sync.Once
	closeErr         error
}

func NewMessageBody(ctx context.Context, reader *MessageReader, config BodyConfig) (*MessageBody, error) {
	if ctx == nil || reader == nil || config.Close == nil || reader.err != nil || reader.message.body == nil || reader.message.trailers || reader.message.finished || reader.data {
		return nil, fmt.Errorf("http3: body requires reader positioned after final headers and cleanup callback")
	}
	ctx, cancel := context.WithCancel(ctx)
	return &MessageBody{reader: reader, ctx: ctx, cancel: cancel, config: config}, nil
}
func (b *MessageBody) Read(p []byte) (int, error) {
	if b.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	if b.err != nil {
		return 0, b.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if err := b.ctx.Err(); err != nil {
			return 0, b.fail(err)
		}
		if b.data {
			n, err := b.reader.Read(p)
			if err == io.EOF {
				b.data = false
				if n > 0 {
					return n, nil
				}
			} else {
				if h3.IsDeadline(err) {
					return n, err
				}
				if err != nil {
					return n, b.fail(err)
				}
				if n > 0 {
					return n, nil
				}
				// Preserve io.Reader no-progress behavior instead of spinning.
				return 0, nil
			}
		}
		if b.skipping {
			if _, err := io.Copy(io.Discard, b.reader); err != nil {
				if h3.IsDeadline(err) {
					return 0, err
				}
				return 0, b.fail(err)
			}
			b.skipping = false
		}
		event, err := b.reader.Next(b.ctx)
		if h3.IsDeadline(err) {
			return 0, err
		}
		if err != nil {
			if err == io.EOF {
				b.complete.Store(true)
			}
			return 0, b.fail(err)
		}
		switch event.Header.Type {
		case h3.Data:
			b.data = true
		case h3.Headers:
			if event.Section != TrailerSection {
				return 0, b.fail(&h3.Error{Code: h3.FrameUnexpected, Reason: "non-trailer HEADERS in body"})
			}
			if b.config.Trailers != nil {
				if err := b.config.Trailers(event.Fields); err != nil {
					return 0, b.fail(err)
				}
			}
		case h3.PushPromise:
			if b.config.Push == nil {
				return 0, b.fail(&h3.Error{Code: h3.IDError, Reason: "push promise without permission"})
			}
			if err := b.config.Push(b.ctx, event, b.reader); err != nil {
				return 0, b.fail(err)
			}
		default:
			b.skipping = true
		}
	}
}
func (b *MessageBody) Close() error {
	b.closeOnce.Do(func() { b.closed.Store(true); b.cancel(); b.closeErr = b.config.Close(b.complete.Load()) })
	return b.closeErr
}
func (b *MessageBody) fail(err error) error {
	// QPACK waits return ctx.Err(); retain the request/session's richer cause.
	// Do not relabel unrelated wire, message or transport failures as rejection.
	if canceled := b.ctx.Err(); canceled != nil && errors.Is(err, canceled) {
		err = context.Cause(b.ctx)
	}
	b.err = err
	return err
}
