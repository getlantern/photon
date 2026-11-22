package http3conn

import (
	"context"
	"fmt"
	quic "github.com/getlantern/photon"
	"io"
	"sync"
	"sync/atomic"
)

// ServerTunnel owns CONNECT DATA framing while its handler remains active.
// Reads and writes may run concurrently; Close interrupts and joins both.
// The handler must join its workers before returning. CloseWrite sends FIN while
// permitting reads; handler return also finishes sending unless Close aborted it.
type ServerTunnel struct {
	mapError        handlerErrorMap
	datagrams       *DatagramStream
	body            *serverRequestBody
	writer          *MessageWriter
	task            *requestTask
	readMu, writeMu sync.Mutex
	closed, sentFIN atomic.Bool
	closeOnce       sync.Once
	closeErr        error
}

func (t *ServerTunnel) Read(p []byte) (int, error) {
	t.readMu.Lock()
	defer t.readMu.Unlock()
	if t.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	return t.body.Read(p)
}
func (t *ServerTunnel) Write(p []byte) (n int, err error) {
	defer func() { err = t.mapError.apply(err) }()
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	if t.closed.Load() || t.sentFIN.Load() {
		return 0, io.ErrClosedPipe
	}
	n, err = t.writer.Write(p)
	if err != nil && !t.closed.Load() {
		t.task.fail(err, InternalError)
	}
	return n, err
}
func (t *ServerTunnel) CloseWrite() (err error) {
	defer func() { err = t.mapError.apply(err) }()
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	if t.closed.Load() {
		return io.ErrClosedPipe
	}
	err = t.writer.Close()
	if err == nil {
		t.sentFIN.Store(true)
	} else if !t.closed.Load() {
		t.task.fail(err, InternalError)
	}
	return err
}
func (t *ServerTunnel) Close() error { return t.closeWithError(0, "", false) }
func (t *ServerTunnel) closeWithError(code quic.StreamErrorCode, reason string, abort bool) error {
	t.closeOnce.Do(func() {
		if abort {
			t.task.fail(fmt.Errorf("http3: tunnel aborted (%s): %w", reason, &quic.StreamError{StreamID: t.task.stream.StreamID(), ErrorCode: code}), uint64(code))
		}
		t.closed.Store(true)
		if t.datagrams != nil {
			t.datagrams.DetachDatagrams()
		}
		if !abort && !t.sentFIN.Load() {
			t.task.stream.CancelWrite(0x10c)
		}
		t.closeErr = t.body.Close()
		t.readMu.Lock()
		t.readMu.Unlock()
		t.writeMu.Lock()
		t.writeMu.Unlock()
	})
	return t.mapError.apply(t.closeErr)
}

// SendDatagram queues an extension payload without its quarter-stream ID.
func (t *ServerTunnel) SendDatagram(ctx context.Context, payload []byte) (err error) {
	defer func() { err = t.mapError.apply(err) }()
	if t.task.ctx.Err() != nil {
		return context.Cause(t.task.ctx)
	}
	if t.datagrams == nil {
		return ErrHTTPDatagramsNotNegotiated
	}
	return t.datagrams.SendDatagram(ctx, payload)
}

// ReceiveDatagram waits for a bounded queued extension payload.
func (t *ServerTunnel) ReceiveDatagram(ctx context.Context) (payload []byte, err error) {
	defer func() { err = t.mapError.apply(err) }()
	if t.task.ctx.Err() != nil {
		return nil, context.Cause(t.task.ctx)
	}
	if t.datagrams == nil {
		return nil, ErrHTTPDatagramsNotNegotiated
	}
	return t.datagrams.ReceiveDatagram(ctx)
}

// CloseWithError aborts both request directions with an extension-selected stream
// code, cancels the handler context and joins tunnel I/O. It never closes the QUIC
// connection. The first request failure determines the code; callers must still
// join their workers before returning from the handler. Once Close or
// CloseWithError completes, later calls preserve that outcome.
func (t *ServerTunnel) CloseWithError(code quic.StreamErrorCode, reason string) error {
	return t.closeWithError(code, reason, true)
}
