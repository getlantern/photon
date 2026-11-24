package quic

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	engineStream "github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/transport"
)

type StreamID = transport.StreamID
type StreamErrorCode = transport.StreamErrorCode
type StreamError = transport.StreamError

// Err0RTTRejected means the peer rejected early data. The associated stream
// handle remains invalid even if a later stream reuses its numeric ID.
var Err0RTTRejected = transport.ErrEarlyDataRejected

// streamDeadline permits changing the deadline of an already blocked operation.
// One operation runs per direction, but senders waiting for their turn are
// bound by the same deadline, so every blocked caller registers a cancellation.
type streamDeadline struct {
	mu         sync.Mutex
	at         time.Time
	generation uint64
	waiters    uint64
	cancels    map[uint64]context.CancelFunc
}

func (d *streamDeadline) set(at time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.at = at
	d.generation++
	for _, cancel := range d.cancels {
		cancel()
	}
}
func (d *streamDeadline) expired() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return !d.at.IsZero() && !time.Now().Before(d.at)
}
func (d *streamDeadline) run(fn func(context.Context) error) error {
	for {
		d.mu.Lock()
		generation := d.generation
		var ctx context.Context
		var cancel context.CancelFunc
		if d.at.IsZero() {
			ctx, cancel = context.WithCancel(context.Background())
		} else {
			ctx, cancel = context.WithDeadline(context.Background(), d.at)
		}
		if d.cancels == nil {
			d.cancels = make(map[uint64]context.CancelFunc)
		}
		d.waiters++
		waiter := d.waiters
		d.cancels[waiter] = cancel
		d.mu.Unlock()
		err := fn(ctx)
		cancel()
		d.mu.Lock()
		changed := generation != d.generation
		delete(d.cancels, waiter)
		d.mu.Unlock()
		// Only an uncommitted operation may be retried. Runtime.Wait returns the
		// actual callback result if cancellation races a committed operation.
		if changed && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			continue
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return os.ErrDeadlineExceeded
		}
		return err
	}
}

type streamHandle struct {
	rejected       atomic.Bool
	conn           *Conn
	id             uint64
	released       bool            // owner goroutine only
	readTerminal   error           // owner goroutine only
	receiveContext context.Context // immutable after construction
	sendContext    context.Context // immutable after construction
	sendTerminal   error           // owner goroutine only
	sendAbandoned  error           // owner goroutine only
	finalSize      uint64          // owner / stoppedReadsMu only
	finalKnown     bool
	finalCallback  func(int64)
}

// markReleased runs on the connection owner before stream IDs can be reused.
// FIN may already have closed the send context normally, but that is not proof
// of delivery. Rejection must override the retained delivery result.
func (h *streamHandle) markReleased(terminal error) {
	h.released, h.readTerminal = true, terminal
	if errors.Is(terminal, Err0RTTRejected) {
		h.finalCallback = nil
		h.sendTerminal = terminal
		h.rejected.Store(true)
	}
}

func (h *streamHandle) markReleasedWithSendError(readErr, sendErr error) {
	h.markReleased(readErr)
	if h.rejected.Load() {
		return
	}
	// The engine releases a live stream only once its send side completes. After
	// connection failure it releases retained receive storage alone, which says
	// nothing about delivery: the send side ended with the connection.
	if h.sendContext != nil {
		h.sendAbandoned = context.Cause(h.conn.ctx)
		if sendErr == nil {
			sendErr = h.sendAbandoned
		}
	}
	if sendErr != nil {
		h.sendTerminal = sendErr
	}
}

func (h *streamHandle) StreamID() StreamID { return StreamID(h.id) }

// Release waits until received data has been consumed and all sent data or reset
// has been acknowledged, then relinquishes engine storage and stream credit.
// Cleanup is automatic; this optional method waits for it to finish. It does
// not wait for credit withheld by Config.RequireStreamCreditRelease.
// Finish or cancel both directions before calling Release.
func (h *streamHandle) Release(ctx context.Context) error {
	if h.rejected.Load() {
		return Err0RTTRejected
	}
	return h.conn.waitReceive(ctx, func() (bool, error) {
		if h.released {
			if h.rejected.Load() {
				return true, Err0RTTRejected
			}
			return true, h.sendAbandoned
		}
		err := h.conn.raw.ReleaseStream(h.id)
		return !errors.Is(err, engineStream.ErrWouldBlock), errUnlessBlocked(err)
	})
}
func errUnlessBlocked(err error) error {
	if errors.Is(err, engineStream.ErrWouldBlock) {
		return nil
	}
	return err
}

// ReceiveStream provides a serialized reader with independently mutable deadlines.
type ReceiveStream struct {
	*streamHandle
	mu       sync.Mutex
	deadline streamDeadline
}

func (s *ReceiveStream) Read(p []byte) (int, error) {
	return s.read(p, false)
}

// Peek fills p without consuming stream data or granting more receive credit.
// It waits for len(p) bytes or a terminal error and respects SetReadDeadline.
// A short terminal prefix is returned with EOF or the stream cancellation error.
func (s *ReceiveStream) Peek(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return s.read(p, true)
}

func (s *ReceiveStream) read(p []byte, peek bool) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rejected.Load() {
		return 0, Err0RTTRejected
	}
	if len(p) == 0 {
		return 0, nil
	}
	var n int
	err := s.deadline.run(func(ctx context.Context) error {
		return s.conn.waitReceive(ctx, func() (bool, error) {
			var err error
			if s.released {
				return true, s.readTerminal
			}
			if peek {
				n, err = s.conn.raw.PeekStream(s.id, p)
			} else {
				n, err = s.conn.raw.ReadStream(s.id, p)
			}
			return !errors.Is(err, engineStream.ErrWouldBlock), errUnlessBlocked(err)
		})
	})
	return n, err
}
func (s *ReceiveStream) SetReadDeadline(t time.Time) error { s.deadline.set(t); return nil }
func (s *ReceiveStream) CancelRead(code StreamErrorCode) {
	_ = s.conn.waitReceive(context.Background(), func() (bool, error) {
		if s.released {
			return true, nil
		}
		return true, s.conn.raw.CancelRead(s.id, uint64(code))
	})
}

// ReleaseStreamCredit allows the peer to open a new stream in place of this one.
// It only has an effect if Config.RequireStreamCreditRelease is set. The peer
// receives the credit once the stream's storage has been reclaimed (see Release)
// and ReleaseStreamCredit has been called, in either order. Repeated calls do
// nothing.
func (s *ReceiveStream) ReleaseStreamCredit() {
	if s.rejected.Load() {
		return
	}
	_ = s.conn.server.Do(context.Background(), func(*transport.ServerEndpoint) error {
		return s.conn.raw.ReleaseStreamCredit(s.id)
	})
}

// SendStream serializes writes, retaining byte order across blocked flow control.
// Close queues FIN after previous writes; it does not close the receive side.
type SendStream struct {
	*streamHandle
	gate     streamGate
	deadline streamDeadline
	closed   bool
}

// streamGate is a mutex whose waiters can give up: a blocked Write holds it
// indefinitely, and WaitAcknowledged holds it under an unrelated context.
type streamGate chan struct{}

func (g streamGate) tryAcquire() bool {
	select {
	case g <- struct{}{}:
		return true
	default:
		return false
	}
}

// acquire prefers an uncontended gate over a finished context, so expiry
// alone never preempts the stream's own terminal errors.
func (g streamGate) acquire(ctx context.Context) error {
	if g.tryAcquire() {
		return nil
	}
	select {
	case g <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (g streamGate) release() { <-g }

func newSendStream(h *streamHandle) *SendStream {
	return &SendStream{streamHandle: h, gate: make(streamGate, 1)}
}

func (s *SendStream) Write(p []byte) (int, error) {
	return s.WriteWithLimit(p, nil)
}

// TryWriteAll queues all of p or returns ErrWouldBlock without queuing any of it.
// It does not wait for QUIC credit, queue capacity or another writer, and ignores
// the write deadline. It can wait briefly for the connection owner to run.
func (s *SendStream) TryWriteAll(p []byte) error {
	if !s.gate.tryAcquire() {
		return ErrWouldBlock
	}
	defer s.gate.release()
	if s.rejected.Load() {
		return Err0RTTRejected
	}
	if s.closed {
		return ErrStreamClosed
	}
	err := s.conn.server.Do(context.Background(), func(*transport.ServerEndpoint) error {
		if s.released {
			if s.sendTerminal != nil && s.sendTerminal != context.Canceled {
				return s.sendTerminal
			}
			return ErrStreamClosed
		}
		return s.conn.raw.TryWriteStreamAll(s.id, p)
	})
	if errors.Is(err, engineStream.ErrWouldBlock) {
		return ErrWouldBlock
	}
	return s.conn.connectionError(err)
}

// WriteWithLimit applies external credit to each new STREAM frame after QUIC
// flow control. The limiter receives a positive maximum and its result is
// clamped to [0, maximum]. A reduced allowance returns the committed prefix and
// ErrWriteLimitReached. Retransmissions never invoke the limiter again.
// The callback runs on the connection owner: it must be concurrency-safe,
// must not block, and must not call QUIC methods. A nil limiter behaves as Write.
func (s *SendStream) WriteWithLimit(p []byte, limiter func(int) int) (int, error) {
	if err := s.deadline.run(s.gate.acquire); err != nil {
		return 0, s.preferEnded(err)
	}
	defer s.gate.release()
	if s.rejected.Load() {
		return 0, Err0RTTRejected
	}
	if s.closed {
		return 0, ErrStreamClosed
	}
	total := 0
	if len(p) == 0 {
		// Nothing to send, but the call reports what a write with data would:
		// why sending ended if it has, else an expired deadline.
		if err := s.sendEnded(); err != nil {
			return 0, err
		}
		if s.deadline.expired() {
			return 0, os.ErrDeadlineExceeded
		}
		return 0, nil
	}
	// Keep one deadline context across partial commits. If the deadline changes,
	// run resumes at total, preserving bytes already accepted by the owner.
	err := s.deadline.run(func(ctx context.Context) error {
		for total < len(p) {
			var n int
			err := s.conn.server.Wait(ctx, func(*transport.ServerEndpoint) (bool, error) {
				var err error
				if s.released {
					if s.sendTerminal != nil && s.sendTerminal != context.Canceled {
						return true, s.sendTerminal
					}
					return true, ErrStreamClosed
				}
				// Bound each owner turn so packet processing, cancellation and
				// other streams can run between batches. Report any committed
				// bytes as progress even when the next fragment would block.
				limit := min(len(p)-total, 32*1024)
				for n < limit {
					var wrote int
					wrote, err = s.conn.raw.WriteStreamWithLimit(s.id, p[total+n:total+limit], limiter)
					n += wrote
					if errors.Is(err, engineStream.ErrWouldBlock) {
						return n > 0, nil
					}
					if err != nil {
						return true, err
					}
					if wrote == 0 {
						return n > 0, nil
					}
				}
				return true, nil
			})
			total += n
			if err != nil {
				return err
			}
		}
		return nil
	})
	return total, s.preferEnded(s.conn.connectionError(err))
}

// sendEnded reports why the send direction can no longer be written, or nil
// while it can. It waits only briefly, for the connection owner, and not on
// the write deadline: an expired deadline must not hide why sending ended.
// Once the endpoint has closed, the owner reports that at once and
// connectionError turns it into the connection's own cause.
func (s *SendStream) sendEnded() error {
	err := s.conn.server.Do(context.Background(), func(*transport.ServerEndpoint) error {
		if s.released {
			if s.sendTerminal != nil && s.sendTerminal != context.Canceled {
				return s.sendTerminal
			}
			return ErrStreamClosed
		}
		return s.conn.raw.SendError(s.id)
	})
	return s.conn.connectionError(err)
}

// preferEnded replaces an expired write deadline with the reason sending
// ended, when it has: a reset or a closed connection is not a timeout, and a
// caller must not be told the call may be retried.
func (s *SendStream) preferEnded(err error) error {
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		return err
	}
	if ended := s.sendEnded(); ended != nil {
		return ended
	}
	return err
}
func (s *SendStream) Close() error {
	if err := s.deadline.run(s.gate.acquire); err != nil {
		return s.preferEnded(err)
	}
	defer s.gate.release()
	if s.rejected.Load() {
		return Err0RTTRejected
	}
	if s.closed {
		return nil
	}
	err := s.deadline.run(func(ctx context.Context) error {
		return s.conn.server.Wait(ctx, func(*transport.ServerEndpoint) (bool, error) {
			if s.released {
				if s.sendTerminal != nil && s.sendTerminal != context.Canceled {
					return true, s.sendTerminal
				}
				return true, ErrStreamClosed
			}
			_, err := s.conn.raw.WriteStream(s.id, nil, true)
			return !errors.Is(err, engineStream.ErrWouldBlock), errUnlessBlocked(err)
		})
	})
	if err == nil {
		s.closed = true
	}
	return s.preferEnded(s.conn.connectionError(err))
}

// WaitAcknowledged waits for delivery of all previously committed bytes and FIN
// (if closed), without closing the stream. It serializes with Write and Close;
// use its context to bound the wait. Write deadlines do not control this wait.
// A reset or connection failure is returned rather than treated as delivery.
func (s *SendStream) WaitAcknowledged(ctx context.Context) error {
	if err := s.gate.acquire(ctx); err != nil {
		return err
	}
	defer s.gate.release()
	if s.rejected.Load() {
		return Err0RTTRejected
	}
	err := s.conn.server.Wait(ctx, func(*transport.ServerEndpoint) (bool, error) {
		if s.released {
			if s.sendTerminal != nil && s.sendTerminal != context.Canceled {
				return true, s.sendTerminal
			}
			return true, nil
		}
		err := s.conn.raw.StreamAcknowledged(s.id)
		return !errors.Is(err, engineStream.ErrWouldBlock), errUnlessBlocked(err)
	})
	return s.conn.connectionError(err)
}

// SetWriteDeadline bounds Write and Close, including ones already waiting. A
// call past the deadline returns os.ErrDeadlineExceeded unless sending has
// already ended (CancelWrite, the peer's STOP_SENDING, or the connection
// closing); then it returns that cause, which is not a timeout to retry.
func (s *SendStream) SetWriteDeadline(t time.Time) error { s.deadline.set(t); return nil }

// SetPriority sets scheduling urgency, clipped to 0–7 (zero is highest).
// Incremental streams share turns; non-incremental streams use ascending IDs.
// The default is urgency 3 and incremental true. Retransmissions precede new
// stream data. Changes affect queued and future data, not sealed packets.
func (s *SendStream) SetPriority(urgency int8, incremental bool) {
	_ = s.conn.server.Do(context.Background(), func(*transport.ServerEndpoint) error {
		if !s.released && !s.rejected.Load() {
			s.conn.raw.SetStreamPriority(s.id, urgency, incremental)
		}
		return nil
	})
}

// SetReliableBoundary marks bytes committed so far for reliable delivery if a
// later CancelWrite uses partial reset. Repeated calls advance the boundary.
// It has no effect when the peer does not support partial stream reset.
func (s *SendStream) SetReliableBoundary() {
	_ = s.conn.server.Do(context.Background(), func(*transport.ServerEndpoint) error {
		if s.released || s.rejected.Load() {
			return nil
		}
		return s.conn.raw.SetReliableBoundary(s.id)
	})
}

func (s *SendStream) CancelWrite(code StreamErrorCode) {
	_ = s.conn.server.Do(context.Background(), func(*transport.ServerEndpoint) error {
		if s.released {
			return nil
		}
		return s.conn.raw.CancelWriteReliable(s.id, uint64(code))
	})
}

// Stream is a bidirectional stream. One reader and one writer can progress
// concurrently. Engine storage and peer credit are reclaimed automatically once
// received data is consumed and sent data or reset is acknowledged; with
// Config.RequireStreamCreditRelease the credit also waits for ReleaseStreamCredit.
type Stream struct {
	*ReceiveStream
	*SendStream
}

func (s *Stream) StreamID() StreamID { return s.ReceiveStream.StreamID() }

// ReleaseStreamCredit behaves as ReceiveStream.ReleaseStreamCredit, once both
// directions have completed. For streams opened locally it does nothing.
func (s *Stream) ReleaseStreamCredit()              { s.ReceiveStream.ReleaseStreamCredit() }
func (s *Stream) Release(ctx context.Context) error { return s.ReceiveStream.Release(ctx) }
func (s *Stream) SetDeadline(t time.Time) error {
	s.ReceiveStream.SetReadDeadline(t)
	return s.SendStream.SetWriteDeadline(t)
}
func newStream(h *streamHandle) *Stream {
	return &Stream{ReceiveStream: &ReceiveStream{streamHandle: h}, SendStream: newSendStream(h)}
}

func (c *Conn) stream(ctx context.Context, uni, accept, wait bool) (*streamHandle, error) {
	var id uint64
	var handle *streamHandle
	operation := func(*transport.ServerEndpoint) (bool, error) {
		if c.raw.EarlyDataRejected() && !c.earlyStreamsRecovered {
			return true, Err0RTTRejected
		}
		var err error
		if accept {
			id, err = c.raw.AcceptStream(uni)
		} else {
			id, err = c.raw.OpenStream(uni)
		}
		if wait && errors.Is(err, engineStream.ErrWouldBlock) {
			return false, nil
		}
		if !accept && !wait && errors.Is(err, engineStream.ErrWouldBlock) {
			return true, &StreamLimitReachedError{}
		}
		if err == nil {
			handle = &streamHandle{conn: c, id: id}
			var cancel, cancelReceive context.CancelCauseFunc
			if !uni || accept {
				handle.receiveContext, cancelReceive = context.WithCancelCause(c.ctx)
				err = c.raw.WatchReceiveClose(id, cancelReceive)
				if err != nil {
					cancelReceive(err)
					return true, err
				}
				err = c.raw.WatchReceiveFinalSize(id, func(size uint64) {
					handle.finalSize, handle.finalKnown = size, true
					callback := handle.finalCallback
					handle.finalCallback = nil
					if callback != nil {
						callback(int64(size))
					}
				})
				if err != nil {
					cancelReceive(err)
					return true, err
				}
			}
			if !uni || !accept {
				handle.sendContext, cancel = context.WithCancelCause(c.ctx)
				err = c.raw.WatchSendClose(id, func(cause error) {
					handle.sendTerminal = cause
					// The handle keeps the engine's normal-close marker; the
					// public Context reports the quic-go cause.
					if cause == context.Canceled {
						cause = ErrStreamClosed
					}
					cancel(cause)
				})
				if err != nil {
					cancel(err)
					if cancelReceive != nil {
						cancelReceive(err)
					}
					return true, err
				}
			}
			err = c.raw.AutoReleaseStreamWithSendError(id, handle.markReleasedWithSendError)
			if err != nil {
				if cancel != nil {
					cancel(err)
				}
				if cancelReceive != nil {
					cancelReceive(err)
				}
			}
		}
		return true, err
	}
	var err error
	if wait {
		err = c.server.Wait(ctx, operation)
	} else {
		err = c.server.Do(ctx, func(e *transport.ServerEndpoint) error { _, err := operation(e); return err })
	}
	if err != nil {
		return nil, c.connectionError(err)
	}
	return handle, nil
}
func (c *Conn) AcceptStream(ctx context.Context) (*Stream, error) {
	h, err := c.stream(ctx, false, true, true)
	if err != nil {
		return nil, err
	}
	return newStream(h), nil
}
func (c *Conn) OpenStream() (*Stream, error) {
	h, err := c.stream(context.Background(), false, false, false)
	if err != nil {
		return nil, err
	}
	return newStream(h), nil
}
func (c *Conn) OpenStreamSync(ctx context.Context) (*Stream, error) {
	h, err := c.stream(ctx, false, false, true)
	if err != nil {
		return nil, err
	}
	return newStream(h), nil
}
func (c *Conn) AcceptUniStream(ctx context.Context) (*ReceiveStream, error) {
	h, err := c.stream(ctx, true, true, true)
	if err != nil {
		return nil, err
	}
	return &ReceiveStream{streamHandle: h}, nil
}
func (c *Conn) OpenUniStream() (*SendStream, error) {
	h, err := c.stream(context.Background(), true, false, false)
	if err != nil {
		return nil, err
	}
	return newSendStream(h), nil
}
func (c *Conn) OpenUniStreamSync(ctx context.Context) (*SendStream, error) {
	h, err := c.stream(ctx, true, false, true)
	if err != nil {
		return nil, err
	}
	return newSendStream(h), nil
}

// Context is canceled when Close queues FIN, CancelWrite resets the stream,
// the peer stops receiving, or the connection terminates. Normal Close uses
// ErrStreamClosed as its cause. This does not wait for delivery or peer EOF.
func (s *SendStream) Context() context.Context { return s.sendContext }

// Completed receive storage outlives the socket owner. Ownership transfers only
// after Done closes, which joins all endpoint cleanup. Serialize all such reads,
// cancellation and release callbacks across streams on the same connection.
func (c *Conn) waitReceive(ctx context.Context, run func() (bool, error)) error {
	err := c.server.Wait(ctx, func(*transport.ServerEndpoint) (bool, error) { return run() })
	if !errors.Is(err, transport.ErrEndpointClosed) {
		return err
	}
	select {
	case <-c.server.Done():
	case <-ctx.Done():
		return ctx.Err()
	}
	c.stoppedReadsMu.Lock()
	defer c.stoppedReadsMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	ready, err := run()
	if !ready && err == nil {
		return transport.ErrEndpointClosed
	}
	return err
}

// ReceiveContext is canceled when all bytes through FIN arrive, the peer resets
// the stream, CancelRead is called, or the connection terminates. Normal FIN uses
// io.EOF as its cause. Buffered stream data remains readable after FIN completion;
// this context does not indicate that the application has consumed it.
func (s *ReceiveStream) ReceiveContext() context.Context { return s.receiveContext }
