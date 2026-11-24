package http3conn

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/transport"
	"github.com/getlantern/photon/internal/wire"
)

// RequestAcceptor must interrupt AcceptRequest when ctx is canceled. The transport
// enforces stream identity and flow-control limits for requests not yet accepted.
type RequestAcceptor interface {
	AcceptRequest(context.Context) (ServerRequestStream, error)
}

func (t QUICTransport) AcceptRequest(ctx context.Context) (ServerRequestStream, error) {
	stream, err := t.Conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	// HTTP/3 handles the stream, so the peer may replace it once it completes.
	stream.ReleaseStreamCredit()
	return stream, nil
}

// ServeRequestsConfig bounds all accepted tasks, including header/QPACK waits and
// error reporting. Request's Encoder, Decoder and Abort are assigned by Session.
// Report runs on the request goroutine and must return when ctx is canceled. It
// must not synchronously join the server. A nil Report discards request errors.
type ServeRequestsConfig struct {
	// Closing Drain stops admission, sends GOAWAY, and waits for acknowledged
	// responses before returning nil. Parent cancellation bounds the drain.
	Drain <-chan struct{}
	// IdleTimeout, if positive, closes the connection with H3_NO_ERROR once no
	// request stream has been active for that long; ServeRequests returns nil.
	IdleTimeout time.Duration
	// DrainGrace, if positive, bounds how long a drained connection waits for
	// its client to close before closing it with H3_NO_ERROR, so a client that
	// never does cannot hold it. Zero waits for the connection to end.
	DrainGrace time.Duration
	MaxActive  int
	Request    ServeRequestConfig
	Report     func(ctx context.Context, id uint64, err error)
}

// ServeRequests runs the server session's sole request accept loop. It applies
// backpressure before accepting another stream, so blocked requests cannot spawn
// unbounded goroutines. Request-scoped failures leave other requests running.
// On termination it cancels the session, closes the connection, interrupts shared
// QPACK writes and joins every accepted task. The owner must subsequently Stop
// the session to join its critical-stream tasks. Drain is graceful; cancellation
// is abortive. A peer-initiated close remains a transport error (including a
// normal application close) rather than proving acknowledgment of queued data,
// except after a drain's GOAWAY: then the client's H3_NO_ERROR close once no
// request is running is how the drain completes, and ServeRequests returns nil.
// This loop exclusively owns server GOAWAY and its cutoff while
// running; callers must not concurrently call SendGoaway.
func (s *Session) ServeRequests(parent context.Context, acceptor RequestAcceptor, config ServeRequestsConfig) (result error) {
	if parent == nil || acceptor == nil || !s.server || config.MaxActive < 1 || config.Request.Handler == nil || config.Request.MaxEncodedHeaders == 0 || config.Request.MaxEncodedHeaders > uint64(^uint(0)>>1) {
		return fmt.Errorf("http3: invalid server request loop configuration")
	}
	control, canDrain := s.control.(interface{ WaitAcknowledged(context.Context) error })
	if config.Drain != nil && !canDrain {
		return fmt.Errorf("http3: graceful drain requires control delivery barrier")
	}
	s.requestsMu.Lock()
	if s.servingRequests {
		s.requestsMu.Unlock()
		return fmt.Errorf("http3: request loop already started")
	}
	s.servingRequests = true
	s.requestsMu.Unlock()
	ctx, cancel := context.WithCancelCause(s.ctx)
	interrupted := make(chan struct{})
	stop := context.AfterFunc(parent, func() { defer close(interrupted); cancel(context.Cause(parent)); s.cancel(context.Cause(parent)) })
	if parent.Err() != nil {
		cancel(context.Cause(parent))
		s.cancel(context.Cause(parent))
	}
	var tasks sync.WaitGroup
	var cutShort atomic.Bool // a request failed because the connection ended
	var abortOnce sync.Once
	abort := func(code uint64, reason string) { abortOnce.Do(func() { s.transport.Abort(code, reason) }) }
	drained := false // a graceful drain completed; how the connection then ended does not matter
	defer func() {
		if ctx.Err() != nil && !drained {
			result = context.Cause(ctx)
		}
		cancel(result)
		s.cancel(result) // interrupts critical writers before waiting for request cleanup
		code := uint64(0x100)
		var protocol *h3.Error
		if errors.As(result, &protocol) {
			code = protocol.Code
		}
		abort(code, "HTTP3 request loop stopped")
		if !stop() {
			<-interrupted
		}
		tasks.Wait()
	}()
	acceptCtx, stopAccept := context.WithCancel(ctx)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-config.Drain:
			stopAccept()
		case <-acceptCtx.Done():
		}
	}()
	defer func() { stopAccept(); <-watchDone }()
	// Observe an already-closed drain synchronously, before waiting for settings.
	select {
	case <-config.Drain:
		stopAccept()
	default:
	}
	var idle *idleTimer
	if config.IdleTimeout > 0 {
		idle = newIdleTimer(config.IdleTimeout, stopAccept)
		defer idle.timer.Stop()
	}
	nextID := uint64(0)
	drain := func() error {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if idle.hasExpired() {
			abort(0x100, "idle timeout")
			return nil
		}
		if err := s.SendGoaway(nextID); err != nil {
			return err
		}
		// No request starts after GOAWAY, and each returns once ctx ends.
		done := make(chan struct{})
		go func() { tasks.Wait(); close(done) }()
		// After GOAWAY the client closes once its requests are done (RFC 9114
		// section 5.2), so its H3_NO_ERROR close completes the drain, whenever
		// it arrives and whichever context ends first, provided no request was
		// cut short by the connection ending. Any other ending keeps its cause.
		ended := func() error {
			<-done
			cause := context.Cause(ctx)
			var peer *transport.PeerCloseError
			if !cutShort.Load() && errors.As(cause, &peer) && peer.Application && peer.Code == 0x100 {
				drained = true
				return nil
			}
			return cause
		}
		if err := control.WaitAcknowledged(ctx); err != nil {
			if ctx.Err() != nil {
				return ended()
			}
			return err
		}
		select {
		case <-done:
		case <-ctx.Done():
			return ended()
		}
		// Every request is done. Leave closing to the client, as quic-go's
		// server does: closing here can cut off response data the client has
		// received but not yet read, which some QUIC stacks discard on
		// CONNECTION_CLOSE. The connection ending, or Server.Close, ends this,
		// and so does the drain grace: then the deferred close ends it.
		var grace <-chan time.Time
		if config.DrainGrace > 0 {
			timer := time.NewTimer(config.DrainGrace)
			defer timer.Stop()
			grace = timer.C
		}
		select {
		case <-ctx.Done():
			return ended()
		case <-grace:
			drained = true
			return nil
		}
	}
	encoder, err := s.WaitEncoder(acceptCtx)
	if err != nil {
		if ctx.Err() == nil && acceptCtx.Err() != nil {
			return drain()
		}
		return err
	}
	request := config.Request
	request.ExtendedConnect = s.settings != nil && s.settings.CanReceiveExtendedConnect()
	request.Encoder, request.Decoder = encoder, s.decoder
	request.Abort = func(code uint64, reason string) {
		cause := &h3.Error{Code: code, Reason: reason}
		s.cancel(cause)
		abort(code, reason)
	}
	slots := make(chan struct{}, config.MaxActive)
	for {
		select {
		case slots <- struct{}{}:
		case <-acceptCtx.Done():
			return drain()
		}
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if acceptCtx.Err() != nil {
			return drain()
		}
		stream, err := acceptor.AcceptRequest(acceptCtx)
		if err != nil {
			if ctx.Err() == nil && acceptCtx.Err() != nil {
				return drain()
			}
			return err
		}
		if stream == nil {
			return fmt.Errorf("http3: acceptor returned nil request stream")
		}
		if uint64(stream.StreamID())%4 != 0 || uint64(stream.StreamID()) > wire.MaxVarint {
			return &h3.Error{Code: h3.StreamCreationError, Reason: "non-request stream in server request loop"}
		}
		select {
		case <-config.Drain:
			stopAccept()
		default:
		}
		// A stream can also arrive just as the idle period expires.
		if acceptCtx.Err() != nil || !idle.begin() {
			stream.CancelRead(0x10b)
			stream.CancelWrite(0x10b)
			if err := s.decoder.Cancel(uint64(stream.StreamID())); err != nil {
				return err
			}
			if err := stream.Release(ctx); err != nil {
				return err
			}
			return drain()
		}
		if s.datagrams != nil {
			raw, ok := stream.(DatagramTransportStream)
			if !ok {
				return fmt.Errorf("http3: datagram server requires stream completion contexts")
			}
			bound, err := s.BindPendingDatagramStream(raw)
			if err != nil {
				stream.CancelRead(0x10c)
				stream.CancelWrite(0x10c)
				return err
			}
			stream = bound
		}
		if request.Priorities != nil {
			if err := request.Priorities.register(stream); err != nil {
				return err
			}
		}
		nextID = max(nextID, uint64(stream.StreamID())+4)
		tasks.Add(1)
		go func() {
			defer tasks.Done()
			// Priority state belongs to the admission slot. Finish removing it
			// before another request can acquire that slot and register itself.
			defer func() { <-slots }()
			defer idle.end()
			if request.Priorities != nil {
				defer request.Priorities.finish(uint64(stream.StreamID()))
			}
			err := ServeRequest(ctx, stream, request)
			if delivered := stream.Release(ctx); delivered != nil {
				if ctx.Err() == nil {
					s.cancel(delivered)
				}
				if err == nil {
					err = delivered
				}
			}
			if err != nil && ctx.Err() != nil {
				cutShort.Store(true)
			}
			if err != nil && config.Report != nil {
				config.Report(ctx, uint64(stream.StreamID()), err)
			}
		}()
	}
}

// idleTimer expires once no request stream has been active for its period.
// A nil timer never expires.
type idleTimer struct {
	mu       sync.Mutex
	period   time.Duration
	timer    *time.Timer
	deadline time.Time // a callback before it lost a race with a new stream
	active   int
	expired  bool
}

func newIdleTimer(period time.Duration, expire func()) *idleTimer {
	t := &idleTimer{period: period, deadline: time.Now().Add(period)}
	t.timer = time.AfterFunc(period, func() { t.fire(expire) })
	return t
}
func (t *idleTimer) fire(expire func()) {
	t.mu.Lock()
	if t.active == 0 && !time.Now().Before(t.deadline) {
		t.expired = true
	}
	expired := t.expired
	t.mu.Unlock()
	if expired {
		expire()
	}
}

// begin counts a stream as active. It fails once the timer has expired.
func (t *idleTimer) begin() bool {
	if t == nil {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.expired {
		return false
	}
	t.active++
	t.timer.Stop()
	return true
}
func (t *idleTimer) end() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active--; t.active == 0 && !t.expired {
		t.deadline = time.Now().Add(t.period)
		t.timer.Reset(t.period)
	}
}
func (t *idleTimer) hasExpired() bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.expired
}
