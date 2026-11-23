package http3conn

import (
	"context"
	"errors"
	"fmt"
	"sync"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/wire"
)

// RequestAcceptor must interrupt AcceptRequest when ctx is canceled. The transport
// enforces stream identity and flow-control limits for requests not yet accepted.
type RequestAcceptor interface {
	AcceptRequest(context.Context) (ServerRequestStream, error)
}

func (t QUICTransport) AcceptRequest(ctx context.Context) (ServerRequestStream, error) {
	return t.Conn.AcceptStream(ctx)
}

// ServeRequestsConfig bounds all accepted tasks, including header/QPACK waits and
// error reporting. Request's Encoder, Decoder and Abort are assigned by Session.
// Report runs on the request goroutine and must return when ctx is canceled. It
// must not synchronously join the server. A nil Report discards request errors.
type ServeRequestsConfig struct {
	// Closing Drain stops admission, sends GOAWAY, and waits for acknowledged
	// responses before returning nil. Parent cancellation bounds the drain.
	Drain     <-chan struct{}
	MaxActive int
	Request   ServeRequestConfig
	Report    func(ctx context.Context, id uint64, err error)
}

// ServeRequests runs the server session's sole request accept loop. It applies
// backpressure before accepting another stream, so blocked requests cannot spawn
// unbounded goroutines. Request-scoped failures leave other requests running.
// On termination it cancels the session, closes the connection, interrupts shared
// QPACK writes and joins every accepted task. The owner must subsequently Stop
// the session to join its critical-stream tasks. Drain is graceful; cancellation
// is abortive. A peer-initiated close remains a transport error (including a
// normal application close) rather than proving acknowledgment of queued data.
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
	var abortOnce sync.Once
	abort := func(code uint64, reason string) { abortOnce.Do(func() { s.transport.Abort(code, reason) }) }
	defer func() {
		if ctx.Err() != nil {
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
	nextID := uint64(0)
	drain := func() error {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if err := s.SendGoaway(nextID); err != nil {
			return err
		}
		if err := control.WaitAcknowledged(ctx); err != nil {
			return err
		}
		done := make(chan struct{})
		go func() { tasks.Wait(); close(done) }()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
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
		if acceptCtx.Err() != nil {
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
			if err != nil && config.Report != nil {
				config.Report(ctx, uint64(stream.StreamID()), err)
			}
		}()
	}
}
