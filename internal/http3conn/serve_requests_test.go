package http3conn

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
)

type requestAcceptor struct {
	streams chan ServerRequestStream
	calls   atomic.Int32
}

func (a *requestAcceptor) AcceptRequest(ctx context.Context) (ServerRequestStream, error) {
	a.calls.Add(1)
	select {
	case stream := <-a.streams:
		return stream, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type numberedRequest struct {
	*serveStream
	id quic.StreamID
}

func (s *numberedRequest) StreamID() quic.StreamID { return s.id }
func requestLoopSession(t *testing.T, config ServeRequestConfig) (*Session, *dispatchTransport) {
	t.Helper()
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(context.Canceled) })
	transport := &dispatchTransport{}
	session := &Session{ctx: ctx, cancel: cancel, server: true, decoder: config.Decoder, transport: transport, ready: make(chan struct{})}
	session.encoder.Store(config.Encoder)
	close(session.ready)
	return session, transport
}
func requestStream(t *testing.T, id quic.StreamID) *numberedRequest {
	return &numberedRequest{serveStream: &serveStream{source: bytes.NewReader(readerHeaders(t, requestFields("/")...))}, id: id}
}
func TestServeRequestsBoundsAndJoins(t *testing.T) {
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	var active atomic.Int32
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		active.Add(1)
		defer active.Add(-1)
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	config.Priorities, _ = NewRequestPriorities(2)
	session, transport := requestLoopSession(t, config)
	acceptor := &requestAcceptor{streams: make(chan ServerRequestStream, 3)}
	for i := 0; i < 3; i++ {
		acceptor.streams <- requestStream(t, quic.StreamID(i*4))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- session.ServeRequests(ctx, acceptor, ServeRequestsConfig{MaxActive: 2, Request: config})
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("handler missing")
		}
	}
	if calls := acceptor.calls.Load(); calls != 2 {
		t.Fatal("accepted beyond task budget", calls)
	}
	release <- struct{}{}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("released slot not reused")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("tasks not joined")
	}
	if active.Load() != 0 {
		t.Fatal("handler outlived loop")
	}
	if len(config.Priorities.active) != 0 {
		t.Fatal("priority state outlived request tasks")
	}
	transport.check(t, 0x100)
	if err := session.ServeRequests(context.Background(), acceptor, ServeRequestsConfig{MaxActive: 1, Request: config}); err == nil {
		t.Fatal("request loop restarted")
	}
}
func TestServeRequestsStreamFailureContinues(t *testing.T) {
	handled := make(chan struct{}, 1)
	reported := make(chan error, 1)
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handled <- struct{}{}; w.WriteHeader(204) }))
	session, transport := requestLoopSession(t, config)
	acceptor := &requestAcceptor{streams: make(chan ServerRequestStream, 2)}
	bad := requestStream(t, 0)
	bad.source = bytes.NewReader(readerHeaders(t, fs(":method", "POST")))
	acceptor.streams <- bad
	acceptor.streams <- requestStream(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- session.ServeRequests(ctx, acceptor, ServeRequestsConfig{MaxActive: 1, Request: config, Report: func(_ context.Context, _ uint64, err error) { reported <- err }})
	}()
	select {
	case err := <-reported:
		var message *MessageError
		if !errors.As(err, &message) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("request failure unreported")
	}
	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("request failure killed loop")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not stop")
	}
	transport.check(t, 0x100)
}
func TestServeRequestsProtocolFailureCancelsOthers(t *testing.T) {
	entered := make(chan struct{})
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done() }))
	session, transport := requestLoopSession(t, config)
	acceptor := &requestAcceptor{streams: make(chan ServerRequestStream, 1)}
	acceptor.streams <- requestStream(t, 0)
	done := make(chan error, 1)
	go func() {
		done <- session.ServeRequests(context.Background(), acceptor, ServeRequestsConfig{MaxActive: 2, Request: config})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler missing")
	}
	bad := requestStream(t, 4)
	bad.source = bytes.NewReader(readerFrame(t, h3.Settings, nil))
	acceptor.streams <- bad
	select {
	case err := <-done:
		var protocol *h3.Error
		if !errors.As(err, &protocol) || protocol.Code != h3.FrameUnexpected {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("protocol failure did not stop loop")
	}
	transport.check(t, h3.FrameUnexpected)
}

func TestServeRequestsCancellationBeforeSettings(t *testing.T) {
	config := serveConfig(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unexpected handler") }))
	session, transport := requestLoopSession(t, config)
	session.ready = make(chan struct{})
	acceptor := &requestAcceptor{streams: make(chan ServerRequestStream)}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("server shutting down")
	cancel(cause)
	if err := session.ServeRequests(ctx, acceptor, ServeRequestsConfig{MaxActive: 1, Request: config}); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if acceptor.calls.Load() != 0 {
		t.Fatal("accepted before settings")
	}
	transport.check(t, 0x100)
}
func TestServeRequestsRejectsInvalidStreamIdentity(t *testing.T) {
	config := serveConfig(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unexpected handler") }))
	session, transport := requestLoopSession(t, config)
	acceptor := &requestAcceptor{streams: make(chan ServerRequestStream, 1)}
	acceptor.streams <- requestStream(t, 1)
	err := session.ServeRequests(context.Background(), acceptor, ServeRequestsConfig{MaxActive: 1, Request: config})
	var protocol *h3.Error
	if !errors.As(err, &protocol) || protocol.Code != h3.StreamCreationError {
		t.Fatal(err)
	}
	transport.check(t, h3.StreamCreationError)
}

// The admission slot owns all per-request state, including priority cleanup.
// Hold that cleanup's mutex to expose a premature slot release.
type releaseObservedRequest struct {
	*numberedRequest
	released chan struct{}
}

func (s *releaseObservedRequest) Release(ctx context.Context) error {
	err := s.numberedRequest.Release(ctx)
	close(s.released)
	return err
}

func TestServeRequestsPriorityCleanupBeforeAdmission(t *testing.T) {
	entered := make(chan struct{}, 2)
	finish := make(chan struct{})
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-finish:
		case <-r.Context().Done():
		}
	}))
	config.Priorities, _ = NewRequestPriorities(1)
	session, _ := requestLoopSession(t, config)
	acceptor := &requestAcceptor{streams: make(chan ServerRequestStream, 2)}
	first := &releaseObservedRequest{numberedRequest: requestStream(t, 0), released: make(chan struct{})}
	acceptor.streams <- first
	acceptor.streams <- requestStream(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- session.ServeRequests(ctx, acceptor, ServeRequestsConfig{MaxActive: 1, Request: config})
	}()
	locked := false
	defer func() {
		if locked {
			config.Priorities.mu.Unlock()
		}
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("request loop did not join")
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first handler missing")
	}
	config.Priorities.mu.Lock()
	locked = true
	close(finish)
	select {
	case <-first.released:
	case <-time.After(time.Second):
		t.Fatal("first request not released")
	}
	// A completed handler cannot release admission while its priority state is held.
	time.Sleep(20 * time.Millisecond)
	if calls := acceptor.calls.Load(); calls != 1 {
		t.Fatalf("accepted request before priority cleanup: calls=%d", calls)
	}
	config.Priorities.mu.Unlock()
	locked = false
	select {
	case <-entered:
	case err := <-done:
		done <- err
		t.Fatal("loop failed", err)
	case <-time.After(time.Second):
		t.Fatal("cleanup failed to release admission")
	}
}
