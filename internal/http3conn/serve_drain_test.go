package http3conn

import (
	"bytes"
	"context"
	"errors"
	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type drainControl struct {
	fakeStream
	ackEntered, ackRelease chan struct{}
	ackErr                 error // returned at once, as when the stream sees the close before ctx
}

func (c *drainControl) WaitAcknowledged(ctx context.Context) error {
	close(c.ackEntered)
	if c.ackErr != nil {
		return c.ackErr
	}
	select {
	case <-c.ackRelease:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
func TestServeRequestsGracefulDrainWaitsForDelivery(t *testing.T) {
	entered := make(chan context.Context, 1)
	finishHandler := make(chan struct{})
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- r.Context()
		<-finishHandler
		w.WriteHeader(204)
	}))
	session, transport := requestLoopSession(t, config)
	control := &drainControl{ackEntered: make(chan struct{}), ackRelease: make(chan struct{})}
	session.control = control
	request := requestStream(t, 8)
	releaseEntered, releaseAllowed := make(chan struct{}), make(chan struct{})
	request.release = func(ctx context.Context) error {
		close(releaseEntered)
		select {
		case <-releaseAllowed:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	acceptor := &requestAcceptor{streams: make(chan ServerRequestStream, 2)}
	late := requestStream(t, 12) // opened before the client saw GOAWAY
	lateRejected := make(chan struct{})
	late.release = func(context.Context) error { close(lateRejected); return nil }
	acceptor.streams <- request
	acceptor.streams <- late
	drain := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	drained := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- session.ServeRequests(ctx, acceptor, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain, drainedHook: func() { close(drained) }})
	}()
	var handlerCtx context.Context
	select {
	case handlerCtx = <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(drain)
	select {
	case <-control.ackEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !bytes.Equal(control.Bytes(), []byte{7, 1, 12}) {
		t.Fatal("GOAWAY cutoff", control.Bytes())
	}
	if handlerCtx.Err() != nil {
		t.Fatal("drain canceled work")
	}
	close(control.ackRelease)
	close(finishHandler)
	select {
	case <-releaseEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-done:
		t.Fatal("closed before response acknowledged", err)
	default:
	}
	close(releaseAllowed)
	// Every request is done, but closing is the client's: the loop must not
	// close the connection under a response the client may not have read.
	select {
	case <-drained:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-done:
		t.Fatal("closed the connection before the client did", err)
	default:
	}
	transport.mu.Lock()
	aborts := transport.aborts
	transport.mu.Unlock()
	if aborts != 0 {
		t.Fatal("closed the connection before the client did")
	}
	select {
	case <-lateRejected:
	case <-ctx.Done():
		t.Fatal("late request not rejected before the client closed", ctx.Err())
	}
	clientCloses(session)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if request.closes != 1 || len(request.writeCodes) != 0 {
		t.Fatal("response not completed", request.closes, request.writeCodes)
	}
	// The stream that arrived after GOAWAY is rejected, not served or left
	// waiting.
	if len(late.writeCodes) != 1 || late.writeCodes[0] != 0x10b || len(late.readCodes) != 1 || late.readCodes[0] != 0x10b {
		t.Fatal("late request not rejected", late.writeCodes, late.readCodes)
	}
	transport.check(t, 0x100)
}

// clientCloses ends the connection as a client does once GOAWAY has drained
// its requests.
func clientCloses(session *Session) {
	session.cancel(&quic.PeerCloseError{Application: true, Code: 0x100, Reason: "client done"})
}
func TestServeRequestsDrainBeforeSettingsAndCancellation(t *testing.T) {
	for _, cancelDrain := range []bool{false, true} {
		config := serveConfig(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler invoked") }))
		session, transport := requestLoopSession(t, config)
		session.ready = make(chan struct{})
		control := &drainControl{ackEntered: make(chan struct{}), ackRelease: make(chan struct{})}
		session.control = control
		drain := make(chan struct{})
		close(drain)
		ctx, cancel := context.WithCancel(context.Background())
		// A stream waits to be accepted: the drain must reject it, never admit it.
		late := requestStream(t, 0)
		rejected := make(chan struct{})
		late.release = func(context.Context) error { close(rejected); return nil }
		acceptor := &requestAcceptor{streams: make(chan ServerRequestStream, 1)}
		acceptor.streams <- late
		done := make(chan error, 1)
		go func() {
			done <- session.ServeRequests(ctx, acceptor, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain})
		}()
		select {
		case <-control.ackEntered:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("GOAWAY waiting for peer settings")
		}
		if !bytes.Equal(control.Bytes(), []byte{7, 1, 0}) {
			t.Fatal(control.Bytes())
		}
		select {
		case <-rejected:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("waiting request not rejected after GOAWAY")
		}
		if cancelDrain {
			cancel()
		} else {
			close(control.ackRelease)
			clientCloses(session)
		}
		select {
		case err := <-done:
			// No request was running, so even the owner's cancellation
			// leaves nothing undelivered.
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			cancel()
			t.Fatal("drain stuck")
		}
		cancel()
		transport.check(t, 0x100)
		if len(late.writeCodes) != 1 || late.writeCodes[0] != 0x10b || len(late.readCodes) != 1 || late.readCodes[0] != 0x10b {
			t.Fatal("waiting request not rejected", late.writeCodes, late.readCodes)
		}
	}
}

type drainRaceAcceptor struct {
	stream  ServerRequestStream
	entered chan struct{}
	once    sync.Once
}

// AcceptRequest returns its stream from the first call, once the drain has
// interrupted it. Later calls, such as the drain's rejecting loop, find no
// stream.
func (a *drainRaceAcceptor) AcceptRequest(ctx context.Context) (ServerRequestStream, error) {
	first := false
	a.once.Do(func() { first = true; close(a.entered) })
	<-ctx.Done()
	if first {
		return a.stream, nil
	}
	return nil, ctx.Err()
}
func TestServeRequestsRejectsAcceptRacingDrain(t *testing.T) {
	config := serveConfig(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler invoked after drain") }))
	session, transport := requestLoopSession(t, config)
	control := &drainControl{ackEntered: make(chan struct{}), ackRelease: make(chan struct{})}
	close(control.ackRelease)
	session.control = control
	stream := requestStream(t, 0)
	// The rejected stream's Release waits until the connection ends, so the
	// wait for the drain to reach WaitAcknowledged, after GOAWAY is sent,
	// times out if anything waits for it first.
	stream.release = func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	acceptor := &drainRaceAcceptor{stream: stream, entered: make(chan struct{})}
	drain := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- session.ServeRequests(ctx, acceptor, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain})
	}()
	select {
	case <-acceptor.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(drain)
	select {
	case <-control.ackEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	clientCloses(session)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if len(stream.writeCodes) != 1 || stream.writeCodes[0] != 0x10b || len(stream.readCodes) != 1 || stream.readCodes[0] != 0x10b {
		t.Fatal("request not rejected", stream.writeCodes, stream.readCodes)
	}
	if !bytes.Equal(control.Bytes(), []byte{7, 1, 0}) {
		t.Fatal("rejected stream changed cutoff", control.Bytes())
	}
	transport.check(t, 0x100)
}

// A client that never closes after GOAWAY cannot hold the connection: once the
// drain grace has passed, the server closes it itself with H3_NO_ERROR.
func TestServeRequestsDrainGraceBoundsWaitForClient(t *testing.T) {
	config := serveConfig(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler invoked") }))
	session, transport := requestLoopSession(t, config)
	control := &drainControl{ackEntered: make(chan struct{}), ackRelease: make(chan struct{})}
	close(control.ackRelease)
	session.control = control
	drain := make(chan struct{})
	close(drain)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- session.ServeRequests(ctx, &requestAcceptor{streams: make(chan ServerRequestStream)}, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain, DrainGrace: 20 * time.Millisecond})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("a client that never closes held the drained connection")
	}
	transport.check(t, 0x100)
}

// TestServeRequestsDrainEndingCause checks which connection endings after
// GOAWAY complete a drain: any, unless a request running in the drain failed
// or a local protocol error ended it.
func TestServeRequestsDrainEndingCause(t *testing.T) {
	for _, tc := range []struct {
		name       string
		midRequest bool
		code       uint64
		thenCancel bool
		ownerOnly  bool // the owner cancels once requests are done; no client close
		holdAck    bool // GOAWAY is never acknowledged, so the drain is not done
		protocol   bool // a protocol error ends the connection once requests are done
		afterDone  bool // the client closes once GOAWAY is acknowledged and requests are done
		clean      bool
	}{
		{name: "client close", code: 0x100, clean: true},
		{name: "client close then owner cancel", code: 0x100, thenCancel: true, clean: true},
		{name: "client close with error code before drained", code: 0x102, holdAck: true, clean: true},
		{name: "client close before drained", code: 0x100, holdAck: true, clean: true},
		{name: "client close mid-request", code: 0x100, midRequest: true},
		{name: "owner cancel after requests done", ownerOnly: true, clean: true},
		{name: "protocol error after requests done", protocol: true},
		{name: "client close after requests done", code: 0x100, afterDone: true, clean: true},
		{name: "client error close after requests done", code: 0x102, afterDone: true, clean: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entered := make(chan struct{})
			config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-r.Context().Done()
			}))
			session, _ := requestLoopSession(t, config)
			control := &drainControl{ackEntered: make(chan struct{}), ackRelease: make(chan struct{})}
			if !tc.holdAck {
				close(control.ackRelease)
			}
			session.control = control
			acceptor := &requestAcceptor{streams: make(chan ServerRequestStream, 1)}
			if tc.midRequest {
				acceptor.streams <- requestStream(t, 0)
			}
			drain := make(chan struct{})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			parent, cancelParent := context.WithCancel(ctx)
			defer cancelParent()
			drained := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- session.ServeRequests(parent, acceptor, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain, drainedHook: func() { close(drained) }})
			}()
			if tc.midRequest {
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			close(drain)
			select {
			case <-control.ackEntered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if tc.ownerOnly || tc.protocol || tc.afterDone {
				select {
				case <-drained:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if tc.protocol {
				session.cancel(&h3.Error{Code: h3.FrameUnexpected, Reason: "bad frame"})
			} else if tc.ownerOnly {
				cancelParent()
			} else {
				session.cancel(&quic.PeerCloseError{Application: true, Code: tc.code, Reason: "client done"})
			}
			if tc.thenCancel {
				cancelParent()
			}
			var err error
			select {
			case err = <-done:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var peer *quic.PeerCloseError
			var protocol *h3.Error
			switch {
			case tc.clean && err != nil:
				t.Fatal("drain reported failure", err)
			case tc.protocol && !errors.As(err, &protocol):
				t.Fatal("drain hid the protocol error", err)
			case !tc.clean && !tc.protocol && !errors.As(err, &peer):
				t.Fatal("drain hid the connection's ending", err)
			}
		})
	}
}

// A client that has read its response may close before acknowledging the
// response's last packets. After GOAWAY that close completes the drain, and
// the request is not reported as failed.
func TestServeRequestsDrainClientCloseBeforeFinalAck(t *testing.T) {
	// releaseFirst: the stream reports the close before ctx does.
	for _, releaseFirst := range []bool{false, true} {
		name := "ctx-first"
		if releaseFirst {
			name = "release-first"
		}
		t.Run(name, func(t *testing.T) {
			testDrainClientCloseBeforeFinalAck(t, releaseFirst)
		})
	}
}

func testDrainClientCloseBeforeFinalAck(t *testing.T, releaseFirst bool) {
	testDrainRelease(t, releaseFirst, nil)
}

// testDrainRelease runs one request through a drain whose client closes with
// H3_NO_ERROR while Release waits. resetErr, if set, is what Release reports
// instead of the close: the drain must then fail and report the request.
func testDrainRelease(t *testing.T, releaseFirst bool, resetErr error) {
	entered := make(chan struct{})
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		w.WriteHeader(204)
	}))
	session, _ := requestLoopSession(t, config)
	control := &drainControl{ackEntered: make(chan struct{}), ackRelease: make(chan struct{})}
	close(control.ackRelease)
	session.control = control
	request := requestStream(t, 0)
	releasing := make(chan struct{})
	request.release = func(ctx context.Context) error {
		close(releasing)
		if releaseFirst {
			<-control.ackEntered // GOAWAY is out
			return &quic.PeerCloseError{Application: true, Code: 0x100, Reason: "client done"}
		}
		<-ctx.Done() // the final ACK never comes
		if resetErr != nil {
			return resetErr
		}
		return context.Cause(ctx)
	}
	acceptor := &requestAcceptor{streams: make(chan ServerRequestStream, 1)}
	acceptor.streams <- request
	drain := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	reports := make(chan error, 1)
	drained := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- session.ServeRequests(ctx, acceptor, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain, drainedHook: func() { close(drained) }, Report: func(_ context.Context, _ uint64, err error) { reports <- err }})
	}()
	for _, c := range []chan struct{}{entered, releasing} {
		select {
		case <-c:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(drain)
	select {
	case <-control.ackEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if releaseFirst {
		// Release has returned the close and the request is joined before
		// ctx sees any close.
		select {
		case <-drained:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	clientCloses(session)
	select {
	case err := <-done:
		if resetErr == nil && err != nil {
			t.Fatal("drain reported failure", err)
		}
		if resetErr != nil && !errors.Is(err, resetErr) {
			t.Fatal("drain did not return the reset response's error:", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-reports:
		if resetErr == nil {
			t.Fatal("delivered request reported", err)
		}
	default:
		if resetErr != nil {
			t.Fatal("reset response not reported")
		}
	}
}

// A reset or STOP_SENDING that Release reports is a failure even when the
// client then closes with H3_NO_ERROR.
func TestServeRequestsDrainResetBeforeCleanClose(t *testing.T) {
	testDrainRelease(t, false, errors.New("stream reset by peer"))
}

// WaitAcknowledged can see the client's close, with any code, before ctx
// does. That close ends the connection, so it completes the drain as the
// same close seen through ctx would.
func TestServeRequestsDrainCloseSeenByAcknowledgment(t *testing.T) {
	for _, code := range []uint64{0x100, 0x102} {
		config := serveConfig(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler invoked") }))
		session, transport := requestLoopSession(t, config)
		session.control = &drainControl{ackEntered: make(chan struct{}), ackRelease: make(chan struct{}), ackErr: &quic.PeerCloseError{Application: true, Code: code, Reason: "client done"}}
		drain := make(chan struct{})
		close(drain)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := session.ServeRequests(ctx, &requestAcceptor{streams: make(chan ServerRequestStream)}, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain})
		cancel()
		if err != nil {
			t.Fatal("code", code, err)
		}
		transport.check(t, 0x100)
	}
}

// A connection that ends during a drain is closed at once, even while a
// handler that ignores cancellation is still running; the outcome waits for
// it.
func TestServeRequestsDrainEndingClosesBeforeJoiningRequests(t *testing.T) {
	entered, finish := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(finish) })
	t.Cleanup(release) // a failed wait must not strand the handler
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-finish // ignores r.Context()
	}))
	session, transport := requestLoopSession(t, config)
	control := &drainControl{ackEntered: make(chan struct{}), ackRelease: make(chan struct{})}
	close(control.ackRelease)
	session.control = control
	acceptor := &requestAcceptor{streams: make(chan ServerRequestStream, 1)}
	acceptor.streams <- requestStream(t, 0)
	drain := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- session.ServeRequests(ctx, acceptor, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(drain)
	select {
	case <-control.ackEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	clientCloses(session)
	for {
		transport.mu.Lock()
		aborts := transport.aborts
		transport.mu.Unlock()
		if aborts == 1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("connection not closed while a request was running")
		case <-time.After(time.Millisecond):
		}
	}
	release()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a request cut short by the close completed the drain")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// Closing DrainCutoff ends a drained connection's wait for its client, whether
// it closes before or after the requests are done.
func TestServeRequestsDrainCutoff(t *testing.T) {
	for _, early := range []bool{false, true} {
		name := "while waiting"
		if early {
			name = "before drained"
		}
		t.Run(name, func(t *testing.T) {
			config := serveConfig(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler invoked") }))
			session, transport := requestLoopSession(t, config)
			control := &drainControl{ackEntered: make(chan struct{}), ackRelease: make(chan struct{})}
			close(control.ackRelease)
			session.control = control
			drain, cutoff, drained := make(chan struct{}), make(chan struct{}), make(chan struct{})
			close(drain)
			if early {
				close(cutoff)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- session.ServeRequests(ctx, &requestAcceptor{streams: make(chan ServerRequestStream)}, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain, DrainCutoff: cutoff, drainedHook: func() { close(drained) }})
			}()
			if !early {
				select {
				case <-drained:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				close(cutoff)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("cutoff did not end the wait", ctx.Err())
			}
			transport.check(t, 0x100)
		})
	}
}

// A request that fails during the drain fails it on every path, with the
// request's own error, including when the grace ends the wait. The
// connection still closes with H3_NO_ERROR.
func TestServeRequestsDrainFailureOnGracePath(t *testing.T) {
	proceed := make(chan struct{})
	release := sync.OnceFunc(func() { close(proceed) })
	t.Cleanup(release) // a failed wait must not strand the handler
	entered := make(chan struct{})
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-proceed
		panic("handler failed during drain")
	}))
	session, transport := requestLoopSession(t, config)
	control := &drainControl{ackEntered: make(chan struct{}), ackRelease: make(chan struct{})}
	close(control.ackRelease)
	session.control = control
	acceptor := &requestAcceptor{streams: make(chan ServerRequestStream, 1)}
	acceptor.streams <- requestStream(t, 0)
	drain := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- session.ServeRequests(ctx, acceptor, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain, DrainGrace: 20 * time.Millisecond})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(drain)
	select {
	case <-control.ackEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	release()
	select {
	case err := <-done:
		var panicked *HandlerPanicError
		if !errors.As(err, &panicked) {
			t.Fatal("grace hid a request that failed during the drain:", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	transport.check(t, 0x100)
}

// A control-stream failure while the connection lives, such as STOP_SENDING,
// is an error, not the connection ending.
func TestServeRequestsDrainControlStreamFailure(t *testing.T) {
	config := serveConfig(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler invoked") }))
	session, _ := requestLoopSession(t, config)
	stopped := errors.New("control stream stopped")
	session.control = &drainControl{ackEntered: make(chan struct{}), ackRelease: make(chan struct{}), ackErr: stopped}
	drain := make(chan struct{})
	close(drain)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := session.ServeRequests(ctx, &requestAcceptor{streams: make(chan ServerRequestStream)}, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain}); !errors.Is(err, stopped) {
		t.Fatal(err)
	}
}

// A local protocol error that ends the connection as the cutoff fires is
// returned, whichever case the wait's select picks.
func TestServeRequestsDrainProtocolErrorRacingCutoff(t *testing.T) {
	for i := 0; i < 20; i++ {
		config := serveConfig(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler invoked") }))
		session, _ := requestLoopSession(t, config)
		control := &drainControl{ackEntered: make(chan struct{}), ackRelease: make(chan struct{})}
		close(control.ackRelease)
		session.control = control
		drain, cutoff := make(chan struct{}), make(chan struct{})
		close(drain)
		close(cutoff) // ready together with ctx below
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := session.ServeRequests(ctx, &requestAcceptor{streams: make(chan ServerRequestStream)}, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain, DrainCutoff: cutoff, drainedHook: func() {
			session.cancel(&h3.Error{Code: h3.FrameUnexpected, Reason: "bad frame"})
		}})
		cancel()
		var protocol *h3.Error
		if !errors.As(err, &protocol) {
			t.Fatal("run", i, "protocol error hidden:", err)
		}
	}
}

// A request stream that arrives while a drained connection waits for its
// client is rejected with H3_REQUEST_REJECTED at once, not left unanswered
// until the connection closes.
func TestServeRequestsDrainRejectsStreamDuringGrace(t *testing.T) {
	config := serveConfig(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler invoked after drain") }))
	session, transport := requestLoopSession(t, config)
	control := &drainControl{ackEntered: make(chan struct{}), ackRelease: make(chan struct{})}
	close(control.ackRelease)
	session.control = control
	acceptor := &requestAcceptor{streams: make(chan ServerRequestStream)}
	// The first late stream's Release waits until the connection ends; the
	// second must still be rejected, so no rejection waits on another.
	slow := requestStream(t, 0)
	slowReleasing := make(chan struct{})
	slow.release = func(ctx context.Context) error { close(slowReleasing); <-ctx.Done(); return ctx.Err() }
	late := requestStream(t, 4)
	rejected := make(chan struct{})
	late.release = func(context.Context) error { close(rejected); return nil }
	drain, drained := make(chan struct{}), make(chan struct{})
	close(drain)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- session.ServeRequests(ctx, acceptor, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain, drainedHook: func() { close(drained) }})
	}()
	select {
	case <-drained:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for _, stream := range []ServerRequestStream{slow, late} {
		select {
		case acceptor.streams <- stream:
		case <-ctx.Done():
			t.Fatal("stream not accepted during the grace", ctx.Err())
		}
	}
	select {
	case <-slowReleasing:
	case <-ctx.Done():
		t.Fatal("slow stream not rejected during the grace", ctx.Err())
	}
	select {
	case <-rejected:
	case <-ctx.Done():
		t.Fatal("stream not rejected during the grace", ctx.Err())
	}
	clientCloses(session)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for _, stream := range []*numberedRequest{slow, late} {
		if len(stream.writeCodes) != 1 || stream.writeCodes[0] != 0x10b || len(stream.readCodes) != 1 || stream.readCodes[0] != 0x10b {
			t.Fatal("late request not rejected", stream.StreamID(), stream.writeCodes, stream.readCodes)
		}
	}
	transport.check(t, 0x100)
}

// firstThenAcceptor hands the request loop its stream on the first call;
// later calls, from the drain's rejecting loop, get next's result.
type firstThenAcceptor struct {
	first  ServerRequestStream
	served atomic.Bool
	next   func(context.Context) (ServerRequestStream, error)
}

func (a *firstThenAcceptor) AcceptRequest(ctx context.Context) (ServerRequestStream, error) {
	if a.served.CompareAndSwap(false, true) {
		return a.first, nil
	}
	return a.next(ctx)
}

// An accept error in the rejecting loop means the connection is ending, so
// the loop leaves the cause to the session: the client's clean close still
// completes the drain for a request finishing in it.
func TestServeRequestsDrainRejectorAcceptErrorKeepsCleanClose(t *testing.T) {
	entered := make(chan struct{})
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		w.WriteHeader(204)
	}))
	session, _ := requestLoopSession(t, config)
	control := &drainControl{ackEntered: make(chan struct{}), ackRelease: make(chan struct{})}
	close(control.ackRelease)
	session.control = control
	request := requestStream(t, 0)
	releasing := make(chan struct{})
	request.release = func(ctx context.Context) error {
		close(releasing)
		<-ctx.Done() // the final ACK never comes
		return ctx.Err()
	}
	rejectorFailed := make(chan struct{})
	acceptor := &firstThenAcceptor{first: request, next: func(ctx context.Context) (ServerRequestStream, error) {
		close(rejectorFailed)
		return nil, errors.New("endpoint closed")
	}}
	drain := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- session.ServeRequests(ctx, acceptor, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain})
	}()
	for _, c := range []chan struct{}{entered, releasing} {
		select {
		case <-c:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(drain)
	select {
	case <-rejectorFailed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	clientCloses(session)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("clean close reported as failure:", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// An acceptor that returns no stream and no error during the drain is a
// local failure the drain's result keeps.
func TestServeRequestsDrainRejectorNilStream(t *testing.T) {
	config := serveConfig(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler invoked") }))
	session, _ := requestLoopSession(t, config)
	control := &drainControl{ackEntered: make(chan struct{}), ackRelease: make(chan struct{})}
	session.control = control
	acceptor := &firstThenAcceptor{next: func(context.Context) (ServerRequestStream, error) { return nil, nil }}
	acceptor.served.Store(true)
	drain := make(chan struct{})
	close(drain)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := session.ServeRequests(ctx, acceptor, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain})
	var protocol *h3.Error
	if !errors.As(err, &protocol) || protocol.Code != 0x102 {
		t.Fatal(err)
	}
}
