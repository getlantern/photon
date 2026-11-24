package http3conn

import (
	"bytes"
	"context"
	"errors"
	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"net/http"
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
	acceptor.streams <- request
	acceptor.streams <- requestStream(t, 12)
	drain := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- session.ServeRequests(ctx, acceptor, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain})
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
	if handlerCtx.Err() != nil || acceptor.calls.Load() != 1 {
		t.Fatal("drain canceled/admitted work")
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
		acceptor := &requestAcceptor{streams: make(chan ServerRequestStream)}
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
		if acceptor.calls.Load() != 0 {
			t.Fatal("accepted while draining")
		}
	}
}

type drainRaceAcceptor struct {
	stream  ServerRequestStream
	entered chan struct{}
}

func (a drainRaceAcceptor) AcceptRequest(ctx context.Context) (ServerRequestStream, error) {
	close(a.entered)
	<-ctx.Done()
	return a.stream, nil
}
func TestServeRequestsRejectsAcceptRacingDrain(t *testing.T) {
	config := serveConfig(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler invoked after drain") }))
	session, transport := requestLoopSession(t, config)
	control := &drainControl{ackEntered: make(chan struct{}), ackRelease: make(chan struct{})}
	close(control.ackRelease)
	session.control = control
	stream := requestStream(t, 0)
	acceptor := drainRaceAcceptor{stream: stream, entered: make(chan struct{})}
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
	done := make(chan error, 1)
	go func() {
		done <- session.ServeRequests(ctx, acceptor, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain, Report: func(_ context.Context, _ uint64, err error) { reports <- err }})
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
	clientCloses(session)
	select {
	case err := <-done:
		if resetErr == nil && err != nil {
			t.Fatal("drain reported failure", err)
		}
		if resetErr != nil && err == nil {
			t.Fatal("drain hid a reset response")
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

// WaitAcknowledged can see the client's clean close before ctx does.
func TestServeRequestsDrainCloseSeenByAcknowledgment(t *testing.T) {
	config := serveConfig(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler invoked") }))
	session, transport := requestLoopSession(t, config)
	session.control = &drainControl{ackEntered: make(chan struct{}), ackRelease: make(chan struct{}), ackErr: &quic.PeerCloseError{Application: true, Code: 0x100, Reason: "client done"}}
	drain := make(chan struct{})
	close(drain)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := session.ServeRequests(ctx, &requestAcceptor{streams: make(chan ServerRequestStream)}, ServeRequestsConfig{MaxActive: 1, Request: config, Drain: drain}); err != nil {
		t.Fatal(err)
	}
	transport.check(t, 0x100)
}

// A connection that ends during a drain is closed at once, even while a
// handler that ignores cancellation is still running; the outcome waits for
// it.
func TestServeRequestsDrainEndingClosesBeforeJoiningRequests(t *testing.T) {
	entered, finish := make(chan struct{}), make(chan struct{})
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
	close(finish)
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
