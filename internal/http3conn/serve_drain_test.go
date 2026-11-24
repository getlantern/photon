package http3conn

import (
	"bytes"
	"context"
	"errors"
	quic "github.com/getlantern/photon"
	"net/http"
	"testing"
	"time"
)

type drainControl struct {
	fakeStream
	ackEntered, ackRelease chan struct{}
}

func (c *drainControl) WaitAcknowledged(ctx context.Context) error {
	close(c.ackEntered)
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
			if cancelDrain && !errors.Is(err, context.Canceled) || !cancelDrain && err != nil {
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
