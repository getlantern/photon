package http3

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/http3conn"
	h3 "github.com/getlantern/photon/internal/http3wire"
)

type ownershipIncoming struct {
	fail   chan error
	aborts atomic.Int32
}

func (t *ownershipIncoming) AcceptUni(ctx context.Context) (http3conn.ReceiveStream, error) {
	select {
	case err := <-t.fail:
		return nil, err
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}
func (t *ownershipIncoming) Abort(uint64, string) error { t.aborts.Add(1); return nil }

type ownershipStream struct{}

func (ownershipStream) Write(b []byte) (int, error)      { return len(b), nil }
func (ownershipStream) SetWriteDeadline(time.Time) error { return nil }

func ownershipClient(t *testing.T) (*ClientConn, *ownershipIncoming, *atomic.Int32) {
	t.Helper()
	ctx, cancel := context.WithCancelCause(context.Background())
	incoming := &ownershipIncoming{fail: make(chan error, 1)}
	remembered := []h3.Setting{}
	stream := ownershipStream{}
	session, err := http3conn.StartSession(ctx, incoming, &http3conn.LocalStreams{Control: stream, Encoder: stream, Decoder: stream}, http3conn.SessionConfig{
		PeerIsServer: true, EarlyPeerSettings: &remembered, MaxActiveStreams: 3, MaxSettingsBytes: 1024, MaxSettingsEntries: 16, MaxEncoderInstruction: 1024,
		Control: func(context.Context, h3.ControlEvent) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	closed := new(atomic.Int32)
	client := &ClientConn{ctx: ctx, lifetime: context.Background(), cancel: cancel, session: session, monitorDone: make(chan struct{}), ownership: &clientOwnership{closeConn: func(uint64, string) error { closed.Add(1); return nil }}}
	go func() { defer close(client.monitorDone); <-ctx.Done(); client.ownership.close("monitor") }()
	t.Cleanup(func() { client.Close() })
	return client, incoming, closed
}

func awaitOwnershipSession(t *testing.T, client *ClientConn) {
	t.Helper()
	select {
	case <-client.session.Done():
	case <-time.After(time.Second):
		t.Fatal("session did not stop")
	}
}

func TestRejectedClientOwnershipTransfer(t *testing.T) {
	client, incoming, closed := ownershipClient(t)
	request, finish, err := client.requestLifetime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if err := client.retireRejected(); err == nil {
		t.Fatal("live session released transport")
	}
	incoming.fail <- quic.Err0RTTRejected
	awaitOwnershipSession(t, client)
	if err := client.retireRejected(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(context.Cause(request), quic.Err0RTTRejected) {
		t.Fatal("request lifetime survived rejection", context.Cause(request))
	}
	if _, _, err := client.requestLifetime(context.Background()); !errors.Is(err, quic.Err0RTTRejected) {
		t.Fatal("late request admission", err)
	}
	if err := client.retireRejected(); err != nil {
		t.Fatal("repeated retirement", err)
	}
	if err := client.CloseWithError(0x123, "obsolete session"); err != nil {
		t.Fatal(err)
	}
	client.Close()
	// A delayed request worker must also lose connection-abort authority.
	oldTransport := ownedClientTransport{ownership: client.ownership}
	if err := oldTransport.Abort(0x202, "late request failure"); err != nil {
		t.Fatal(err)
	}
	if closed.Load() != 0 || incoming.aborts.Load() != 0 {
		t.Fatal("retired client closed transport", closed.Load(), incoming.aborts.Load())
	}
}

func TestClientOwnershipRejectsOtherFailures(t *testing.T) {
	client, incoming, closed := ownershipClient(t)
	incoming.fail <- errors.New("ordinary transport failure")
	awaitOwnershipSession(t, client)
	if err := client.retireRejected(); err == nil {
		t.Fatal("ordinary failed session handed off transport")
	}
	client.Close()
	if closed.Load() != 1 {
		t.Fatal("ordinary failure did not close", closed.Load())
	}
}

func TestClientCloseRacesRejectedHandoff(t *testing.T) {
	for i := 0; i < 100; i++ {
		client, incoming, closed := ownershipClient(t)
		incoming.fail <- quic.Err0RTTRejected
		awaitOwnershipSession(t, client)
		start := make(chan struct{})
		result := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			if i%2 == 0 {
				if err := client.CloseWithError(0x123, "racing handoff"); err != nil {
					t.Error(err)
				}
			} else {
				client.Close()
			}
		}()
		go func() { defer wg.Done(); <-start; result <- client.retireRejected() }()
		close(start)
		wg.Wait()
		err := <-result
		if err == nil {
			if closed.Load() != 0 {
				t.Fatal("released ownership also closed transport")
			}
		} else if !errors.Is(err, ErrClientClosed) || closed.Load() != 1 {
			t.Fatal("close/handoff outcome", err, closed.Load())
		}
	}
}

func TestOwnedClientTransportPreservesAbort(t *testing.T) {
	var code uint64
	var reason string
	expected := errors.New("transport close result")
	owner := &clientOwnership{closeConn: func(c uint64, r string) error { code, reason = c, r; return expected }}
	transport := ownedClientTransport{ownership: owner}
	if err := transport.Abort(0x202, "decoder failure"); err != expected {
		t.Fatal(err)
	}
	if code != 0x202 || reason != "decoder failure" {
		t.Fatal(code, reason)
	}
	if err := owner.release(); !errors.Is(err, ErrClientClosed) {
		t.Fatal("aborted transport released", err)
	}
	owner.close("later close")
	if code != 0x202 || reason != "decoder failure" {
		t.Fatal("abort overwritten", code, reason)
	}
}

func TestClientApplicationCloseOwnership(t *testing.T) {
	var calls int
	var code uint64
	var reason string
	c := &ClientConn{ownership: &clientOwnership{closeConn: func(value uint64, msg string) error { calls++; code = value; reason = msg; return nil }}}
	if err := c.CloseWithError(1<<62, "invalid"); err == nil || calls != 0 || c.ownership.closed {
		t.Fatal("invalid close consumed ownership", err)
	}
	if err := c.CloseWithError(0x123, "custom reason"); err != nil || calls != 1 || code != 0x123 || reason != "custom reason" {
		t.Fatal("close lost cause", err, calls, code, reason)
	}
	if err := c.CloseWithError(0x124, "second"); err != nil || calls != 1 {
		t.Fatal("close overwritten", err, calls)
	}
}
