package http3conn

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
)

type earlyRejectedReader struct{}

func (earlyRejectedReader) Read([]byte) (int, error) { return 0, quic.Err0RTTRejected }

func TestEarlySessionRejectionDoesNotAbortQUIC(t *testing.T) {
	for _, prefix := range [][]byte{nil, {0x40}, {0}, {0, 4, 10}, {2}, {2, 0x41, 'x', 100}, {3}, {3, 0xff}} {
		t.Run(fmt.Sprintf("%x", prefix), func(t *testing.T) {
			transport := &dispatchTransport{streams: make(chan ReceiveStream, 1)}
			local := &LocalStreams{Control: &fakeStream{interrupt: make(chan struct{})}, Encoder: &fakeStream{interrupt: make(chan struct{})}, Decoder: &fakeStream{interrupt: make(chan struct{})}}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			remembered := []h3.Setting{}
			session, err := StartSession(ctx, transport, local, earlySessionConfig(&remembered))
			if err != nil {
				t.Fatal(err)
			}
			defer session.Stop(context.Canceled)
			transport.streams <- &fakeReceive{Reader: io.MultiReader(bytes.NewReader(prefix), earlyRejectedReader{}), id: 3}
			select {
			case <-session.Done():
			case <-ctx.Done():
				t.Fatal("rejected session did not join")
			}
			if _, err := session.WaitEncoder(ctx); !errors.Is(err, quic.Err0RTTRejected) {
				t.Fatal("rejection lost", err)
			}
			transport.mu.Lock()
			aborts := transport.aborts
			transport.mu.Unlock()
			if aborts != 0 {
				t.Fatal("rejection closed QUIC", aborts)
			}
			// A fresh session can own the same transport after the old workers joined.
			freshLocal := &LocalStreams{Control: &fakeStream{interrupt: make(chan struct{})}, Encoder: &fakeStream{interrupt: make(chan struct{})}, Decoder: &fakeStream{interrupt: make(chan struct{})}}
			fresh, err := StartSession(ctx, transport, freshLocal, earlySessionConfig(nil))
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.Stop(context.Canceled)
			control := heldSessionStream(3, []byte{0, 4, 0})
			transport.streams <- control
			if _, err := fresh.WaitEncoder(ctx); err != nil {
				t.Fatal("fresh session failed", err)
			}
			fresh.Stop(context.Canceled)
		})
	}
}
func TestEarlyEncoderSetupRejectionDoesNotAbort(t *testing.T) {
	remembered := []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 128}}
	transport := &dispatchTransport{streams: make(chan ReceiveStream)}
	local := &LocalStreams{Control: &fakeStream{interrupt: make(chan struct{})}, Encoder: &fakeStream{fail: quic.Err0RTTRejected, interrupt: make(chan struct{})}, Decoder: &fakeStream{interrupt: make(chan struct{})}}
	session, err := StartSession(context.Background(), transport, local, earlySessionConfig(&remembered))
	if session != nil || !errors.Is(err, quic.Err0RTTRejected) {
		t.Fatal(session, err)
	}
	if transport.aborts != 0 {
		t.Fatal("setup rejection aborted QUIC")
	}
}

type rejectedOpenTransport struct {
	*fakeTransport
	at int
}

func (t rejectedOpenTransport) OpenUni(ctx context.Context) (SendStream, error) {
	if t.opens+1 == t.at {
		return nil, quic.Err0RTTRejected
	}
	return t.fakeTransport.OpenUni(ctx)
}
func TestOpenLocalRejectionDoesNotAbort(t *testing.T) {
	for i := 0; i < 3; i++ {
		for _, opening := range []bool{false, true} {
			base := fixture()
			var transport Transport = base
			if opening {
				transport = rejectedOpenTransport{fakeTransport: base, at: i + 1}
			} else {
				base.streams[i].fail = quic.Err0RTTRejected
			}
			local, err := OpenLocal(context.Background(), transport, nil)
			if local != nil || !errors.Is(err, quic.Err0RTTRejected) || base.aborted != 0 {
				t.Fatal(i, opening, err, base.aborted)
			}
		}
	}
}
