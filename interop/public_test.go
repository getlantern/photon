package interop

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestPublicListener(t *testing.T) {
	clientTLS, serverTLS := tlsConfigs(t)
	l, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		peer, err := dialUpstreamAddr(t, ctx, l.Addr().String(), clientTLS, &upstream.Config{EnableDatagrams: true, Versions: []upstream.Version{upstream.Version(v)}})
		if err != nil {
			t.Fatalf("dial version %#x: %v", v, err)
		}
		defer peer.CloseWithError(0, "")
		c, err := l.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		state := c.ConnectionState()
		if state.Version != v || !state.SupportsDatagrams || !state.TLS.HandshakeComplete {
			t.Fatalf("state: %+v", state)
		}
		if c.RemoteAddr().(*net.UDPAddr).Port != peer.LocalAddr().(*net.UDPAddr).Port || c.LocalAddr().String() != l.Addr().String() {
			t.Fatal("addresses")
		}
		canceled, stop := context.WithCancel(ctx)
		stop()
		if _, err := c.ReceiveDatagram(canceled); !errors.Is(err, context.Canceled) {
			t.Fatal("receive cancellation", err)
		}
		if err := peer.SendDatagram([]byte("request")); err != nil {
			t.Fatal(err)
		}
		if b, err := c.ReceiveDatagram(ctx); err != nil || string(b) != "request" {
			t.Fatal("request", string(b), err)
		}
		if err := c.SendDatagram([]byte("response")); err != nil {
			t.Fatal(err)
		}
		if b, err := peer.ReceiveDatagram(ctx); err != nil || string(b) != "response" {
			t.Fatal("response", string(b), err)
		}
		if v == quic.Version1 {
			if err := c.CloseWithError(42, "public close"); err != nil {
				t.Fatal(err)
			}
			select {
			case <-peer.Context().Done():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var app *upstream.ApplicationError
			if !errors.As(context.Cause(peer.Context()), &app) || app.ErrorCode != 42 {
				t.Fatal("close cause", context.Cause(peer.Context()))
			}
		} else {
			// Listener shutdown must wake an outstanding receive and cancel its connection.
			result := make(chan error, 1)
			go func() { _, err := c.ReceiveDatagram(ctx); result <- err }()
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("receive succeeded after shutdown")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if _, err := l.Accept(ctx); err == nil {
				t.Fatal("accept after shutdown")
			}
		}
		select {
		case <-c.Context().Done():
			if context.Cause(c.Context()) == nil {
				t.Fatal("missing cause")
			}
		case <-ctx.Done():
			t.Fatal("connection context", ctx.Err())
		}
	}
}
