package interop

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

type admissionContextKey struct{}

func TestTransportConnContextIndependent(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			inputs := make(chan context.Context, 1)
			values := make(chan any, 1)
			st.GetConfigForClient = func(h *tls.ClientHelloInfo) (*tls.Config, error) {
				if got := h.Context().Value(quic.QUICVersionContextKey); got != version {
					return nil, fmt.Errorf("TLS version context = %v, want %v", got, version)
				}
				values <- h.Context().Value(admissionContextKey{})
				return nil, nil
			}
			tr := &quic.Transport{Conn: socket, ConnContext: func(ctx context.Context, _ *quic.ClientInfo) (context.Context, error) {
				inputs <- ctx
				return context.WithValue(ctx, admissionContextKey{}, "independent"), nil
			}}
			defer tr.Close()
			listener, err := tr.Listen(st, &quic.Config{Versions: []quic.Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseWithError(0, "")
			server, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if got := <-values; got != "independent" {
				t.Fatal(got)
			}
			stream, err := server.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if stream.Context().Value(admissionContextKey{}) != "independent" {
				t.Fatal("lost stream context")
			}
			if err := client.CloseWithError(91, "peer close"); err != nil {
				t.Fatal(err)
			}
			for _, lifetime := range []context.Context{<-inputs, server.Context(), stream.Context()} {
				select {
				case <-lifetime.Done():
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				var app *quic.ApplicationError
				if !errors.As(context.Cause(lifetime), &app) || app.ErrorCode != 91 || !app.Remote {
					t.Fatal(context.Cause(lifetime))
				}
			}
		})
	}
}

func TestDialContextValuesIndependent(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			listener, err := upstream.ListenAddr("127.0.0.1:0", st, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			dialCtx, stop := context.WithCancel(context.WithValue(ctx, admissionContextKey{}, "outgoing"))
			defer stop()
			client, err := quic.DialAddr(dialCtx, listener.Addr().String(), ct, &quic.Config{Versions: []quic.Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseWithError(0, "")
			server, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			stop()
			if client.Context().Value(admissionContextKey{}) != "outgoing" || client.Context().Err() != nil {
				t.Fatal("dial context propagation")
			}
			stream, err := client.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if stream.Context().Value(admissionContextKey{}) != "outgoing" {
				t.Fatal("stream context propagation")
			}
			if _, err := stream.Write([]byte("after cancellation")); err != nil {
				t.Fatal(err)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			received, err := server.AcceptUniStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			received.SetReadDeadline(time.Now().Add(time.Second))
			payload, err := io.ReadAll(received)
			if err != nil || string(payload) != "after cancellation" {
				t.Fatal(string(payload), err)
			}
			if err := server.CloseWithError(92, "terminal cause"); err != nil {
				t.Fatal(err)
			}
			select {
			case <-client.Context().Done():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var app *quic.ApplicationError
			if !errors.As(context.Cause(client.Context()), &app) || app.ErrorCode != 92 || !app.Remote {
				t.Fatal(context.Cause(client.Context()))
			}
		})
	}
}
