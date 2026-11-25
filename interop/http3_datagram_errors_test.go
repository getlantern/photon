package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

func TestHTTPDatagramsPublicClientErrors(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, mode := range []string{"missing-http", "missing-quic", "unsupported-request"} {
			t.Run(fmt.Sprintf("%x/%s", version, mode), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				ct.NextProtos = []string{"h3"}
				st.NextProtos = []string{"h3"}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				handlerErrors := make(chan error, 1)
				server := &peerh3.Server{TLSConfig: st, EnableDatagrams: mode == "unsupported-request", QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, EnableDatagrams: mode != "missing-quic"}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "CONNECT" {
						handlerErrors <- errors.New("unnegotiated CONNECT reached peer")
						return
					}
					if r.URL.Path == "/bad" {
						stream := peerHTTPStream(w)
						handlerErrors <- stream.SendDatagram([]byte("forbidden on GET"))
						<-r.Context().Done()
						return
					}
					io.WriteString(w, "ok")
				})}
				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				served := make(chan error, 1)
				go func() { served <- server.Serve(socket) }()
				defer func() { server.Close(); socket.Close(); <-served }()
				origin := "https://" + socket.LocalAddr().String()
				client, err := ownh3.Dial(ctx, origin, ct, &quic.Config{Versions: []quic.Version{version}}, &ownh3.ClientConfig{EnableDatagrams: true, MaxConcurrentRequests: 1})
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				if mode != "unsupported-request" {
					request, _ := http.NewRequestWithContext(ctx, "CONNECT", origin+"/echo", nil)
					tunnel, response, err := client.OpenDatagramTunnel(request, "datagram-test")
					if err != ownh3.ErrHTTPDatagramsNotNegotiated || tunnel != nil || response != nil {
						t.Fatal(tunnel, response, err)
					}
				} else {
					request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/bad", nil)
					response, err := client.RoundTrip(request)
					if err == nil {
						_, err = response.Body.Read(make([]byte, 1))
						response.Body.Close()
					}
					var reset *quic.StreamError
					if !errors.As(err, &reset) || reset.ErrorCode != 0x33 {
						t.Fatal("wrong request error scope", err)
					}
					select {
					case err := <-handlerErrors:
						if err != nil {
							t.Fatal(err)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/ok", nil)
				response, err := client.RoundTrip(request)
				if err != nil {
					t.Fatal("connection or reservation lost", err)
				}
				data, err := io.ReadAll(response.Body)
				if err != nil || string(data) != "ok" {
					t.Fatal(string(data), err)
				}
				response.Body.Close()
				select {
				case err := <-handlerErrors:
					t.Fatal(err)
				default:
				}
			})
		}
	}
}

func TestHTTPDatagramsPublicServerRequestAbort(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			ct.NextProtos = []string{"h3"}
			st.NextProtos = []string{"h3"}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			listener, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{version}, EnableDatagrams: true})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			canceled := make(chan error, 1)
			server := &ownh3.Server{EnableDatagrams: true, MaxConcurrentRequests: 1, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/bad" {
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
					<-r.Context().Done() // no stream/body read to notice reset
					canceled <- context.Cause(r.Context())
					return
				}
				io.WriteString(w, "ok")
			})}
			served := make(chan error, 1)
			go func() { served <- server.Serve(ctx, listener) }()
			defer func() { server.Close(); cancel(); <-served }()
			peer, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, EnableDatagrams: true})
			if err != nil {
				t.Fatal(err)
			}
			defer peer.CloseWithError(0, "done")
			peerTransport := &peerh3.Transport{Dial: loopbackDial(t), EnableDatagrams: true}
			peerClient := peerTransport.NewClientConn(peer)
			stream, err := peerClient.OpenRequestStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			origin := "https://" + listener.Addr().String()
			request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/bad", nil)
			if err := stream.SendRequestHeader(request); err != nil {
				t.Fatal(err)
			}
			if _, err := stream.ReadResponse(); err != nil {
				t.Fatal(err)
			}
			if err := stream.SendDatagram([]byte("not permitted")); err != nil {
				t.Fatal(err)
			}
			_, err = stream.Read(make([]byte, 1))
			var reset *peerh3.Error
			if !errors.As(err, &reset) || !reset.Remote || reset.ErrorCode != 0x33 {
				t.Fatal("wrong reset scope", err)
			}
			select {
			case err := <-canceled:
				var coded interface{ Code() uint64 }
				if !errors.As(err, &coded) || coded.Code() != 0x33 {
					t.Fatal("handler lost datagram cause", err)
				}
			case <-ctx.Done():
				t.Fatal("handler not canceled", ctx.Err())
			}
			stream.CancelWrite(0x10c)
			request, _ = http.NewRequestWithContext(ctx, "GET", origin+"/ok", nil)
			response, err := peerClient.RoundTrip(request)
			if err != nil {
				t.Fatal("connection or request slot lost", err)
			}
			data, err := io.ReadAll(response.Body)
			if err != nil || string(data) != "ok" {
				t.Fatal(string(data), err)
			}
			response.Body.Close()
		})
	}
}

func TestHTTPDatagramServerRequiresQUICReception(t *testing.T) {
	ct, st := tlsConfigs(t)
	ct.NextProtos = []string{"h3"}
	st.NextProtos = []string{"h3"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	listener, err := quic.ListenAddr("127.0.0.1:0", st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server := &ownh3.Server{EnableDatagrams: true}
	if err := server.Serve(ctx, listener); err == nil || ctx.Err() != nil {
		t.Fatal("missing QUIC reception was not rejected before serving", err)
	}
	// Failed validation leaves the caller's listener usable and unowned.
	peer, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.CloseWithError(0, "done")
	raw, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	raw.CloseWithError(0, "done")
}

func TestHTTPDatagramSelectedDisabledConnection(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			ct.NextProtos, st.NextProtos = []string{"h3"}, []string{"h3"}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			listener, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{
				Versions:           []quic.Version{version},
				GetConfigForClient: func(*quic.ClientInfo) (*quic.Config, error) { return &quic.Config{EnableDatagrams: false}, nil },
			})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			if !listener.DatagramsEnabled() {
				t.Fatal("selector capability missing")
			}
			server := &ownh3.Server{EnableDatagrams: true}
			served := make(chan error, 1)
			go func() { served <- server.Serve(ctx, listener) }()
			defer func() { server.Close(); cancel(); <-served }()
			peer, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, EnableDatagrams: true})
			if peer != nil {
				defer peer.CloseWithError(0, "")
				select {
				case <-peer.Context().Done():
					err = context.Cause(peer.Context())
				case <-ctx.Done():
					t.Fatal("disabled connection admitted to HTTP/3", ctx.Err())
				}
			}
			var closeErr *upstream.ApplicationError
			if !errors.As(err, &closeErr) || !closeErr.Remote || closeErr.ErrorCode != 0x100 {
				t.Fatal("selected connection was not closed by HTTP/3 admission", err)
			}
		})
	}
}
