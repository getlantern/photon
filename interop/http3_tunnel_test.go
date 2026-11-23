package interop

import (
	"bytes"
	"context"
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

func TestHTTPConnectTunnelIndependentServer(t *testing.T) {
	for _, extended := range []bool{false, true} {
		for _, version := range []quic.Version{quic.Version1, quic.Version2} {
			for _, host := range []string{"127.0.0.1", "::1"} {
				t.Run(fmt.Sprintf("%x/%s/extended=%t", version, host, extended), func(t *testing.T) {
					ct, st := tlsConfigs(t)
					ct.NextProtos = []string{"h3"}
					st.NextProtos = []string{"h3"}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					handlerErrors := make(chan error, 8)
					halfClosed := make(chan []byte, 1)
					server := &peerh3.Server{TLSConfig: st, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.Method == "CONNECT" && extended && (r.Proto != "echo-test" || r.URL.Path != "/echo" || r.URL.RawQuery != "q=1") {
							handlerErrors <- fmt.Errorf("wrong Extended CONNECT metadata: %s %s", r.Proto, r.URL)
							return
						}
						if r.Method != "CONNECT" {
							io.WriteString(w, "ordinary")
							return
						}
						if r.Host == "denied.example:443" {
							w.WriteHeader(403)
							io.WriteString(w, "denied")
							return
						}
						if r.Host == "receive-only.example:443" {
							// Upstream refuses HTTPStream after body bytes; send them on the stream instead.
							w.WriteHeader(200)
							raw := peerHTTPStream(w)
							if _, err := raw.Write([]byte("first")); err != nil {
								handlerErrors <- err
								return
							}
							if err := raw.Close(); err != nil {
								handlerErrors <- err
								return
							}
							data, err := io.ReadAll(r.Body)
							if err != nil {
								handlerErrors <- err
								return
							}
							halfClosed <- data
							return
						}
						if r.Host != "echo.example:443" {
							handlerErrors <- fmt.Errorf("wrong tunnel authority: %s", r.Host)
							return
						}
						w.WriteHeader(200)
						w.(http.Flusher).Flush()
						b := make([]byte, 4096)
						for {
							n, err := r.Body.Read(b)
							if n > 0 {
								if _, e := w.Write(b[:n]); e != nil {
									handlerErrors <- e
									return
								}
								w.(http.Flusher).Flush()
							}
							if err == io.EOF {
								_, e := io.WriteString(w, "tail")
								handlerErrors <- e
								return
							}
							if err != nil {
								return
							} // canceled tunnel is tested below
						}
					})}
					socket, err := net.ListenPacket("udp", net.JoinHostPort(host, "0"))
					if err != nil {
						t.Fatal(err)
					}
					served := make(chan error, 1)
					go func() { served <- server.Serve(socket) }()
					defer func() { server.Close(); socket.Close(); <-served }()
					origin := "https://" + socket.LocalAddr().String()
					client, err := ownh3.Dial(ctx, origin, ct, &quic.Config{Versions: []quic.Version{version}}, &ownh3.ClientConfig{MaxConcurrentRequests: 1})
					if err != nil {
						t.Fatal(err)
					}
					defer client.Close()
					openTunnel := client.OpenTunnel
					if extended {
						openTunnel = func(r *http.Request) (*ownh3.Tunnel, *http.Response, error) {
							return client.OpenExtendedTunnel(r, "echo-test")
						}
					}
					request, _ := http.NewRequestWithContext(ctx, "CONNECT", origin+"/echo?q=1", nil)
					request.Host = "echo.example:443"
					tunnel, response, err := openTunnel(request)
					if err != nil {
						t.Fatal(err)
					}
					if tunnel == nil || response.StatusCode != 200 || response.Body != tunnel || response.Request != request {
						t.Fatal("missing tunnel", response)
					}
					defer tunnel.Close()
					size := 11000
					payload := bytes.Repeat([]byte("tunnel"), size)
					sent := make(chan error, 1)
					go func() {
						_, err := tunnel.Write(payload)
						if err == nil {
							err = tunnel.CloseWrite()
						}
						sent <- err
					}()
					got, err := io.ReadAll(tunnel)
					if err != nil || !bytes.Equal(got, append(append([]byte(nil), payload...), []byte("tail")...)) {
						t.Fatal("tunnel echo", len(got), err)
					}
					if err := <-sent; err != nil {
						t.Fatal(err)
					}
					if err := tunnel.Close(); err != nil {
						t.Fatal(err)
					}
					select {
					case err := <-handlerErrors:
						if err != nil {
							t.Fatal(err)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					// The one-request reservation must be freed, including after peer EOF.
					denied := request.Clone(ctx)
					denied.Host = "denied.example:443"
					tunnel, response, err = openTunnel(denied)
					if err != nil || tunnel != nil || response.StatusCode != 403 {
						t.Fatal("rejection", response, err)
					}
					got, err = io.ReadAll(response.Body)
					if err != nil || string(got) != "denied" {
						t.Fatal(string(got), err)
					}
					if err := response.Body.Close(); err != nil {
						t.Fatal(err)
					}
					halfRequest := request.Clone(ctx)
					halfRequest.Host = "receive-only.example:443"
					tunnel, _, err = openTunnel(halfRequest)
					if err != nil {
						t.Fatal(err)
					}
					got, err = io.ReadAll(tunnel)
					if err != nil || string(got) != "first" {
						t.Fatal("peer half close", string(got), err)
					}
					if _, err := tunnel.Write([]byte("after peer FIN")); err != nil {
						t.Fatal(err)
					}
					if err := tunnel.CloseWrite(); err != nil {
						t.Fatal(err)
					}
					select {
					case data := <-halfClosed:
						if string(data) != "after peer FIN" {
							t.Fatal(string(data))
						}
					case err := <-handlerErrors:
						t.Fatal(err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if err := tunnel.Close(); err != nil {
						t.Fatal(err)
					}
					tunnelCtx, stop := context.WithCancel(ctx)
					tunnel, response, err = openTunnel(request.Clone(tunnelCtx))
					if err != nil {
						t.Fatal(err)
					}
					readDone := make(chan error, 1)
					go func() { _, err := tunnel.Read(make([]byte, 1)); readDone <- err }()
					stop()
					select {
					case err := <-readDone:
						if err != context.Canceled {
							t.Fatal("canceled read", err)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if _, err := tunnel.Write([]byte("late")); err != context.Canceled {
						t.Fatal("canceled write", err)
					}
					tunnel.Close()
					get, _ := http.NewRequestWithContext(ctx, "GET", origin, nil)
					response, err = client.RoundTrip(get)
					if err != nil {
						t.Fatal("request after tunnels", err)
					}
					got, err = io.ReadAll(response.Body)
					if err != nil || string(got) != "ordinary" {
						t.Fatal(string(got), err)
					}
					response.Body.Close()
				})
			}
		}
	}
}

func TestHTTPConnectTunnelIndependentClient(t *testing.T) {
	for _, extended := range []bool{false, true} {
		for _, version := range []quic.Version{quic.Version1, quic.Version2} {
			for _, host := range []string{"127.0.0.1", "::1"} {
				t.Run(fmt.Sprintf("%x/%s/extended=%t", version, host, extended), func(t *testing.T) {
					ct, st := tlsConfigs(t)
					ct.NextProtos = []string{"h3"}
					st.NextProtos = []string{"h3"}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					listener, err := quic.ListenAddr(net.JoinHostPort(host, "0"), st, &quic.Config{Versions: []quic.Version{version}})
					if err != nil {
						t.Fatal(err)
					}
					defer listener.Close()
					results := make(chan error, 4)
					server := &ownh3.Server{MaxConcurrentRequests: 1, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.Method == "CONNECT" && extended && (ownh3.ExtendedConnectProtocol(r) != "echo-test" || r.Proto != "HTTP/3.0" || r.URL.Path != "/echo" || r.URL.RawQuery != "q=1") {
							results <- fmt.Errorf("wrong incoming Extended CONNECT metadata")
							return
						}
						if r.Method != "CONNECT" {
							io.WriteString(w, "ordinary")
							return
						}
						tunnel, err := ownh3.AcceptTunnel(w)
						if err != nil {
							results <- err
							return
						}
						if r.Host == "half.example:443" {
							if _, err = tunnel.Write([]byte("first")); err == nil {
								err = tunnel.CloseWrite()
							}
							if err != nil {
								results <- err
								return
							}
							data, e := io.ReadAll(tunnel)
							if e == nil && string(data) != "after FIN" {
								e = fmt.Errorf("unexpected late upload %q", data)
							}
							results <- e
							return
						}
						if _, err = io.Copy(tunnel, tunnel); err == nil {
							_, err = tunnel.Write([]byte("tail"))
						}
						results <- err
					})}
					if extended {
						server.ExtendedConnectHandlers = map[string]http.Handler{"echo-test": server.Handler}
					}
					served := make(chan error, 1)
					go func() { served <- server.Serve(ctx, listener) }()
					defer func() { server.Close(); cancel(); <-served }()
					peer, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
					if err != nil {
						t.Fatal(err)
					}
					defer peer.CloseWithError(0, "done")
					peerTransport := &peerh3.Transport{}
					peerClient := peerTransport.NewClientConn(peer)
					origin := "https://" + listener.Addr().String()
					for _, half := range []bool{false, true} {
						stream, err := peerClient.OpenRequestStream(ctx)
						if err != nil {
							t.Fatal(err)
						}
						request, _ := http.NewRequestWithContext(ctx, "CONNECT", origin+"/echo?q=1", nil)
						if extended {
							request.Proto = "echo-test"
						}
						request.Host = "echo.example:443"
						if half {
							request.Host = "half.example:443"
						}
						if err := stream.SendRequestHeader(request); err != nil {
							t.Fatal(err)
						}
						response, err := stream.ReadResponse()
						if err != nil || response.StatusCode != 200 {
							t.Fatal(response, err)
						}
						if half {
							data, err := io.ReadAll(stream)
							if err != nil || string(data) != "first" {
								t.Fatal(string(data), err)
							}
							if _, err := stream.Write([]byte("after FIN")); err != nil {
								t.Fatal(err)
							}
							if err := stream.Close(); err != nil {
								t.Fatal(err)
							}
						} else {
							payload := bytes.Repeat([]byte("reverse tunnel"), 700)
							sent := make(chan error, 1)
							go func() {
								_, err := stream.Write(payload)
								if err == nil {
									err = stream.Close()
								}
								sent <- err
							}()
							data, err := io.ReadAll(stream)
							if err != nil || !bytes.Equal(data, append(append([]byte(nil), payload...), []byte("tail")...)) {
								t.Fatal("reverse echo", len(data), err)
							}
							if err := <-sent; err != nil {
								t.Fatal(err)
							}
						}
						select {
						case err := <-results:
							if err != nil {
								t.Fatal(err)
							}
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
					}
					if extended {
						stream, err := peerClient.OpenRequestStream(ctx)
						if err != nil {
							t.Fatal(err)
						}
						unknown, _ := http.NewRequestWithContext(ctx, "CONNECT", origin+"/unknown", nil)
						unknown.Proto = "unknown-protocol"
						if err := stream.SendRequestHeader(unknown); err != nil {
							t.Fatal(err)
						}
						if err := stream.Close(); err != nil {
							t.Fatal(err)
						}
						response, err := stream.ReadResponse()
						if err != nil || response.StatusCode != 501 {
							t.Fatal("unknown protocol response", response, err)
						}
						if _, err := io.ReadAll(stream); err != nil {
							t.Fatal(err)
						}
					}
					request, _ := http.NewRequestWithContext(ctx, "GET", origin, nil)
					response, err := peerClient.RoundTrip(request)
					if err != nil {
						t.Fatal(err)
					}
					data, err := io.ReadAll(response.Body)
					if err != nil || string(data) != "ordinary" {
						t.Fatal(string(data), err)
					}
					response.Body.Close()
				})
			}
		}
	}
}
