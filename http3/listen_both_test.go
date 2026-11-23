package http3

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
)

// listenTCPWithFreeUDP returns a TCP listener whose port is also free for UDP.
// serveTLSAndQUIC binds QUIC to the TCP port number and stops, closing the TCP
// listener, if another socket holds it. A free TCP port says nothing about UDP,
// and test binaries running alongside bind ephemeral UDP ports all the time.
func listenTCPWithFreeUDP(t *testing.T) net.Listener {
	t.Helper()
	for range 20 {
		tcp, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		probe, err := net.ListenPacket("udp4", tcp.Addr().String())
		if err == nil {
			probe.Close()
			return tcp
		}
		tcp.Close()
	}
	t.Fatal("no port free for both TCP and UDP")
	return nil
}

func TestCombinedTLSAndQUIC(t *testing.T) {
	origin := httptest.NewTLSServer(nil)
	credentials := origin.TLS.Clone()
	// httptest pins ALPN to http/1.1, which would keep the TCP server off HTTP/2.
	credentials.NextProtos = nil
	origin.Close()
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, stopQUIC := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stop-quic=%v", version, stopQUIC), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				servers := make(chan *Server, 1)
				handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.ProtoMajor == 3 {
						select {
						case servers <- r.Context().Value(ServerContextKey).(*Server):
						default:
						}
					}
					io.WriteString(w, r.Proto)
				})
				var tcp net.Listener
				var done chan error
				var address, port string
				start := func() {
					tcp = listenTCPWithFreeUDP(t)
					address = tcp.Addr().String()
					_, port, _ = net.SplitHostPort(address)
					done = make(chan error, 1)
					go func(tcp net.Listener, done chan error) { done <- serveTLSAndQUIC(tcp, credentials, handler) }(tcp, done)
				}
				start()
				defer func() { tcp.Close() }()
				defer func() {
					select {
					case err := <-done:
						done <- err
					case <-time.After(time.Second):
						t.Error("combined server did not join")
					}
				}()
				// Cleanup must stop admission before the join above even on an early failure.
				defer func() { tcp.Close() }()
				restarts := 0
				for i := 0; i < 2; i++ {
					h2 := i == 1
					client := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, ForceAttemptHTTP2: h2}
					request, _ := http.NewRequestWithContext(ctx, "GET", "https://"+address+"/", nil)
					response, err := client.RoundTrip(request)
					if err != nil {
						client.CloseIdleConnections()
						// The request already failed. If the server stopped, its own
						// error says why, and it may still be on its way.
						select {
						case serveErr := <-done:
							// Another socket can take the UDP port after the probe
							// released it and before the server bound it. Start over
							// on a fresh port; any other startup failure is fatal.
							if i == 0 && restarts < 5 && errors.Is(serveErr, syscall.EADDRINUSE) {
								restarts++
								tcp.Close()
								start()
								i--
								continue
							}
							done <- serveErr
							t.Fatal(err, "; server stopped:", serveErr)
						case <-time.After(time.Second):
							t.Fatal(err)
						}
					}
					body, err := io.ReadAll(response.Body)
					response.Body.Close()
					client.CloseIdleConnections()
					want := "HTTP/1.1"
					if h2 {
						want = "HTTP/2.0"
					}
					if err != nil || string(body) != want || response.Header.Get("Alt-Svc") != fmt.Sprintf(`h3=":%s"; ma=2592000`, port) {
						t.Fatal("HTTPS response", string(body), response.Header, err)
					}
				}
				client, err := Dial(ctx, "https://"+address, &tls.Config{InsecureSkipVerify: true}, &quic.Config{Versions: []quic.Version{version}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				request, _ := http.NewRequestWithContext(ctx, "GET", "https://"+address+"/", nil)
				response, err := client.RoundTrip(request)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || string(body) != "HTTP/3.0" {
					t.Fatal("HTTP3 response", string(body), err)
				}
				if stopQUIC {
					select {
					case server := <-servers:
						server.Close()
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				} else {
					tcp.Close()
				}
				select {
				case err := <-done:
					done <- err
					if err == nil {
						t.Fatal("serve returned nil")
					}
				case <-ctx.Done():
					t.Fatal("combined server did not stop", ctx.Err())
				}
				udp, err := net.ListenPacket("udp4", address)
				if err != nil {
					t.Fatal("UDP retained", err)
				}
				udp.Close()
				rebound, err := net.Listen("tcp4", address)
				if err != nil {
					t.Fatal("TCP retained", err)
				}
				rebound.Close()
			})
		}
	}
}

func TestCombinedStartupFailure(t *testing.T) {
	if err := ListenAndServeTLS("127.0.0.1:0", filepath.Join(t.TempDir(), "missing"), "missing", nil); err == nil {
		t.Fatal("missing credentials accepted")
	}
	origin := httptest.NewTLSServer(nil)
	credentials := origin.TLS.Clone()
	origin.Close()
	tcp, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := tcp.Addr().String()
	blocker, err := net.ListenPacket("udp4", address)
	if err != nil {
		tcp.Close()
		t.Fatal(err)
	}
	defer blocker.Close()
	if err := serveTLSAndQUIC(tcp, credentials, nil); err == nil {
		t.Fatal("UDP conflict ignored")
	}
	rebound, err := net.Listen("tcp4", address)
	if err != nil {
		t.Fatal("failed startup retained TCP", err)
	}
	rebound.Close()
}
