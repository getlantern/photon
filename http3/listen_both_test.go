package http3

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
)

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
				tcp, err := net.Listen("tcp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				address := tcp.Addr().String()
				_, port, _ := net.SplitHostPort(address)
				servers := make(chan *Server, 1)
				done := make(chan error, 1)
				go func() {
					done <- serveTLSAndQUIC(tcp, credentials, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.ProtoMajor == 3 {
							select {
							case servers <- r.Context().Value(ServerContextKey).(*Server):
							default:
							}
						}
						io.WriteString(w, r.Proto)
					}))
				}()
				defer tcp.Close()
				defer func() {
					select {
					case err := <-done:
						done <- err
					case <-time.After(time.Second):
						t.Error("combined server did not join")
					}
				}()
				// Cleanup must stop admission before the join above even on an early failure.
				defer tcp.Close()
				for _, h2 := range []bool{false, true} {
					client := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, ForceAttemptHTTP2: h2}
					request, _ := http.NewRequestWithContext(ctx, "GET", "https://"+address+"/", nil)
					response, err := client.RoundTrip(request)
					if err != nil {
						client.CloseIdleConnections()
						t.Fatal(err)
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
