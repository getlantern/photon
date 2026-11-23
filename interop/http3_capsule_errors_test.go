package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	own "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peer "github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

// Keep diagnostics limited to event counters; no traffic secrets or payloads.
type capsulePolicyTrace struct {
	phase    atomic.Uint64
	received atomic.Uint64
	sent     atomic.Uint64
}

func (t *capsulePolicyTrace) AddProducer() qlogwriter.Recorder { return t }
func (t *capsulePolicyTrace) SupportsSchemas(string) bool      { return true }
func (t *capsulePolicyTrace) Close() error                     { return nil }
func (t *capsulePolicyTrace) RecordEvent(event qlogwriter.Event) {
	switch e := event.(type) {
	case qlog.KeyUpdated:
		t.phase.Store(uint64(e.KeyPhase))
	case qlog.PacketReceived:
		if e.Header.PacketType == qlog.PacketType1RTT {
			t.received.Add(1)
		}
	case qlog.PacketSent:
		if e.Header.PacketType == qlog.PacketType1RTT {
			t.sent.Add(1)
		}
	}
}

func TestCapsuleClientResponseRejection(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		paths := []string{"/type", "/length", "/204", "/205", "/206", "/rejection", "/informational", "/payload"}
		groups := make([][]string, 0, len(paths)+1)
		for _, path := range paths {
			groups = append(groups, []string{path})
		}
		groups = append(groups, paths)
		for index, paths := range groups {
			name := fmt.Sprintf("group%d", index)
			if len(paths) > 1 {
				name = "sequential"
			}
			t.Run(fmt.Sprintf("%x/%s", version, name), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				ct.NextProtos = []string{"h3"}
				st.NextProtos = []string{"h3"}
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				resets := make(chan error, 8)
				trace := new(capsulePolicyTrace)
				defer func() {
					t.Logf("final peer phase=%d received1RTT=%d sent1RTT=%d", trace.phase.Load(), trace.received.Load(), trace.sent.Load())
				}()
				server := &peer.Server{TLSConfig: st, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != "CONNECT" {
						io.WriteString(w, "healthy")
						return
					}
					status := 200
					switch r.URL.Path {
					case "/type":
						w.Header().Set("Content-Type", "application/octet-stream")
					case "/length":
						w.Header().Set("Content-Length", "0")
					case "/204":
						status = 204
					case "/205":
						status = 205
					case "/206":
						status = 206
					case "/rejection":
						status = 403
						w.Header().Set("Capsule-Protocol", "?1")
					case "/informational":
						w.Header().Set("Capsule-Protocol", "?1")
						w.WriteHeader(103)
						w.Header().Del("Capsule-Protocol")
					}
					w.WriteHeader(status)
					raw := peerHTTPStream(w)
					if r.URL.Path == "/payload" {
						raw.Write([]byte{0, 3, 'x'})
						raw.Close()
					}
					_, err := io.ReadAll(r.Body)
					resets <- err
					raw.Close()
				})}
				socket, err := net.ListenPacket("udp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				served := make(chan error, 1)
				go func() { served <- server.Serve(socket) }()
				defer func() { server.Close(); socket.Close(); <-served }()
				origin := "https://" + socket.LocalAddr().String()
				client, err := own.Dial(ctx, origin, ct, &quic.Config{Versions: []quic.Version{version}}, &own.ClientConfig{CapsuleProtocols: map[string]bool{"capsule-test": true}, MaxConcurrentRequests: 1})
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				for _, path := range paths {
					t.Logf("starting %s: peer phase=%d received1RTT=%d sent1RTT=%d", path, trace.phase.Load(), trace.received.Load(), trace.sent.Load())
					request, _ := http.NewRequestWithContext(ctx, "CONNECT", origin+path, nil)
					tunnel, response, err := client.OpenExtendedTunnel(request, "capsule-test")
					if path == "/payload" {
						if err != nil || tunnel == nil {
							t.Fatal("valid capsule handshake failed", err)
						}
						parser := own.NewCapsuleParser(tunnel)
						_, payload, parseErr := parser.Next()
						if parseErr == nil {
							parseErr = payload.Discard()
						}
						var malformed *own.CapsuleError
						if !errors.As(parseErr, &malformed) {
							t.Fatalf("wrong capsule error: %T %v", parseErr, parseErr)
						}
						if _, _, again := parser.Next(); again != parseErr {
							t.Fatal("capsule failure not sticky", again)
						}
						err = parseErr
						tunnel = nil
						response = nil // automatic abort owns cleanup; next GET proves release
					}
					if tunnel != nil {
						tunnel.Close()
					}
					if response != nil {
						response.Body.Close()
					}
					if err == nil || tunnel != nil || response != nil {
						t.Fatalf("%s: accepted malformed response: %v", path, err)
					}
					select {
					case err := <-resets:
						var reset *peer.Error
						if !errors.As(err, &reset) || reset.ErrorCode != 0x10e || !reset.Remote {
							t.Fatalf("%s: peer observed %T %v", path, err, err)
						}
					case <-ctx.Done():
						t.Fatalf("%s: %v; peer key phase=%d received1RTT=%d sent1RTT=%d", path, ctx.Err(), trace.phase.Load(), trace.received.Load(), trace.sent.Load())
					}
					// Every failed exchange must free the sole reservation and preserve QUIC.
					healthy, _ := http.NewRequestWithContext(ctx, "GET", origin+"/healthy", nil)
					response, err = client.RoundTrip(healthy)
					if err != nil {
						t.Fatalf("%s: healthy request failed: %v", path, err)
					}
					body, err := io.ReadAll(response.Body)
					response.Body.Close()
					if err != nil || string(body) != "healthy" {
						t.Fatal(path, string(body), err)
					}
				}
			})
		}
	}
}
