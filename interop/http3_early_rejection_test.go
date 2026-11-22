package interop

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	"github.com/getlantern/photon/internal/http3conn"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

// Exercise session ownership against real transport rejection. Remembered
// defaults are a fixture here: the rejecting peer cannot consume the early
// request. This does not establish a production HTTP ticket/settings policy.
func TestHTTP3EarlySessionRejectionRecovery(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			ct, st := tlsConfigs(t)
			ct.NextProtos, st.NextProtos = []string{"h3"}, []string{"h3"}
			cache := &observedSessionCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), stored: make(chan bool, 8)}
			ct.ClientSessionCache = cache
			gate := make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(gate) }) }
			// Release before testing cleanup joins the blocked server TLS callback.
			defer release()
			var hellos atomic.Int32
			st.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
				if hellos.Add(1) > 1 {
					select {
					case <-gate:
					case <-ctx.Done():
					}
				}
				return nil, nil
			}
			for round := 0; round < 2; round++ {
				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				arrival := &earlyArrivalObserver{PacketConn: socket, seen: make(chan struct{})}
				tr := &upstream.Transport{Conn: arrival}
				t.Cleanup(func() { socket.Close(); tr.Close() })
				listener, err := tr.ListenEarly(st, &upstream.Config{Allow0RTT: round == 0, Versions: []upstream.Version{upstream.Version(version)}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { listener.Close() })
				conn, err := quic.DialAddrEarly(ctx, listener.Addr().String(), ct, &quic.Config{Versions: []quic.Version{version}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { conn.CloseWithError(0, "") })
				if round == 0 {
					if _, err := listener.Accept(ctx); err != nil {
						t.Fatal(err)
					}
					waitEarlyTicket(t, ctx, cache)
					conn.CloseWithError(0, "seed complete")
					continue
				}
				select {
				case <-conn.HandshakeComplete():
					t.Fatal("resumed dial was not early")
				default:
				}
				transport := http3conn.QUICTransport{Conn: conn}
				local, err := http3conn.OpenLocal(ctx, transport, nil)
				if err != nil {
					t.Fatal(err)
				}
				remembered := []h3.Setting{}
				session, err := http3conn.StartSession(ctx, transport, local, http3conn.SessionConfig{
					PeerIsServer: true, EarlyPeerSettings: &remembered,
					Decoder:          qpack.DecoderLimits{MaxBufferedBytes: 4096, MaxEncodedSection: 4096, MaxDecodedSection: 4096, MaxFields: 32, MaxInstructionLiteralBytes: 1024},
					Encoder:          http3conn.EncoderLimits{MaxSections: 32, MaxReferences: 64, MaxEncodedSection: 4096, MaxFields: 32, MaxInstruction: 1024, MaxFieldSection: 4096},
					MaxActiveStreams: 4, MaxSettingsBytes: 1024, MaxSettingsEntries: 16, MaxEncoderInstruction: 1024,
					Control: func(context.Context, h3.ControlEvent) error { return nil },
				})
				if err != nil {
					t.Fatal(err)
				}
				defer session.Stop(context.Canceled)
				encoder, err := session.WaitEncoder(ctx)
				if err != nil {
					t.Fatal(err)
				}
				early, err := conn.OpenStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				block, err := encoder.Encode(uint64(early.StreamID()), []qpack.DecodedField{
					{Field: qpack.Field{Name: ":method", Value: "GET"}}, {Field: qpack.Field{Name: ":scheme", Value: "https"}},
					{Field: qpack.Field{Name: ":authority", Value: listener.Addr().String()}}, {Field: qpack.Field{Name: ":path", Value: "/early"}},
				})
				if err != nil {
					t.Fatal(err)
				}
				frame, err := h3.AppendHeader(nil, h3.Header{Type: h3.Headers, Length: uint64(len(block))})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := early.Write(append(frame, block...)); err != nil {
					t.Fatal(err)
				}
				if err := early.Close(); err != nil {
					t.Fatal(err)
				}
				select {
				case <-arrival.seen:
				case <-ctx.Done():
					t.Fatal("no 0-RTT wire packet", ctx.Err())
				}
				release()
				select {
				case <-session.Done():
				case <-ctx.Done():
					t.Fatal("HTTP session did not stop on rejection", ctx.Err())
				}
				if _, err := session.WaitEncoder(ctx); !errors.Is(err, quic.Err0RTTRejected) {
					t.Fatal("session rejection cause", err)
				}
				if conn.Context().Err() != nil {
					t.Fatal("session closed QUIC", context.Cause(conn.Context()))
				}
				recovered, err := conn.NextConnection(ctx)
				if err != nil || recovered != conn {
					t.Fatal("QUIC recovery", err)
				}
				if !conn.ConnectionState().TLS.DidResume || conn.ConnectionState().Used0RTT {
					t.Fatal("unexpected TLS/early state", conn.ConnectionState())
				}
				if err := early.WaitAcknowledged(ctx); !errors.Is(err, quic.Err0RTTRejected) {
					t.Fatal("stale request handle", err)
				}
				peerConn, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				var requests atomic.Int32
				server := &peerh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests.Add(1)
					if r.URL.Path != "/fresh" {
						http.Error(w, "unexpected request", 500)
						return
					}
					w.Write([]byte("fresh HTTP session"))
				})}
				done := make(chan struct{})
				go func() { defer close(done); server.ServeQUICConn(peerConn) }()
				t.Cleanup(func() { peerConn.CloseWithError(0, ""); server.Close(); <-done })
				origin := "https://" + listener.Addr().String()
				client, err := ownh3.NewClientConn(ctx, recovered, origin, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				// Obsolete cancellation must not reset new streams with reused IDs.
				early.CancelRead(42)
				early.CancelWrite(42)
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/fresh", nil)
				if err != nil {
					t.Fatal(err)
				}
				response, err := client.RoundTrip(request)
				if err != nil {
					t.Fatal("fresh HTTP request", err)
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || response.StatusCode != 200 || string(body) != "fresh HTTP session" {
					t.Fatal(response.StatusCode, string(body), err)
				}
				if requests.Load() != 1 {
					t.Fatal("rejected request reached HTTP handler", requests.Load())
				}
			}
		})
	}
}
