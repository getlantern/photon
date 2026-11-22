package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlogwriter"
)

// Exercise our advertised credit with an independent sender in both TLS roles.
func TestIncomingStreamLimitsUpstream(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, server := range []bool{false, true} {
			for _, limits := range [][2]int64{{2, 3}, {-1, 2}, {2, -1}, {64, 1}, {1, 64}, {100, 100}, {300, 300}} {
				t.Run(fmt.Sprintf("%x/server=%t/%d/%d", version, server, limits[0], limits[1]), func(t *testing.T) {
					if version == quic.Version2 && limits[0] == 300 && os.Getenv("QUIC_INTEROP_KNOWN_FAILURES") != "1" {
						t.Skip("pinned upstream v2 key-update mismatch; enable QUIC_INTEROP_KNOWN_FAILURES=1 for strict reproduction")
					}
					clientTLS, serverTLS := tlsConfigs(t)
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					cfg := &quic.Config{Versions: []quic.Version{version}, MaxIncomingStreams: limits[0], MaxIncomingUniStreams: limits[1]}
					trace := new(ecnSenderTrace)
					t.Cleanup(func() {
						if t.Failed() {
							t.Logf("upstream key phase: %d", trace.keyPhase.Load())
						}
					})
					peerCfg := &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }}
					var receiver *quic.Conn
					var sender *upstream.Conn
					if server {
						l, err := quic.ListenAddr("127.0.0.1:0", serverTLS, cfg)
						if err != nil {
							t.Fatal(err)
						}
						defer l.Close()
						sender, err = dialUpstreamAddr(t, ctx, l.Addr().String(), clientTLS, peerCfg)
						if err != nil {
							t.Fatal(err)
						}
						defer sender.CloseWithError(0, "")
						receiver, err = l.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
					} else {
						l, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, peerCfg)
						if err != nil {
							t.Fatal(err)
						}
						defer l.Close()
						receiver, err = quic.DialAddr(ctx, l.Addr().String(), clientTLS, cfg)
						if err != nil {
							t.Fatal(err)
						}
						defer receiver.CloseWithError(0, "")
						sender, err = l.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
						defer sender.CloseWithError(0, "")
					}
					var allBidi []*upstream.Stream
					var firstBidi *upstream.Stream
					for i := int64(0); i < max(limits[0], 0); i++ {
						s, err := sender.OpenStream()
						if err != nil {
							t.Fatalf("bidi %d: %v", i, err)
						}
						allBidi = append(allBidi, s)
						if firstBidi == nil {
							firstBidi = s
						}
					}
					assertLimited := func(err error) {
						t.Helper()
						var limit *upstream.StreamLimitReachedError
						if !errors.As(err, &limit) {
							t.Fatalf("expected upstream stream credit error, got %v", err)
						}
					}
					_, err := sender.OpenStream()
					assertLimited(err)
					var allUni []*upstream.SendStream
					var firstUni *upstream.SendStream
					for i := int64(0); i < max(limits[1], 0); i++ {
						s, err := sender.OpenUniStream()
						if err != nil {
							t.Fatalf("uni %d: %v", i, err)
						}
						allUni = append(allUni, s)
						if firstUni == nil {
							firstUni = s
						}
					}
					_, err = sender.OpenUniStream()
					assertLimited(err)
					for _, uni := range []bool{false, true} {
						blocked, stop := context.WithTimeout(ctx, 10*time.Millisecond)
						if uni {
							_, err = sender.OpenUniStreamSync(blocked)
						} else {
							_, err = sender.OpenStreamSync(blocked)
						}
						stop()
						if !errors.Is(err, context.DeadlineExceeded) {
							t.Fatalf("blocked open uni=%t: %v", uni, err)
						}
					}
					var receivedBidi *quic.Stream
					var receivedUni *quic.ReceiveStream
					if len(allBidi) > 64 && len(allUni) > 64 {
						// Keep both directions live together. Sending on every
						// higher ID exercises real receive allocation, not just
						// the peer's interpretation of transport parameters.
						for _, st := range allBidi[1:] {
							if _, err := st.Write([]byte("b")); err != nil {
								t.Fatal(err)
							}
						}
						for _, st := range allUni[1:] {
							if _, err := st.Write([]byte("u")); err != nil {
								t.Fatal(err)
							}
						}
						for i := range allBidi {
							st, err := receiver.AcceptStream(ctx)
							if err != nil {
								t.Fatal(err)
							}
							if i == 0 {
								receivedBidi = st
								continue
							}
							st.SetReadDeadline(time.Now().Add(time.Second))
							var b [1]byte
							if _, err := io.ReadFull(st, b[:]); err != nil || b[0] != 'b' {
								t.Fatalf("concurrent bidi %d: %q %v", i, b, err)
							}
						}
						for i := range allUni {
							st, err := receiver.AcceptUniStream(ctx)
							if err != nil {
								t.Fatal(err)
							}
							if i == 0 {
								receivedUni = st
								continue
							}
							st.SetReadDeadline(time.Now().Add(time.Second))
							var b [1]byte
							if _, err := io.ReadFull(st, b[:]); err != nil || b[0] != 'u' {
								t.Fatalf("concurrent uni %d: %q %v", i, b, err)
							}
						}
					}
					if firstBidi != nil {
						firstBidi.SetDeadline(time.Now().Add(time.Second))
						if _, err := firstBidi.Write([]byte("request")); err != nil {
							t.Fatal(err)
						}
						if err := firstBidi.Close(); err != nil {
							t.Fatal(err)
						}
						s, err := receivedBidi, error(nil)
						if s == nil {
							s, err = receiver.AcceptStream(ctx)
						}
						if err != nil {
							t.Fatal(err)
						}
						s.SetDeadline(time.Now().Add(time.Second))
						if b, err := io.ReadAll(s); err != nil || string(b) != "request" {
							t.Fatalf("request %q: %v", b, err)
						}
						if _, err := s.Write([]byte("response")); err != nil {
							t.Fatal(err)
						}
						if err := s.Close(); err != nil {
							t.Fatal(err)
						}
						if b, err := io.ReadAll(firstBidi); err != nil || string(b) != "response" {
							t.Fatalf("response %q: %v", b, err)
						}
						if _, err := sender.OpenStreamSync(ctx); err != nil {
							t.Fatalf("bidi replenishment: %v", err)
						}
						_, err = sender.OpenStream()
						assertLimited(err)
					}
					if firstUni != nil {
						firstUni.SetWriteDeadline(time.Now().Add(time.Second))
						if _, err := firstUni.Write([]byte("one way")); err != nil {
							t.Fatal(err)
						}
						if err := firstUni.Close(); err != nil {
							t.Fatal(err)
						}
						s, err := receivedUni, error(nil)
						if s == nil {
							s, err = receiver.AcceptUniStream(ctx)
						}
						if err != nil {
							t.Fatal(err)
						}
						s.SetReadDeadline(time.Now().Add(time.Second))
						if b, err := io.ReadAll(s); err != nil || string(b) != "one way" {
							t.Fatalf("uni %q: %v", b, err)
						}
						if _, err := sender.OpenUniStreamSync(ctx); err != nil {
							t.Fatalf("uni replenishment: %v", err)
						}
						_, err = sender.OpenUniStream()
						assertLimited(err)
					}
				})
			}
		}
	}
}
