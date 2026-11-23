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
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

// The rejecting peer reduces stream count or per-stream byte credit. Reading
// fresh critical prefixes proves handoff occurred; setup then blocks on opening
// another stream or finishing SETTINGS. No production hook selects the race.
func TestHTTP3RecoveryCancellationDuringSetup(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, block := range []struct {
			name     string
			credit   int64
			window   uint64
			prefixes int64
		}{
			{"encoder-open", 1, 0, 1}, {"decoder-open", 2, 0, 2}, {"settings-write", 3, 1, 1},
		} {
			t.Run(fmt.Sprintf("%x/%s", version, block.name), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				ct, st := tlsConfigs(t)
				st.NextProtos = []string{"h3"}
				cache := &observedSessionCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), stored: make(chan bool, 8)}
				ct.ClientSessionCache = cache
				gate := make(chan struct{})
				var once sync.Once
				release := func() { once.Do(func() { close(gate) }) }
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
				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				arrival := &earlyArrivalObserver{PacketConn: socket, seen: make(chan struct{})}
				transport := &upstream.Transport{Conn: arrival}
				t.Cleanup(func() { socket.Close(); transport.Close() })
				origin := "https://" + socket.LocalAddr().String()
				seedListener, err := transport.ListenEarly(st, &upstream.Config{Allow0RTT: true, Versions: []upstream.Version{upstream.Version(version)}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { seedListener.Close() })
				server := &peerh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("seed")) })}
				done := make(chan struct{})
				go func() {
					defer close(done)
					peer, err := seedListener.Accept(ctx)
					if err == nil {
						server.ServeQUICConn(peer)
					}
				}()
				t.Cleanup(func() { server.Close(); seedListener.Close(); <-done })
				seed, err := ownh3.DialEarly(ctx, origin, ct, &quic.Config{Versions: []quic.Version{version}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { seed.Close() })
				request, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/", nil)
				response, err := seed.RoundTrip(request)
				if err != nil {
					t.Fatal(err)
				}
				_, err = io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				waitEarlyTicket(t, ctx, cache)
				seed.Close()
				server.Close()
				seedListener.Close()
				<-done

				listener, err := transport.ListenEarly(st, &upstream.Config{MaxIncomingUniStreams: block.credit, InitialStreamReceiveWindow: block.window, MaxStreamReceiveWindow: block.window, Versions: []upstream.Version{upstream.Version(version)}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { listener.Close() })
				client, err := ownh3.DialEarly(ctx, origin, ct, &quic.Config{Versions: []quic.Version{version}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { client.Close() })
				select {
				case <-arrival.seen:
				case <-ctx.Done():
					t.Fatal("no early packet", ctx.Err())
				}
				release()
				peer, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { peer.CloseWithError(0, "") })
				cause := errors.New("cancel fresh HTTP critical stream setup")
				recoveryCtx, stopRecovery := context.WithCancelCause(ctx)
				defer stopRecovery(context.Canceled)
				type result struct {
					client *ownh3.ClientConn
					err    error
				}
				recovered := make(chan result, 1)
				go func() { fresh, err := client.NextConnection(recoveryCtx); recovered <- result{fresh, err} }()
				for i := int64(0); i < block.prefixes; i++ {
					stream, err := peer.AcceptUniStream(ctx)
					if err != nil {
						t.Fatal("fresh critical stream not opened", err)
					}
					stream.SetReadDeadline(time.Now().Add(time.Second))
					var prefix [1]byte
					expected := byte(0)
					if i == 1 {
						expected = 2
					}
					if _, err := io.ReadFull(stream, prefix[:]); err != nil || prefix[0] != expected {
						t.Fatal("fresh critical prefix", prefix, err)
					}
				}
				// The first caller holds recovery serialization while blocked in OpenLocal.
				// A second caller must still be able to cancel its own serialization wait.
				waitCause := errors.New("cancel queued recovery")
				waitCtx, stopWait := context.WithTimeoutCause(ctx, 20*time.Millisecond, waitCause)
				next, waitErr := client.NextConnection(waitCtx)
				stopWait()
				if next != nil || !errors.Is(waitErr, waitCause) {
					t.Fatal("queued cancellation", next, waitErr)
				}
				select {
				case got := <-recovered:
					t.Fatal("setup did not stay blocked", got.err)
				default:
				}
				stopRecovery(cause)
				select {
				case got := <-recovered:
					if got.client != nil || !errors.Is(got.err, cause) {
						t.Fatal("recovery cancellation cause", got.client, got.err)
					}
				case <-ctx.Done():
					t.Fatal("canceled setup did not join", ctx.Err())
				}
				select {
				case <-peer.Context().Done():
				case <-ctx.Done():
					t.Fatal("partial HTTP setup left QUIC alive", ctx.Err())
				}
				for i := 0; i < 8; i++ {
					next, err := client.NextConnection(ctx)
					if next != nil || !errors.Is(err, cause) {
						t.Fatal("terminal recovery result changed", next, err)
					}
				}
			})
		}
	}
}
