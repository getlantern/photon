package interop

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlogwriter"
)

// The peer never reads the control stream or grants more credit. Observing the
// blocked frame establishes that cancellation interrupts real startup work.
func TestHTTP3ServerContextBlockedStartup(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, block := range []string{"stream-count", "stream-bytes", "connection-bytes"} {
			for _, cancellation := range []string{"callback", "owner", "deadline"} {
				t.Run(fmt.Sprintf("%x/%s/%s", version, block, cancellation), func(t *testing.T) {
					ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
					defer stop()
					parent, cancelParent := context.WithCancelCause(ctx)
					defer cancelParent(context.Canceled)
					// Deliberately detach the callback result from the owner. The
					// server must retain both lifetimes even during critical setup.
					selected, cancelSelected := context.WithCancelCause(context.Background())
					defer cancelSelected(context.Canceled)
					countTrace := &streamsBlockedTrace{frames: make(chan blockedObservation, 8)}
					streamTrace := &streamDataBlockedTrace{frames: make(chan dataBlockedObservation, 8)}
					connTrace := &connectionDataBlockedTrace{frames: make(chan connectionBlockedObservation, 8)}
					var trace qlogwriter.Trace
					peerConfig := &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}
					switch block {
					case "stream-count":
						peerConfig.MaxIncomingUniStreams = -1
						trace = countTrace
					case "stream-bytes":
						peerConfig.InitialStreamReceiveWindow = 1
						peerConfig.MaxStreamReceiveWindow = 1
						trace = streamTrace
					case "connection-bytes":
						peerConfig.InitialConnectionReceiveWindow = 1
						peerConfig.MaxConnectionReceiveWindow = 1
						trace = connTrace
					}
					peerConfig.Tracer = func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }
					ct, st := tlsConfigs(t)
					ct.NextProtos, st.NextProtos = []string{"h3"}, []string{"h3"}
					listener, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{version}})
					if err != nil {
						t.Fatal(err)
					}
					defer listener.Close()
					peer, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, peerConfig)
					if err != nil {
						t.Fatal(err)
					}
					defer peer.CloseWithError(0, "")
					conn, err := listener.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer conn.CloseWithError(0, "")
					cause := errors.New("cancel blocked HTTP/3 setup")
					if cancellation == "deadline" {
						var cancelDeadline context.CancelFunc
						selected, cancelDeadline = context.WithTimeoutCause(selected, time.Second, cause)
						defer cancelDeadline()
					}
					var callbacks, requests atomic.Int32
					server := &ownh3.Server{
						ConnContext: func(context.Context, *quic.Conn) context.Context { callbacks.Add(1); return selected },
						Handler:     http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }),
					}
					result := make(chan error, 1)
					finished := make(chan struct{})
					go func() {
						defer close(finished)
						result <- server.ServeQUICConn(parent, conn)
					}()
					defer func() {
						cancelSelected(context.Canceled)
						server.Close()
						select {
						case <-finished:
						case <-time.After(time.Second):
							t.Error("startup worker did not stop")
						}
					}()
					select {
					case frame := <-countTrace.frames:
						if block != "stream-count" || !frame.uni || frame.limit != 0 {
							t.Fatalf("unexpected STREAMS_BLOCKED: %+v", frame)
						}
					case frame := <-streamTrace.frames:
						if block != "stream-bytes" || frame.id != 3 || frame.limit != 1 {
							t.Fatalf("unexpected STREAM_DATA_BLOCKED: %+v", frame)
						}
					case frame := <-connTrace.frames:
						if block != "connection-bytes" || frame.limit != 1 {
							t.Fatalf("unexpected DATA_BLOCKED: %+v", frame)
						}
					case err := <-result:
						t.Fatalf("startup ended before blocking: %v", err)
					case <-ctx.Done():
						t.Fatal("no blocked frame observed", ctx.Err())
					}
					if cancellation == "owner" {
						cancelParent(cause)
					} else if cancellation == "callback" {
						cancelSelected(cause)
					}
					select {
					case err := <-result:
						// Owner cancellation can close QUIC before the detached
						// context bridge runs. Callback cancellation has no such race.
						if err == nil || (cancellation != "owner" && !errors.Is(err, cause)) {
							t.Fatalf("startup cancellation: %v", err)
						}
					case <-ctx.Done():
						t.Fatal("startup remained blocked", ctx.Err())
					}
					if callbacks.Load() != 1 || requests.Load() != 0 {
						t.Fatalf("callbacks=%d requests=%d", callbacks.Load(), requests.Load())
					}
					if err := server.Shutdown(ctx); err != nil {
						t.Fatal("admission leaked after startup cancellation", err)
					}
					select {
					case <-conn.Context().Done():
					default:
						t.Fatal("canceled startup left QUIC open")
					}
				})
			}
		}
	}
}
