package interop

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

type benchmarkStream interface {
	io.ReadWriteCloser
	SetDeadline(time.Time) error
}

// echoWindows fixes both servers' receive windows: initial and maximum are
// equal, so window autotuning plays no part in a comparison.
type echoWindows struct{ stream, connection uint64 }

// echoServer is a photon (independent) or quic-go server that echoes each
// accepted stream back to its sender.
type echoServer struct {
	addr   string
	accept func() (func(context.Context) (benchmarkStream, error), func(), error)
	close  func()
}

func startEchoServer(b *testing.B, ctx context.Context, independent bool, serverTLS *tls.Config, w echoWindows) echoServer {
	b.Helper()
	if independent {
		l, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{Versions: []quic.Version{quic.Version1}, InitialStreamReceiveWindow: w.stream, MaxStreamReceiveWindow: w.stream, InitialConnectionReceiveWindow: w.connection, MaxConnectionReceiveWindow: w.connection, MaxIncomingStreams: 16, MaxIncomingUniStreams: 16})
		if err != nil {
			b.Fatal(err)
		}
		return echoServer{addr: l.Addr().String(), close: func() { l.Close() }, accept: func() (func(context.Context) (benchmarkStream, error), func(), error) {
			c, err := l.Accept(ctx)
			if err != nil {
				return nil, nil, err
			}
			return func(ctx context.Context) (benchmarkStream, error) { return c.AcceptStream(ctx) }, func() { c.CloseWithError(0, "") }, nil
		}}
	}
	// Match receive windows and explicitly pin the same stream-count policy
	// on both servers, independently of changing public defaults.
	l, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version1}, InitialStreamReceiveWindow: w.stream, MaxStreamReceiveWindow: w.stream, InitialConnectionReceiveWindow: w.connection, MaxConnectionReceiveWindow: w.connection, MaxIncomingStreams: 16, MaxIncomingUniStreams: 16})
	if err != nil {
		b.Fatal(err)
	}
	return echoServer{addr: l.Addr().String(), close: func() { l.Close() }, accept: func() (func(context.Context) (benchmarkStream, error), func(), error) {
		c, err := l.Accept(ctx)
		if err != nil {
			return nil, nil, err
		}
		return func(ctx context.Context) (benchmarkStream, error) { return c.AcceptStream(ctx) }, func() { c.CloseWithError(0, "") }, nil
	}}
}

// runEcho times b.N exchanges on peer: each opens a stream and sends payload
// while reading the identical echo concurrently, then checks every byte.
func runEcho(b *testing.B, ctx context.Context, peer *upstream.Conn, accept func(context.Context) (benchmarkStream, error), payload []byte, deadline time.Duration) {
	b.Helper()
	serverDone := make(chan error, 1)
	ready := make(chan struct{})
	go func() {
		buf := make([]byte, 32*1024)
		close(ready)
		for i := 0; i < b.N; i++ {
			s, err := accept(ctx)
			if err != nil {
				serverDone <- err
				return
			}
			s.SetDeadline(time.Now().Add(deadline))
			if _, err := io.CopyBuffer(s, s, buf); err != nil {
				serverDone <- err
				return
			}
			if err := s.Close(); err != nil {
				serverDone <- err
				return
			}
		}
		serverDone <- nil
	}()
	response := make([]byte, len(payload))
	sent := make(chan error, 1)
	b.SetBytes(int64(2 * len(payload)))
	b.ReportAllocs()
	<-ready
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := peer.OpenStreamSync(ctx)
		if err != nil {
			b.Fatal(err)
		}
		s.SetDeadline(time.Now().Add(deadline))
		go func() {
			_, err := s.Write(payload)
			if err == nil {
				err = s.Close()
			}
			sent <- err
		}()
		if _, err := io.ReadFull(s, response); err != nil {
			b.Fatal(err)
		}
		var tail [1]byte
		if n, err := s.Read(tail[:]); n != 0 || err != io.EOF {
			b.Fatal("echo FIN", n, err)
		}
		if err := <-sent; err != nil {
			b.Fatal(err)
		}
		if !bytes.Equal(payload, response) {
			b.Fatal(fmt.Sprintf("echo mismatch at iteration %d", i))
		}
	}
	b.StopTimer()
	if err := <-serverDone; err != nil {
		b.Fatal(err)
	}
}

func serverName(independent bool) string {
	if independent {
		return "independent"
	}
	return "upstream"
}

// BenchmarkServerStreamEcho compares public server APIs against the same
// upstream client. Setup/handshake are excluded; each operation opens a stream
// and exchanges a 1 MiB request plus identical response concurrently.
func BenchmarkServerStreamEcho(b *testing.B) {
	for _, independent := range []bool{true, false} {
		b.Run(serverName(independent), func(b *testing.B) {
			clientTLS, serverTLS := tlsConfigs(b)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			server := startEchoServer(b, ctx, independent, serverTLS, echoWindows{stream: 65536, connection: 1 << 20})
			defer server.close()
			dialCtx, stop := context.WithTimeout(ctx, 5*time.Second)
			peer, err := dialUpstreamAddr(b, dialCtx, server.addr, clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version1}})
			stop()
			if err != nil {
				b.Fatal(err)
			}
			defer peer.CloseWithError(0, "")
			accept, closeServer, err := server.accept()
			if err != nil {
				b.Fatal(err)
			}
			defer closeServer()
			runEcho(b, ctx, peer, accept, bytes.Repeat([]byte("0123456789abcdef"), 65536), 30*time.Second)
		})
	}
}

// wanScenarios are symmetric paths: each link is one direction.
var wanScenarios = []struct {
	name string
	link wanLink
}{
	// 100 Mbit/s with a 50 ms queue, about one bandwidth-delay product.
	{"rtt50ms", wanLink{delay: 25 * time.Millisecond, rate: 100e6 / 8, queue: 50 * time.Millisecond}},
	{"rtt50ms-loss1pct", wanLink{delay: 25 * time.Millisecond, loss: 0.01, rate: 100e6 / 8, queue: 50 * time.Millisecond}},
	// A slower, longer path: 20 Mbit/s with a 100 ms queue.
	{"rtt100ms-loss0.5pct-20mbit", wanLink{delay: 50 * time.Millisecond, loss: 0.005, rate: 20e6 / 8, queue: 100 * time.Millisecond}},
}

// BenchmarkServerStreamEchoWAN runs the echo through a relay that simulates a
// wide-area path, where ACK timing, congestion control and loss recovery
// matter more than CPU. Each exchange is 4 MiB each way. Receive windows are
// large enough not to limit either server: 8 MiB per stream, 16 MiB in all.
// Run with a fixed -benchtime, such as -benchtime=5x, since exchanges are slow.
//
// Besides time per exchange, it reports, per exchange: QUIC packets the
// client received and processed from the server (srv-pkts/op; after the
// handshake, about one per datagram), packets the client declared lost
// (cli-lost/op), and datagrams the relay dropped toward the client
// (drop-down/op) and toward the server (drop-up/op).
func BenchmarkServerStreamEchoWAN(b *testing.B) {
	windows := echoWindows{stream: 8 << 20, connection: 16 << 20}
	for _, scenario := range wanScenarios {
		for _, independent := range []bool{true, false} {
			b.Run(scenario.name+"/"+serverName(independent), func(b *testing.B) {
				clientTLS, serverTLS := tlsConfigs(b)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				server := startEchoServer(b, ctx, independent, serverTLS, windows)
				defer server.close()
				relay := newWANRelay(b, server.addr, scenario.link, scenario.link)
				dialCtx, stop := context.WithTimeout(ctx, 30*time.Second)
				peer, err := dialUpstreamAddr(b, dialCtx, relay.addr(), clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version1}, InitialStreamReceiveWindow: windows.stream, MaxStreamReceiveWindow: windows.stream, InitialConnectionReceiveWindow: windows.connection, MaxConnectionReceiveWindow: windows.connection})
				stop()
				if err != nil {
					b.Fatal(err)
				}
				defer peer.CloseWithError(0, "")
				accept, closeServer, err := server.accept()
				if err != nil {
					b.Fatal(err)
				}
				defer closeServer()
				before := peer.ConnectionStats()
				up, down := relay.dropped[0].Load(), relay.dropped[1].Load()
				runEcho(b, ctx, peer, accept, bytes.Repeat([]byte("0123456789abcdef"), 4<<16), 5*time.Minute)
				after := peer.ConnectionStats()
				n := float64(b.N)
				b.ReportMetric(float64(after.PacketsReceived-before.PacketsReceived)/n, "srv-pkts/op")
				// quic-go documents PacketsLost as not monotonic, so a difference
				// of unsigned counters could wrap.
				b.ReportMetric((float64(after.PacketsLost)-float64(before.PacketsLost))/n, "cli-lost/op")
				b.ReportMetric(float64(relay.dropped[1].Load()-down)/n, "drop-down/op")
				b.ReportMetric(float64(relay.dropped[0].Load()-up)/n, "drop-up/op")
			})
		}
	}
}
