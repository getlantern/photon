package interop

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

// coexistServer is one competitor: photon with a congestion controller, or
// quic-go (Cubic by default).
type coexistServer struct {
	name        string
	independent bool
	congestion  quic.CongestionControl
}

var (
	coexistPhotonBBR     = coexistServer{"photon-bbr", true, quic.CongestionControlBBR}
	coexistPhotonNewReno = coexistServer{"photon-newreno", true, quic.CongestionControlNewReno}
	coexistQUICGo        = coexistServer{"quic-go", false, 0}
)

// BenchmarkServerCoexistWAN measures how two servers' downloads share one
// bottleneck: 100 Mbit/s with a 50 ms RTT and no random loss, at several
// queue sizes. Each server sends to its own quic-go client through its own
// relay; the relays share the link in both directions. The first flow runs
// alone for 2 s, then the second joins, and each flow's goodput is measured
// over the last 20 s of a 32 s run. Run with -benchtime=1x.
//
// It reports each flow's goodput (a-Mbit/s, b-Mbit/s) and the first flow's
// share of the total (a-share-%).
func BenchmarkServerCoexistWAN(b *testing.B) {
	for _, queue := range []time.Duration{10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond} {
		link := wanLink{delay: 25 * time.Millisecond, rate: 100e6 / 8, queue: queue}
		for _, pair := range [][2]coexistServer{
			{coexistQUICGo, coexistQUICGo},
			{coexistPhotonBBR, coexistQUICGo},
			{coexistQUICGo, coexistPhotonBBR},
			{coexistPhotonNewReno, coexistQUICGo},
			{coexistPhotonBBR, coexistPhotonBBR},
		} {
			name := fmt.Sprintf("queue%s/%s-vs-%s", queue, pair[0].name, pair[1].name)
			b.Run(name, func(b *testing.B) {
				var a, c float64
				for range b.N {
					ga, gc := runCoexist(b, link, pair, 2*time.Second, 12*time.Second, 32*time.Second)
					a += ga
					c += gc
				}
				n := float64(b.N)
				b.ReportMetric(a/n/1e6, "a-Mbit/s")
				b.ReportMetric(c/n/1e6, "b-Mbit/s")
				b.ReportMetric(100*a/(a+c), "a-share-%")
			})
		}
	}
}

// runCoexist runs pair's downloads over one shared link, the second joining
// after join, and returns each one's goodput in bits per second from
// measureFrom to end.
func runCoexist(b *testing.B, link wanLink, pair [2]coexistServer, join, measureFrom, end time.Duration) (float64, float64) {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	up, down := newWANBottleneck(link), newWANBottleneck(link)
	windows := echoWindows{stream: 16 << 20, connection: 32 << 20}
	var received [2]atomic.Uint64
	start := time.Now()
	for i, server := range pair {
		clientTLS, serverTLS := tlsConfigs(b)
		w := windows
		w.congestion = server.congestion
		srv := startEchoServer(b, ctx, server.independent, serverTLS, w)
		b.Cleanup(srv.close)
		relay := newSharedWANRelay(b, srv.addr, up, down, uint64(i+1))
		if i == 1 {
			time.Sleep(time.Until(start.Add(join)))
		}
		dialCtx, stop := context.WithTimeout(ctx, 30*time.Second)
		peer, err := dialUpstreamAddr(b, dialCtx, relay.addr(), clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version1}, InitialStreamReceiveWindow: windows.stream, MaxStreamReceiveWindow: windows.stream, InitialConnectionReceiveWindow: windows.connection, MaxConnectionReceiveWindow: windows.connection})
		stop()
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { peer.CloseWithError(0, "") })
		accept, closeServer, err := srv.accept()
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(closeServer)
		go streamDownload(ctx, peer, accept, &received[i])
	}
	time.Sleep(time.Until(start.Add(measureFrom)))
	a0, c0 := received[0].Load(), received[1].Load()
	time.Sleep(time.Until(start.Add(end)))
	a1, c1 := received[0].Load(), received[1].Load()
	window := (end - measureFrom).Seconds()
	return float64(a1-a0) * 8 / window, float64(c1-c0) * 8 / window
}

// streamDownload asks the server for an endless download and counts what
// arrives in received until ctx ends or the connection closes.
func streamDownload(ctx context.Context, peer *upstream.Conn, accept func(context.Context) (benchmarkStream, error), received *atomic.Uint64) {
	go func() {
		s, err := accept(ctx)
		if err != nil {
			return
		}
		chunk := make([]byte, 64<<10)
		for {
			if _, err := s.Write(chunk); err != nil {
				return
			}
		}
	}()
	s, err := peer.OpenStreamSync(ctx)
	if err != nil {
		return
	}
	if _, err := s.Write([]byte("get")); err != nil {
		return
	}
	buf := make([]byte, 64<<10)
	for {
		n, err := s.Read(buf)
		received.Add(uint64(n))
		if err != nil {
			return
		}
	}
}
