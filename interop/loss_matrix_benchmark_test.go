package interop

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	hquic "github.com/apernet/quic-go"
	upstream "github.com/quic-go/quic-go"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/interop/internal/hysteria/bbr"
)

// startHysteriaServer is a Hysteria2-style echo server: its BBR, set on each
// connection as Hysteria's server does, over its fork of quic-go.
func startHysteriaServer(b *testing.B, ctx context.Context, serverTLS *tls.Config, w echoWindows) echoServer {
	b.Helper()
	l, err := hquic.ListenAddr("127.0.0.1:0", serverTLS, &hquic.Config{Versions: []hquic.Version{hquic.Version1}, InitialStreamReceiveWindow: w.stream, MaxStreamReceiveWindow: w.stream, InitialConnectionReceiveWindow: w.connection, MaxConnectionReceiveWindow: w.connection, MaxIncomingStreams: 16, MaxIncomingUniStreams: 16})
	if err != nil {
		b.Fatal(err)
	}
	return echoServer{addr: l.Addr().String(), close: func() { l.Close() }, accept: func() (func(context.Context) (benchmarkStream, error), func(), error) {
		c, err := l.Accept(ctx)
		if err != nil {
			return nil, nil, err
		}
		c.SetCongestionControl(bbr.NewBbrSender(bbr.DefaultClock{}, min(c.InitialPacketSize(), bbr.GetInitialPacketSize(c.RemoteAddr())), bbr.ProfileStandard))
		return func(ctx context.Context) (benchmarkStream, error) { return c.AcceptStream(ctx) }, func() { c.CloseWithError(0, "") }, nil
	}}
}

// BenchmarkLossMatrix downloads 8 MiB through a 50 Mbit/s relay at each RTT
// and random loss rate, from photon BBR, quic-go and Hysteria2's BBR. Loss
// applies both ways, to ACKs too; the queue holds one bandwidth-delay
// product. Run with -benchtime=1x: one download a cell.
func BenchmarkLossMatrix(b *testing.B) {
	payload := bytes.Repeat([]byte("0123456789abcdef"), 8<<16)
	windows := echoWindows{stream: 16 << 20, connection: 32 << 20, congestion: quic.CongestionControlBBR}
	for _, rtt := range []time.Duration{50 * time.Millisecond, 150 * time.Millisecond, 300 * time.Millisecond} {
		for _, loss := range []float64{0, 0.005, 0.01, 0.02, 0.05} {
			link := wanLink{delay: rtt / 2, loss: loss, rate: 50e6 / 8, queue: rtt}
			for _, sender := range []string{"photon-bbr", "quic-go", "hysteria-bbr"} {
				b.Run(fmt.Sprintf("rtt%dms/loss%.1f%%/%s", rtt.Milliseconds(), 100*loss, sender), func(b *testing.B) {
					clientTLS, serverTLS := tlsConfigs(b)
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					var server echoServer
					switch sender {
					case "photon-bbr":
						server = startEchoServer(b, ctx, true, serverTLS, windows)
					case "quic-go":
						server = startEchoServer(b, ctx, false, serverTLS, windows)
					default:
						server = startHysteriaServer(b, ctx, serverTLS, windows)
					}
					defer server.close()
					relay := newWANRelay(b, server.addr, link, link)
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
					runDownload(b, ctx, peer, accept, payload, 5*time.Minute)
				})
			}
		}
	}
}
