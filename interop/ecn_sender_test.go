//go:build linux || darwin

package interop

import (
	"context"
	"fmt"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

// More than ten independently observed marked packets proves our sender left
// its bounded testing state. Sequential echo exchanges give upstream feedback
// opportunities without injecting synthetic ACKs or inspecting private state.
func TestECNSenderContinuesAfterValidationUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, address := range []string{"127.0.0.1:0", "[::1]:0"} {
			for _, server := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/%s/server=%v", v, address, server), func(t *testing.T) {
					ct, st := tlsConfigs(t)
					trace := &ecnSenderTrace{datagrams: make(chan qlog.ECN, 64)}
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					config := &quic.Config{Versions: []quic.Version{v}, EnableDatagrams: true, CongestionControl: quic.CongestionControlNewReno}
					upstreamConfig := &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, EnableDatagrams: true, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }}
					var c *quic.Conn
					var peer *upstream.Conn
					var err error
					if server {
						listener, listenErr := quic.ListenAddr(address, st, config)
						if listenErr != nil {
							t.Fatal(listenErr)
						}
						defer listener.Close()
						peer, err = dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, upstreamConfig)
						if err != nil {
							t.Fatal(err)
						}
						defer peer.CloseWithError(0, "")
						c, err = listener.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
						defer c.CloseWithError(0, "")
					} else {
						listener, listenErr := upstream.ListenAddr(address, st, upstreamConfig)
						if listenErr != nil {
							t.Fatal(listenErr)
						}
						defer listener.Close()
						c, err = quic.DialAddr(ctx, listener.Addr().String(), ct, config)
						if err != nil {
							t.Fatal(err)
						}
						defer c.CloseWithError(0, "")
						peer, err = listener.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
						defer peer.CloseWithError(0, "")
					}
					marked := 0
					for i := 0; i < 24; i++ {
						payload := fmt.Sprintf("ECN probe continuation %d", i)
						if err := c.SendDatagram([]byte(payload)); err != nil {
							t.Fatal(err)
						}
						received, err := peer.ReceiveDatagram(ctx)
						if err != nil || string(received) != payload {
							t.Fatal(string(received), err)
						}
						select {
						case mark := <-trace.datagrams:
							if mark == qlog.ECT0 {
								marked++
							} else if mark != qlog.ECTNot && mark != qlog.ECNUnsupported {
								t.Fatal("unexpected marking", mark)
							}
						case <-ctx.Done():
							t.Fatal("no independent marking observation", ctx.Err())
						}
						if err := peer.SendDatagram(received); err != nil {
							t.Fatal(err)
						}
						echoed, err := c.ReceiveDatagram(ctx)
						if err != nil || string(echoed) != payload {
							t.Fatal(string(echoed), err)
						}
					}
					if marked <= 10 {
						t.Fatalf("marking did not continue beyond probe budget: %d/24", marked)
					}
				})
			}
		}
	}
}
