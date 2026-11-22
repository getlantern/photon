//go:build linux || darwin

package interop

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

func TestECNSenderRevalidatesAfterMigrationUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, address := range []string{"127.0.0.1:0", "[::1]:0"} {
			t.Run(fmt.Sprintf("%x/%s", v, address), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				trace := &ecnSenderTrace{datagrams: make(chan qlog.ECN, 64)}
				listener, err := upstream.ListenAddr(address, st, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, EnableDatagrams: true, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				local, err := net.ResolveUDPAddr("udp", address)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				c, err := quic.DialAddr(ctx, listener.Addr().String(), ct, &quic.Config{Versions: []quic.Version{v}, EnableDatagrams: true, LocalAddr: local})
				if err != nil {
					t.Fatal(err)
				}
				defer c.CloseWithError(0, "")
				peer, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "")
				packets := 24
				if v == quic.Version2 && os.Getenv("QUIC_INTEROP_KNOWN_FAILURES") != "1" {
					packets = 12
				}
				previous := c.LocalAddr().String()
				for round := 0; round < 3; round++ {
					if round > 0 {
						if err := c.Migrate(ctx, local); err != nil {
							t.Fatal("migration", round, err)
						}
						if c.LocalAddr().String() == previous {
							t.Fatal("migration kept local tuple")
						}
						previous = c.LocalAddr().String()
					}
					marked := 0
					for i := 0; i < packets; i++ {
						payload := fmt.Sprintf("ECN migration %d/%d", round, i)
						if err := c.SendDatagram([]byte(payload)); err != nil {
							t.Fatal(err)
						}
						b, err := peer.ReceiveDatagram(ctx)
						if err != nil || string(b) != payload {
							t.Fatalf("round=%d packet=%d upstreamKeyPhase=%d data=%q err=%v", round, i, trace.keyPhase.Load(), b, err)
						}
						select {
						case mark := <-trace.datagrams:
							if mark == qlog.ECT0 {
								marked++
							} else if mark != qlog.ECNUnsupported && mark != qlog.ECTNot {
								t.Fatal("unexpected ECN", mark)
							}
						case <-ctx.Done():
							t.Fatal("no marking observation", ctx.Err())
						}
						if err := peer.SendDatagram(b); err != nil {
							t.Fatal(err)
						}
						b, err = c.ReceiveDatagram(ctx)
						if err != nil || string(b) != payload {
							t.Fatalf("round=%d packet=%d upstreamKeyPhase=%d data=%q err=%v", round, i, trace.keyPhase.Load(), b, err)
						}
					}
					if marked <= 10 {
						t.Fatalf("round %d did not validate ECN: %d/%d marked", round, marked, packets)
					}
					if peer.RemoteAddr().String() != previous {
						t.Fatal("peer did not use migrated tuple", peer.RemoteAddr(), previous)
					}
				}
			})
		}
	}
}
