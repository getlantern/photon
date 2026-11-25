package interop

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestUpstreamStagedProbeIndependentServer(t *testing.T) { testUpstreamStagedProbeServer(t, false) }
func TestUpstreamAbandonedProbeIndependentServer(t *testing.T) {
	testUpstreamStagedProbeServer(t, true)
}

func testUpstreamStagedProbeServer(t *testing.T, abandon bool) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			l, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			makeTransport := func() *upstream.Transport {
				socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				tr := &upstream.Transport{Conn: socket}
				t.Cleanup(func() { tr.Close(); socket.Close() })
				return tr
			}
			original, candidate := makeTransport(), makeTransport()
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			peer, err := original.Dial(ctx, l.Addr(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
			if err != nil {
				t.Fatal(err)
			}
			defer peer.CloseWithError(0, "")
			server, err := l.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			path, err := peer.AddPath(candidate)
			if err != nil {
				t.Fatal(err)
			}
			defer path.Close()
			if err = path.Probe(ctx); err != nil {
				t.Fatal("upstream probe", err)
			}
			transfer := func(want net.Addr) {
				t.Helper()
				tx, err := peer.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = tx.Write([]byte("staged migration")); err != nil {
					t.Fatal(err)
				}
				if err = tx.Close(); err != nil {
					t.Fatal(err)
				}
				rx, err := server.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				rx.SetReadDeadline(time.Now().Add(time.Second))
				data, err := io.ReadAll(rx)
				if err != nil || string(data) != "staged migration" {
					t.Fatal(string(data), err)
				}
				// quic-go's Path.Switch takes effect on its next run-loop
				// pass, so data written just after it can still leave on the
				// old path; photon moves the binding when the first packet on
				// the new path arrives.
				for deadline := time.Now().Add(time.Second); server.RemoteAddr().String() != want.String(); time.Sleep(time.Millisecond) {
					if time.Now().After(deadline) {
						t.Fatal("wrong peer binding", server.RemoteAddr(), want)
					}
				}
			}
			transfer(original.Conn.LocalAddr())
			if err = path.Switch(); err != nil {
				t.Fatal(err)
			}
			transfer(candidate.Conn.LocalAddr())
			// Repeat beyond the initial active CID supply. Retiring the previous
			// path must permit new probes while the selected binding stays live.
			current := candidate
			previous := path
			for i := 0; i < 5; i++ {
				if abandon {
					unused, err := peer.AddPath(makeTransport())
					if err != nil {
						t.Fatal(err)
					}
					defer unused.Close()
					if err := unused.Probe(ctx); err != nil {
						t.Fatal("probe before abandonment", i, err)
					}
					if err := unused.Close(); err != nil {
						t.Fatal("abandon unused path", i, err)
					}
					transfer(current.Conn.LocalAddr())
				}

				next := makeTransport()
				nextPath, err := peer.AddPath(next)
				if err != nil {
					t.Fatal("add repeated path", i, err)
				}
				defer nextPath.Close()
				if err := nextPath.Probe(ctx); err != nil {
					t.Fatal("repeated probe", i, err)
				}
				transfer(current.Conn.LocalAddr())
				if err := nextPath.Switch(); err != nil {
					t.Fatal("repeated switch", i, err)
				}
				transfer(next.Conn.LocalAddr())
				if err := previous.Close(); err != nil {
					t.Fatal("retire previous path", i, err)
				}
				previous, current = nextPath, next
			}

		})
	}
}
