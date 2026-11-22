package transport

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func TestEndpointTerminalAdmissionKeepsRoutes(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, network := range []bool{false, true} {
			for _, mode := range []string{"illegal-frame", "peer-close"} {
				t.Run(fmt.Sprintf("%s/network=%v/%s", version, network, mode), func(t *testing.T) {
					client, unused := testPair(t, version)
					unused.Close()
					codec, err := token.NewRetryTokens([32]byte{5}, nil, time.Minute)
					if err != nil {
						t.Fatal(err)
					}
					e, err := NewServerEndpoint(Config{Version: version, TLS: unused.config.TLS, Parameters: wire.DefaultTransportParameters()}, codec, 2)
					if err != nil {
						t.Fatal(err)
					}
					defer e.Close()
					e.VerifySourceAddress = func(netip.AddrPort) bool { return false }
					if network {
						if err := e.bindNetworkOwner(netip.MustParseAddrPort("192.0.2.1:443")); err != nil {
							t.Fatal(err)
						}
					}
					peer := netip.MustParseAddrPort("192.0.2.2:444")
					now := time.Unix(100, 0)
					var raw []byte
					if mode == "illegal-frame" {
						raw = makePeerPacket(t, client, initialSpace, 0, wire.HandshakeDoneFrame{})
					} else {
						if err := client.CloseTransportError(7, 0x18, "bad frame", now); err != nil {
							t.Fatal(err)
						}
						if raw, err = client.NextDatagram(now); err != nil || len(raw) == 0 {
							t.Fatal("client close", err)
						}
					}
					conn, response, err := e.handleNetworkDatagram(context.Background(), raw, e.local, peer, now)
					if conn == nil || response != nil || err == nil || len(e.conns) != 1 {
						t.Fatal("terminal admission lost its routes", conn != nil, response, err, len(e.conns))
					}
					if mode == "peer-close" {
						if !errors.Is(err, ErrPeerClosed) || !conn.Draining() {
							t.Fatal("not draining", err)
						}
						again, response, _ := e.handleNetworkDatagram(context.Background(), raw, e.local, peer, now)
						if again != conn || response != nil || len(e.conns) != 1 {
							t.Fatal("draining input readmitted")
						}
						return
					}
					if !conn.Closing() {
						t.Fatal("not closing", err)
					}
					out, err := e.NextDatagram(now)
					if err != nil || out == nil || out.Conn != conn || out.Local != e.local || out.Peer != peer {
						t.Fatal("prepared close not emitted", out, err)
					}
					var closed *PeerCloseError
					if err := client.HandleDatagram(out.Data, now); !errors.As(err, &closed) || closed.Application || closed.Code != 0xa {
						t.Fatalf("client close: %v", err)
					}
				})
			}
		}
	}
}
