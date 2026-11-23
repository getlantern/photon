package transport

import (
	"context"
	"errors"
	"github.com/getlantern/photon/internal/handshake"
	"strings"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathAdvertisementGuard(t *testing.T) {
	for _, server := range []bool{false, true} {
		for _, v := range []uint64{0, 1, 1<<32 - 1} {
			p := wire.DefaultTransportParameters()
			p.InitialMaxPathID = &v
			c, err := NewConn(Config{Server: server, Version: wire.Version1, LocalCID: []byte("localcid"), PeerCID: []byte("original"), OriginalDCID: []byte("original"), Parameters: p})
			if err == nil {
				c.Close()
				t.Fatal("engine advertised multipath without a capable owner")
			}
			if !strings.Contains(err.Error(), "multipath advertisement requires a capable owner") {
				t.Fatalf("unexpected rejection: %v", err)
			}
		}
	}
}

func TestMultipathPeerCIDRequirement(t *testing.T) {
	for _, local := range [][]byte{nil, {}, []byte("localcid")} {
		for _, peer := range [][]byte{nil, {}, []byte("peercid0")} {
			c := &Conn{config: Config{LocalCID: local}, initialPeerCID: peer}
			p := wire.DefaultTransportParameters()
			if err := c.validatePeerMultipathParameters(p); err != nil {
				t.Fatal("base QUIC CID rejected", err)
			}
			value := uint64(0)
			p.InitialMaxPathID = &value
			err := c.validatePeerMultipathParameters(p)
			if len(local) == 0 || len(peer) == 0 {
				var te *TransportError
				if !errors.As(err, &te) || te.Code != 0xa {
					t.Fatalf("expected PROTOCOL_VIOLATION: %v", err)
				}
			} else if err != nil {
				t.Fatal("valid unilateral advertisement rejected", err)
			}
		}
	}
}

// A TLS-only peer advertises the extension. Our side must complete the base
// handshake without treating one-sided advertisement as negotiation.
func TestMultipathUnilateralAdvertisementHandshake(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, advertisingServer := range []bool{false, true} {
			clientTLS, serverTLS := testTLS(t)
			config := Config{Version: version, TLS: clientTLS, LocalCID: []byte("client00"), PeerCID: []byte("original"), OriginalDCID: []byte("original"), Parameters: wire.DefaultTransportParameters()}
			client, err := NewConn(config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { client.Close() })
			config.Server = true
			config.TLS = serverTLS
			config.LocalCID = []byte("server00")
			config.PeerCID = []byte("client00")
			server, err := NewConn(config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { server.Close() })
			advertiser, receiver := client, server
			if advertisingServer {
				advertiser, receiver = server, client
			}
			p := advertiser.config.Parameters
			value := uint64(1)
			p.InitialMaxPathID = &value
			encoded, err := p.Marshal(advertisingServer)
			if err != nil {
				t.Fatal(err)
			}
			advertiser.tls.Close()
			advertiser.tls, err = handshake.New(advertisingServer, version, advertiser.config.TLS, encoded)
			if err != nil {
				t.Fatal(err)
			}
			if err = client.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err = server.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			drivePair(t, client, server, false, false)
			if receiver.peerParams == nil || receiver.peerParams.InitialMaxPathID == nil || *receiver.peerParams.InitialMaxPathID != 1 {
				t.Fatal("lost peer advertisement")
			}
			if receiver.config.Parameters.InitialMaxPathID != nil {
				t.Fatal("unilateral advertisement enabled multipath")
			}
		}
	}
}
