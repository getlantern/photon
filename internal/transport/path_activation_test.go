package transport

import (
	"context"
	"net/netip"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func negotiatedPathPair(t *testing.T, version wire.Version, clientMax, serverMax *uint64) (*Conn, *Conn) {
	t.Helper()
	ct, st := testTLS(t)
	cfg := Config{Version: version, TLS: ct, LocalCID: []byte("client00"), PeerCID: []byte("original"), OriginalDCID: []byte("original"), Parameters: streamParameters()}
	cfg.Parameters.InitialMaxPathID = clientMax
	if clientMax != nil {
		cfg.multipathCapacity = 4
	}
	client, err := NewConn(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	cfg.Server = true
	cfg.TLS = st
	cfg.LocalCID = []byte("server00")
	cfg.PeerCID = []byte("client00")
	cfg.Parameters.InitialMaxPathID = serverMax
	cfg.multipathCapacity = 0
	if serverMax != nil {
		cfg.multipathCapacity = 4
	}
	server, err := NewConn(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	for _, c := range []*Conn{client, server} {
		if err := c.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	return client, server
}

func TestMultipathTLSNegotiationAndActivation(t *testing.T) {
	zero, three := uint64(0), uint64(3)
	local := netip.MustParseAddrPort("192.0.2.1:443")
	remote := netip.MustParseAddrPort("192.0.2.2:444")
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, offers := range [][2]*uint64{{&zero, &three}, {&three, &zero}, {&three, nil}, {nil, &three}, {nil, nil}} {
			client, server := negotiatedPathPair(t, version, offers[0], offers[1])
			if _, err := client.activateNegotiatedMultipath(local, remote); err == nil {
				t.Fatal("unconfirmed activation")
			}
			drivePair(t, client, server, false, false)
			for i, c := range []*Conn{client, server} {
				want := offers[0] != nil && offers[1] != nil
				if want {
					if _, err := c.activateNegotiatedMultipath(netip.AddrPort{}, remote); err == nil || c.multipath != nil {
						t.Fatal("invalid binding changed ownership")
					}
				}
				active, err := c.activateNegotiatedMultipath(local, remote)
				if err != nil || active != want {
					t.Fatal(active, want, err)
				}
				if !want {
					if c.multipath != nil {
						t.Fatal("unilateral negotiation")
					}
					continue
				}
				if c.multipath.localMax != *offers[i] || c.multipath.peerMax != *offers[1-i] || c.multipath.paths[0].application.network == nil {
					t.Fatal("limits/binding did not come from negotiated parameters")
				}
				if _, err := c.activateNegotiatedMultipath(local, remote); err == nil {
					t.Fatal("duplicate activation")
				}
			}
		}
	}
}
