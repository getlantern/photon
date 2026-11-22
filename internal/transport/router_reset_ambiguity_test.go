package transport

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestEndpointUnknownCIDResetAmbiguity(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, network := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/network=%v", version, network), func(t *testing.T) {
				local := netip.MustParseAddrPort("192.0.2.1:443")
				remote := netip.MustParseAddrPort("192.0.2.2:444")
				endpoint, err := NewClientOnlyEndpoint(8, 4)
				if err != nil {
					t.Fatal(err)
				}
				defer endpoint.Close()
				token := [16]byte{42}
				var conns []*Conn
				for i := 0; i < 2; i++ {
					c, peer := testPair(t, version, streamParameters())
					drivePair(t, c, peer, false, false)
					c.config.LocalCID = []byte(fmt.Sprintf("local-%02d", i))
					c.localIDs = newLocalIDs(c.config.LocalCID)
					id := c.peerIDs.active[c.peerIDs.current]
					id.used, id.hasToken, id.token = true, true, token
					c.peerIDs.active[c.peerIDs.current] = id
					if err := endpoint.AddClient(c, remote); err != nil {
						t.Fatal(err)
					}
					conns = append(conns, c)
				}
				if network {
					if err := endpoint.bindNetworkOwner(local); err != nil {
						t.Fatal(err)
					}
				}
				now := time.Unix(101, 0)
				raw := resetDatagram(token)
				for i := 0; i < 20; i++ {
					c, response, err := endpoint.handleNetworkDatagram(context.Background(), raw, local, remote, now)
					if c != nil || response != nil || err != nil {
						t.Fatalf("ambiguous reset: connection=%p response=%p error=%v", c, response, err)
					}
					for _, c := range conns {
						if c.Draining() || c.closed {
							t.Fatal("ambiguous reset terminated connection")
						}
					}
				}
				id := conns[1].peerIDs.active[conns[1].peerIDs.current]
				id.used = false
				conns[1].peerIDs.active[conns[1].peerIDs.current] = id
				if err := endpoint.router.Sync(conns[1]); err != nil {
					t.Fatal(err)
				}
				c, response, err := endpoint.handleNetworkDatagram(context.Background(), raw, local, remote, now)
				if c != conns[0] || response != nil || !errors.Is(err, ErrStatelessReset) {
					t.Fatalf("unique reset: connection=%p response=%p error=%v", c, response, err)
				}
			})
		}
	}
}

func TestRouterResetAcrossMigratedTuple(t *testing.T) {
	local := netip.MustParseAddrPort("192.0.2.1:443")
	remote := netip.MustParseAddrPort("192.0.2.2:444")
	oldRemote := netip.MustParseAddrPort("192.0.2.3:444")
	wrongLocal := netip.MustParseAddrPort("192.0.2.1:445")
	token := [16]byte{42}
	legacy := indexedResetConn("legacy00", token, true)
	migrated := resetPublicationBenchmarkConn(local, remote, 1)
	state := migrated.multipath.paths[0]
	for seq, id := range state.peer.active {
		id.used, id.hasToken, id.token = true, true, token
		state.peer.active[seq] = id
	}
	router, _ := NewRouter(8, 4)
	router.entries[legacy] = connectionRoute{peer: remote}
	router.entries[migrated] = connectionRoute{peer: oldRemote}
	router.peers[remote] = map[*Conn]struct{}{legacy: {}}
	router.peers[oldRemote] = map[*Conn]struct{}{migrated: {}}
	raw := resetDatagram(token)
	if c, known := router.RouteNetwork(raw, local, remote); c != nil || !known {
		t.Fatalf("mixed fixed/migrated ambiguity: connection=%p known=%v", c, known)
	}
	// The migrated token must not count on a different local tuple.
	if c, known := router.RouteNetwork(raw, wrongLocal, remote); c != legacy || !known {
		t.Fatalf("wrong-local hint introduced ambiguity: connection=%p known=%v", c, known)
	}
	legacy.closed = true
	if c, known := router.RouteNetwork(raw, local, remote); c != migrated || !known {
		t.Fatalf("closed fixed-peer owner hid live migrated owner: connection=%p known=%v", c, known)
	}
	legacy.closed = false
	// CID routing still wins over reset suffixes, including a new remote address.
	router.ids["migrated"] = migrated
	packet := append([]byte{0x40}, []byte("migrated")...)
	packet = append(packet, raw...)
	if c, known := router.RouteNetwork(packet, wrongLocal, remote); c != migrated || !known {
		t.Fatalf("token hints displaced CID route: connection=%p known=%v", c, known)
	}
}
