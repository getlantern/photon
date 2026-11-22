package transport

import (
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestAuthenticatedCIDHandoff(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		maximum := uint64(3)
		client, server := negotiatedPathPair(t, version, &maximum, &maximum)
		if len(client.localIDs.observed) != 0 || len(server.localIDs.observed) != 0 {
			t.Fatal("construction invented authentication evidence")
		}
		drivePair(t, client, server, false, false)
		local := netip.MustParseAddrPort("192.0.2.1:443")
		remote := netip.MustParseAddrPort("192.0.2.2:444")
		for _, c := range []*Conn{client, server} {
			if len(c.localIDs.observed) == 0 {
				t.Fatal("handshake did not retain CID authentication")
			}
			for _, tuple := range c.localIDs.observed {
				if tuple != (pathTuple{}) {
					t.Fatal("unowned handshake invented tuple")
				}
			}
			if _, err := c.activateNegotiatedMultipath(local, remote); err != nil {
				t.Fatal(err)
			}
			ids := c.multipath.paths[0].local
			if !ids.receivedOn(c.config.LocalCID, pathTuple{local, remote}) {
				t.Fatal("activation lost handshake CID evidence")
			}
			// New path CID advertisements are not authenticated use by the peer.
			if err := c.advertisePathCIDs(1); err != nil {
				t.Fatal(err)
			}
			if len(c.multipath.paths[1].local.observed) != 0 {
				t.Fatal("issuance granted CID evidence")
			}
		}
	}
}

func TestCIDHandoffDoesNotOverwriteBoundEvidence(t *testing.T) {
	ids := newLocalIDs([]byte("initial0"))
	first := pathTuple{netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.2:444")}
	second := pathTuple{first.local, netip.MustParseAddrPort("192.0.2.2:445")}
	ids.noteAuthenticated([]byte("initial0"))
	ids.bindAuthenticated(first)
	ids.noteAuthenticated([]byte("initial0"))
	ids.bindAuthenticated(second)
	if !ids.receivedOn([]byte("initial0"), first) || ids.receivedOn([]byte("initial0"), second) {
		t.Fatal("handoff overwrote bound evidence")
	}
}

func TestInitialPacketDoesNotGrantCIDUseEvidence(t *testing.T) {
	maximum := uint64(3)
	client, server := negotiatedPathPair(t, wire.Version1, &maximum, &maximum)
	now := time.Unix(101, 0)
	first, err := client.NextDatagram(now)
	if err != nil || first == nil {
		t.Fatal(err)
	}
	if err := server.HandleDatagram(first, now); err != nil {
		t.Fatal(err)
	}
	response, err := server.NextDatagram(now)
	if err != nil || response == nil {
		t.Fatal(err)
	}
	h, err := wire.ParseHeader(response, len(client.config.LocalCID))
	if err != nil || h.Type != wire.Initial {
		t.Fatal("expected Initial", err)
	}
	if err := client.HandleDatagram(response[:h.PacketLen], now); err != nil {
		t.Fatal(err)
	}
	if client.authenticatedPackets == 0 {
		t.Fatal("Initial was not processed")
	}
	if len(client.localIDs.observed) != 0 {
		t.Fatal("publicly protected Initial granted CID-use evidence")
	}
}
