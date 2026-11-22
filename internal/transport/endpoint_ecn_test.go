package transport

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func TestEndpointAdmissionECN(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		client, unused := testPair(t, v)
		unused.Close()
		codec, _ := token.NewRetryTokens([32]byte{7}, nil, time.Minute)
		endpoint, err := NewServerEndpoint(Config{Version: v, TLS: unused.config.TLS, Parameters: wire.DefaultTransportParameters()}, codec, 2)
		if err != nil {
			t.Fatal(err)
		}
		defer endpoint.Close()
		ctx := context.Background()
		now := time.Unix(100, 0)
		address := netip.MustParseAddrPort("192.0.2.1:1234")
		first := drain(t, client, now)[0]
		c, retry, err := endpoint.HandleDatagramECN(ctx, first, address, now, ECNECT1)
		if err != nil || c != nil || retry == nil || len(endpoint.conns) != 0 {
			t.Fatal("stateless Retry", err)
		}
		if err := client.HandleDatagram(retry.Data, now); err != nil {
			t.Fatal(err)
		}
		flights := drain(t, client, now)
		server, reply, err := endpoint.HandleDatagramECN(ctx, flights[0], address, now, ECNCE)
		if err != nil || server == nil || reply != nil {
			t.Fatal("admission", err)
		}
		tracker := server.spaces[initialSpace].received
		ack, err := tracker.Ack(now, 0)
		if err != nil || ack.ECN == nil || *ack.ECN != [3]uint64{0, 0, 1} {
			t.Fatal("admission marking", ack.ECN, err)
		}
		// The retransmitted Initial routes to the existing connection.
		same, _, err := endpoint.HandleDatagramECN(ctx, flights[0], address, now, ECNECT0)
		if err != nil || same != server {
			t.Fatal("existing route", err)
		}
		ack, _ = tracker.Ack(now, 0)
		if *ack.ECN != [3]uint64{0, 0, 1} {
			t.Fatal("duplicate counted", *ack.ECN)
		}
		raw := makePeerPacket(t, client, initialSpace, 100, wire.PingFrame{})
		if _, _, err := endpoint.HandleDatagramECN(ctx, raw, address, now, ECNECT0); err != nil {
			t.Fatal(err)
		}
		ack, _ = tracker.Ack(now, 0)
		if *ack.ECN != [3]uint64{1, 0, 1} {
			t.Fatal("established marking", *ack.ECN)
		}
		before := tracker.Expected()
		if _, _, err := endpoint.HandleDatagramECN(ctx, raw, address, now, ECN(4)); err == nil || tracker.Expected() != before {
			t.Fatal("invalid metadata")
		}
	}
}

func TestNetworkOwnerECNAfterActivation(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, peer := testPair(t, v)
		drivePair(t, c, peer, false, false)
		c.config.EnableMigration = true
		local := netip.MustParseAddrPort("192.0.2.1:1234")
		remote := netip.MustParseAddrPort("192.0.2.2:443")
		owner, err := newNetworkOwner(c, local, remote)
		if err != nil {
			t.Fatal(err)
		}
		now := time.Unix(101, 0)
		first := makePeerPacket(t, peer, applicationSpace, 100, wire.PingFrame{})
		if err := owner.receiveECN(first, local, remote, now, ECNECT0); err != nil {
			t.Fatal(err)
		}
		if c.multipath == nil || !c.multipath.baseOnly {
			t.Fatal("base path owner not activated")
		}
		second := makePeerPacket(t, peer, applicationSpace, 101, wire.PingFrame{})
		if err := owner.receiveECN(second, local, remote, now, ECNCE); err != nil {
			t.Fatal(err)
		}
		ack, err := c.multipath.paths[0].application.space.received.Ack(now, 0)
		if err != nil || ack.ECN == nil || *ack.ECN != [3]uint64{1, 0, 1} {
			t.Fatal("owner handoff lost ECN", ack.ECN, err)
		}
	}
}
