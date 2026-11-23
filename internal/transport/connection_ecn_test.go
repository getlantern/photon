package transport

import (
	"bytes"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestConnectionECNAuthenticationAndReplay(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, peer := testPair(t, v)
		drivePair(t, c, peer, false, false)
		now := time.Unix(101, 0)
		raw := makePeerPacket(t, peer, applicationSpace, 100, wire.PingFrame{})
		bad := bytes.Clone(raw)
		bad[len(bad)-1] ^= 1
		if err := c.HandleDatagramECN(bad, now, ECNCE); err != nil {
			t.Fatal(err)
		}
		r := c.spaces[applicationSpace].received
		if r.haveECN {
			t.Fatal("unauthenticated packet counted")
		}
		if err := c.HandleDatagramECN(raw, now, ECNECT0); err != nil {
			t.Fatal(err)
		}
		if err := c.HandleDatagramECN(raw, now, ECNCE); err != nil {
			t.Fatal(err)
		}
		ack, err := r.Ack(now, 0)
		if err != nil || ack.ECN == nil || *ack.ECN != [3]uint64{1, 0, 0} {
			t.Fatal("authenticated counts", ack.ECN, err)
		}
		before := r.Expected()
		if err := c.HandleDatagramECN(raw, now, ECN(4)); err == nil || r.Expected() != before || c.err != nil {
			t.Fatal("invalid metadata damaged connection", err)
		}
	}
}

func TestConnectionDeferredECN(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := testPair(t, v)
		now := time.Unix(100, 0)
		for _, raw := range drain(t, client, now) {
			if err := server.HandleDatagram(raw, now); err != nil {
				t.Fatal(err)
			}
		}
		var initials, handshakes [][]byte
		for _, raw := range drain(t, server, now) {
			h, err := wire.ParseHeader(raw, len(client.config.LocalCID))
			if err != nil {
				t.Fatal(err)
			}
			switch h.Type {
			case wire.Initial:
				initials = append(initials, raw)
			case wire.Handshake:
				handshakes = append(handshakes, raw)
			}
		}
		if len(initials) == 0 || len(handshakes) == 0 {
			t.Fatal("missing handshake flights")
		}
		for _, raw := range handshakes {
			for repeat := 0; repeat < 2; repeat++ {
				if err := client.HandleDatagramECN(raw, now, ECNCE); err != nil {
					t.Fatal(err)
				}
			}
		}
		if len(client.deferred) != 2*len(handshakes) {
			t.Fatal("missing deferred packets")
		}
		for _, raw := range initials {
			if err := client.HandleDatagramECN(raw, now.Add(time.Millisecond), ECNECT0); err != nil {
				t.Fatal(err)
			}
		}
		if len(client.deferred) != 0 {
			t.Fatal("deferred replay incomplete")
		}
		ack, err := client.spaces[handshakeSpace].received.Ack(now, 0)
		if err != nil || ack.ECN == nil || *ack.ECN != [3]uint64{0, 0, uint64(len(handshakes))} {
			t.Fatal("deferred marking lost or double counted", ack.ECN, err)
		}
		initialAck, err := client.spaces[initialSpace].received.Ack(now, 0)
		if err != nil || initialAck.ECN == nil || *initialAck.ECN != [3]uint64{uint64(len(initials)), 0, 0} {
			t.Fatal("replay borrowed current datagram marking", initialAck.ECN, err)
		}
	}
}

func TestConnectionCoalescedECN(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := testPair(t, v)
		now := time.Unix(100, 0)
		for _, raw := range drain(t, client, now) {
			if err := server.HandleDatagram(raw, now); err != nil {
				t.Fatal(err)
			}
		}
		var datagram []byte
		var counts [2]uint64
		for _, raw := range drain(t, server, now) {
			h, err := wire.ParseHeader(raw, len(client.config.LocalCID))
			if err != nil {
				t.Fatal(err)
			}
			switch h.Type {
			case wire.Initial:
				counts[0]++
			case wire.Handshake:
				counts[1]++
			default:
				continue // This fixture coalesces only the two long-header spaces.
			}
			datagram = append(datagram, raw...)
		}
		if counts[0] == 0 || counts[1] == 0 {
			t.Fatal("fixture lacks coalesced spaces")
		}
		for repeat := 0; repeat < 2; repeat++ {
			if err := client.HandleDatagramECN(datagram, now, ECNECT1); err != nil {
				t.Fatal(err)
			}
		}
		for space, want := range counts {
			ack, err := client.spaces[space].received.Ack(now, 0)
			if err != nil || ack.ECN == nil || *ack.ECN != [3]uint64{0, want, 0} {
				t.Fatal("coalesced counts", space, ack.ECN, err)
			}
		}
	}
}
