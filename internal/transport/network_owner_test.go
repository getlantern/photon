package transport

import (
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestNetworkOwnerHandshakeHandoff(t *testing.T) {
	maximum := uint64(3)
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, peerOffer := range []*uint64{&maximum, nil} {
			client, server := negotiatedPathPair(t, version, &maximum, peerOffer)
			ca := netip.MustParseAddrPort("192.0.2.1:443")
			sa := netip.MustParseAddrPort("192.0.2.2:444")
			co, err := newNetworkOwner(client, ca, sa)
			if err != nil {
				t.Fatal(err)
			}
			so, err := newNetworkOwner(server, sa, ca)
			if err != nil {
				t.Fatal(err)
			}
			owners := []*networkOwner{co, so}
			now := time.Unix(100, 0)
			complete := false
			for step := 0; step < 200; step++ {
				transmitted := false
				for i, owner := range owners {
					_, tuple, b, err := owner.next(now)
					if err != nil {
						t.Fatal(err)
					}
					if b == nil {
						continue
					}
					if err := owners[1-i].receive(b, tuple.remote, tuple.local, now); err != nil {
						t.Fatal(err)
					}
					transmitted = true
				}
				if client.confirmed && server.confirmed && !transmitted && client.queuedBytes == 0 && server.queuedBytes == 0 {
					complete = true
					break
				}
				now = now.Add(time.Millisecond)
			}
			if !complete {
				t.Fatal("address-aware handshake did not converge")
			}
			for _, owner := range owners {
				if !owner.activationChecked || (owner.conn.multipath != nil) != (peerOffer != nil) {
					t.Fatal("handoff/fallback mismatch")
				}
				if len(owner.conn.pendingMultipath) != 0 {
					t.Fatal("activation left retained packets")
				}
			}
		}
	}
}
