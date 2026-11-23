package transport

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestBasePathWireCompatibility(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, serverSide := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/server=%v", version, serverSide), func(t *testing.T) {
				client, server := testPair(t, version, streamParameters())
				drivePair(t, client, server, false, false)
				c, peer := client, server
				if serverSide {
					c, peer = server, client
				}
				local := netip.MustParseAddrPort("192.0.2.1:443")
				remote := netip.MustParseAddrPort("192.0.2.2:444")
				oldSpace := &c.spaces[applicationSpace]
				if err := c.activateBasePath(local, remote); err != nil {
					t.Fatal(err)
				}
				if c.SupportsMultipath() || !c.multipath.baseOnly || c.multipath.paths[0].application.space != oldSpace {
					t.Fatal("base handoff changed negotiation or number space")
				}
				if err := peer.RotatePeerID(); err != nil {
					t.Fatal(err)
				}
				if err := peer.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
					t.Fatal(err)
				}
				now := time.Unix(101, 0)
				sent := 0
				for range 40 {
					input, err := peer.NextDatagram(now)
					if err != nil {
						t.Fatal(err)
					}
					if input != nil {
						if err := c.handleBoundMultipathPacket(input, local, remote, now); err != nil {
							t.Fatal(err)
						}
					}
					_, tuple, output, err := c.nextBoundMultipathPacket(now)
					if err != nil {
						t.Fatal(err)
					}
					if output != nil {
						sent++
						if tuple != (pathTuple{local, remote}) {
							t.Fatal("wrong binding")
						}
						// This legacy peer rejects unnegotiated multipath ACK/CID frames.
						if err := peer.HandleDatagram(output, now); err != nil {
							t.Fatal("base peer rejected path-zero wire output", err)
						}
					}
					now = now.Add(10 * time.Millisecond)
				}
				if sent == 0 || c.localIDs.highest < 2 || peer.HasOutstanding() {
					t.Fatal("CID replenishment/ACK exchange incomplete")
				}
				before := c.queuedBytes
				for _, frame := range []wire.Frame{
					wire.PathStatusFrame{PathID: 0},
					wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}},
					wire.PathRetireConnectionIDFrame{PathID: 1, Sequence: 0},
				} {
					if err := c.queueFrameOnPath(0, frame); err == nil || c.queuedBytes != before {
						t.Fatal("base owner queued extension frame")
					}
				}
				if err := peer.prepareApplicationKeys(); err != nil {
					t.Fatal(err)
				}
				payload, _ := (wire.PathStatusFrame{PathID: 0}).Append(nil)
				raw := sealPathPayload(t, peer.writeKeys(applicationSpace), c.localIDs.active[c.localIDs.highest].cid, 0, peer.spaces[applicationSpace].nextPN, payload)
				if err := c.handleBoundMultipathPacket(raw, local, remote, now); err == nil {
					t.Fatal("base owner accepted authenticated extension frame")
				}
			})
		}
	}
}

func TestBaseRuntimeHandoffFallbackGuards(t *testing.T) {
	for _, zeroCID := range []bool{false, true} {
		client, server := testPair(t, wire.Version1)
		drivePair(t, client, server, false, false)
		server.config.EnableMigration = true
		if zeroCID {
			// Model a confirmed peer that selected a zero-length source CID.
			server.peerCID = nil
			peer := server.peerIDs.active[server.peerIDs.current]
			peer.cid = nil
			server.peerIDs.active[server.peerIDs.current] = peer
		} else {
			if err := server.RotatePeerID(); err != nil {
				t.Fatal(err)
			}
		}
		owner, err := newNetworkOwner(server, netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.2:444"))
		if err != nil {
			t.Fatal(err)
		}
		if err := owner.activate(time.Unix(101, 0)); err != nil {
			t.Fatal("fallback closed connection", err)
		}
		if server.multipath != nil || server.Err() != nil || !owner.activationChecked {
			t.Fatal("unsafe base handoff")
		}
	}
}
