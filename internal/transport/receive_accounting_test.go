package transport

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestReceivedPacketAccounting(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, multipath := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/multipath=%t", version, multipath), func(t *testing.T) {
				var sender, receiver *Conn
				if multipath {
					sender, receiver = sendingPathPair(t, version)
				} else {
					sender, receiver = testPair(t, version)
					drivePair(t, sender, receiver, false, false)
				}
				now := time.Unix(101, 0)
				local := netip.MustParseAddrPort("192.0.2.1:443")
				remote := netip.MustParseAddrPort("192.0.2.2:444")
				var packet []byte
				if multipath {
					if err := receiver.bindPreparedPath(1, local, remote); err != nil {
						t.Fatal(err)
					}
					sender.multipath.paths[1].application.budget.Validate()
					if err := sender.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
						t.Fatal(err)
					}
					var err error
					packet, err = sender.nextPacketOnPath(1, now)
					if err != nil || len(packet) == 0 {
						t.Fatal(err)
					}
				} else {
					packet = makePeerPacket(t, sender, applicationSpace, 1000, wire.PingFrame{})
				}
				corrupt := append([]byte(nil), packet...)
				corrupt[len(corrupt)-1] ^= 1
				want := receiver.receivedTotals
				for _, data := range [][]byte{packet, packet, corrupt, {0xff}, nil} {
					var err error
					if multipath {
						err = receiver.handleBoundMultipathPacket(data, local, remote, now)
					} else {
						err = receiver.HandleDatagram(data, now)
					}
					if err != nil {
						t.Fatal(err)
					}
					if len(data) > 0 {
						want.packets++
					}
					want.bytes += uint64(len(data))
					if receiver.receivedTotals != want {
						t.Fatal(receiver.receivedTotals, want)
					}
				}
			})
		}
	}
}

func TestDeferredReplayDoesNotCountArrival(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(version), func(t *testing.T) {
			client, server := testPair(t, version)
			now := time.Unix(101, 0)
			for _, packet := range drain(t, client, now) {
				if err := server.HandleDatagram(packet, now); err != nil {
					t.Fatal(err)
				}
			}
			flights := drain(t, server, now)
			want := client.receivedTotals
			deferred := false
			for i := len(flights) - 1; i >= 0; i-- {
				if err := client.HandleDatagram(flights[i], now); err != nil {
					t.Fatal(err)
				}
				deferred = deferred || len(client.deferred) > 0
				want.packets++
				want.bytes += uint64(len(flights[i]))
				if client.receivedTotals != want {
					t.Fatal("replay counted as arrival", client.receivedTotals, want)
				}
			}
			if !deferred || len(client.deferred) != 0 {
				t.Fatal("fixture did not defer and replay server flight")
			}
		})
	}
}
