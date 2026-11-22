package transport

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestStatisticsMultipathCrossPathACK(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, serverSends := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/server=%t", version, serverSends), func(t *testing.T) {
				client, server := sendingPathPair(t, version)
				sender, peer := client, server
				if serverSends {
					sender, peer = server, client
				}
				local := netip.MustParseAddrPort("192.0.2.1:443")
				remote := netip.MustParseAddrPort("192.0.2.2:444")
				for _, id := range []uint64{0, 1} {
					if err := sender.bindPreparedPath(id, local, remote); err != nil {
						t.Fatal(err)
					}
					sender.multipath.paths[id].application.budget.Validate()
				}
				now := time.Unix(101, 0)
				before := sender.Statistics()
				zero := sender.multipath.paths[0].application.sent
				zeroFlight, zeroRTT := zero.BytesInFlight, zero.RTT
				one := sender.multipath.paths[1].application
				first := one.space.nextPN
				var sizes [4]int
				for i := range sizes {
					if err := sender.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
						t.Fatal(err)
					}
					packet, err := sender.nextPacketOnPath(1, now)
					if err != nil || len(packet) == 0 {
						t.Fatal("missing path packet", err)
					}
					sizes[i] = len(packet)
				}
				now = now.Add(time.Millisecond)
				ack := func(arrival, pn, acknowledged uint64) {
					t.Helper()
					payload, err := (wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: acknowledged, Largest: acknowledged}}}}).Append(nil)
					if err != nil {
						t.Fatal(err)
					}
					cid := sender.multipath.paths[arrival].local.active[0].cid
					packet := sealPathPayload(t, peer.writeKeys(applicationSpace), cid, uint32(arrival), pn, payload)
					if err := sender.handleBoundMultipathPacket(packet, local, remote, now); err != nil {
						t.Fatal(err)
					}
				}
				ack(0, 1000, first+3)
				lost := sender.Statistics()
				if lost.PacketsSent != before.PacketsSent+4 || lost.BytesSent != before.BytesSent+uint64(sizes[0]+sizes[1]+sizes[2]+sizes[3]) {
					t.Fatal("path-one send totals", before, lost, sizes)
				}
				if lost.PacketsLost != before.PacketsLost+1 || lost.BytesLost != before.BytesLost+uint64(sizes[0]) {
					t.Fatal("cross-path loss", before, lost, sizes)
				}
				if zero.BytesInFlight != zeroFlight || zero.RTT != zeroRTT || !one.sent.RTT.Sampled {
					t.Fatal("ACK changed arrival-path recovery")
				}
				if lost.MinRTT != before.MinRTT || lost.SmoothedRTT != before.SmoothedRTT {
					t.Fatal("path-one ACK changed selected path-zero RTT")
				}
				ack(0, 1001, first)
				corrected := sender.Statistics()
				if corrected.PacketsLost != before.PacketsLost || corrected.BytesLost != before.BytesLost {
					t.Fatal("cross-path correction", before, corrected)
				}
				// Repeat identical ACK evidence on another arrival path, including
				// a packet number already used on path zero.
				ack(1, 1001, first)
				duplicate := sender.Statistics()
				if duplicate.PacketsLost != corrected.PacketsLost || duplicate.BytesLost != corrected.BytesLost || duplicate.PacketsReceived != corrected.PacketsReceived+1 {
					t.Fatal("cross-path duplicate", corrected, duplicate)
				}
				if err := sender.abandonFailedPath(1); err != nil {
					t.Fatal(err)
				}
				if err := sender.Close(); err != nil {
					t.Fatal(err)
				}
				final := sender.Statistics()
				if final.PacketsLost != duplicate.PacketsLost || final.BytesLost != duplicate.BytesLost || final.BytesSent != duplicate.BytesSent || final.BytesReceived != duplicate.BytesReceived {
					t.Fatal("path disposal changed totals", duplicate, final)
				}
			})
		}
	}
}
