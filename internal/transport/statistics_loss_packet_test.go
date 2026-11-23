package transport

import (
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestStatisticsProtectedLossCorrection(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, serverSends := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/server=%t", version, serverSends), func(t *testing.T) {
				client, server := testPair(t, version)
				drivePair(t, client, server, false, false)
				sender, peer := client, server
				if serverSends {
					sender, peer = server, client
				}
				now := time.Unix(101, 0)
				ackNumber := uint64(1000)
				ack := func(lo, hi uint64) []byte {
					t.Helper()
					packet := makePeerPacket(t, peer, applicationSpace, ackNumber, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: lo, Largest: hi}}})
					ackNumber++
					if err := sender.HandleDatagram(packet, now); err != nil {
						t.Fatal(err)
					}
					return packet
				}
				// Clear prior application-space feedback metadata with valid ACK
				// evidence, so later loss totals identify only the new four sends.
				first := sender.spaces[applicationSpace].nextPN
				if first > 0 {
					ack(0, first-1)
				}
				before := sender.Statistics()
				var sizes [4]int
				for i := range sizes {
					if err := sender.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
						t.Fatal(err)
					}
					packet, err := sender.NextDatagram(now)
					if err != nil || len(packet) == 0 {
						t.Fatal("missing PING", i, err)
					}
					header, err := wire.ParseHeader(packet, len(peer.config.LocalCID))
					if err != nil {
						t.Fatal(err)
					}
					_, plain, pn, err := peer.readKeys(applicationSpace).Open(packet, header.PNOffset, first+uint64(i))
					if err != nil || pn != first+uint64(i) {
						t.Fatal("packet number", pn, err)
					}
					frame, _, err := wire.ConsumeFrame(plain)
					if _, ok := frame.(wire.PingFrame); err != nil || !ok {
						t.Fatal("expected PING", frame, err)
					}
					sizes[i] = len(packet)
				}
				now = now.Add(time.Millisecond)
				ack(first+3, first+3)
				lost := sender.Statistics()
				if lost.PacketsLost != before.PacketsLost+1 || lost.BytesLost != before.BytesLost+uint64(sizes[0]) {
					t.Fatal("loss estimate", before, lost, sizes)
				}
				if lost.PacketsSent != before.PacketsSent+4 || lost.BytesSent != before.BytesSent+uint64(sizes[0]+sizes[1]+sizes[2]+sizes[3]) {
					t.Fatal("send totals", before, lost)
				}
				late := ack(first, first)
				corrected := sender.Statistics()
				if corrected.PacketsLost != before.PacketsLost || corrected.BytesLost != before.BytesLost {
					t.Fatal("late ACK not corrected", before, corrected)
				}
				if err := sender.HandleDatagram(late, now); err != nil {
					t.Fatal(err)
				}
				ack(first, first) // Same ACK evidence in a new protected packet.
				duplicate := sender.Statistics()
				if duplicate.PacketsLost != corrected.PacketsLost || duplicate.BytesLost != corrected.BytesLost || duplicate.PacketsSent != corrected.PacketsSent || duplicate.BytesSent != corrected.BytesSent {
					t.Fatal("duplicate correction", corrected, duplicate)
				}
				if err := sender.Close(); err != nil {
					t.Fatal(err)
				}
				if sender.Statistics() != duplicate {
					t.Fatal("close lost corrected snapshot")
				}
			})
		}
	}
}
