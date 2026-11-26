package transport

import (
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestPartialResetProtectedACKOrders(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, serverSends := range []bool{false, true} {
			for _, resetFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/server=%t/reset-first=%t", version, serverSends, resetFirst), func(t *testing.T) {
					params := streamParameters()
					params.ResetStreamAt = true
					client, server := testPair(t, version, params)
					drivePair(t, client, server, false, false)
					sender, peer := client, server
					if serverSends {
						sender, peer = server, client
					}
					id, err := sender.OpenStream(true)
					if err != nil {
						t.Fatal(err)
					}
					now := time.Unix(101, 0)
					// Emit each logical item separately, and verify its protected wire
					// contents before selectively acknowledging its packet number.
					emit := func(want wire.Frame) uint64 {
						t.Helper()
						packet, err := sender.NextDatagram(now)
						if err != nil || len(packet) == 0 {
							t.Fatal("missing packet", err)
						}
						pn := sender.spaces[applicationSpace].largestSent
						header, err := wire.ParseHeader(packet, len(peer.config.LocalCID))
						if err != nil || header.Type != wire.OneRTT {
							t.Fatal("wrong header", header, err)
						}
						_, plain, gotPN, err := peer.readKeys(applicationSpace).Open(packet, header.PNOffset, pn)
						if err != nil || gotPN != pn {
							t.Fatal("packet number", gotPN, pn, err)
						}
						found := false
						for len(plain) > 0 {
							frame, n, err := wire.ConsumeFrame(plain)
							if err != nil {
								t.Fatal(err)
							}
							plain = plain[n:]
							switch f := frame.(type) {
							case wire.StreamFrame:
								w, ok := want.(wire.StreamFrame)
								if !ok || f.StreamID != id || f.Offset != w.Offset || string(f.Data) != string(w.Data) || f.Fin != w.Fin {
									t.Fatalf("unexpected STREAM: %+v", f)
								}
								found = true
							case wire.ResetStreamAtFrame:
								w, ok := want.(wire.ResetStreamAtFrame)
								if !ok || f != w {
									t.Fatalf("unexpected reset: %+v", f)
								}
								found = true
							}
						}
						if !found {
							t.Fatal("expected frame absent")
						}
						return pn
					}
					write := func(data string, fin bool, offset uint64) uint64 {
						t.Helper()
						if n, err := sender.WriteStream(id, []byte(data), fin); n != len(data) || err != nil {
							t.Fatal(n, err)
						}
						return emit(wire.StreamFrame{StreamID: id, Offset: offset, Data: []byte(data), Fin: fin})
					}
					first := write("abcdefgh", false, 0)
					last := write("ijklmnop", true, 8)
					if err := sender.CancelWriteAt(id, 7, 12); err != nil {
						t.Fatal(err)
					}
					reset := emit(wire.ResetStreamAtFrame{StreamID: id, ErrorCode: 7, FinalSize: 16, ReliableSize: 12})
					state := sender.streams.objects[id]
					released := 0
					if err := sender.AutoReleaseStream(id, func(err error) {
						if err != nil {
							t.Error(err)
						}
						released++
					}); err != nil {
						t.Fatal(err)
					}
					ackPN := uint64(1000)
					ack := func(pn uint64) {
						t.Helper()
						packet := makePeerPacket(t, peer, applicationSpace, ackPN, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: pn, Largest: pn}}})
						ackPN++
						if err := sender.HandleDatagram(packet, now); err != nil {
							t.Fatal(err)
						}
					}
					if resetFirst {
						ack(reset)
					}
					ack(last)
					ack(last) // Fresh protected packet repeating already received ACK evidence.
					if state.send.Complete() || released != 0 {
						t.Fatal("ACK skipped missing prefix")
					}
					ack(first)
					if !resetFirst {
						ack(reset)
					}
					ack(reset)
					if !state.send.Complete() || released != 1 || sender.streams.objects[id] != nil || sender.queuedBytes != 0 {
						t.Fatal("incorrect retirement", released, sender.queuedBytes)
					}
				})
			}
		}
	}
}
