package transport

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestNonElicitingPacketGapsDoNotLoop(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, serverRole := range []bool{false, true} {
			for _, kind := range []string{"ack", "ack-ecn", "padding"} {
				t.Run(fmt.Sprintf("%x/server=%t/%s", version, serverRole, kind), func(t *testing.T) {
					client, server := testPair(t, version)
					drivePair(t, client, server, false, false)
					target, peer := client, server
					if serverRole {
						target, peer = server, client
					}
					now := time.Unix(200, 0)
					state := &target.spaces[applicationSpace]
					if state.nextPN == 0 {
						t.Fatal("fixture has no sent application packet to acknowledge")
					}
					ack := wire.AckFrame{Ranges: []wire.AckRange{{Smallest: state.nextPN - 1, Largest: state.nextPN - 1}}}
					if kind == "ack-ecn" {
						ack.ECN = &[3]uint64{}
					}
					var frame wire.Frame = ack
					if kind == "padding" {
						frame = wire.PaddingFrame{}
					}
					assertSilent := func() {
						t.Helper()
						raw, err := target.NextDatagram(now)
						if err != nil || len(raw) != 0 || state.ackPending {
							t.Fatalf("non-eliciting input triggered output: bytes=%d pending=%t err=%v", len(raw), state.ackPending, err)
						}
					}
					assertSilent()
					for _, pn := range []uint64{1000, 2000} {
						raw := makePeerPacket(t, peer, applicationSpace, pn, frame)
						for duplicate := 0; duplicate < 2; duplicate++ {
							if err := target.HandleDatagram(raw, now); err != nil {
								t.Fatal(err)
							}
							assertSilent()
						}
					}
					// Authentication failure must neither add an ACK range nor schedule ACKs.
					before := state.received.Expected()
					bad := bytes.Clone(makePeerPacket(t, peer, applicationSpace, 4000, wire.PingFrame{}))
					bad[len(bad)-1] ^= 1
					if err := target.HandleDatagram(bad, now); err != nil {
						t.Fatal(err)
					}
					if state.received.Expected() != before {
						t.Fatal("unauthenticated packet changed receive history")
					}
					assertSilent()
					// A later PING legitimately elicits one ACK that includes earlier silent
					// packets. Decrypt it rather than inferring output from local bookkeeping.
					raw := makePeerPacket(t, peer, applicationSpace, 3000, wire.PingFrame{})
					if err := target.HandleDatagram(raw, now); err != nil {
						t.Fatal(err)
					}
					output, err := target.NextDatagram(now)
					if err != nil || len(output) == 0 {
						t.Fatal("PING failed to elicit ACK", err)
					}
					header, err := wire.ParseHeader(output, len(peer.config.LocalCID))
					if err != nil {
						t.Fatal(err)
					}
					_, plain, _, err := peer.readKeys(applicationSpace).Open(output, header.PNOffset, peer.spaces[applicationSpace].received.Expected())
					if err != nil {
						t.Fatal(err)
					}
					decoded, _, err := wire.ConsumeFrame(plain)
					if err != nil {
						t.Fatal(err)
					}
					received, ok := decoded.(wire.AckFrame)
					if !ok {
						t.Fatalf("expected ACK, got %T", decoded)
					}
					for _, pn := range []uint64{1000, 2000, 3000} {
						if !received.Acknowledges(pn) {
							t.Fatalf("ACK omitted processed packet %d", pn)
						}
					}
					if received.Acknowledges(4000) {
						t.Fatal("ACK included failed authentication")
					}
					assertSilent()
				})
			}
		}
	}
}
