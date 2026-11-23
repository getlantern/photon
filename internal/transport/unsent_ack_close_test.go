package transport

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestUnsentACKProtectedClose(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, space := range []int{initialSpace, handshakeSpace, applicationSpace} {
			for _, serverRole := range []bool{false, true} {
				for _, ecn := range []bool{false, true} {
					t.Run(fmt.Sprintf("%x/space=%d/server=%t/ecn=%t", version, space, serverRole, ecn), func(t *testing.T) {
						client, server := testPair(t, version)
						now := time.Unix(100, 0)
						if space == handshakeSpace {
							for _, raw := range drain(t, client, now) {
								if err := server.HandleDatagram(raw, now); err != nil {
									t.Fatal(err)
								}
							}
							for _, raw := range drain(t, server, now) {
								if err := client.HandleDatagram(raw, now); err != nil {
									t.Fatal(err)
								}
							}
							// Do not deliver client Finished: both endpoints retain Handshake keys.
						} else if space == applicationSpace {
							drivePair(t, client, server, false, false)
						}
						target, peer := client, server
						if serverRole {
							target, peer = server, client
						}
						if target.readKeys(space) == nil || peer.writeKeys(space) == nil {
							t.Fatal("fixture lacks packet protection keys")
						}
						unsent := target.spaces[space].nextPN
						ack := wire.AckFrame{Ranges: []wire.AckRange{{Smallest: unsent, Largest: unsent}}}
						if ecn {
							ack.ECN = &[3]uint64{1, 0, 0}
						}
						raw := makePeerPacket(t, peer, space, 1000, ack)
						err := target.HandleDatagram(raw, now)
						var local *ConnectionTransportError
						if !errors.As(err, &local) || local.Remote || local.ErrorCode != 0xa || local.FrameType != ack.Type() || !target.Closing() {
							t.Fatalf("unsent ACK did not cause protocol close: %v", err)
						}
						// The peer decrypts and processes the resulting close, checking its wire
						// code and trigger independently of the target's local error object.
						output, err := target.NextDatagram(now)
						if err != nil || len(output) == 0 {
							t.Fatal("missing protected close", err)
						}
						err = peer.HandleDatagram(output, now)
						var remote *ConnectionTransportError
						if !errors.As(err, &remote) || !remote.Remote || remote.ErrorCode != 0xa || remote.FrameType != ack.Type() || !peer.Draining() {
							t.Fatalf("peer did not receive matching transport close: %v", err)
						}
					})
				}
			}
		}
	}
}
