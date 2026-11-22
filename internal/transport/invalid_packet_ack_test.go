package transport

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestSemanticFailureNeverEmitsACK(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, serverRole := range []bool{false, true} {
			for _, failure := range []string{"stream-credit", "stream-count", "final-size"} {
				t.Run(fmt.Sprintf("%x/server=%t/%s", version, serverRole, failure), func(t *testing.T) {
					client, server := testPair(t, version, streamParameters())
					drivePair(t, client, server, false, false)
					target, peer := client, server
					id := uint64(1) // peer is server, using its first bidirectional stream
					if serverRole {
						target, peer = server, client
						id = 0
					}
					frames := []wire.Frame{wire.PingFrame{}}
					code := uint64(3)
					var invalid wire.Frame
					switch failure {
					case "stream-credit":
						invalid = wire.StreamFrame{StreamID: id, Offset: 8192, Data: []byte{1}}
					case "stream-count":
						code = 4
						invalid = wire.StreamFrame{StreamID: id + 16, Data: []byte{1}}
					case "final-size":
						code = 6
						frames = append(frames, wire.StreamFrame{StreamID: id, Fin: true, Data: []byte{1}})
						invalid = wire.StreamFrame{StreamID: id, Offset: 1, Data: []byte{2}}
					}
					frames = append(frames, invalid)
					raw := makePeerPacket(t, peer, applicationSpace, 1000, frames...)
					now := time.Unix(200, 0)
					err := target.HandleDatagram(raw, now)
					var local *ConnectionTransportError
					if !errors.As(err, &local) || local.Remote || uint64(local.ErrorCode) != code || local.FrameType != invalid.Type() || !target.Closing() {
						t.Fatalf("wrong semantic failure: %v", err)
					}
					// Receipt bookkeeping can precede semantic processing. The observable
					// invariant is that close output must never contain an ACK of that packet.
					output, err := target.NextDatagram(now)
					if err != nil || len(output) == 0 {
						t.Fatal("missing close output", err)
					}
					header, err := wire.ParseHeader(output, len(peer.config.LocalCID))
					if err != nil {
						t.Fatal(err)
					}
					_, plain, _, err := peer.readKeys(applicationSpace).Open(output, header.PNOffset, peer.spaces[applicationSpace].received.Expected())
					if err != nil {
						t.Fatal(err)
					}
					haveClose := false
					for len(plain) > 0 {
						frame, n, err := wire.ConsumeFrame(plain)
						if err != nil {
							t.Fatal(err)
						}
						plain = plain[n:]
						switch frame := frame.(type) {
						case wire.AckFrame:
							t.Fatal("failed packet elicited ACK", frame)
						case wire.ConnectionCloseFrame:
							if frame.Application || frame.ErrorCode != code || frame.TriggerFrame != invalid.Type() {
								t.Fatal("wrong wire close", frame)
							}
							haveClose = true
						case wire.PaddingFrame:
						default:
							t.Fatalf("unexpected frame during close: %T", frame)
						}
					}
					if !haveClose || target.spaces[applicationSpace].ackPending {
						t.Fatal("failed packet retained ACK output")
					}
				})
			}
		}
	}
}
