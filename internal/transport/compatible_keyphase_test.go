package transport

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

func TestCompatibleVersionKeyUpdates(t *testing.T) {
	for _, original := range []wire.Version{wire.Version1, wire.Version2} {
		selected := wire.Version1
		if selected == original {
			selected = wire.Version2
		}
		t.Run(fmt.Sprintf("%x-to-%x", original, selected), func(t *testing.T) {
			// Test-only TLS secrets stay in memory and are never logged to test output.
			var log bytes.Buffer
			client, server := compatiblePair(t, original, selected, &log)
			drivePair(t, client, server, false, false)
			var references, wrong [2]*packet.Keys
			labels := []string{"CLIENT_TRAFFIC_SECRET_0", "SERVER_TRAFFIC_SECRET_0"}
			for i, label := range labels {
				found := false
				for _, line := range strings.Split(log.String(), "\n") {
					fields := strings.Fields(line)
					if len(fields) != 3 || fields[0] != label {
						continue
					}
					if found {
						t.Fatal("duplicate application traffic secret")
					}
					found = true
					secret, err := hex.DecodeString(fields[2])
					if err != nil {
						t.Fatal("invalid test key log encoding")
					}
					references[i], err = packet.TrafficKeys(selected, client.ConnectionState().CipherSuite, secret)
					if err != nil {
						t.Fatal(err)
					}
					wrong[i], err = packet.TrafficKeys(original, client.ConnectionState().CipherSuite, secret)
					if err != nil {
						t.Fatal(err)
					}
					clear(secret)
				}
				if !found {
					t.Fatal("missing test application traffic secret")
				}
			}
			clear(log.Bytes())
			log.Reset()
			peers := []*Conn{client, server}
			for generation := 0; generation < 5; generation++ {
				if generation != 0 {
					for i := range references {
						var err error
						references[i], err = references[i].Next()
						if err != nil {
							t.Fatal(err)
						}
						wrong[i], err = wrong[i].Next()
						if err != nil {
							t.Fatal(err)
						}
					}
					if err := peers[generation%2].InitiateKeyUpdate(); err != nil {
						t.Fatal(generation, err)
					}
					// Real recovery and peer ACKs must converge before another update.
					drivePair(t, client, server, true, true)
					// An ACK-only reply does not itself require an ACK. Send PINGs
					// in both directions so either endpoint may initiate next time.
					for _, c := range peers {
						if err := c.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
							t.Fatal(err)
						}
					}
					drivePair(t, client, server, true, true)
				}
				for i, sender := range peers {
					receiver := peers[1-i]
					if sender.applicationKeys.phase != (generation%2 != 0) || sender.applicationKeys.awaitingRead {
						t.Fatal("key phase failed to converge")
					}
					// Use an isolated synthetic PN only for local cryptographic verification;
					// this packet is not injected into either live engine's number space.
					pn := uint64(100000 + generation)
					raw := phasePacket(t, receiver, sender.writeKeys(applicationSpace), sender.applicationKeys.phase, pn)
					_, plain, got, err := references[i].Open(raw, 1+len(receiver.config.LocalCID), pn)
					if err != nil || got != pn || !bytes.Equal(plain, []byte{1}) {
						t.Fatalf("generation=%d sender=%d negotiated derivation failed: %v", generation, i, err)
					}
					if _, _, _, err := wrong[i].Open(raw, 1+len(receiver.config.LocalCID), pn); err == nil {
						t.Fatal("original-version keys accepted negotiated traffic")
					}
				}
			}
		})
	}
}
