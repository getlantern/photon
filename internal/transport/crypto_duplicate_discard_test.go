package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestDuplicateFinishedInOnePacket(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := testPair(t, version)
		now := time.Unix(100, 0)
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
		var finished []wire.Frame
		for _, item := range client.spaces[handshakeSpace].queue {
			if frame, ok := item.frame.(wire.CryptoFrame); ok {
				finished = append(finished, frame)
			}
		}
		if len(finished) != 1 || !client.complete || server.complete {
			t.Fatal("fixture: client Finished", len(finished), client.complete, server.complete)
		}
		raw := makePeerPacket(t, client, handshakeSpace, 0, finished[0], finished[0])
		if err := server.HandleDatagram(raw, now); err != nil {
			t.Fatal("duplicate Finished closed the connection:", err)
		}
		if !server.confirmed || server.err != nil || server.Closing() || !server.spaces[handshakeSpace].discarded {
			t.Fatal("server state", server.confirmed, server.err)
		}
	}
}
