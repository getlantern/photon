package transport

import (
	"context"
	"github.com/getlantern/photon/internal/wire"
	"testing"
)

func TestForeignLongHeaderResetHint(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(version.String(), func(t *testing.T) {
			destination, c, router, _, route := endpointForeignFixture(t, version)
			state := c.multipath.paths[1]
			state.application.network.local = route.local
			peer := state.application.network.remote
			token := state.peer.active[state.peer.current]
			token.used, token.hasToken = true, true
			token.token = [16]byte{0xab, 0xcd}
			state.peer.active[state.peer.current] = token
			route.publishResetHints(c)
			raw := resetDatagram(token.token)
			raw[0] = 0x80 // Unparseable long header: its token still determines reset receipt.
			source, err := NewClientOnlyEndpoint(8, 4)
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			source.router = router
			buffer := udpReadBuffers.Get().(*[65535]byte)
			copy(buffer[:], raw)
			(&UDPServer{endpoint: source}).handleReceived(context.Background(), receivedUDP{buffer: buffer, data: buffer[:len(raw)], local: route.local, peer: peer, marking: ECNUnknown})
			var forwarded receivedUDP
			select {
			case forwarded = <-route.target.packets:
			default:
				t.Fatal("long-header reset was not forwarded")
			}
			if !forwarded.resetFallback || forwarded.resetReturn != nil || forwarded.buffer != buffer {
				t.Fatal("invalid reset-hint provenance")
			}
			(&UDPServer{endpoint: destination}).handleReceived(context.Background(), forwarded)
			if !c.Draining() {
				t.Fatal("long-header reset was discarded")
			}
		})
	}
}
