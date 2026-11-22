package transport

import (
	"context"
	"fmt"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestUnknownCIDResetAcrossOwners(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, long := range []bool{false, true} {
			for _, mode := range []string{"ambiguous", "local", "foreign", "stale-foreign"} {
				t.Run(fmt.Sprintf("%s/long=%v/%s", version, long, mode), func(t *testing.T) {
					destination, foreign, source, writer, route := endpointForeignFixture(t, version)
					state := foreign.multipath.paths[1]
					state.application.network.local = route.local
					remote := state.application.network.remote
					local, peer := testPair(t, version, streamParameters())
					drivePair(t, local, peer, false, false)
					endpoint, err := NewClientOnlyEndpoint(8, 4)
					if err != nil {
						t.Fatal(err)
					}
					endpoint.router = source
					defer endpoint.Close()
					if err := endpoint.AddClient(local, remote); err != nil {
						t.Fatal(err)
					}
					if err := endpoint.bindNetworkOwner(route.local); err != nil {
						t.Fatal(err)
					}
					if err := destination.bindForeignWriter(foreign, writer, route); err != nil {
						t.Fatal(err)
					}
					token := [16]byte{42}
					localID := local.peerIDs.active[local.peerIDs.current]
					localID.used, localID.hasToken, localID.token = mode != "foreign", true, token
					local.peerIDs.active[local.peerIDs.current] = localID
					foreignID := state.peer.active[state.peer.current]
					foreignID.used, foreignID.hasToken, foreignID.token = mode != "local", true, token
					state.peer.active[state.peer.current] = foreignID
					if _, err := destination.router.publishForwardCIDs(foreign, nil); err != nil {
						t.Fatal(err)
					}
					source.applyForwardUpdates()
					if mode == "stale-foreign" {
						foreignID.used = false
						state.peer.active[state.peer.current] = foreignID
					}
					raw := resetDatagram(token)
					if long {
						raw[0] = 0xc0
					} // malformed long header still carries a reset suffix
					if source.forwardRoute(raw) != nil {
						t.Fatal("fixture selected a foreign CID")
					}
					runtime := &UDPServer{endpoint: endpoint}
					buffer := udpReadBuffers.Get().(*[65535]byte)
					copy(buffer[:], raw)
					if !runtime.handleReceived(context.Background(), receivedUDP{buffer: buffer, data: buffer[:len(raw)], local: route.local, peer: remote, marking: ECNUnknown}) {
						t.Fatal("source stopped")
					}
					if mode == "foreign" {
						select {
						case packet := <-route.target.packets:
							if packet.buffer != buffer || !packet.resetFallback || packet.resetReturn != nil {
								t.Fatal("incorrect handoff provenance")
							}
							target := &UDPServer{endpoint: destination}
							target.handleReceived(context.Background(), packet)
						default:
							t.Fatal("unique foreign reset not forwarded")
						}
					}
					if local.Draining() != (mode == "local") || foreign.Draining() != (mode == "foreign") {
						t.Fatal("incorrect reset termination", local.Draining(), foreign.Draining())
					}
					if len(route.target.packets) != 0 {
						t.Fatal("ambiguous reset forwarded")
					}
					if endpoint.authenticatedDatagrams != 0 {
						t.Fatal("fixture unexpectedly authenticated")
					}
				})
			}
		}
	}
}
