package transport

import (
	"context"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestOwnedCIDForeignResetHandoff(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []string{"authenticated", "closing-authenticated", "key-phase", "reset", "retired", "wrong-tuple", "revoked"} {
			t.Run(fmt.Sprintf("%s/%s", version, mode), func(t *testing.T) {
				destination, b, source, writer, route := endpointForeignFixture(t, version)
				state := b.multipath.paths[1]
				state.application.network.local = route.local
				remote := state.application.network.remote
				a, peerA := testPair(t, version, streamParameters())
				drivePair(t, a, peerA, false, false)
				endpoint, err := NewClientOnlyEndpoint(8, 4)
				if err != nil {
					t.Fatal(err)
				}
				endpoint.router = source
				defer endpoint.Close()
				if err := endpoint.AddClient(a, remote); err != nil {
					t.Fatal(err)
				}
				if err := endpoint.bindNetworkOwner(route.local); err != nil {
					t.Fatal(err)
				}
				if err := destination.bindForeignWriter(b, writer, route); err != nil {
					t.Fatal(err)
				}
				raw := makePeerPacket(t, peerA, applicationSpace, 1000, wire.PingFrame{})
				if mode == "key-phase" {
					header, err := peerA.makeHeader(applicationSpace, 1000, 17)
					if err != nil {
						t.Fatal(err)
					}
					header[0] ^= 4
					raw, err = peerA.writeKeys(applicationSpace).Seal(header, []byte{1}, 1000)
					if err != nil {
						t.Fatal(err)
					}
				}
				if mode == "closing-authenticated" {
					if err := a.CloseWithError(0, "done", time.Now()); err != nil {
						t.Fatal(err)
					}
				}

				token := state.peer.active[state.peer.current]
				token.used, token.hasToken = true, true
				copy(token.token[:], raw[len(raw)-16:])
				state.peer.active[state.peer.current] = token
				if _, err := destination.router.publishForwardCIDs(b, nil); err != nil {
					t.Fatal(err)
				}
				source.applyForwardUpdates()
				if source.forwardRoute(raw) != nil {
					t.Fatal("CID belongs to foreign owner, not local fixture")
				}
				if source.forwardResetRoute(raw, route.local, remote) != nil {
					t.Fatal("reset hint preempted local authentication")
				}
				authenticated := mode == "authenticated" || mode == "closing-authenticated" || mode == "key-phase"
				if !authenticated {
					raw[1+len(a.config.LocalCID)] ^= 1
				}
				buffer := udpReadBuffers.Get().(*[65535]byte)
				copy(buffer[:], raw)
				sourceRuntime := &UDPServer{endpoint: endpoint}
				if !sourceRuntime.handleReceived(context.Background(), receivedUDP{buffer: buffer, data: buffer[:len(raw)], local: route.local, peer: remote, marking: ECNUnknown}) {
					t.Fatal("source stopped")
				}
				h := route.target
				if authenticated {
					if endpoint.authenticatedDatagrams != 1 || len(h.packets) != 0 || b.Draining() {
						t.Fatal("authenticated packet stolen by reset hint")
					}
					return
				}
				var forwarded receivedUDP
				select {
				case forwarded = <-h.packets:
				default:
					t.Fatal("CID collision hid foreign reset")
				}
				if endpoint.authenticatedDatagrams != 0 || !forwarded.forwarded || !forwarded.resetFallback || forwarded.route != route || forwarded.buffer != buffer {
					t.Fatal("handoff lost authentication/ownership metadata")
				}
				switch mode {
				case "retired":
					token.hasToken = false
					state.peer.active[state.peer.current] = token
				case "wrong-tuple":
					state.application.network.local = netip.MustParseAddrPort("192.0.2.1:446")
				case "revoked":
					route.revoke()
				}
				targetRuntime := &UDPServer{endpoint: destination}
				if !targetRuntime.handleReceived(context.Background(), forwarded) {
					t.Fatal("destination stopped")
				}
				if (mode == "reset") != b.Draining() || a.Draining() {
					t.Fatal("incorrect reset target", a.Draining(), b.Draining())
				}
				if len(h.packets) != 0 {
					t.Fatal("forwarded datagram crossed owners twice")
				}
			})
		}
	}
}
