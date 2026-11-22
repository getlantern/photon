package transport

import (
	"context"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestForeignCIDResetReturn(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, foreignTarget := range []bool{false, true} {
			for _, mode := range []string{"reset", "authenticated", "closing-authenticated", "key-phase", "retired", "removed", "wrong-tuple", "source-closed", "destination-closed", "return-full", "handoff-full", "ambiguous", "replacement", "late-ambiguity", "final-full", "long-reset", "long-authenticated", "long-closing-authenticated"} {
				if mode == "final-full" && !foreignTarget {
					continue
				}
				t.Run(fmt.Sprintf("%s/foreign=%v/%s", version, foreignTarget, mode), func(t *testing.T) {
					local := netip.MustParseAddrPort("192.0.2.1:445")
					remote := netip.MustParseAddrPort("192.0.2.2:444")
					newEndpoint := func() *ServerEndpoint {
						e, err := NewClientOnlyEndpoint(8, 4)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(e.Close)
						return e
					}
					source := &UDPServer{endpoint: newEndpoint()}
					t.Cleanup(func() {
						if source.resetReturns != nil {
							source.resetReturns.close()
						}
					})
					add := func(e *ServerEndpoint, cid string) (*Conn, *Conn) {
						c, peer := testPair(t, version, streamParameters())
						drivePair(t, c, peer, false, false)
						if cid != "" {
							c.config.LocalCID = []byte(cid)
							c.localIDs = newLocalIDs(c.config.LocalCID)
						}
						if err := e.AddClient(c, remote); err != nil {
							t.Fatal(err)
						}
						if e.networks == nil {
							if err := e.bindNetworkOwner(local); err != nil {
								t.Fatal(err)
							}
						}
						if err := c.activateBasePath(local, remote); err != nil {
							t.Fatal(err)
						}
						return c, peer
					}
					bind := func(e *ServerEndpoint, c *Conn) *handoffRoute {
						h := newPacketHandoff()
						t.Cleanup(h.close)
						e.router.handoff = h
						route := updateTestRoute(t, source.endpoint.router, h, [][]byte{c.config.LocalCID})
						route.local = local
						if err := h.bindConnection(route, c); err != nil {
							t.Fatal(err)
						}
						return route
					}
					firstEndpoint := newEndpoint()
					a, peerA := add(firstEndpoint, "")
					first := bind(firstEndpoint, a)
					bEndpoint := source.endpoint
					if foreignTarget {
						bEndpoint = newEndpoint()
					}
					b, _ := add(bEndpoint, "othercid")
					var final *handoffRoute
					if foreignTarget {
						final = bind(bEndpoint, b)
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
					if mode == "closing-authenticated" || mode == "long-closing-authenticated" {
						if err := a.CloseWithError(0, "done", time.Now()); err != nil {
							t.Fatal(err)
						}
					}
					prefixLength := 0
					if mode == "long-reset" || mode == "long-authenticated" || mode == "long-closing-authenticated" {
						header, err := wire.AppendLongHeader(nil, version, wire.Handshake, a.config.LocalCID, peerA.config.LocalCID, nil, 0, 4, 16)
						if err != nil {
							t.Fatal(err)
						}
						prefix := append(header, make([]byte, 16)...)
						prefixLength = len(prefix)
						raw = append(prefix, raw...)
					}
					state := b.multipath.paths[0]
					token := state.peer.active[state.peer.current]
					token.used, token.hasToken = true, true
					copy(token.token[:], raw[len(raw)-16:])
					state.peer.active[state.peer.current] = token
					if final != nil {
						final.publishResetHints(b)
					}
					authenticated := mode == "authenticated" || mode == "closing-authenticated" || mode == "key-phase" || mode == "long-authenticated" || mode == "long-closing-authenticated"
					if !authenticated {
						raw[prefixLength+1+len(a.config.LocalCID)] ^= 1
					}
					if mode == "ambiguous" {
						first.resetHints.Store(&forwardResetSnapshot{peers: map[netip.AddrPort]struct{}{remote: {}}, tokens: map[[16]byte]struct{}{token.token: {}}})
					}
					if mode == "return-full" {
						source.resetReturns = newResetReturnMailbox()
						source.returned = source.resetReturns.packets
						for i := 0; i < cap(source.resetReturns.packets); i++ {
							ticket := source.resetReturns.reserve(resetReturnTarget{conn: b})
							t.Cleanup(ticket.cancel)
						}
					}
					if mode == "handoff-full" {
						for i := 0; i < cap(first.target.packets); i++ {
							if !first.target.offer(receivedUDP{buffer: udpReadBuffers.Get().(*[65535]byte)}) {
								t.Fatal("fill handoff")
							}
						}
					}
					buffer := udpReadBuffers.Get().(*[65535]byte)
					copy(buffer[:], raw)
					packet := receivedUDP{buffer: buffer, data: buffer[:len(raw)], local: local, peer: remote, marking: ECNUnknown}
					if !source.handleReceived(context.Background(), packet) {
						t.Fatal("source stopped")
					}
					if mode == "handoff-full" {
						if source.resetReturns.reserved != 0 {
							t.Fatal("failed dispatch leaked return reservation")
						}
						return
					}
					if mode == "destination-closed" {
						first.target.close()
						if source.resetReturns.reserved != 0 {
							t.Fatal("destination shutdown leaked return reservation")
						}
						return
					}
					var forwarded receivedUDP
					select {
					case forwarded = <-first.target.packets:
					default:
						t.Fatal("CID authentication dispatch bypassed")
					}
					noTicket := mode == "return-full" || mode == "ambiguous"
					if (forwarded.resetReturn == nil) != noTicket {
						t.Fatal("incorrect return reservation")
					}
					if forwarded.buffer != buffer || forwarded.route != first {
						t.Fatal("initial handoff lost identity")
					}
					if mode == "source-closed" {
						source.resetReturns.close()
					}
					before := a.authenticatedPackets
					(&UDPServer{endpoint: firstEndpoint}).handleReceived(context.Background(), forwarded)
					if authenticated || noTicket || mode == "source-closed" {
						if source.resetReturns != nil && len(source.resetReturns.packets) != 0 {
							t.Fatal("unexpected return")
						}
						if authenticated && a.authenticatedPackets == before {
							t.Fatal("fixture did not authenticate")
						}
						if b.Draining() {
							t.Fatal("reset target closed without return")
						}
						return
					}
					var result resetReturnResult
					select {
					case result = <-source.returned:
					default:
						t.Fatal("failed authentication did not return packet")
					}
					if result.packet.buffer != buffer || result.packet.resetReturn != nil {
						t.Fatal("return lost buffer or retained ticket")
					}
					var replacement *Conn
					switch mode {
					case "retired":
						token.hasToken = false
						state.peer.active[state.peer.current] = token
					case "removed":
						bEndpoint.Remove(b)
					case "wrong-tuple":
						state.application.network.local = netip.MustParseAddrPort("192.0.2.1:9999")
					case "replacement", "late-ambiguity":
						newOwner := source.endpoint
						if mode == "replacement" {
							bEndpoint.Remove(b)
							source.endpoint.router.pruneForwardRoutes()
							newOwner = bEndpoint
						}
						replacement, _ = add(newOwner, "newercid")
						ids := replacement.multipath.paths[0].peer
						entry := ids.active[ids.current]
						entry.used, entry.hasToken, entry.token = true, true, token.token
						ids.active[ids.current] = entry
						if err := newOwner.router.Sync(replacement); err != nil {
							t.Fatal(err)
						}
						if mode == "replacement" && foreignTarget {
							bind(newOwner, replacement).publishResetHints(replacement)
						}
					case "final-full":
						for i := 0; i < cap(final.target.packets); i++ {
							if !final.target.offer(receivedUDP{buffer: udpReadBuffers.Get().(*[65535]byte)}) {
								t.Fatal("fill final queue")
							}
						}
					}
					source.handleResetReturn(result, time.Now())
					if final != nil && len(final.target.packets) != 0 && mode != "final-full" {
						last := <-final.target.packets
						if last.resetReturn != nil || !last.resetFallback || last.buffer != buffer {
							t.Fatal("invalid final handoff")
						}
						(&UDPServer{endpoint: bEndpoint}).handleReceived(context.Background(), last)
					}
					if replacement != nil && replacement.Draining() {
						t.Fatal("return selected a replacement target")
					}
					if (mode == "reset" || mode == "long-reset") != b.Draining() || a.Draining() {
						t.Fatal("incorrect reset acceptance", a.Draining(), b.Draining())
					}
					if source.resetReturns.reserved != 0 || len(source.returned) != 0 || len(first.target.packets) != 0 {
						t.Fatal("return leaked or recursed")
					}
				})
			}
		}
	}
}
