package transport

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestForwardReceiveConnectionAuthorization(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, base := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/base=%v", uint32(version), base), func(t *testing.T) {
				c, peer := testPair(t, version, streamParameters())
				drivePair(t, c, peer, false, false)
				old, next, remote := netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.1:445"), netip.MustParseAddrPort("192.0.2.2:444")
				if err := c.activateBasePath(old, remote); err != nil {
					t.Fatal(err)
				}
				if err := peer.activateBasePath(remote, old); err != nil {
					t.Fatal(err)
				}
				c.multipath.baseOnly, peer.multipath.baseOnly = base, base
				c.peerParams.DisableActiveMigration = false
				now := time.Unix(101, 0)
				candidate, err := c.prepareLocalPathProbe(0, next, now)
				if err != nil {
					t.Fatal(err)
				}
				raw, _, err := c.nextLocalMigrationProbe(0, now)
				if err != nil || raw == nil {
					t.Fatal(err)
				}
				decoded, err := peer.authenticateMultipathPacket(raw, now)
				if err != nil || decoded == nil {
					t.Fatal(err)
				}
				var challenge *wire.PathChallengeFrame
				for _, frame := range decoded.packet.frames {
					if f, ok := frame.(wire.PathChallengeFrame); ok {
						challenge = &f
					}
				}
				if challenge == nil {
					t.Fatal("missing challenge")
				}
				response, _ := (wire.PathResponseFrame{Data: challenge.Data}).Append(nil)
				proof := sealPathPayload(t, peer.writeKeys(applicationSpace), peer.peerCID, 0, peer.spaces[applicationSpace].nextPN+20, response)
				e, _ := NewClientOnlyEndpoint(8, 4)
				if err := e.router.Add(c, remote); err != nil {
					t.Fatal(err)
				}
				e.conns = append(e.conns, c)
				e.locals = map[netip.AddrPort]bool{old: true}
				e.networks = map[*Conn]*networkOwner{c: {conn: c, primary: pathTuple{old, remote}, activationChecked: true}, peer: {conn: peer, primary: pathTuple{remote, old}, activationChecked: true}}
				h := newPacketHandoff()
				defer h.close()
				e.router.handoff = h
				source, _ := NewRouter(8, 4)
				route := &handoffRoute{router: source, target: h, local: next}
				if !h.subscribe(route) {
					t.Fatal("subscribe")
				}
				if err := h.bindConnection(route, c); err != nil {
					t.Fatal(err)
				}
				packet := receivedUDP{route: route, forwarded: true, local: next, peer: remote, data: proof}
				for _, mode := range []string{"unmarked", "missing", "unbound", "other-connection", "wrong-local", "wrong-peer", "revoked", "ciphertext", "long", "closed"} {
					bad := packet
					switch mode {
					case "unmarked":
						bad.forwarded = false
					case "missing":
						bad.route = nil
					case "unbound":
						bad.route = &handoffRoute{router: source, target: h, local: next}
					case "other-connection":
						other := &handoffRoute{router: source, target: h, local: next}
						h.subscribe(other)
						if err := h.bindConnection(other, peer); err != nil {
							t.Fatal(err)
						}
						bad.route = other
					case "wrong-local":
						bad.local = old
					case "wrong-peer":
						bad.peer = netip.MustParseAddrPort("192.0.2.3:444")
					case "revoked":
						route.revoked.Store(true)
					case "ciphertext":
						bad.data = append([]byte(nil), proof...)
						bad.data[len(bad.data)-1] ^= 1
					case "long":
						bad.data = append([]byte(nil), proof...)
						bad.data[0] |= 0x80
					case "closed":
						e.closed = true
					}
					e.handleForwardedDatagram(bad, now)
					route.revoked.Store(false)
					e.closed = false
					if candidate.validation.state == PathValidated || c.closed || candidate.committed {
						t.Fatal("invalid forwarding authority accepted", mode)
					}
				}
				// The normal socket entry point continues to reject this foreign address.
				if _, _, err := e.handleNetworkDatagramECN(context.Background(), proof, next, remote, now, ECNUnknown); err != errPathBinding {
					t.Fatal("foreign address became globally authorized", err)
				}
				e.handleForwardedDatagram(packet, now)
				if candidate.validation.state != PathValidated {
					t.Fatal("authorized encrypted response not processed")
				}
				if candidate.committed || c.multipath.paths[0].application.network.local != old || e.locals[next] {
					t.Fatal("receive authority switched or globally registered socket")
				}
			})
		}
	}
}

func TestForwardReceiveNoRawFallback(t *testing.T) {
	r, _ := NewRouter(8, 4)
	s := &UDPServer{endpoint: &ServerEndpoint{router: r}, nonQUIC: new(nonQUICQueue)}
	p := handoffPacket(7)
	p.forwarded = true
	if !s.handleReceived(context.Background(), p) {
		t.Fatal("drop stopped runtime")
	}
	// A forwarded marker without registration provenance cannot enter the raw queue.
	if s.nonQUIC.count != 0 {
		t.Fatal("unauthorized forwarding reached raw delivery")
	}
}

func TestForwardReceiveSourceSocketScope(t *testing.T) {
	r, _ := NewRouter(8, 4)
	h := newPacketHandoff()
	defer h.close()
	route := updateTestRoute(t, r, h, [][]byte{{1}})
	local, other := netip.MustParseAddrPort("127.0.0.1:443"), netip.MustParseAddrPort("127.0.0.1:445")
	route.local = local // Complete fixture setup before any packet processing.
	s := &UDPServer{endpoint: &ServerEndpoint{router: r, locals: map[netip.AddrPort]bool{local: true, other: true}}, nonQUIC: new(nonQUICQueue)}
	packet := func(address netip.AddrPort) receivedUDP {
		p := handoffPacket(0)
		p.local = address
		p.buffer[1] = 1
		p.data = p.buffer[:2]
		return p
	}
	s.handleReceived(context.Background(), packet(local))
	select {
	case got := <-h.packets:
		if got.route != route || got.local != local || !got.forwarded {
			t.Error("missing provenance")
		}
		udpReadBuffers.Put(got.buffer)
	default:
		t.Fatal("matching socket did not forward")
	}
	s.handleReceived(context.Background(), packet(other))
	if len(h.packets) != 0 || s.nonQUIC.count != 0 {
		t.Fatal("registration authorized another source socket")
	}
	failed := packet(local)
	failed.err = io.EOF
	if !s.handleReceived(context.Background(), failed) {
		t.Fatal("failure stopped surviving local socket")
	}
	if !route.revoked.Load() || len(r.foreignIDs) != 0 || len(r.forwardRegistrations) != 0 || len(h.routes) != 0 {
		t.Fatal("socket failure retained forwarding authority")
	}
}
