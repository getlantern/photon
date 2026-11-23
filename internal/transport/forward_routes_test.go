package transport

import (
	"context"
	"errors"
	"github.com/getlantern/photon/internal/wire"
	"net"
	"net/netip"
	"testing"
	"time"
)

func TestForwardCIDRegistrationAtomicity(t *testing.T) {
	r, _ := NewRouter(8, 8)
	h := newPacketHandoff()
	defer h.close()
	a := &handoffRoute{router: r, target: h}
	ids := [][]byte{{1, 2}, {3, 4, 5, 6}}
	if err := r.setForwardCIDs(a, ids); err != nil {
		t.Fatal(err)
	}
	ids[0][0] = 99
	for _, header := range []byte{0x40, 0x00} {
		if r.forwardTarget([]byte{header, 1, 2, 9}) != h || r.forwardTarget([]byte{header, 3, 4, 5, 6, 9}) != h {
			t.Fatal("CID ownership or variable-length routing")
		}
	}
	if r.forwardTarget([]byte{0xc0, 1, 2, 9}) != nil || r.forwardTarget([]byte{0x40, 1}) != nil {
		t.Fatal("malformed long/truncated short packet forwarded")
	}
	b := &handoffRoute{router: r, target: newPacketHandoff()}
	defer b.target.close()
	for _, bad := range [][][]byte{{{1}}, {{1, 2, 3}}, {{7}, {7, 8}}, {{}}, {make([]byte, 21)}} {
		if err := r.setForwardCIDs(b, bad); err == nil {
			t.Fatal("ambiguous/invalid CID admitted", bad)
		}
		if len(r.foreignIDs) != 2 || len(b.ids) != 0 {
			t.Fatal("failed registration changed routes")
		}
	}
	if err := r.setForwardCIDs(a, [][]byte{{8, 9}}); err != nil {
		t.Fatal(err)
	}
	if r.forwardTarget([]byte{0, 1, 2}) != nil || r.forwardTarget([]byte{0, 8, 9}) != h {
		t.Fatal("replacement not atomic")
	}
	if err := r.setForwardCIDs(a, nil); err != nil || len(r.foreignIDs) != 0 {
		t.Fatal("removal", err)
	}
	if err := r.setForwardCIDs(b, make([][]byte, maxForwardCIDs+1)); !errors.Is(err, ErrRouterCapacity) {
		t.Fatal(err)
	}
}

func TestForwardCIDLocalCollision(t *testing.T) {
	_, c := testPair(t, wire.Version1)
	r, _ := NewRouter(8, 8)
	h := newPacketHandoff()
	defer h.close()
	foreign := &handoffRoute{router: r, target: h}
	// Foreign-first collision must reject local registration before advertising it.
	if err := r.setForwardCIDs(foreign, [][]byte{c.config.LocalCID[:2]}); err != nil {
		t.Fatal(err)
	}
	peer := netip.MustParseAddrPort("192.0.2.1:444")
	if err := r.Add(c, peer); !errors.Is(err, ErrRouteCollision) {
		t.Fatal(err)
	}
	if len(r.entries) != 0 || len(r.ids) != 0 {
		t.Fatal("collision partially installed local route")
	}
	if err := r.setForwardCIDs(foreign, nil); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(c, peer); err != nil {
		t.Fatal(err)
	}
	// Local-first collision must equally preserve existing ownership.
	if err := r.setForwardCIDs(foreign, [][]byte{c.config.LocalCID}); !errors.Is(err, ErrRouteCollision) {
		t.Fatal(err)
	}
	if len(r.foreignIDs) != 0 {
		t.Fatal("foreign route stole local CID")
	}
}

func TestForwardCIDSocketDispatch(t *testing.T) {
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	e, err := NewClientOnlyEndpoint(8, 4)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewUDPServerBorrowed(socket, e)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	h := newPacketHandoff()
	defer h.close()
	route, err := s.registerHandoffRoute(ctx, h, [][]byte{{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	sender, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	packet := []byte{0, 1, 2, 3, 4, 5}
	if _, err = sender.WriteToUDP(packet, socket.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-h.packets:
		if !got.forwarded || got.route != route || string(got.data) != string(packet) || got.local != socket.LocalAddr().(*net.UDPAddr).AddrPort() || got.peer != sender.LocalAddr().(*net.UDPAddr).AddrPort() {
			t.Error("handoff lost source tuple or payload")
		}
		udpReadBuffers.Put(got.buffer)
	case <-ctx.Done():
		t.Fatal("socket did not forward", ctx.Err())
	}
	// Forwarded packets cannot loop even if another transport reserves the same CID.
	p := handoffPacket(0)
	copy(p.buffer[:], packet)
	p.data = p.buffer[:len(packet)]
	p.forwarded = true
	if err := s.Do(ctx, func(*ServerEndpoint) error { s.handleReceived(ctx, p); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(h.packets) != 0 {
		t.Fatal("forwarding loop")
	}
	if err := s.updateHandoffRoute(ctx, route, nil); err != nil {
		t.Fatal(err)
	}
}

func TestForwardCIDFailureDoesNotFallThrough(t *testing.T) {
	r, _ := NewRouter(8, 4)
	h := newPacketHandoff()
	defer h.close()
	route := &handoffRoute{router: r, target: h, local: netip.MustParseAddrPort("127.0.0.1:443")}
	if err := r.setForwardCIDs(route, [][]byte{{1}}); err != nil {
		t.Fatal(err)
	}
	s := &UDPServer{endpoint: &ServerEndpoint{router: r}, nonQUIC: new(nonQUICQueue)}
	for range 64 {
		p := handoffPacket(3)
		if !h.offer(p) {
			udpReadBuffers.Put(p.buffer)
			t.Fatal("inbox unexpectedly full")
		}
	}
	send := func() {
		p := handoffPacket(0)
		p.buffer[1] = 1
		p.data = p.buffer[:2]
		if !s.handleReceived(context.Background(), p) {
			t.Fatal("forwarding failure stopped receiver")
		}
	}
	send()
	if len(h.packets) != 64 || s.nonQUIC.count != 0 {
		t.Fatal("overflow fell through to local processing")
	}
	h.close()
	send()
	if len(h.packets) != 0 || s.nonQUIC.count != 0 {
		t.Fatal("closed inbox fell through to local processing")
	}
}

func TestForwardCIDRejectsZeroCIDCoexistence(t *testing.T) {
	c, _ := testPair(t, wire.Version1)
	c.config.LocalCID = nil
	c.localIDs = newLocalIDs(nil)
	r, _ := NewRouter(8, 4)
	h := newPacketHandoff()
	defer h.close()
	route := &handoffRoute{router: r, target: h}
	peer := netip.MustParseAddrPort("192.0.2.1:444")
	if err := r.Add(c, peer); err != nil {
		t.Fatal(err)
	}
	if err := r.setForwardCIDs(route, [][]byte{{1, 2}}); !errors.Is(err, ErrRouteCollision) {
		t.Fatal("foreign over zero", err)
	}
	r.Remove(c)
	if err := r.setForwardCIDs(route, [][]byte{{1, 2}}); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(c, peer); !errors.Is(err, ErrRouteCollision) {
		t.Fatal("zero over foreign", err)
	}
}
