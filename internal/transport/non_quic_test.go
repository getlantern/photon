package transport

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestNonQUICRoutingPrecedenceAndBounds(t *testing.T) {
	c, server := testPair(t, wire.Version1)
	drivePair(t, c, server, false, false)
	peer := netip.MustParseAddrPort("192.0.2.1:443")
	e, err := NewClientEndpoint(c, peer)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	s := &UDPServer{endpoint: e, nonQUIC: new(nonQUICQueue)}
	known := append([]byte{0}, c.config.LocalCID...)
	known = append(known, make([]byte, 32)...)
	if s.captureNonQUIC(known, netip.AddrPort{}, peer) {
		t.Fatal("greased known CID diverted")
	}
	id := c.peerIDs.active[c.peerIDs.current]
	id.used, id.hasToken = true, true
	id.token = [16]byte{73, 14}
	c.peerIDs.active[c.peerIDs.current] = id
	reset := resetDatagram(id.token)
	reset[0] = 0
	if s.captureNonQUIC(reset, netip.AddrPort{}, peer) {
		t.Fatal("stateless reset diverted")
	}
	for _, first := range []byte{0x40, 0x80, 0xc0} {
		if s.captureNonQUIC([]byte{first}, netip.AddrPort{}, peer) {
			t.Fatal("QUIC header classified raw")
		}
	}
	if s.captureNonQUIC(nil, netip.AddrPort{}, peer) {
		t.Fatal("empty datagram classified raw")
	}
	for i := 0; i < 2*maxNonQUICPackets; i++ {
		data := []byte{0x3f, byte(i)}
		if !s.captureNonQUIC(data, netip.AddrPort{}, peer) {
			t.Fatal("raw packet rejected")
		}
		data[1] = 255
	}
	if s.nonQUIC.count != maxNonQUICPackets {
		t.Fatal("unbounded queue")
	}
	for i, p := range s.nonQUIC.packets {
		if len(p.data) != 2 || p.data[1] != byte(i) || p.peer != peer {
			t.Fatal("queue ownership/order", i, p)
		}
	}
}

func TestNonQUICConcurrentReadersAndShutdown(t *testing.T) {
	s := testUDPServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Do(ctx, func(e *ServerEndpoint) error {
		s.nonQUIC = new(nonQUICQueue)
		for i := 0; i < maxNonQUICPackets; i++ {
			s.captureNonQUIC([]byte{0, byte(i)}, s.Addr().(*net.UDPAddr).AddrPort(), netip.MustParseAddrPort("192.0.2.1:443"))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	results := make(chan int, maxNonQUICPackets)
	for i := 0; i < maxNonQUICPackets; i++ {
		go func() {
			var data [2]byte
			n, _, err := s.ReadNonQUICPacket(ctx, data[:])
			if err != nil || n != 2 {
				t.Error(n, err)
				results <- -1
				return
			}
			results <- int(data[1])
		}()
	}
	seen := make(map[int]bool)
	for i := 0; i < maxNonQUICPackets; i++ {
		n := <-results
		if n < 0 || seen[n] {
			t.Fatal("duplicate or missing datagram", n)
		}
		seen[n] = true
	}
	blocked := make(chan error, 1)
	go func() { _, _, err := s.ReadNonQUICPacket(ctx, nil); blocked <- err }()
	s.Close()
	if err := <-blocked; !errors.Is(err, ErrEndpointClosed) {
		t.Fatal("blocked read at shutdown", err)
	}
	if s.nonQUIC != nil {
		t.Fatal("shutdown retained queue")
	}
}
