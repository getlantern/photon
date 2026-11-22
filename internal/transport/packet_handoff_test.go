package transport

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"
)

func handoffPacket(value byte) receivedUDP {
	b := udpReadBuffers.Get().(*[65535]byte)
	b[0] = value
	return receivedUDP{buffer: b, data: b[:1], marking: ECNECT0, local: netip.MustParseAddrPort("127.0.0.1:443"), peer: netip.MustParseAddrPort("127.0.0.1:444")}
}

func TestPacketHandoffBoundsAndOwnership(t *testing.T) {
	h := newPacketHandoff()
	defer h.close()
	for i := 0; i < 64; i++ {
		p := handoffPacket(byte(i))
		if !h.offer(p) {
			udpReadBuffers.Put(p.buffer)
			t.Fatal("premature overflow")
		}
	}
	overflow := handoffPacket(255)
	if h.offer(overflow) {
		t.Fatal("unbounded inbox")
	}
	if overflow.data[0] != 255 {
		t.Fatal("failed offer consumed caller buffer")
	}
	udpReadBuffers.Put(overflow.buffer)
	for i := 0; i < 64; i++ {
		p := <-h.packets
		if p.data[0] != byte(i) || p.marking != ECNECT0 || p.local.Port() != 443 || p.peer.Port() != 444 {
			t.Fatal("lost order or metadata")
		}
		udpReadBuffers.Put(p.buffer)
	}
	rejected := handoffPacket(1)
	rejected.err = errors.New("socket reader failure")
	if h.offer(rejected) {
		t.Fatal("forwarded source socket failure")
	}
	udpReadBuffers.Put(rejected.buffer)
	if h.offer(receivedUDP{}) {
		t.Fatal("accepted unowned buffer")
	}
	h.close()
	h.close()
	p := handoffPacket(2)
	if h.offer(p) {
		t.Fatal("accepted after close")
	}
	udpReadBuffers.Put(p.buffer)
}

func TestPacketHandoffConcurrentClose(t *testing.T) {
	h := newPacketHandoff()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				p := handoffPacket(3)
				if !h.offer(p) {
					udpReadBuffers.Put(p.buffer)
				}
			}
		})
	}
	wg.Go(h.close)
	wg.Wait()
	h.close()
	if len(h.packets) != 0 {
		t.Fatal("shutdown retained datagrams")
	}
}

func TestRuntimeHandoffDeliveryAndShutdown(t *testing.T) {
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
	if err := s.Do(ctx, func(*ServerEndpoint) error {
		if s.handoff != nil || s.forwarded != nil {
			return errors.New("eager inbox allocation")
		}
		s.nonQUIC = new(nonQUICQueue)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	h, err := s.handoffTarget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.handoffTarget(ctx)
	if err != nil || h != again {
		t.Fatal("duplicate inbox", err)
	}
	p := handoffPacket(1)
	wantPeer := p.peer
	if !h.offer(p) {
		udpReadBuffers.Put(p.buffer)
		t.Fatal("rejected live inbox")
	}
	var data [10]byte
	n, peer, err := s.ReadNonQUICPacket(ctx, data[:])
	if err != nil || n != 1 || data[0] != 1 || peer.String() != wantPeer.String() {
		t.Fatal(n, peer, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	p = handoffPacket(2)
	if h.offer(p) {
		t.Fatal("late sender accepted")
	}
	udpReadBuffers.Put(p.buffer)
	if _, err := s.handoffTarget(ctx); err == nil {
		t.Fatal("closed runtime created target")
	}
}
