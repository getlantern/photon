package transport

import (
	"context"
	"net"
	"net/netip"
)

const maxNonQUICPackets = 32

type nonQUICPacket struct {
	data []byte
	peer netip.AddrPort
}

type nonQUICQueue struct {
	packets     [maxNonQUICPackets]nonQUICPacket
	head, count int
}

// captureNonQUIC runs on the endpoint owner. Known routing identities, including
// greased short headers and reset tokens, take precedence over raw delivery.
func (s *UDPServer) captureNonQUIC(data []byte, local, peer netip.AddrPort) bool {
	q := s.nonQUIC
	if q == nil || len(data) == 0 || data[0]&0xc0 != 0 {
		return false
	}
	e := s.endpoint
	_, known := e.router.Route(data, peer)
	if e.networks != nil {
		_, known = e.router.RouteNetwork(data, local, peer)
	}
	if known {
		return false
	}
	if q.count < len(q.packets) {
		index := (q.head + q.count) % len(q.packets)
		q.packets[index] = nonQUICPacket{data: append([]byte(nil), data...), peer: peer}
		q.count++
	}
	return true
}

// ReadNonQUICPacket enables bounded raw reception on first use. It copies one
// complete queued datagram into b, discarding any truncated suffix. Concurrent
// readers are serialized by the owner and consume different datagrams.
func (s *UDPServer) ReadNonQUICPacket(ctx context.Context, b []byte) (int, net.Addr, error) {
	var n int
	var peer netip.AddrPort
	err := s.Wait(ctx, func(*ServerEndpoint) (bool, error) {
		if s.nonQUIC == nil {
			s.nonQUIC = new(nonQUICQueue)
		}
		q := s.nonQUIC
		if q.count == 0 {
			return false, nil
		}
		packet := q.packets[q.head]
		q.packets[q.head] = nonQUICPacket{}
		q.head = (q.head + 1) % len(q.packets)
		q.count--
		n = copy(b, packet.data)
		peer = packet.peer
		return true, nil
	})
	if err != nil {
		return 0, nil, err
	}
	return n, net.UDPAddrFromAddrPort(peer), nil
}

// WriteTo sends exactly the supplied bytes on the primary socket, without QUIC
// packet construction or accounting. The owner holds the input until I/O returns.
func (s *UDPServer) WriteTo(b []byte, addr net.Addr) (int, error) {
	var n int
	err := s.Do(context.Background(), func(*ServerEndpoint) error {
		var err error
		n, err = s.packetConn.WriteTo(b, addr)
		return err
	})
	return n, err
}
