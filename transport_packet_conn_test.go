package quic

import (
	"net"
	"testing"
)

// Expose only the generic interface, with a non-comparable dynamic value. Socket
// ownership must not depend on comparing PacketConn interfaces to one another.
type packetOnlyConn struct {
	net.PacketConn
	identity []byte
}

func TestTransportTypedNilPacketConn(t *testing.T) {
	_, tlsConfig := dialTLS(t)
	var socket *net.UDPConn
	var wrapped *packetOnlyConn
	for _, conn := range []net.PacketConn{nil, socket, wrapped} {
		tr := &Transport{Conn: conn}
		if _, err := tr.Listen(tlsConfig, nil); err == nil {
			tr.Close()
			t.Fatal("nil packet connection accepted")
		}
		if tr.runtime != nil {
			tr.Close()
			t.Fatal("failed initialization retained a runtime")
		}
	}
}

// Embed the public capability interface so it cannot be mistaken for the
// concrete UDPConn fast path, including promoted AddrPort methods.
type oobOnlyConn struct {
	OOBCapablePacketConn
	identity []byte
}

func transportTestWrapSocket(socket *net.UDPConn, kind string) net.PacketConn {
	switch kind {
	case "plain":
		return packetOnlyConn{PacketConn: socket, identity: []byte{1}}
	case "oob":
		return oobOnlyConn{OOBCapablePacketConn: socket, identity: []byte{1}}
	default:
		return socket
	}
}
