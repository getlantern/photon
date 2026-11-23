package quic

import (
	"net"
	"testing"
)

func TestAddressResultsDoNotAliasConnection(t *testing.T) {
	local := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1000}
	remote := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 2), Port: 2000}
	c := &Conn{local: local, remote: remote}
	for _, test := range []struct {
		read func() net.Addr
		want string
	}{
		{c.LocalAddr, "127.0.0.1:1000"},
		{c.RemoteAddr, "127.0.0.2:2000"},
	} {
		got := test.read().(*net.UDPAddr)
		got.IP[0] ^= 255
		got.Port = 99
		if test.read().String() != test.want {
			t.Fatal("caller mutated stored address")
		}
	}
}
