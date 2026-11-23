package transport

import (
	"context"
	"net"
	"net/netip"
	"testing"
)

type opaquePacketConn struct {
	net.PacketConn
	identity []byte
}

type numericPacketAddress string

func (a numericPacketAddress) Network() string { return "udp" }
func (a numericPacketAddress) String() string  { return string(a) }

func TestPacketConnAddressConversion(t *testing.T) {
	for _, address := range []net.Addr{
		&net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 443},
		numericPacketAddress("192.0.2.1:443"),
		numericPacketAddress("[::ffff:192.0.2.1]:443"),
	} {
		got, err := packetAddrPort(address)
		if err != nil || got != netip.MustParseAddrPort("192.0.2.1:443") {
			t.Fatal(got, err)
		}
	}
	var nilAddress *net.UDPAddr
	for _, address := range []net.Addr{nil, nilAddress, numericPacketAddress("bad"), numericPacketAddress("192.0.2.1:0"), &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 65536}} {
		if _, err := packetAddrPort(address); err == nil {
			t.Fatal("invalid address accepted", address)
		}
	}
}

func TestPacketConnRuntimeCapabilities(t *testing.T) {
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	e, err := NewClientOnlyEndpoint(8, 2)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewPacketServerBorrowed(opaquePacketConn{socket, []byte{1}}, e)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if err := runtime.Do(context.Background(), func(e *ServerEndpoint) error {
		if e.config.enableECN || e.config.enablePathMTUDiscovery {
			t.Error("generic socket advertised kernel capabilities")
		}
		if e.networks == nil {
			t.Error("generic concrete socket lost address-aware ownership")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	runtime.Close()
	if _, err := socket.WriteToUDP([]byte{1}, socket.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal("generic socket not returned", err)
	}
}
