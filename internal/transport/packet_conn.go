package transport

import (
	"errors"
	"net"
	"net/netip"
	"reflect"
	"syscall"
)

func nilPacketConn(conn net.PacketConn) bool {
	if conn == nil {
		return true
	}
	value := reflect.ValueOf(conn)
	switch value.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
		return value.IsNil()
	}
	return false
}

func packetAddrPort(address net.Addr) (netip.AddrPort, error) {
	var result netip.AddrPort
	if udp, ok := address.(*net.UDPAddr); ok {
		if udp != nil && udp.Port > 0 && udp.Port <= 65535 {
			result = udp.AddrPort()
		}
	} else if address != nil {
		result, _ = netip.ParseAddrPort(address.String())
	}
	if !result.IsValid() || result.Port() == 0 {
		return netip.AddrPort{}, errors.New("quic: PacketConn requires UDP IP/port addresses")
	}
	return canonicalPeer(result), nil
}

// OOBCapablePacketConn preserves UDP ancillary-data operations through wrappers.
// The runtime calls the wrapper methods; it never unwraps the underlying socket.
type OOBCapablePacketConn interface {
	net.PacketConn
	SetReadBuffer(int) error
	SyscallConn() (syscall.RawConn, error)
	ReadMsgUDP(b []byte, oob []byte) (n int, oobn int, flags int, addr *net.UDPAddr, err error)
	WriteMsgUDP(b []byte, oob []byte, addr *net.UDPAddr) (n int, oobn int, err error)
}

type socketController interface {
	SyscallConn() (syscall.RawConn, error)
}
