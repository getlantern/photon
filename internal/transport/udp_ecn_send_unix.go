//go:build linux || darwin

package transport

import (
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"unsafe"

	"github.com/getlantern/photon/internal/recovery"
	"golang.org/x/sys/unix"
)

const udpSupportsECN = true

// writeECNDatagram applies only this datagram's traffic class. It never changes
// a socket-wide option: sockets may serve connections with different ECN states.
// The caller must account for the selected marking and handle write failure.
// DSCP is zero; a future DSCP policy must compose its bits with this codepoint.
func writeECNDatagram(socket OOBCapablePacketConn, data []byte, peer netip.AddrPort, marking recovery.ECNMark) (int, error) {
	if marking > recovery.ECT0 {
		return 0, errors.New("quic: invalid outgoing ECN marking")
	}
	if udp, ok := socket.(*net.UDPConn); ok {
		// Keep this buffer separate from the interface call below: passing it
		// to an arbitrary implementation would force native sends to allocate.
		var control [64]byte
		size := encodeECNControl(control[:], peer, marking)
		n, _, err := udp.WriteMsgUDPAddrPort(data, control[:size], peer)
		return n, err
	}
	var control [64]byte
	size := encodeECNControl(control[:], peer, marking)
	n, _, err := socket.WriteMsgUDP(data, control[:size], net.UDPAddrFromAddrPort(peer))
	return n, err
}

func encodeECNControl(control []byte, peer netip.AddrPort, marking recovery.ECNMark) int {
	size := unix.CmsgSpace(4)
	header := (*unix.Cmsghdr)(unsafe.Pointer(&control[0]))
	header.Level = unix.IPPROTO_IP
	header.Type = unix.IP_TOS
	if peer.Addr().Is6() && !peer.Addr().Is4In6() {
		header.Level = unix.IPPROTO_IPV6
		header.Type = unix.IPV6_TCLASS
	}
	header.SetLen(unix.CmsgLen(4))
	binary.NativeEndian.PutUint32(control[unix.CmsgLen(0):], uint32(marking))
	return size
}

func unsupportedECNError(err error) bool {
	return errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOPROTOOPT) || errors.Is(err, unix.EOPNOTSUPP)
}
