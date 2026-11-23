//go:build linux || darwin

package transport

import (
	"encoding/binary"

	"golang.org/x/sys/unix"
)

// ECN reception is optional. If an option is unavailable, ordinary QUIC still
// works and packets lacking control metadata carry ECNUnknown.
func configureReceiveECN(socket socketController) {
	raw, err := socket.SyscallConn()
	if err != nil {
		return
	}
	_ = raw.Control(func(fd uintptr) {
		address, err := unix.Getsockname(int(fd))
		if err != nil {
			return
		}
		switch address.(type) {
		case *unix.SockaddrInet4:
			_ = unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_RECVTOS, 1)
		case *unix.SockaddrInet6:
			_ = unix.SetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_RECVTCLASS, 1)
			only6, err := unix.GetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_V6ONLY)
			if err == nil && only6 == 0 {
				_ = unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_RECVTOS, 1)
			}
		}
	})
}

func receivedECN(oob []byte, flags int) ECN {
	if flags&unix.MSG_CTRUNC != 0 {
		return ECNUnknown
	}
	marking := ECNUnknown
	for len(oob) > 0 {
		h, data, rest, err := unix.ParseOneSocketControlMessage(oob)
		if err != nil {
			return ECNUnknown
		}
		oob = rest
		var next ECN
		switch {
		case h.Level == unix.IPPROTO_IP && h.Type == udpECNTOS:
			if len(data) < 1 {
				return ECNUnknown
			}
			next = ECN(data[0] & 3)
		case h.Level == unix.IPPROTO_IPV6 && h.Type == unix.IPV6_TCLASS:
			if len(data) != 4 {
				return ECNUnknown
			}
			next = ECN(binary.NativeEndian.Uint32(data) & 3)
		default:
			continue
		}
		if marking != ECNUnknown && marking != next {
			return ECNUnknown
		}
		marking = next
	}
	return marking
}

func udpPayloadTruncated(flags int) bool { return flags&unix.MSG_TRUNC != 0 }
