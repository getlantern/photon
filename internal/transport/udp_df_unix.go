//go:build linux || darwin

package transport

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// configureUDPSocket disables IP fragmentation before the runtime takes
// ownership or starts readers. Socket-option failures leave ownership unchanged.
func configureUDPSocket(socket socketController) error {
	raw, err := socket.SyscallConn()
	if err != nil {
		return err
	}
	var optionErr error
	err = raw.Control(func(fd uintptr) { optionErr = disableUDPFragmentation(int(fd)) })
	if err != nil {
		return err
	}
	if optionErr != nil {
		return fmt.Errorf("quic: disabling UDP fragmentation: %w", optionErr)
	}
	return nil
}
func disableUDPFragmentation(fd int) error {
	address, err := unix.Getsockname(fd)
	if err != nil {
		return err
	}
	switch address.(type) {
	case *unix.SockaddrInet4:
		return unix.SetsockoptInt(fd, unix.IPPROTO_IP, udpDFOption4, udpDFValue)
	case *unix.SockaddrInet6:
		if err := unix.SetsockoptInt(fd, unix.IPPROTO_IPV6, udpDFOption6, udpDFValue); err != nil {
			return err
		}
		only6, err := unix.GetsockoptInt(fd, unix.IPPROTO_IPV6, unix.IPV6_V6ONLY)
		if err != nil {
			return err
		}
		if only6 == 0 && udpDFDualNeedsIPv4 {
			return unix.SetsockoptInt(fd, unix.IPPROTO_IP, udpDFOption4, udpDFValue)
		}
		return nil
	default:
		return fmt.Errorf("unexpected UDP socket family")
	}
}

const udpSupportsMTUDiscovery = true
