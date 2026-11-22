//go:build !linux && !darwin

package transport

import (
	"errors"
	"net"
	"net/netip"

	"github.com/getlantern/photon/internal/recovery"
)

const udpSupportsECN = false

func writeECNDatagram(socket OOBCapablePacketConn, data []byte, peer netip.AddrPort, marking recovery.ECNMark) (int, error) {
	if marking != recovery.NotECT {
		return 0, errors.New("quic: outgoing ECN unsupported on this platform")
	}
	return socket.WriteTo(data, net.UDPAddrFromAddrPort(peer))
}

func unsupportedECNError(error) bool { return false }
