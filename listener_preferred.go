package quic

import (
	"errors"
	"net"
	"net/netip"
)

func listenPreferredSockets(primary string, requested []netip.AddrPort) (sockets []*net.UDPConn, preferred []netip.AddrPort, err error) {
	if len(requested) > 2 {
		return nil, nil, errors.New("quic: at most one preferred address per IP family")
	}
	requested = append([]netip.AddrPort(nil), requested...)
	families := map[bool]bool{}
	for i, address := range requested {
		ip := address.Addr().Unmap()
		if !ip.IsValid() || ip.IsUnspecified() || ip.IsMulticast() || ip.Zone() != "" || families[ip.Is4()] {
			return nil, nil, errors.New("quic: preferred addresses require distinct concrete IP families")
		}
		families[ip.Is4()] = true
		requested[i] = netip.AddrPortFrom(ip, address.Port())
	}
	primaryAddress, err := net.ResolveUDPAddr("udp", primary)
	if err != nil {
		return nil, nil, err
	}
	if len(requested) > 0 && (len(primaryAddress.IP) == 0 || primaryAddress.IP.IsUnspecified()) {
		return nil, nil, errors.New("quic: preferred addresses require a concrete primary bind address")
	}
	socket, err := net.ListenUDP("udp", primaryAddress)
	if err != nil {
		return nil, nil, err
	}
	sockets = append(sockets, socket)
	// Capture the growing list separately from named results, which may become
	// nil in an error return before this cleanup runs.
	owned := []*net.UDPConn{socket}
	defer func() {
		if err != nil {
			for _, s := range owned {
				s.Close()
			}
		}
	}()
	for _, address := range requested {
		network := "udp6"
		if address.Addr().Is4() {
			network = "udp4"
		}
		extra, listenErr := net.ListenUDP(network, net.UDPAddrFromAddrPort(address))
		if listenErr != nil {
			return nil, nil, listenErr
		}
		owned = append(owned, extra)
		sockets = append(sockets, extra)
		preferred = append(preferred, extra.LocalAddr().(*net.UDPAddr).AddrPort())
	}
	return sockets, preferred, nil
}
