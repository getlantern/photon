package transport

import "golang.org/x/sys/unix"

// PROBE sets DF but does not let cached route PMTU suppress future userspace
// probes. QUIC's packet builder remains responsible for its validated size limit.
const (
	udpDFDualNeedsIPv4 = true
	udpDFOption4       = unix.IP_MTU_DISCOVER
	udpDFOption6       = unix.IPV6_MTU_DISCOVER
	udpDFValue         = unix.IP_PMTUDISC_PROBE
)
