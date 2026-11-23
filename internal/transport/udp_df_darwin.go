package transport

import "golang.org/x/sys/unix"

// Darwin IPV6_DONTFRAG also sets INP2_DONTFRAG, which the IPv4 UDP output
// path consults for mapped destinations. IP_DONTFRAG on AF_INET6 is invalid.
const (
	udpDFDualNeedsIPv4 = false
	udpDFOption4       = unix.IP_DONTFRAG
	udpDFOption6       = unix.IPV6_DONTFRAG
	udpDFValue         = 1
)
