package transport

import "net/netip"

// activateNegotiatedMultipath transfers a confirmed connection to the bound
// multipath interface only when both TLS transport-parameter sets advertise it.
// Present zero is negotiated support. False means ordinary single-path fallback.
// The owner serializes activation before delivering further application packets
// and must subsequently use bound packet input/output, including path zero.
func (c *Conn) activateNegotiatedMultipath(local, remote netip.AddrPort) (bool, error) {
	if err := c.check(); err != nil {
		return false, err
	}
	if !c.confirmed || c.peerParams == nil {
		return false, ErrHandshakeUnconfirmed
	}
	ours, theirs := c.config.Parameters.InitialMaxPathID, c.peerParams.InitialMaxPathID
	if ours == nil || theirs == nil {
		return false, nil
	}
	if c.config.multipathCapacity == 0 {
		return false, errPathInitiation
	}
	tuple, err := canonicalPathTuple(local, remote)
	if err != nil {
		return false, err
	}
	p, err := adoptPathZero(c, *ours, *theirs, c.config.multipathCapacity)
	if err != nil {
		return false, err
	}
	p.paths[0].application.network = &tuple
	p.paths[0].local.bindAuthenticated(tuple)
	c.multipath = p
	if !c.handshakeRemote.IsValid() {
		c.handshakeRemote = tuple.remote
	}
	c.publishPathAddress(0, tuple)
	pending := c.pendingMultipath
	c.pendingMultipath = nil
	c.pendingMultipathBytes = 0
	for _, received := range pending {
		if err := c.dispatchMultipathPacket(&received.packet, received.at); err != nil {
			return true, err
		}
	}
	return true, nil
}
