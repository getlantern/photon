package transport

import (
	"net/netip"

	"github.com/getlantern/photon/internal/wire"
)

// activateBasePath installs the address-aware path-zero engine without changing
// extension negotiation. The owner must subsequently use its bound input/output.
// Concrete-address runtimes activate this mode after confirmation.
func (c *Conn) activateBasePath(local, remote netip.AddrPort) error {
	tuple, err := canonicalPathTuple(local, remote)
	if err != nil {
		return err
	}
	if c.multipathNegotiated() {
		return errPathInitiation
	}
	p, err := adoptPathZero(c, 0, 0, 1)
	if err != nil {
		return err
	}
	p.baseOnly = true
	p.paths[0].application.network = &tuple
	p.paths[0].local.bindAuthenticated(tuple)
	c.multipath = p
	if !c.handshakeRemote.IsValid() {
		c.handshakeRemote = tuple.remote
	}
	c.publishPathAddress(0, tuple)
	return nil
}

// pathWireFrame converts internal path-zero feedback to RFC 9000 wire frames.
// Nonzero path IDs and extension-only controls remain unchanged so base-mode
// frame validation rejects them rather than silently retargeting another path.
func (c *Conn) pathWireFrame(frame wire.Frame) wire.Frame {
	if c.multipath == nil || !c.multipath.baseOnly {
		return frame
	}
	switch f := frame.(type) {
	case wire.PathAckFrame:
		if f.PathID == 0 {
			return f.AckFrame
		}
	case wire.PathNewConnectionIDFrame:
		if f.PathID == 0 {
			return f.NewConnectionIDFrame
		}
	case wire.PathRetireConnectionIDFrame:
		if f.PathID == 0 {
			return wire.RetireConnectionIDFrame{Sequence: f.Sequence}
		}
	}
	return frame
}
