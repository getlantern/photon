package transport

import (
	"errors"
	"net/netip"
	"time"
)

// networkOwner is the serialized engine boundary for an address-aware socket
// runtime. Construction supplies the already associated primary socket tuple;
// input cannot choose or overwrite that association. It performs the negotiated
// handoff at confirmation and returns the source/destination pair for all output.
// Socket allocation, CID demultiplexing and concurrent application calls remain
// the runtime's responsibility.
type networkOwner struct {
	conn              *Conn
	primary           pathTuple
	activationChecked bool
}

func newNetworkOwner(c *Conn, local, remote netip.AddrPort) (*networkOwner, error) {
	if c == nil || c.multipath != nil {
		return nil, errors.New("quic: invalid network owner handoff")
	}
	if err := c.check(); err != nil {
		return nil, err
	}
	tuple, err := canonicalPathTuple(local, remote)
	if err != nil {
		return nil, err
	}
	c.handshakeRemote = tuple.remote
	c.publishRemoteAddress(tuple.remote)
	return &networkOwner{conn: c, primary: tuple}, nil
}

func (o *networkOwner) activate(now time.Time) error {
	c := o.conn
	if o.activationChecked || !c.confirmed || c.Closing() || c.Draining() {
		return nil
	}
	activated, err := c.activateNegotiatedMultipath(o.primary.local, o.primary.remote)
	if err != nil {
		return c.closeReceiveFailure(err, now)
	}
	if !activated && c.config.EnableMigration && len(c.peerCID) > 0 && len(c.config.LocalCID) > 0 && c.peerIDs.retireBefore == 0 {
		if err := c.activateBasePath(o.primary.local, o.primary.remote); err != nil {
			return c.closeReceiveFailure(err, now)
		}
	}
	if activated && c.config.EnableMultipath {
		for id := uint64(1); id <= min(uint64(3), c.multipath.localMax, c.multipath.peerMax); id++ {
			if err := c.advertisePathCIDs(id); err != nil {
				return c.closeReceiveFailure(err, now)
			}
		}
	}
	o.activationChecked = true
	return nil
}

func (o *networkOwner) receive(data []byte, local, remote netip.AddrPort, now time.Time) (err error) {
	return o.receiveECN(data, local, remote, now, ECNUnknown)
}
func (o *networkOwner) receiveECN(data []byte, local, remote netip.AddrPort, now time.Time, marking ECN) (err error) {

	c := o.conn
	if c.multipath != nil {
		defer func() { err = c.closeReceiveFailure(err, now) }()
		return c.handleBoundMultipathPacketECN(data, local, remote, now, marking)
	}
	tuple, tupleErr := canonicalPathTuple(local, remote)
	if tupleErr != nil || tuple != o.primary {
		return nil
	}
	if err = c.HandleDatagramECN(data, now, marking); err != nil {
		return err
	}
	return o.activate(now)
}

func (o *networkOwner) next(now time.Time) (uint64, pathTuple, []byte, error) {
	if err := o.activate(now); err != nil {
		return 0, pathTuple{}, nil, err
	}
	if err := o.conn.startRuntimeMTUSearches(); err != nil {
		return 0, pathTuple{}, nil, err
	}
	if o.conn.multipath != nil {
		return o.conn.nextBoundMultipathPacket(now)
	}
	b, err := o.conn.NextDatagram(now)
	return 0, o.primary, b, err
}

// nextRouted reserves current CIDs in every socket router before packet
// construction can mark an advertisement sent. The runtime supplies all routers
// associated with this connection. A reservation failure returns no packet and
// consumes no packet number; the runtime can remove the connection on collision.
func (o *networkOwner) nextRouted(now time.Time, routers ...*Router) (uint64, pathTuple, []byte, error) {
	if len(routers) == 0 {
		return 0, pathTuple{}, nil, errors.New("quic: missing socket routers")
	}
	if err := o.activate(now); err != nil {
		return 0, pathTuple{}, nil, err
	}
	for _, r := range routers {
		if r == nil {
			return 0, pathTuple{}, nil, errors.New("quic: missing socket router")
		}
		if err := r.Sync(o.conn); err != nil {
			return 0, pathTuple{}, nil, err
		}
	}
	return o.next(now)
}
