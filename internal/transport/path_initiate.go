package transport

import (
	"errors"
	"net/netip"
)

var errPathInitiation = errors.New("quic: path cannot be initiated")

// initiateMultipathPath starts validation on an unused, CID-prepared path ID.
// Only a client can initiate. A successfully queued attempt consumes the ID
// even if validation subsequently fails; abandonment leaves its tombstone.
// Failure before queuing any probe leaves the ID available for another attempt.
func (c *Conn) initiateMultipathPath(id uint64, local, remote netip.AddrPort) error {
	if err := c.check(); err != nil {
		return err
	}
	if c.multipath == nil || !c.confirmed {
		return ErrHandshakeUnconfirmed
	}
	p := c.multipath
	if c.config.Server || id <= p.highestInitiated || id > p.localMax || id > p.peerMax || p.abandoned[id] != nil {
		return errPathInitiation
	}
	tuple, err := canonicalPathTuple(local, remote)
	if err != nil {
		return err
	}
	// draft-ietf-quic-multipath-21 section 2.2 retains the handshake-address
	// restriction. Other server addresses may still be used for new paths.
	if c.peerParams.DisableActiveMigration && (!c.handshakeRemote.IsValid() || tuple.remote == c.handshakeRemote) {
		return errPathInitiation
	}
	state := p.paths[id]
	if state == nil || state.application != nil || len(state.peer.active[state.peer.current].cid) == 0 {
		return errPathInitiation
	}
	advertised := false
	for seq := range state.local.active {
		if state.local.sent[seq] {
			advertised = true
			break
		}
	}
	if !advertised {
		return errPathInitiation
	}
	app := newPathApplication()
	app.network = &tuple
	// Client transmissions are exempt from the server's amplification limit.
	// This is independent of the PATH_CHALLENGE reachability/MTU validation.
	app.budget.Validate()
	state.application = app
	if err := c.validateMultipathPath(id); err != nil {
		state.application = nil
		return err
	}
	p.highestInitiated = id
	c.publishPathAddress(id, tuple)
	return nil
}
