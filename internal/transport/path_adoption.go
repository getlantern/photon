package transport

import "errors"

// adoptPathZero builds the multipath state around a confirmed connection's
// existing initial path without resetting recovery, packet numbers, queued
// data, timers, congestion, pacing, or CID ownership. This is a handoff helper,
// not extension negotiation. After handoff the owner must route all CID and
// packet operations through the new table; concurrent legacy mutation is not
// supported. Both base migration and negotiated multipath use this handoff.
func adoptPathZero(c *Conn, localMax, peerMax uint64, capacity int) (*pathCIDs, error) {
	if c == nil || !c.confirmed || c.closed || c.err != nil || c.peerParams == nil || c.peerIDs == nil || c.localIDs == nil || c.recovery == nil || c.congestion == nil || c.spaces[applicationSpace].received == nil {
		return nil, errors.New("quic: path-zero handoff requires a live confirmed connection")
	}
	if c.multipath != nil {
		return nil, errors.New("quic: multipath ownership already established")
	}
	// Retired peer CID history is unavailable in the legacy manager. Negotiation
	// must hand off before rotation, so no lifetime bindings are silently lost.
	if c.peerIDs.retireBefore != 0 {
		return nil, errors.New("quic: path-zero handoff after peer CID retirement")
	}
	current, ok := c.peerIDs.active[c.peerIDs.current]
	if !ok || len(current.cid) == 0 {
		return nil, errors.New("quic: missing path-zero peer CID")
	}
	p, err := newPathCIDs(c.config.LocalCID, current.cid, localMax, peerMax, c.config.Parameters.ActiveConnectionIDLimit, c.peerParams.ActiveConnectionIDLimit, capacity)
	if err != nil {
		return nil, err
	}
	clear(p.peerHistory)
	clear(p.peerByCID)
	for seq, id := range c.peerIDs.active {
		binding := pathCIDBinding{sequence: seq}
		p.peerHistory[binding] = peerID{cid: clone(id.cid), token: id.token, hasToken: id.hasToken}
		p.peerByCID[string(id.cid)] = binding
	}
	p.localTemplate = &localIDs{length: c.localIDs.length, used: c.localIDs.used, resetKey: c.localIDs.resetKey, generate: c.localIDs.generate}
	p.paths[0] = &pathCIDState{local: c.localIDs, peer: c.peerIDs, application: &pathApplication{
		probePending: c.probeAllowance[applicationSpace], pacingAt: c.pacingAt,
		mtuNeedsValidation: c.baseMTUNeedsValidation && c.validation.state != PathValidated,
		mtuChallenge:       c.baseMTUChallenge,
		validation:         &c.validation, budget: &c.budget, space: &c.spaces[applicationSpace], sent: c.recovery, congestion: c.congestion, timer: &c.timer, pacer: &c.pacer,
	}}
	c.seedInitialPathMTU(p.paths[0].application)
	c.baseMTUChallenge, c.baseMTUNeedsValidation = nil, false
	return p, nil
}
