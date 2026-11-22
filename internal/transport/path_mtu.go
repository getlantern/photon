package transport

import "errors"

const defaultPathMTUCeiling = 1452

// startPathMTUSearch initializes discovery for a single validated path.
// Ceiling is a UDP payload size constrained by the peer's limit.
func (c *Conn) startPathMTUSearch(id uint64, ceiling int) error {
	if err := c.check(); err != nil {
		return err
	}
	if c.multipath == nil || !c.confirmed || c.peerParams == nil {
		return ErrHandshakeUnconfirmed
	}
	state := c.multipath.paths[id]
	if state == nil || state.application == nil || c.multipath.abandoned[id] != nil {
		return errNoPathForControl
	}
	app := state.application
	if app.mtu != nil {
		return errors.New("quic: MTU search already initialized")
	}
	if id != 0 && app.validation.state != PathValidated {
		return errors.New("quic: MTU search requires validated path")
	}
	if ceiling < minimumQUICMTU || uint64(ceiling) > c.peerParams.MaxUDPPayloadSize {
		return errors.New("quic: MTU search exceeds peer UDP limit")
	}
	search, err := newMTUSearch(ceiling)
	if err != nil {
		return err
	}
	app.mtu = &search
	return nil
}

// packetPayloadLimit uses the configured original-path size or authenticated
// probe confirmation.
// Larger ordinary writes reserve split metadata for portable fallback.
func (a *pathApplication) packetPayloadLimit() int {
	if a.mtu != nil {
		return max(minimumQUICMTU, a.mtu.confirmed)
	}
	return minimumQUICMTU
}
func (a *pathApplication) streamPayloadLimit() int {
	limit := a.packetPayloadLimit() - (minimumQUICMTU - portableStreamPayload)
	if limit >= 1<<14 {
		limit -= 2
	} // STREAM length grows from two to four bytes.
	return limit
}

// Start lazily at output selection, including paths validated after handshake.
// 1452 fits the usual 1500-byte IPv6 link without assuming that it is reachable;
// every increase still requires an authenticated probe acknowledgment.
func (c *Conn) startRuntimeMTUSearches() error {
	if !c.config.enablePathMTUDiscovery || c.config.DisablePathMTUDiscovery || !c.confirmed || c.multipath == nil || c.peerParams == nil || c.Closing() || c.Draining() {
		return nil
	}
	ceiling := int(min(uint64(defaultPathMTUCeiling), c.peerParams.MaxUDPPayloadSize))
	if ceiling <= minimumQUICMTU {
		return nil
	}
	for id, state := range c.multipath.paths {
		a := state.application
		if a == nil || a.network == nil || a.mtu != nil || c.multipath.abandoned[id] != nil || (id != 0 && a.validation.state != PathValidated) {
			continue
		}
		if err := c.startPathMTUSearch(id, ceiling); err != nil {
			return err
		}
	}
	return nil
}

// seedInitialPathMTU carries the administrator's initial size onto the original
// confirmed path. It is not discovery evidence for any other network tuple.
// Keeping a search object even when discovery is disabled lets EMSGSIZE and
// black-hole handling reuse the ordinary split/retransmission fallback.
func (c *Conn) seedInitialPathMTU(app *pathApplication) {
	size := c.basePacketPayloadLimit()
	if size <= minimumQUICMTU && c.basePacketLimit == 0 {
		return
	}
	ceiling := int(min(c.peerParams.MaxUDPPayloadSize, uint64(defaultPathMTUCeiling)))
	search, _ := newMTUSearch(ceiling)
	search.confirmed = size
	search.fixed = c.config.DisablePathMTUDiscovery || !c.config.enablePathMTUDiscovery
	if c.basePacketLimit != 0 {
		search.upper = size
	}
	app.mtu = &search
	if c.basePacketLimit != 0 {
		app.updateMTURaise(c.baseMTUFallbackAt)
	}
}

// basePacketPayloadLimit covers the original tuple before address-aware path
// adoption, including runtimes bound to a wildcard local address.
func (c *Conn) basePacketPayloadLimit() int {
	limit := max(minimumQUICMTU, int(c.config.InitialPacketSize))
	if c.basePacketLimit != 0 {
		limit = min(limit, c.basePacketLimit)
	}
	if c.peerParams != nil {
		limit = min(limit, int(min(c.peerParams.MaxUDPPayloadSize, uint64(defaultPathMTUCeiling))))
	}
	return limit
}
