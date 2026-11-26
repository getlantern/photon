package transport

import (
	"bytes"
	"errors"
	"net/netip"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

var errPathBinding = errors.New("quic: invalid or missing path address binding")

type pathTuple struct{ local, remote netip.AddrPort }

func canonicalPathTuple(local, remote netip.AddrPort) (pathTuple, error) {
	local, remote = canonicalPeer(local), canonicalPeer(remote)
	valid := func(a netip.AddrPort) bool {
		return a.IsValid() && a.Port() != 0 && !a.Addr().IsUnspecified() && !a.Addr().IsMulticast()
	}
	if !valid(local) || !valid(remote) {
		return pathTuple{}, errPathBinding
	}
	return pathTuple{canonicalPeer(local), canonicalPeer(remote)}, nil
}

// bindPreparedPath records an already established network context. It neither
// initiates a path nor validates reachability. The owner must enforce client-only
// initiation and authenticate a server's first incoming packet before binding.
// Address changes need a separate migration transaction; this method cannot
// silently overwrite a binding or reset its packet numbers/credit.
func (c *Conn) bindPreparedPath(id uint64, local, remote netip.AddrPort) error {
	if err := c.check(); err != nil {
		return err
	}
	if c.multipath == nil || !c.confirmed {
		return ErrHandshakeUnconfirmed
	}
	tuple, err := canonicalPathTuple(local, remote)
	if err != nil {
		return err
	}
	state := c.multipath.paths[id]
	if state == nil || state.application == nil || c.multipath.abandoned[id] != nil {
		return errPathBinding
	}
	app := state.application
	if app.network != nil {
		if *app.network != tuple {
			return errPathBinding
		}
		return nil
	}
	if id == 0 && !c.handshakeRemote.IsValid() {
		c.handshakeRemote = tuple.remote
	}
	app.network = &tuple
	c.publishPathAddress(id, tuple)
	return nil
}

// handleBoundMultipathPacket handles an address-aware UDP datagram. A server accepts a first authenticated packet on a CID-prepared,
// unused path. Existing paths require their bound tuple; NAT rebinding needs a
// separate migration transaction. Authenticated replay packets can contribute
// credit, but frames are dispatched only once.
func (c *Conn) handleBoundMultipathPacket(raw []byte, local, remote netip.AddrPort, now time.Time) (result error) {
	return c.handleBoundMultipathPacketECN(raw, local, remote, now, ECNUnknown)
}
func (c *Conn) handleBoundMultipathPacketECN(raw []byte, local, remote netip.AddrPort, now time.Time, marking ECN) (result error) {
	if marking > ECNCE && marking != ECNUnknown {
		return errors.New("quic: invalid ECN codepoint")
	}
	c.recordReceivedDatagram(raw)
	defer c.publishStatistics()

	// Stateless resets may have an unknown CID or an unparseable header. Test
	// their token only after normal processing fails to authenticate, and only
	// for a known network tuple. Authenticated payload suffixes are never resets.
	before := c.authenticatedPackets
	tuple, tupleErr := canonicalPathTuple(local, remote)
	bound := tupleErr == nil && c.resetTupleKnown(tuple, now)
	defer func() {
		if bound && !c.closed && !c.Draining() && c.authenticatedPackets == before && c.isStatelessReset(raw) {
			result = c.beginDraining(ErrStatelessReset, now)
		}
	}()
	if len(raw) == 0 || uint64(len(raw)) > c.config.Parameters.MaxUDPPayloadSize {
		return nil
	}
	if raw[0]&0x80 == 0 {
		return c.handleBoundMultipathData(raw, local, remote, now, len(raw), marking)
	}
	first, err := c.parseHeader(raw)
	if err != nil {
		return nil
	}
	// Confirmation retires Initial/Handshake processing, including an original
	// compatible version. Their parseable lengths
	// still delimit a trailing 1-RTT packet; never rescan bytes after a bad header.
	for rest := raw; len(rest) > 0; {
		header, err := c.parseHeader(rest)
		if err != nil || header.PacketLen <= 0 || header.PacketLen > len(rest) {
			return nil
		}
		packet := rest[:header.PacketLen]
		rest = rest[header.PacketLen:]
		if !bytes.Equal(header.DCID, first.DCID) {
			continue
		}
		if header.Long {
			continue
		}
		return c.handleBoundMultipathData(packet, local, remote, now, len(raw), marking)
	}
	return nil
}

func (c *Conn) handleBoundMultipathData(raw []byte, local, remote netip.AddrPort, now time.Time, datagramBytes int, marking ECN) error {
	if c.Closing() {
		return c.receiveClosingMultipath(raw, local, remote, now)
	}
	if err := c.check(); err != nil {
		return err
	}
	if c.multipath == nil || !c.confirmed {
		return ErrHandshakeUnconfirmed
	}
	tuple, err := canonicalPathTuple(local, remote)
	if err != nil {
		return nil
	}
	h, err := c.parseHeader(raw)
	if err != nil || h.Long || h.Type != wire.OneRTT {
		return nil
	}
	id, ok := c.multipath.lookupLocal(h.DCID)
	if !ok {
		return nil
	}
	state := c.multipath.paths[id]
	accepting := state.application == nil
	if accepting {
		if !c.config.Server || id == 0 || id > c.multipath.peerMax || c.multipath.abandoned[id] != nil || len(state.peer.active[state.peer.current].cid) == 0 || uint64(len(raw)) > c.config.Parameters.MaxUDPPayloadSize {
			return nil
		}
		// CID state and advertised local routing already exist. Allocate only a
		// tentative packet context needed for path-specific key/nonce handling;
		// failed authentication cannot consume the ID or bind an address.
		state.application = newPathApplication(c.multipath.congestionControl)
	} else if state.application.network == nil || *state.application.network != tuple {
		if state.application.localMigration != nil && !c.config.Server {
			return c.receiveLocalMigrationCandidate(raw, tuple, now)
		}
		if c.config.allowCandidateMigration {
			return c.receiveMigrationCandidateECN(raw, tuple, now, datagramBytes, marking)
		}
		return nil
	}
	decoded, err := c.decodeMultipathPacketECN(raw, now, marking)
	if err != nil || decoded == nil {
		if accepting {
			state.application = nil
		}
		return err
	}
	app := state.application
	state.local.noteReceived(decoded.destinationCID, tuple)
	app.space.noteNonProbing(decoded.number, decoded.frames)
	if app.migration != nil {
		if _, err := app.migration.observeAuthenticated(tuple, decoded.number, hasNonProbing(decoded.frames), datagramBytes, now, c.migrationPTO(app)); err != nil {
			return err
		}
	}
	if accepting {
		app.network = &tuple
		c.publishPathAddress(id, tuple)
	}
	if err := app.budget.Received(datagramBytes); err != nil {
		return c.fail(err)
	}
	if err := c.dispatchMultipathPacket(decoded, now); err != nil {
		return err
	}
	if accepting && c.multipath.abandoned[id] == nil {
		if err := c.validateMultipathPath(id); err != nil {
			return c.fail(err)
		}
	}
	c.armPathRecovery(id, now)
	return nil
}

// nextBoundMultipathPacket commits output together with its immutable address
// pair. The UDP owner must send using that local address/socket and destination,
// or terminate on a failed write. Binding all sendable contexts is mandatory so
// selecting an unbound path cannot consume a packet number before failing.
func (c *Conn) nextBoundMultipathPacket(now time.Time) (uint64, pathTuple, []byte, error) {
	if c.pendingWrite != nil && !c.Closing() && !c.Draining() && !c.closed {
		return 0, pathTuple{}, nil, nil
	}

	if c.Closing() {
		s := c.closing
		if s.network == nil {
			return 0, pathTuple{}, nil, errPathBinding
		}
		if c.packetRoutes != nil && !c.packetRoutes.reserve(*s.network) {
			return 0, pathTuple{}, nil, nil
		}
		b, err := c.nextClosingDatagram(now)
		return s.pathID, *s.network, b, err
	}
	if err := c.check(); err != nil {
		return 0, pathTuple{}, nil, err
	}
	if c.multipath == nil || !c.confirmed {
		return 0, pathTuple{}, nil, ErrHandshakeUnconfirmed
	}
	// Migration probes below return sealed packets before nextMultipathPacket
	// runs, so check the keys' usage here too.
	if err := c.updateKeysBeforeLimit(now); err != nil {
		return 0, pathTuple{}, nil, c.fail(err)
	}
	for id, state := range c.multipath.paths {
		if state.application != nil {
			state.application.routeBlocked = false
		}
		if c.multipath.abandoned[id] == nil && state.application != nil && len(state.peer.active[state.peer.current].cid) > 0 && state.application.network == nil {
			return 0, pathTuple{}, nil, errPathBinding
		}
	}
	for id, state := range c.multipath.paths {
		if state.application == nil || c.multipath.abandoned[id] != nil {
			continue
		}
		if state.application.localMigration != nil {
			raw, tuple, err := c.nextLocalMigrationProbe(id, now)
			if err != nil || raw != nil {
				return id, tuple, raw, err
			}
		}
		if !c.config.allowCandidateMigration {
			if err := c.retireMigrationReservations(id, nil); err != nil && !errors.Is(err, ErrSendBuffer) {
				return id, pathTuple{}, nil, err
			}
		}
		if c.config.allowCandidateMigration {
			raw, tuple, err := c.nextMigrationProbe(id, now)
			if err != nil || raw != nil {
				return id, tuple, raw, err
			}
		}
	}
	id, raw, err := c.nextMultipathPacket(now)
	if err == nil && raw == nil {
		c.noteMultipathSendIdle()
	}
	if err != nil || raw == nil {
		return id, pathTuple{}, raw, err
	}
	return id, *c.multipath.paths[id].application.network, raw, nil
}
