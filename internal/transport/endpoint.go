package transport

import (
	"container/list"
	"context"
	"errors"
	"net/netip"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

var ErrEndpointClosed = errors.New("quic: endpoint closed")
var ErrListenerClosed = errors.New("quic: listener closed")

// Outbound is a committed datagram. The owner must send it immediately. On a
// socket write failure, call HandleWriteError; never retry these bytes as
// a new transmission. A nil Conn identifies a stateless response.
type Outbound struct {
	ecn       ecnCommit
	migration *outboundMigrationProbe
	// ECN must match the marking committed to recovery for every packet in Data.
	ECN       recovery.ECNMark
	baseLarge *outboundBaseLarge
	datagram  *outboundMTUProbe
	large     *outboundLargeStream
	mtu       *outboundMTUProbe
	Local     netip.AddrPort
	Conn      *Conn
	Peer      netip.AddrPort
	Data      []byte
}

// ServerEndpoint combines fixed-path routing, Retry admission and scheduling.
// All calls, including application operations on its Conns, must be serialized.
// It owns admitted Conns but no socket or goroutine. Capacity is bounded; the
// socket owner must additionally enforce its deployment's admission rate limit.
type ServerEndpoint struct {
	authenticatedDatagrams uint64 // owner-only authentication progress across routed receives
	publicationWake        chan<- struct{}
	foreignWriters         map[*Conn]map[netip.AddrPort]foreignPacketRoute
	foreignWriterCount     int
	// GenerateConnectionID is endpoint-wide, fixed before socket ownership.
	GenerateConnectionID func() ([]byte, error)
	// ConnContext is an incoming admission hook fixed before socket ownership.
	ConnContext func(context.Context, netip.AddrPort, bool) (context.Context, error)
	// DisableVersionNegotiationPackets is an endpoint-wide admission policy.
	// Configure it before the socket owner starts; listener restarts retain it.
	DisableVersionNegotiationPackets bool
	// ResetKey enables responses to unknown short-header CIDs. Set before use.
	ResetKey    *ResetKey
	resetWindow time.Time
	resetCount  uint32
	// VerifySourceAddress overrides listener Retry policy when non-nil.
	// Configure before starting the socket owner; retained across restarts.
	VerifySourceAddress func(netip.AddrPort) bool
	locals              map[netip.AddrPort]bool
	networks            map[*Conn]*networkOwner
	local               netip.AddrPort
	clientOnly          bool
	listenerClosed      bool
	listenerGeneration  uint64
	accepted            map[*Conn]bool
	closeWhenIdle       bool
	onTerminal          func(*Conn, error)
	ready               list.List
	readyNodes          map[*Conn]*list.Element
	announced           map[*Conn]bool
	config              Config
	tokens              *token.RetryTokens
	router              *Router
	conns               []*Conn
	cursor              int
	closed              bool
}

func NewServerEndpoint(config Config, tokens *token.RetryTokens, capacity int) (*ServerEndpoint, error) {
	if (config.EnableEarlyData || config.enableEarlyData) && (config.EnableMultipath || config.multipathCapacity != 0) {
		return nil, errors.New("quic: early data with multipath is not integrated")
	}
	if tokens == nil || config.TLS == nil || (config.Version != 0 && !config.Version.Supported()) {
		return nil, errors.New("quic: invalid server endpoint configuration")
	}
	if err := validatePreferredAddresses(config.PreferredAddresses); err != nil {
		return nil, err
	}
	if len(config.PreferredAddresses) > 0 && !config.EnableMigration {
		return nil, errors.New("quic: preferred addresses require migration support")
	}
	config.PreferredAddresses = append([]netip.AddrPort(nil), config.PreferredAddresses...)
	config.preferredSocketsReady = false
	if config.AdmissionCIDLength == 0 && config.GenerateConnectionID == nil {
		config.AdmissionCIDLength = 8
	}
	if config.AdmissionCIDLength < 0 || config.AdmissionCIDLength > 20 {
		return nil, errors.New("quic: invalid admission CID length")
	}
	if config.AdmissionCIDLength == 0 && (config.EnableMultipath || config.multipathCapacity > 0) {
		return nil, errors.New("quic: multipath requires nonzero CIDs")
	}
	router, err := NewRouter(config.AdmissionCIDLength, capacity)
	if err != nil {
		return nil, err
	}
	sourceTLS := config.TLS
	config.TLS = sourceTLS.Clone()
	// Keep the caller's synchronized ticket-key lifecycle across the listener
	// snapshot. SetSessionTicketKeys must affect subsequent handshakes without
	// replacing the listener or mutating its other configuration fields.
	if config.TLS.WrapSession == nil {
		config.TLS.WrapSession = sourceTLS.EncryptTicket
	}
	if config.TLS.UnwrapSession == nil {
		config.TLS.UnwrapSession = sourceTLS.DecryptTicket
	}
	return &ServerEndpoint{listenerGeneration: 1, config: config, tokens: tokens, router: router, readyNodes: make(map[*Conn]*list.Element), announced: make(map[*Conn]bool)}, nil
}

// HandleDatagram routes before admission, so repeated Initials cannot allocate
// another connection. A returned Conn identifies which connection made progress;
// application readiness is determined by the configured Accept policy. Errors are
// per-datagram/per-connection and do not terminate other endpoint connections.
func (e *ServerEndpoint) HandleDatagram(ctx context.Context, data []byte, peer netip.AddrPort, now time.Time) (*Conn, *Outbound, error) {
	return e.handleNetworkDatagram(ctx, data, e.local, peer, now)
}

// HandleDatagramECN is the single-local-address endpoint's metadata-aware input.
func (e *ServerEndpoint) HandleDatagramECN(ctx context.Context, data []byte, peer netip.AddrPort, now time.Time, marking ECN) (*Conn, *Outbound, error) {
	return e.handleNetworkDatagramECN(ctx, data, e.local, peer, now, marking)
}

func (e *ServerEndpoint) handleNetworkDatagram(ctx context.Context, data []byte, local, peer netip.AddrPort, now time.Time) (*Conn, *Outbound, error) {
	return e.handleNetworkDatagramECN(ctx, data, local, peer, now, ECNUnknown)
}
func (e *ServerEndpoint) handleNetworkDatagramECN(ctx context.Context, data []byte, local, peer netip.AddrPort, now time.Time, marking ECN) (*Conn, *Outbound, error) {
	if marking > ECNCE && marking != ECNUnknown {
		return nil, nil, errors.New("quic: invalid ECN codepoint")
	}

	if e.networks != nil && !e.locals[canonicalPeer(local)] {
		return nil, nil, errPathBinding
	}
	if e.closed {
		return nil, nil, ErrEndpointClosed
	}
	c, known := e.router.Route(data, peer)
	if e.networks != nil {
		c, known = e.router.RouteNetwork(data, local, peer)
	}
	if known {
		if c == nil {
			reset, err := e.receiveResetFallback(data, local, peer, now, marking, nil)
			return reset, nil, err
		}
		before := c.authenticatedPackets
		err := e.receiveRoutedDatagram(c, data, local, peer, now, marking)
		if c.authenticatedPackets == before && !errors.Is(err, ErrStatelessReset) {
			if reset, resetErr := e.receiveResetFallback(data, local, peer, now, marking, c); reset != nil {
				return reset, nil, resetErr
			}
		}
		return c, nil, err
	}
	if response, err := e.resetUnknown(data, local, peer, now); response != nil || err != nil {
		return nil, response, err
	}
	if e.clientOnly || e.listenerClosed {
		return nil, nil, nil
	}
	if response, err := e.negotiateVersion(data, peer); response != nil || err != nil {
		if response != nil {
			response.Local = local
		}
		return nil, response, err
	}
	if len(e.conns) >= e.router.capacity {
		return nil, nil, ErrRouterCapacity
	}
	config := e.config
	if e.GenerateConnectionID != nil {
		config.GenerateConnectionID = e.GenerateConnectionID
	}
	if e.ResetKey != nil {
		config.ResetKey = e.ResetKey
	}
	if config.preferredSocketsReady {
		config.PreferredAddresses = nil
		for _, address := range e.config.PreferredAddresses {
			if e.locals[canonicalPeer(address)] {
				config.PreferredAddresses = append(config.PreferredAddresses, address)
			}
		}
	}
	if config.Version == 0 {
		header, err := wire.ParseHeaderWithQUICBit(data, 0, config.Parameters.GreaseQUICBit)
		if err != nil || !header.Version.Supported() {
			return nil, nil, nil
		}
		config.Version = header.Version
	}
	available := []wire.Version{e.config.Version}
	if e.config.Version == 0 {
		available = []wire.Version{wire.Version1, wire.Version2}
	}
	config.Parameters.VersionInformation = &wire.VersionInformation{Chosen: config.Version, Available: available}
	if e.ConnContext != nil {
		config.ConnContext = e.ConnContext
	}
	if e.VerifySourceAddress != nil {
		config.VerifySourceAddress = e.VerifySourceAddress
	}
	c, response, err := AdmitInitialECN(ctx, config, e.tokens, data, peer, now, marking)
	if c != nil {
		if addErr := e.router.Add(c, peer); addErr != nil {
			c.Close()
			return nil, nil, addErr
		}
		e.conns = append(e.conns, c)
		if e.networks != nil {
			owner, ownerErr := newNetworkOwner(c, local, peer)
			if ownerErr != nil {
				e.Remove(c)
				return nil, nil, ownerErr
			}
			e.networks[c] = owner
		}
		if syncErr := e.syncAt(c, now); syncErr != nil {
			return c, nil, syncErr
		}
	}
	if len(response) > 0 {
		return c, &Outbound{Local: local, Peer: peer, Data: response}, err
	}
	return c, nil, err
}
func (e *ServerEndpoint) sync(c *Conn) error {
	if c.err != nil && e.onTerminal != nil {
		e.onTerminal(c, c.err)
	}
	if c.closed || (c.err != nil && !c.Closing() && !c.Draining()) {
		e.Remove(c)
		return nil
	}
	if err := e.router.Sync(c); err != nil {
		e.Remove(c)
		return err
	}
	if c.err != nil {
		e.removeReady(c)
	} else if !e.listenerClosed && c.config.Server && (c.confirmed || (c.config.AcceptEarly && c.serverApplicationReady())) && !e.announced[c] {
		e.announced[c] = true
		e.readyNodes[c] = e.ready.PushBack(c)
	}

	return nil
}

// NextDatagram schedules one packet round-robin and synchronizes issued IDs
// before exposing the packet to the socket. Nil output means currently blocked.
func (e *ServerEndpoint) NextDatagram(now time.Time) (*Outbound, error) {
	return e.nextDatagramInto(now, nil, nil, nil)
}

// nextDatagramInto optionally reuses a descriptor after its synchronous write
// and error handling finish. Optional packetStorage is borrowed only until the
// next call; callers must complete the write and error handling before reuse.
// Optional baseStorage follows the same synchronous lifetime as out.
func (e *ServerEndpoint) nextDatagramInto(now time.Time, out *Outbound, packetStorage []byte, baseStorage *outboundBaseLarge) (*Outbound, error) {
	if e.closed {
		return nil, ErrEndpointClosed
	}
	for left := len(e.conns); left > 0 && len(e.conns) > 0; left-- {
		e.cursor %= len(e.conns)
		c := e.conns[e.cursor]
		e.cursor++
		if c.closed {
			e.Remove(c)
			continue
		}
		if c.pendingWrite != nil {
			pending := c.pendingWrite
			completed, result, ready := pending.poll()
			if ready && result.err != nil && !e.rejectForeignMigration(completed, result, now) {
				if result.n == 0 {
					e.HandleWriteError(completed, result.err, now)
				} else {
					e.Remove(c)
				}
			}
			if c.pendingWrite != nil || c.closed {
				continue
			}
		}
		if c.Draining() {
			continue
		}
		c.datagramECN = recovery.NotECT
		var data []byte
		idleBefore := c.idle
		var pathID uint64
		var err error
		var tuple pathTuple
		var ticket *foreignWriteReservation
		pathID, tuple, data, ticket, err = e.nextConnectionPacket(c, now, packetStorage)
		if len(data) > 0 && !e.locals[tuple.local] && e.networks[c] != nil && ticket == nil {
			e.Remove(c)
			return nil, errPathBinding
		}
		local, peer := tuple.local, tuple.remote

		if syncErr := e.syncAt(c, now); syncErr != nil {
			if ticket != nil {
				ticket.cancel()
			}
			return nil, syncErr
		}
		if err != nil {
			if ticket != nil {
				ticket.cancel()
			}
			return nil, err
		}
		if len(data) > 0 {
			if out == nil {
				out = new(Outbound)
			}
			*out = Outbound{ecn: c.datagramECNCommit, ECN: c.datagramECN, Conn: c, Local: local, Peer: peer, Data: data}
			out.mtu = c.outboundMTUProbe(pathID, len(data))
			if out.mtu != nil {
				out.mtu.idleBefore = idleBefore
			}
			out.large = c.outboundLargeStream(pathID, len(data))
			if out.large != nil {
				out.large.idleBefore = idleBefore
			}
			out.datagram = c.outboundLargeDatagram(pathID, len(data))
			if out.datagram != nil {
				out.datagram.idleBefore = idleBefore
			}
			out.baseLarge = c.outboundBaseLarge(len(data), baseStorage)
			if out.baseLarge != nil {
				out.baseLarge.idleBefore = idleBefore
			}
			if ticket != nil {
				out.migration = c.outboundMigration(pathID, tuple, ticket.route, idleBefore)
				if c.Closing() {
					// Cached close packets carry no recoverable stream state.
					// The bounded source queue owns the bytes until its attempt;
					// closing expiry bounds connection lifetime independently.
					_, err = ticket.submit(out.Data, out.Peer, out.ECN, e.publicationWake)
				} else {
					_, err = beginForeignWrite(ticket, out, e.publicationWake)
				}
				*out = Outbound{} // Submission retained/copied all borrowed state.
				if baseStorage != nil {
					*baseStorage = outboundBaseLarge{}
				}
				if err != nil {
					e.Remove(c)
					return nil, err
				}
				continue
			}
			return out, nil
		}
	}
	return nil, nil
}

// Scope the borrowed socket buffer to construction only. Owned callers and
// cached close packets keep their independent packet lifetimes.
func (e *ServerEndpoint) nextConnectionPacket(c *Conn, now time.Time, storage []byte) (uint64, pathTuple, []byte, *foreignWriteReservation, error) {
	c.sealStorage = storage
	defer func() { c.sealStorage = nil }()
	if owner := e.networks[c]; owner != nil {
		if err := owner.activate(now); err != nil {
			return 0, pathTuple{}, nil, nil, err
		}
		if err := e.startPreferredMigration(c, now); err != nil {
			return 0, pathTuple{}, nil, nil, err
		}
		e.pruneForeignWriters(c)
		if err := e.serviceLocalMigrations(c); err != nil {
			return 0, pathTuple{}, nil, nil, err
		}
		if len(e.foreignWriters[c]) > 0 {
			if err := c.startRuntimeMTUSearches(); err != nil {
				return 0, pathTuple{}, nil, nil, err
			}
			scope := e.foreignPacketRoutes(c)
			return c.nextBoundPacketWithRoutes(now, scope)
		}
		e.refreshForwardPublication(c)
		id, tuple, data, err := owner.nextRouted(now, e.router)
		return id, tuple, data, nil, err
	}
	tuple := pathTuple{local: e.local, remote: e.router.entries[c].peer}
	e.refreshForwardPublication(c)
	data, err := c.NextDatagram(now)
	return 0, tuple, data, nil, err
}

func (e *ServerEndpoint) Deadline() time.Time {
	var earliest time.Time
	for _, c := range e.conns {
		if at := c.RecoveryDeadline(); !at.IsZero() && (earliest.IsZero() || at.Before(earliest)) {
			earliest = at
		}
	}
	return earliest
}
func (e *ServerEndpoint) HandleTimeout(now time.Time) error {
	if e.closed {
		return ErrEndpointClosed
	}
	var result error
	for _, c := range append([]*Conn(nil), e.conns...) {
		if at := c.RecoveryDeadline(); !at.IsZero() && !now.Before(at) {
			result = errors.Join(result, c.HandleTimeout(now))
		}
		result = errors.Join(result, e.syncAt(c, now))
	}
	return result
}

// Remove disposes one connection and its routes, including after socket failure.
func (e *ServerEndpoint) Remove(c *Conn) {
	if _, ok := e.router.entries[c]; !ok {
		return
	}
	for local := range e.foreignWriters[c] {
		e.removeForeignWriter(c, local)
	}
	e.removeReady(c)
	delete(e.announced, c)
	delete(e.accepted, c)
	delete(e.networks, c)
	c.Close()
	if e.onTerminal != nil {
		cause := c.err
		if cause == nil {
			cause = ErrEndpointClosed
		}
		e.onTerminal(c, cause)
	}
	e.router.Remove(c)
	for i, item := range e.conns {
		if item == c {
			copy(e.conns[i:], e.conns[i+1:])
			e.conns[len(e.conns)-1] = nil
			e.conns = e.conns[:len(e.conns)-1]
			if i < e.cursor {
				e.cursor--
			}
			break
		}
	}
}
func (e *ServerEndpoint) Close() {
	for len(e.conns) > 0 {
		e.Remove(e.conns[len(e.conns)-1])
	}
	e.closed = true
}

var ErrAcceptWouldBlock = errors.New("quic: no confirmed connection ready for acceptance")

func (e *ServerEndpoint) removeReady(c *Conn) {
	if node := e.readyNodes[c]; node != nil {
		e.ready.Remove(node)
		delete(e.readyNodes, c)
	}
}

// Accept returns each ready connection exactly once, in readiness order.
// By default readiness requires confirmation. AcceptEarly instead permits server
// application keys and validated peer transport limits before client Finished.
// It is nonblocking; the future socket owner can wait on its event loop when
// ErrAcceptWouldBlock is returned. Acceptance does not transfer endpoint ownership.
func (e *ServerEndpoint) Accept() (*Conn, netip.AddrPort, error) {
	if e.closed {
		return nil, netip.AddrPort{}, ErrEndpointClosed
	}
	if e.listenerClosed {
		return nil, netip.AddrPort{}, ErrListenerClosed
	}
	for e.ready.Len() > 0 {
		c := e.ready.Front().Value.(*Conn)
		e.removeReady(c)
		if c.closed {
			e.Remove(c)
			continue
		}
		if c.err != nil {
			continue
		}
		if e.accepted == nil {
			e.accepted = make(map[*Conn]bool)
		}
		e.accepted[c] = true
		return c, e.router.entries[c].peer, nil
	}
	return nil, netip.AddrPort{}, ErrAcceptWouldBlock
}

// NewClientEndpoint owns a single already-started client. Unknown datagrams
// cannot admit new connections. The caller owns the Conn until this succeeds.
func NewClientEndpoint(c *Conn, peer netip.AddrPort) (*ServerEndpoint, error) {
	if c == nil || c.config.Server || !c.started {
		return nil, errors.New("quic: invalid client endpoint")
	}
	e, err := NewClientOnlyEndpoint(len(c.config.LocalCID), 1)
	if err != nil {
		return nil, err
	}
	if err := e.router.Add(c, peer); err != nil {
		return nil, err
	}
	e.conns = append(e.conns, c)
	return e, nil
}

// NewClientOnlyEndpoint creates an empty endpoint for outgoing connections.
// Unknown datagrams cannot admit connections. Calls must be serialized by the
// socket owner, including AddClient after the runtime has started.
func NewClientOnlyEndpoint(cidLength, capacity int) (*ServerEndpoint, error) {
	router, err := NewRouter(cidLength, capacity)
	if err != nil {
		return nil, err
	}
	return &ServerEndpoint{clientOnly: true, router: router, readyNodes: make(map[*Conn]*list.Element), announced: make(map[*Conn]bool)}, nil
}

// AddClient transfers an already-started client to this endpoint on success.
// Failure leaves ownership with the caller. Outgoing clients never enter the
// listener's acceptance queue, including on an endpoint admitting servers.
func (e *ServerEndpoint) AddClient(c *Conn, peer netip.AddrPort) error {
	if e.closed {
		return ErrEndpointClosed
	}
	if e.closeWhenIdle {
		return errors.New("quic: dedicated endpoint cannot add clients after dial")
	}
	if c == nil || c.config.Server || !c.started || c.err != nil || c.closed {
		return errors.New("quic: invalid client endpoint")
	}
	if e.networks == nil && c.config.EnableMultipath {
		return errors.New("quic: multipath client requires address-aware endpoint")
	}
	if err := e.router.Add(c, peer); err != nil {
		return err
	}
	if e.networks != nil {
		owner, err := newNetworkOwner(c, e.local, peer)
		if err != nil {
			e.router.Remove(c)
			return err
		}
		e.networks[c] = owner
	}
	c.config.enableECN = e.config.enableECN
	c.config.enablePathMTUDiscovery = e.config.enablePathMTUDiscovery
	e.conns = append(e.conns, c)
	return nil
}

// CloseWhenIdle lets a dedicated client socket stop after closing/draining
// finishes. Enable after the dialer has captured the handshake result.
func (e *ServerEndpoint) CloseWhenIdle() {
	if e.clientOnly {
		e.closeWhenIdle = true
	}
}

// syncAt adds optional issuance to timestamped endpoint events. The router's
// committed peer address, rather than packet-supplied metadata, scopes the token.
func (e *ServerEndpoint) syncAt(c *Conn, now time.Time) error {
	if err := e.sync(c); err != nil {
		return err
	}
	if c.err != nil || c.closed || !c.config.Server || !c.confirmed || c.addressTokenIssued || c.config.AddressTokens == nil {
		return nil
	}
	route, ok := e.router.entries[c]
	if !ok {
		return nil
	}
	err := c.IssueAddressToken(route.peer, now)
	if errors.Is(err, ErrSendBuffer) {
		return nil
	} // Retry on a later owner event.
	if err != nil {
		return err
	}
	c.addressTokenIssued = true
	return nil
}

// ConnectionIDLength is the endpoint routing length, immutable after construction.
func (e *ServerEndpoint) ConnectionIDLength() int { return e.router.cidLength }
