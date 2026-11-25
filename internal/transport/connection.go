package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/getlantern/photon/internal/handshake"
	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

// Config describes an established routing context on a single fixed path.
// For a server, PeerCID is the client's first Initial SCID and OriginalDCID is
// that Initial's DCID. A client starts with PeerCID == OriginalDCID and learns
// the server's SCID from an authenticated Initial. Clients process Retry; server
// token generation and validation belong to the future listener/router.
type Config struct {
	// AdapterData is immutable per-connection metadata owned by the public adapter.
	AdapterData any
	// ConnContext selects incoming connection lifetime metadata before TLS starts.
	ConnContext func(context.Context, netip.AddrPort, bool) (context.Context, error)
	// ResetKey is copied by NewConn and derives tokens for all locally issued CIDs.
	ResetKey *ResetKey
	// AdmissionCIDLength chooses generated server CIDs. Zero selects eight bytes
	// with the default generator, or empty IDs with a custom generator.
	AdmissionCIDLength int
	// GenerateConnectionID supplies local IDs; output is copied and length-checked.
	// It runs on the owner and must not block or reenter connection operations.
	GenerateConnectionID func() ([]byte, error)
	// VerifySourceAddress requests Retry for an unvalidated source. Nil requires
	// Retry. Admission invokes this only after Initial integrity verification.
	VerifySourceAddress func(netip.AddrPort) bool
	// AddressVerified is token evidence supplied to ConfigureClient by admission.
	// Setting it does not grant amplification credit.
	AddressVerified bool
	// ConfigureClient selects connection settings after Initial integrity and
	// address-token processing, before allocating connection/TLS state. The
	// endpoint owner invokes it synchronously; it must not block or reenter.
	// The input is a shallow template copy: referenced data must not be mutated.
	// Routing/version identity and token policy remain admission-owned. An error
	// refuses admission with a fixed CONNECTION_REFUSED response, without
	// exposing the local error text to the peer. Nil preserves the template.
	ConfigureClient            func(Config, netip.AddrPort) (Config, error)
	MaxStreamReceiveWindow     uint64
	MaxConnectionReceiveWindow uint64
	// AcceptEarly exposes server connections after ClientHello processing.
	// It does not enable acceptance of replayable data.
	AcceptEarly bool
	// EnableEarlyData requires an application policy accepting replayable data.
	EnableEarlyData bool
	// Internal integration gate; public early-data policy and handles are unfinished.
	enableEarlyData bool
	// InitialToken is optional client address-validation evidence.
	InitialToken []byte
	// OnToken runs on the connection owner and must not block or reenter it.
	// The callback owns its token bytes.
	OnToken func([]byte, time.Time)

	// AddressTokens validates previously issued NEW_TOKEN values during server admission.
	AddressTokens           *token.AddressTokens
	DisablePathMTUDiscovery bool
	// Enabled only by a runtime that configured nonfragmenting sockets.
	enablePathMTUDiscovery bool
	// Set by the UDP runtime with per-message marking and local-error fallback.
	enableECN bool
	// Internal policy gates; public multipath enables server peer-address rebinding.
	allowCandidateMigration bool
	allowFreshCIDMigration  bool
	allowIPMigration        bool
	allowActiveMigration    bool
	// EnableMultipath enables the UDP runtime policy: four paths and automatic CID advertisement.
	EnableMultipath bool
	// EnableMigration installs a base-protocol path owner on concrete sockets.
	EnableMigration bool
	// RequireStreamCreditRelease withholds the MAX_STREAMS credit of a released
	// peer-initiated stream until ReleaseStreamCredit names it.
	RequireStreamCreditRelease bool
	// PreferredAddresses is a server-only list (at most one address per family).
	// The socket owner must register every address before admitting connections.
	PreferredAddresses    []netip.AddrPort
	preferredSocketsReady bool
	// multipathCapacity is an internal owner capability gate. A nonzero value
	// permits advertising multipath; the owner must explicitly activate the
	// bound path interface after confirmation. EnableMultipath selects capacity four.
	multipathCapacity int
	// Negotiation carries client-only history for an attempt following VN.
	Negotiation *VersionNegotiationState
	// CompatibleVersions opts the engine into v1/v2 switching, in server preference order.
	CompatibleVersions []wire.Version
	// InitialPacketSize controls Initial padding and the original confirmed path
	// size. Public adapters normalize their default and clipping policy.
	// Zero uses 1200; accepted explicit sizes are 1200 through 1452.
	InitialPacketSize uint16
	// HandshakeIdleTimeout limits pre-completion inactivity; zero disables this
	// additional local limit. Public adapters supply their default.
	HandshakeIdleTimeout time.Duration
	// HandshakeTimeout bounds time to TLS completion. Zero selects 10 seconds.
	HandshakeTimeout                time.Duration
	KeepAlivePeriod                 time.Duration
	Server                          bool
	Version                         wire.Version
	TLS                             *tls.Config
	LocalCID, PeerCID, OriginalDCID []byte
	// RetrySourceCID is server-only and must come from the router's verified
	// Retry token context. Non-nil means Retry was used, including an empty CID.
	// This field alone does not lift anti-amplification limits.
	RetrySourceCID []byte
	Parameters     wire.TransportParameters
}

// TransportError is a local protocol failure. Receive failures automatically
// prepare CONNECTION_CLOSE; callers must continue servicing closing output.
type TransportError struct {
	Code, FrameType uint64
	Reason          string
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("quic: transport error 0x%x (frame 0x%x): %s", e.Code, e.FrameType, e.Reason)
}

var ErrUnsupportedFrame = errors.New("quic: frame processing not implemented by handshake engine")
var ErrSendBuffer = errors.New("quic: handshake send buffer limit exceeded")
var ErrPeerClosed = errors.New("quic: peer closed connection")

// PeerCloseError preserves an authenticated peer CONNECTION_CLOSE. Application
// distinguishes application codes from transport codes with the same value.
type PeerCloseError struct {
	Application     bool
	Code, FrameType uint64
	Reason          string
}

func (e *PeerCloseError) Error() string {
	return fmt.Sprintf("%v: code 0x%x, reason %q", ErrPeerClosed, e.Code, e.Reason)
}
func (e *PeerCloseError) Unwrap() error { return ErrPeerClosed }
func peerCloseError(f wire.ConnectionCloseFrame) error {
	peer := &PeerCloseError{Application: f.Application, Code: f.ErrorCode, FrameType: f.TriggerFrame, Reason: string(f.Reason)}
	if f.Application {
		return &ApplicationError{Remote: true, ErrorCode: ApplicationErrorCode(f.ErrorCode), ErrorMessage: peer.Reason, cause: peer}
	}
	return &ConnectionTransportError{Remote: true, FrameType: f.TriggerFrame, ErrorCode: TransportErrorCode(f.ErrorCode), ErrorMessage: peer.Reason, cause: peer}
}

const (
	initialSpace     = 0
	handshakeSpace   = 1
	applicationSpace = 2
	maxDeferredBytes = 64 * 1024
	maxSendBytes     = 1024 * 1024
	maxSentPackets   = 4096
)

var levels = [3]tls.QUICEncryptionLevel{tls.QUICEncryptionLevelInitial, tls.QUICEncryptionLevelHandshake, tls.QUICEncryptionLevelApplication}
var packetTypes = [3]wire.PacketType{wire.Initial, wire.Handshake, wire.OneRTT}

type sendItem struct {
	delivery      *streamItemDelivery
	frame         wire.Frame
	queued, acked bool
	transmitted   bool
	bytes         int
}
type numberSpace struct {
	authenticatedExpected uint64 // decoding reference, separate from ACK admission
	highestNonProbing     uint64
	haveNonProbing        bool
	received              *Received
	nextPN                uint64
	sentAny               bool
	largestSent           uint64
	sent                  map[uint64]*sendItem
	queue                 []*sendItem
	ackPending, discarded bool
	unackedEliciting      int       // ack-eliciting packets since the last ACK
	ackDueAt              time.Time // when a delayed ACK must be sent; zero if none
}
type deferredPacket struct {
	marking ECN
	data    []byte
	at      time.Time
}

// Conn is the serialized transport engine. Except for explicit atomic snapshot
// accessors, its methods and fields require the endpoint owner's goroutine.
type Conn struct {
	forwardingBlocked                     bool // CID advertisements await foreign reservations.
	lifetimeContext                       context.Context
	cancelLifetimeContext                 context.CancelCauseFunc
	statistics                            statisticsPublication
	receivedTotals                        receiveAccounting
	sentTotals                            sendAccounting
	loss                                  lossAccounting
	basePacketLimit                       int
	baseMTUFallbackAt                     time.Time
	baseMTUNeedsValidation                bool
	baseMTUChallenge                      *wire.PathChallengeFrame
	lastBaseApplication, lastBaseDatagram bool

	allowConnectionWindowIncrease   func(uint64) bool
	onHandshakeConfirmed            func()
	onHandshakeComplete             func()
	earlyRejectedPackets            uint64
	earlyReadExpiry                 time.Time
	earlyInitialized, earlyRejected bool
	earlySealed                     uint64
	quicBits                        [32]byte
	quicBitsLeft                    uint16
	// datagramECN belongs to the most recently committed serialized packet.
	datagramECN        recovery.ECNMark
	datagramECNCommit  ecnCommit
	addressTokenIssued bool
	remoteSnapshot     atomic.Pointer[netip.AddrPort]
	handshakeRemote    netip.AddrPort
	preferredAttempted bool
	pathSnapshots      atomic.Pointer[pathAddressSnapshot]
	// multipath is populated by an address-aware owner after mutual negotiation.
	multipath             *pathCIDs
	pendingMultipath      []pendingMultipathPacket
	pendingMultipathBytes int
	// Serialized frame scratch is never retained by packets or retransmission records.
	payloadScratch                       [defaultPathMTUCeiling]byte
	handshakeStarted                     time.Time
	retryUsed                            bool
	retryToken, retrySourceCID           []byte
	receivedTokens                       map[[32]byte]struct{}
	validation                           pathValidation
	datagrams                            datagramBuffer
	pendingDatagrams                     int
	pacer                                recovery.Pacer
	pacingAt                             time.Time
	closing                              *closingState
	drainUntil                           time.Time
	sealed                               [3]uint64
	failedAuth, integrityLimit           uint64
	idle                                 idleActivity
	idleRevision                         uint64
	pendingWrite                         *pendingForeignWrite
	packetRoutes                         *packetRouteReservations // scoped to serialized packet construction
	keepAliveAttempt                     time.Time
	authenticatedPackets                 uint64
	config                               Config
	streams                              streamSet
	completedReads                       map[uint64]*streamState
	applicationKeys                      applicationKeys
	tls                                  *handshake.TLS
	initialRead, initialWrite            *packet.Keys
	originalInitialRead                  *packet.Keys
	candidateInitial                     candidateInitialKeys
	decodeScratch                        []byte
	sendBuffers                          [][]byte
	cachedSendBufferBytes                int
	sealStorage                          []byte // borrowed only during synchronous socket output
	originalVersion                      wire.Version
	versionChosen                        bool
	spaces                               [3]numberSpace
	budget                               Amplification
	recovery                             *recovery.Sent
	timer                                recovery.Timer
	congestion                           *recovery.NewReno
	probeAllowance                       [3]bool
	handshakeACK                         bool
	peerCID                              []byte
	peerCIDKnown                         bool
	initialPeerCID                       []byte
	peerIDs                              *peerIDs
	localIDs                             *localIDs
	peerParams                           *wire.TransportParameters
	deferred                             []deferredPacket
	deferredBytes                        int
	queuedBytes                          int
	reservedResetBytes                   int
	complete, confirmed, started, closed bool
	err                                  error
}

func NewConn(config Config) (*Conn, error) {
	config.enableEarlyData = config.enableEarlyData || config.EnableEarlyData
	if len(config.InitialToken) > 512 || (config.Server && len(config.InitialToken) != 0) {
		return nil, errors.New("quic: invalid client Initial token")
	}
	config.InitialToken = clone(config.InitialToken)

	if config.InitialPacketSize == 0 {
		config.InitialPacketSize = minimumQUICMTU
	}
	if config.InitialPacketSize < minimumQUICMTU || config.InitialPacketSize > defaultPathMTUCeiling {
		return nil, errors.New("quic: unsupported initial packet size")
	}
	if config.KeepAlivePeriod < 0 {
		return nil, errors.New("quic: negative keepalive period")
	}
	if config.HandshakeTimeout < 0 || config.HandshakeIdleTimeout < 0 {
		return nil, errors.New("quic: negative handshake timeout")
	}
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = 10 * time.Second
	}
	if !config.Version.Supported() {
		return nil, wire.ErrVersion
	}
	if len(config.LocalCID) > 20 || len(config.PeerCID) > 20 || len(config.OriginalDCID) < 8 || len(config.OriginalDCID) > 20 {
		return nil, errors.New("quic: invalid handshake connection IDs")
	}
	if !config.Server && !bytes.Equal(config.PeerCID, config.OriginalDCID) {
		return nil, errors.New("quic: client peer CID must initially equal original destination CID")
	}
	if config.RetrySourceCID != nil && (!config.Server || len(config.RetrySourceCID) > 20 || bytes.Equal(config.RetrySourceCID, config.OriginalDCID)) {
		return nil, errors.New("quic: invalid server Retry context")
	}
	if config.Negotiation != nil {
		n := config.Negotiation
		validPreference := len(n.Preferences) == 2 && ((n.Preferences[0] == n.Original && n.Preferences[1] == config.Version) || (n.Preferences[1] == n.Original && n.Preferences[0] == config.Version))
		if config.Server || n.Original == config.Version || !validPreference || !n.Original.Supported() {
			return nil, errors.New("quic: invalid version negotiation history")
		}
		config.Negotiation = &VersionNegotiationState{Original: n.Original, Preferences: append([]wire.Version(nil), n.Preferences...)}
	}
	p := config.Parameters
	if config.EnableMigration && config.Server {
		config.allowCandidateMigration, config.allowFreshCIDMigration, config.allowIPMigration = true, true, true
	}
	if config.EnableMultipath {
		config.multipathCapacity = 4
		// RFC 9000 section 9 permits reactive client-address changes only.
		config.allowCandidateMigration = config.Server
		config.allowFreshCIDMigration = config.Server
		config.allowIPMigration = config.Server
	}
	if config.multipathCapacity < 0 || config.multipathCapacity > 64 || (config.multipathCapacity > 0 && p.InitialMaxPathID == nil) {
		return nil, errors.New("quic: invalid multipath owner configuration")
	}
	if p.InitialMaxPathID != nil && (config.multipathCapacity == 0 || len(config.LocalCID) == 0) {
		return nil, errors.New("quic: multipath advertisement requires a capable owner and nonzero CID")
	}
	if p.PreferredAddress != nil || p.RetrySourceConnectionID != nil || p.StatelessResetToken != nil {
		return nil, errors.New("quic: unsupported handshake-engine parameter feature")
	}
	if config.ResetKey != nil {
		if len(config.LocalCID) == 0 {
			return nil, errors.New("quic: reset key requires nonzero connection IDs")
		}
		key := *config.ResetKey
		config.ResetKey = &key
		if config.Server {
			token := key.token(config.LocalCID)
			p.StatelessResetToken = &token
		}
	}
	if len(config.PreferredAddresses) > 0 {
		if !config.Server || !config.EnableMigration || !config.preferredSocketsReady || len(config.LocalCID) == 0 {
			return nil, errors.New("quic: preferred advertisement requires registered server sockets")
		}
		preferred, err := makePreferredAddressWithGenerator(config.PreferredAddresses, config.LocalCID, config.GenerateConnectionID)
		if err != nil {
			return nil, err
		}
		if config.ResetKey != nil {
			preferred.StatelessResetToken = config.ResetKey.token(preferred.ConnectionID)
		}
		p.PreferredAddress = preferred
	}
	if (config.MaxStreamReceiveWindow != 0 && config.MaxStreamReceiveWindow < max(p.InitialMaxStreamDataBidiLocal, p.InitialMaxStreamDataBidiRemote, p.InitialMaxStreamDataUni)) || config.MaxStreamReceiveWindow > 16<<20 || (config.MaxConnectionReceiveWindow != 0 && config.MaxConnectionReceiveWindow < p.InitialMaxData) || config.MaxConnectionReceiveWindow > wire.MaxVarint {
		return nil, errors.New("quic: invalid maximum receive window")
	}
	if max(p.InitialMaxStreamDataBidiLocal, p.InitialMaxStreamDataBidiRemote, p.InitialMaxStreamDataUni) > 16<<20 {
		return nil, errors.New("quic: stream receive policy exceeds engine capacity")
	}
	config.LocalCID = clone(config.LocalCID)
	config.PeerCID = clone(config.PeerCID)
	config.OriginalDCID = clone(config.OriginalDCID)
	if config.RetrySourceCID != nil {
		config.RetrySourceCID = clone(config.RetrySourceCID)
	}
	p.InitialSourceConnectionID = clone(config.LocalCID)
	if config.Server {
		p.OriginalDestinationConnectionID = clone(config.OriginalDCID)
		if config.RetrySourceCID != nil {
			p.RetrySourceConnectionID = clone(config.RetrySourceCID)
		}
	}
	if p.VersionInformation == nil {
		p.VersionInformation = &wire.VersionInformation{Chosen: config.Version, Available: []wire.Version{config.Version}}
	}
	if p.VersionInformation.Chosen != config.Version {
		return nil, errors.New("quic: configured chosen version differs from packet version")
	}
	if len(config.CompatibleVersions) != 0 {
		config.CompatibleVersions = append([]wire.Version(nil), config.CompatibleVersions...)
		for _, v := range config.CompatibleVersions {
			if !v.Supported() {
				return nil, wire.ErrVersion
			}
		}
	}
	if !config.Server {
		for _, v := range p.VersionInformation.Available {
			if v != config.Version && uint32(v)&0x0f0f0f0f != 0x0a0a0a0a && !(len(config.CompatibleVersions) != 0 && v.Supported()) {
				return nil, errors.New("quic: compatible version switching is not implemented")
			}
		}
	}
	p.DisableActiveMigration = p.DisableActiveMigration || len(config.LocalCID) == 0 || !config.Server || !(config.allowActiveMigration || config.multipathCapacity > 0)
	encoded, err := p.Marshal(config.Server)
	if err != nil {
		return nil, err
	}
	// Own all parameter slices as well as the routing IDs.
	config.Parameters, err = wire.ParseTransportParameters(encoded, config.Server)
	if err != nil {
		return nil, err
	}
	var c *Conn
	var h *handshake.TLS

	if config.enableEarlyData && (config.EnableMultipath || config.multipathCapacity != 0) {
		return nil, errors.New("quic: early data with multipath is not integrated")
	}
	if config.Server && len(config.CompatibleVersions) != 0 {
		constructor := handshake.NewNegotiatingServer
		if config.enableEarlyData {
			constructor = handshake.NewNegotiatingServerWithEarlyData
		}
		h, err = constructor(config.Version, config.TLS, encoded, func(peer wire.TransportParameters) (wire.Version, []byte, error) {
			return c.selectCompatibleVersion(peer)
		})
	} else if config.enableEarlyData {
		h, err = handshake.NewWithEarlyData(config.Server, config.Version, config.TLS, encoded)
	} else {
		h, err = handshake.New(config.Server, config.Version, config.TLS, encoded)
	}

	if err != nil {
		return nil, err
	}
	initialDCID := config.OriginalDCID
	if config.RetrySourceCID != nil {
		initialDCID = config.RetrySourceCID
	}
	clientKeys, serverKeys, err := packet.InitialKeys(config.Version, initialDCID)
	if err != nil {
		h.Close()
		return nil, err
	}
	c = &Conn{originalVersion: config.Version, config: config, tls: h, peerCID: clone(config.PeerCID), peerCIDKnown: config.Server, initialRead: serverKeys, initialWrite: clientKeys}
	c.retryToken = config.InitialToken
	c.initStreams()
	c.localIDs = newLocalIDs(config.LocalCID)
	c.localIDs.resetKey = config.ResetKey
	c.localIDs.generate = config.GenerateConnectionID
	if p.StatelessResetToken != nil {
		id := c.localIDs.active[0]
		id.token, id.hasToken = *p.StatelessResetToken, true
		c.localIDs.active[0] = id
	}
	if err := c.localIDs.reservePreferred(config.Parameters.PreferredAddress); err != nil {
		h.Close()
		return nil, err
	}
	c.recovery, _ = recovery.NewSent(maxSentPackets)
	c.congestion, _ = recovery.NewCongestion(uint64(config.InitialPacketSize))
	if config.Server {
		c.initialPeerCID = clone(config.PeerCID)
		c.peerIDs = newPeerIDs(config.PeerCID, config.Parameters.ActiveConnectionIDLimit)
		c.initialRead, c.initialWrite = clientKeys, serverKeys
	} else {
		c.budget.Validate()
	}
	for i := range c.spaces {
		r, _ := NewReceived(32)
		c.spaces[i] = numberSpace{received: r, sent: make(map[uint64]*sendItem)}
	}
	return c, nil
}
func clone(b []byte) []byte { out := make([]byte, len(b)); copy(out, b); return out }
func (c *Conn) fail(err error) error {
	if c.err == nil {
		c.err = connectionFailure(err)
		if c.cancelLifetimeContext != nil {
			c.cancelLifetimeContext(c.err)
		}
		c.onHandshakeComplete = nil
		c.onHandshakeConfirmed = nil
		clear(c.payloadScratch[:])
		c.tls.Close()
	}
	return c.err
}
func (c *Conn) check() error {
	if c.err != nil {
		return c.err
	}
	if c.closed {
		return errors.New("quic: connection closed")
	}
	if !c.started {
		return errors.New("quic: connection not started")
	}
	return nil
}

func (c *Conn) Start(ctx context.Context) error {
	if c.started || c.closed {
		return errors.New("quic: connection already started or closed")
	}
	c.started = true
	if c.lifetimeContext == nil {
		// Preserve dial values without making a completed connection depend on
		// the handshake caller's deadline or later cancellation.
		c.lifetimeContext, c.cancelLifetimeContext = context.WithCancelCause(context.WithoutCancel(ctx))
	}
	if err := c.tls.Start(ctx); err != nil {
		return c.fail(err)
	}
	return c.syncTLS()
}
func (c *Conn) Close() error {
	if c.closed {
		return nil
	}
	c.publishStatistics()
	c.closed = true
	if c.pendingWrite != nil {
		c.pendingWrite.release()
	}
	if c.cancelLifetimeContext != nil {
		c.cancelLifetimeContext(ErrEndpointClosed)
	}
	c.onHandshakeComplete = nil
	c.onHandshakeConfirmed = nil
	c.closing = nil
	// QUICConn.Close can report the local TLS close-notify generated by
	// cancelling an unfinished handshake. Shutdown itself has no I/O here;
	// protocol failures are returned by the operation that encountered them.
	_ = c.tls.Close()
	c.releaseTransportBuffers()
	c.multipath = nil
	c.peerIDs = nil
	c.localIDs = nil
	c.drainUntil = time.Time{}
	return nil
}
func (c *Conn) HandshakeComplete() bool              { return c.complete }
func (c *Conn) HandshakeConfirmed() bool             { return c.confirmed }
func (c *Conn) ConnectionState() tls.ConnectionState { return c.tls.ConnectionState() }

func spaceFor(t wire.PacketType) (int, bool) {
	switch t {
	case wire.Initial:
		return initialSpace, true
	case wire.Handshake:
		return handshakeSpace, true
	case wire.OneRTT, wire.ZeroRTT:
		return applicationSpace, true
	}
	return 0, false
}
func (c *Conn) readKeys(space int) *packet.Keys {
	if c.spaces[space].discarded {
		return nil
	}
	if space == initialSpace {
		return c.initialRead
	}
	if space == applicationSpace && c.applicationKeys.read != nil {
		return c.applicationKeys.read
	}
	return c.tls.ReadKeys(levels[space])
}
func (c *Conn) writeKeys(space int) *packet.Keys {
	if c.spaces[space].discarded {
		return nil
	}
	if space == initialSpace {
		return c.initialWrite
	}
	if space == applicationSpace && c.applicationKeys.write != nil {
		return c.applicationKeys.write
	}
	if space == applicationSpace && c.sendingEarly() {
		return c.tls.WriteKeys(tls.QUICEncryptionLevelEarly)
	}
	return c.tls.WriteKeys(levels[space])
}

func (c *Conn) enqueue(space int, f wire.Frame, size int) error {
	destination := &c.spaces[space]
	var selectedPath uint64
	if space == applicationSpace && c.multipath != nil && c.confirmed {
		id, ok := c.survivingPath(^uint64(0))
		switch f.(type) {
		case wire.StreamFrame, wire.DatagramFrame:
			if selected, ready := c.applicationEnqueuePath(); ready {
				id, ok = selected, true
			}
		}
		if !ok {
			return c.fail(&TransportError{Code: 0x10, Reason: "no viable path for application output"})
		}
		destination = c.multipath.paths[id].application.space
		selectedPath = id
	}
	if destination.discarded {
		return nil
	}
	if size > c.availableSendBytes() {
		return c.fail(ErrSendBuffer)
	}
	item := &sendItem{frame: f, queued: true, bytes: size}
	if frame, ok := f.(wire.StreamFrame); ok && len(frame.Data) > portableStreamPayload {
		reserve := streamSplitReservation(len(frame.Data))
		if size < len(frame.Data)+32+reserve {
			return c.fail(ErrSendBuffer)
		}
		if err := reserveStreamItem(item, frame); err != nil {
			return c.fail(err)
		}
	}
	c.queuedBytes += size
	if _, ok := f.(wire.DatagramFrame); ok {
		c.pendingDatagrams++
	}
	destination.queue = append(destination.queue, item)
	if space == applicationSpace && c.multipath != nil && c.confirmed {
		switch f.(type) {
		case wire.StreamFrame, wire.DatagramFrame:
			c.multipath.lastEnqueued, c.multipath.haveEnqueued = selectedPath, true
		}
	}
	return nil
}
func (c *Conn) syncTLS() error {
	if err := c.syncClientEarly(); err != nil {
		return c.fail(err)
	}
	if !c.config.Server && c.tls.WriteKeys(tls.QUICEncryptionLevelApplication) != nil {
		c.discardEarlyKeys()
	}
	if c.peerParams == nil {
		if encoded, ok := c.tls.PeerParameters(); ok {
			p, err := wire.ParseTransportParameters(encoded, !c.config.Server)
			if err != nil {
				return c.fail(err)
			}
			if err := c.validateVersionInformation(p.VersionInformation); err != nil {
				return c.fail(err)
			}
			if !c.peerCIDKnown {
				return c.fail(errors.New("quic: transport parameters before peer Initial CID"))
			}
			ids := wire.ConnectionIDContext{PeerIsServer: !c.config.Server, InitialSourceConnectionID: c.initialPeerCID, OriginalDestinationConnectionID: c.config.OriginalDCID, RetryUsed: c.retryUsed, RetrySourceConnectionID: c.retrySourceCID}
			if err := p.ValidateConnectionIDs(ids); err != nil {
				return c.fail(err)
			}
			if err := c.validatePeerMultipathParameters(p); err != nil {
				return c.fail(err)
			}
			// RFC 9000 sections 5.1.1 and 18.2 assign sequence 1 to the
			// preferred-address CID, including when address migration is deferred.
			// Register it before exposing parameters so ordinary CID limits,
			// retirement, reset tokens and path-zero adoption include it.
			if preferred := p.PreferredAddress; preferred != nil {
				_, err := c.peerIDs.addWithType(wire.NewConnectionIDFrame{
					Sequence: 1, ConnectionID: preferred.ConnectionID,
					StatelessResetToken: preferred.StatelessResetToken,
				}, 0)
				if err != nil {
					return c.fail(err)
				}
			}
			c.peerParams = &p
			if c.config.Server {
				if err := c.applyPeerStreamLimits(p.EarlyLimits()); err != nil {
					return c.fail(err)
				}
			}
			if p.StatelessResetToken != nil {
				id := c.peerIDs.active[0]
				id.token = *p.StatelessResetToken
				id.hasToken = true
				c.peerIDs.active[0] = id
			}
		}
	}
	for _, flight := range c.tls.Drain() {
		space := -1
		for i, l := range levels {
			if flight.Level == l {
				space = i
			}
		}
		if space < 0 {
			return c.fail(errors.New("quic: unsupported TLS encryption level"))
		}
		for offset := 0; offset < len(flight.Data); {
			end := min(offset+900, len(flight.Data))
			frame := wire.CryptoFrame{Offset: flight.Offset + uint64(offset), Data: clone(flight.Data[offset:end])}
			if err := c.enqueue(space, frame, len(frame.Data)); err != nil {
				return err
			}
			offset = end
		}
	}
	if c.peerParams != nil {
		if err := c.issueLocalIDs(); err != nil {
			return err
		}
	}
	if !c.complete && c.tls.Complete() {
		if c.peerParams == nil {
			return c.fail(errors.New("quic: TLS completed without peer parameters"))
		}
		if !c.config.Server {
			if err := c.applyPeerStreamLimits(c.peerParams.EarlyLimits()); err != nil {
				return c.fail(err)
			}
		}
		c.complete = true
		if c.config.Server {
			c.confirmed = true
			c.discard(handshakeSpace)
			if err := c.enqueue(applicationSpace, wire.HandshakeDoneFrame{}, 1); err != nil {
				return err
			}
		}
		c.notifyHandshakeProgress()
	}
	return nil
}

func (c *Conn) discard(space int) {
	s := &c.spaces[space]
	if s.discarded {
		return
	}
	s.discarded = true
	c.recovery.Discard(recovery.Space(space))
	c.timer.DiscardKeys()
	c.probeAllowance[space] = false
	s.clearACK()
	// Several transmissions can refer to one item, including a queued probe.
	seen := make(map[*sendItem]bool)
	for _, item := range s.queue {
		seen[item] = true
	}
	for _, item := range s.sent {
		seen[item] = true
	}
	for item := range seen {
		if !item.acked {
			c.queuedBytes -= retireSendItem(item)
		}
	}
	s.queue = nil
	s.sent = nil
	if space == initialSpace {
		c.originalInitialRead = nil
		c.candidateInitial = candidateInitialKeys{}
		c.initialRead = nil
		c.initialWrite = nil
	}
	c.tls.DiscardKeys(levels[space])
}

// HandleDatagram accepts an entire UDP payload on the configured peer path.
// The adapter must also deliver otherwise-unroutable datagrams from that peer
// for stateless-reset detection; it must enforce the peer-address association.
// Unauthenticated garbage is silently discarded; authenticated violations fail
// the engine. Typed transport failures and TLS alerts prepare close output;
// the adapter must continue NextDatagram/HandleTimeout while Closing is true.
// Coalesced packets are processed independently.
func (c *Conn) HandleDatagram(data []byte, now time.Time) error {
	return c.HandleDatagramECN(data, now, ECNUnknown)
}

// HandleDatagramECN applies one enclosing IP marking to every processed QUIC
// packet in a datagram. Missing socket metadata must use ECNUnknown.
func (c *Conn) HandleDatagramECN(data []byte, now time.Time, marking ECN) (result error) {
	if marking > ECNCE && marking != ECNUnknown {
		return errors.New("quic: invalid ECN codepoint")
	}
	c.recordReceivedDatagram(data)
	defer c.publishStatistics()

	if c.Closing() {
		return c.receiveClosing(data, now)
	}
	if err := c.check(); err != nil {
		return err
	}
	if err := c.checkHandshakeTimeout(now); err != nil {
		return err
	}

	before := c.authenticatedPackets
	defer func() {
		if result == nil && c.authenticatedPackets == before && c.isStatelessReset(data) {
			result = c.beginDraining(ErrStatelessReset, now)
		}
		result = c.closeReceiveFailure(result, now)
	}()
	if len(data) == 0 || uint64(len(data)) > c.config.Parameters.MaxUDPPayloadSize {
		return nil
	}
	first, err := c.parseHeader(data)
	if err != nil || !c.acceptCID(first) {
		return nil
	}
	if first.Long && first.Version == 0 {
		return c.handleVersionNegotiation(first)
	}
	if first.Type == wire.Retry {
		if !c.acceptPacketVersion(first) {
			return nil
		}
		return c.handleRetry(data, first, now)
	}
	// Known long-header versions have parseable packet boundaries even when
	// their version is no longer accepted. Apply version policy per packet so
	// a late original-version Initial cannot hide a coalesced negotiated packet.
	if c.config.Server && first.Type == wire.Initial && len(data) < 1200 {
		return nil
	}
	// Count this routed datagram once, not once per packet or deferred replay.
	if err := c.budget.Received(len(data)); err != nil {
		return c.fail(err)
	}
	defer c.armTimer(now)
	for rest := data; len(rest) > 0; {
		h, err := c.parseHeader(rest)
		if err != nil {
			break
		}
		raw := rest[:h.PacketLen]
		rest = rest[h.PacketLen:]
		if !bytes.Equal(h.DCID, first.DCID) {
			continue
		}
		if c.config.Server && h.Type == wire.Initial && len(data) < 1200 {
			continue
		}
		if _, err := c.receiveECN(raw, h, now, true, marking); err != nil {
			return err
		}
	}
	return c.replayDeferred()
}
func (c *Conn) acceptCID(h wire.Header) bool {
	if c.localIDs.accept(h.DCID) {
		return true
	}
	initialDCID := c.config.OriginalDCID
	if c.config.RetrySourceCID != nil {
		initialDCID = c.config.RetrySourceCID
	}
	return c.config.Server && bytes.Equal(h.DCID, initialDCID) && ((h.Type == wire.Initial && !c.spaces[initialSpace].discarded) || (h.Type == wire.ZeroRTT && c.config.enableEarlyData))
}

// receive returns ready=false only while keys are unavailable. When keys exist,
// even a packet failing authentication is ready to remove from the deferred list.
func (c *Conn) receive(raw []byte, h wire.Header, now time.Time, buffer bool) (bool, error) {
	return c.receiveECN(raw, h, now, buffer, ECNUnknown)
}

func (c *Conn) receiveECN(raw []byte, h wire.Header, now time.Time, buffer bool, marking ECN) (bool, error) {
	if !c.acceptCID(h) || !c.acceptPacketVersion(h) {
		return true, nil
	}
	if h.Type == wire.ZeroRTT && (!c.config.Server || !c.config.enableEarlyData) {
		return true, nil
	}
	c.expireEarlyKeys(now)
	space, ok := spaceFor(h.Type)
	if !ok {
		return true, nil
	}
	s := &c.spaces[space]
	if s.discarded {
		return true, nil
	}
	keys := c.packetReadKeys(h.Type, space)
	if h.Type == wire.ZeroRTT {
		// Once ClientHello has been processed, missing keys mean rejection or retirement.
		if keys == nil && c.peerParams != nil {
			return true, nil
		}
	}
	if h.Type == wire.Initial && h.Version != c.config.Version {
		if c.config.Server {
			keys = c.originalInitialRead
		} else {
			candidate, err := c.candidateInitialRead(h.Version)
			if err != nil {
				return true, nil
			}
			keys = candidate
		}
	}
	if keys == nil {
		if buffer && len(c.deferred) < 64 && len(raw) <= maxDeferredBytes-c.deferredBytes {
			c.deferred = append(c.deferred, deferredPacket{data: clone(raw), at: now, marking: marking})
			c.deferredBytes += len(raw)
		}
		return false, nil
	}
	if err := c.checkIntegrityLimit(keys); err != nil {
		return true, err
	}
	// Multipath handoff retains decoded frame slices. Other receive paths
	// consume frames synchronously and copy any application/TLS data they keep.
	var decode []byte
	if space != applicationSpace || !c.multipathNegotiated() {
		if cap(c.decodeScratch) < len(raw) {
			c.decodeScratch = make([]byte, len(raw))
		}
		decode = c.decodeScratch[:len(raw)]
	}
	var header, plain []byte
	var pn uint64
	var err error
	if h.Type == wire.OneRTT {
		header, plain, pn, err = c.openApplicationPathInto(decode, 0, raw, h.PNOffset, max(s.authenticatedExpected, s.received.Expected()), now)
	} else {
		header, plain, pn, err = keys.OpenInto(decode, raw, h.PNOffset, max(s.authenticatedExpected, s.received.Expected()))
	}
	if err != nil {
		return true, c.packetOpenFailure(err)
	}
	if h.Type == wire.Initial && c.peerCIDKnown && !bytes.Equal(c.initialPeerCID, h.SCID) {
		return true, nil
	}
	if !c.config.Server && h.Long && h.Version != c.config.Version {
		if err := c.tls.SetNegotiatedVersion(h.Version); err != nil {
			return true, c.fail(err)
		}
		if err := c.switchVersion(h.Version); err != nil {
			return true, c.fail(err)
		}
	}
	if h.Type == wire.OneRTT {
		c.startEarlyKeyRetirement(now)
	}
	s.authenticatedExpected = max(s.authenticatedExpected, pn+1)
	c.authenticatedPackets++
	c.receivedIdleActivity(now)
	_ = header
	if len(plain) == 0 {
		return true, c.fail(&TransportError{Code: 0xa, Reason: "packet payload has no frames"})
	}
	if h.Type == wire.Initial {
		if c.peerCIDKnown && !bytes.Equal(c.initialPeerCID, h.SCID) {
			return true, nil
		}
		if !c.peerCIDKnown {
			c.peerCID = clone(h.SCID)
			c.peerCIDKnown = true
			c.initialPeerCID = clone(h.SCID)
			c.peerIDs = newPeerIDs(h.SCID, c.config.Parameters.ActiveConnectionIDLimit)
		}
	}
	frames, eliciting, err := parsePacketFrames(plain, wire.FrameContext{PacketType: h.Type, SenderIsServer: !c.config.Server, ResetStreamAtEnabled: c.config.Parameters.ResetStreamAt, MaxDatagramFrameSize: c.config.Parameters.MaxDatagramFrameSize, MultipathNegotiated: c.multipathNegotiated()})
	if err != nil {
		return true, c.fail(err)
	}
	// Initial protection is publicly derivable and cannot establish CID-use
	// evidence. Handshake/1-RTT authentication can, before the owner binds it.
	if h.Type == wire.Handshake || h.Type == wire.OneRTT {
		c.localIDs.noteAuthenticated(h.DCID)
	}
	if h.Type == wire.OneRTT {
		s.noteNonProbing(pn, frames)
	}
	if space == applicationSpace && c.multipath == nil && c.multipathNegotiated() {
		return true, c.retainMultipathBeforeActivationECN(h.DCID, pn, frames, eliciting, len(raw), now, marking)
	}
	previous, hadPrevious := s.received.Largest()
	receipt, err := s.received.RecordECN(pn, now, marking)
	if err != nil {
		return true, c.fail(err)
	}
	if receipt == ForgottenPacket {
		return true, nil
	}
	if eliciting {
		s.scheduleACK(space, pn, previous, hadPrevious, marking, now, c.ackDelay())
	}
	if receipt == DuplicatePacket {
		return true, nil
	}
	for _, f := range frames {
		switch f := f.(type) {
		case wire.StopSendingFrame, wire.StreamFrame, wire.ResetStreamFrame, wire.ResetStreamAtFrame, wire.StreamDataBlockedFrame, wire.MaxStreamDataFrame, wire.MaxDataFrame, wire.MaxStreamsFrame, wire.DataBlockedFrame, wire.StreamsBlockedFrame:
			if err := c.handleStreamFrame(f); err != nil {
				return true, err
			}
		case wire.PathChallengeFrame:
			// The adapter associates this engine with one fixed peer path. Migration
			// will need to retain that path identity with each queued response.
			if err := c.enqueue(applicationSpace, wire.PathResponseFrame{Data: f.Data}, 32); err != nil {
				return true, err
			}
			q := c.spaces[applicationSpace].queue
			response := q[len(q)-1]
			copy(q[1:], q[:len(q)-1])
			q[0] = response
		case wire.PathResponseFrame:
			if err := c.handlePathResponse(f, now); err != nil {
				return true, err
			}
		case wire.DatagramFrame:
			c.receiveDatagram(f.Data)
		case wire.PingFrame:
		case wire.CryptoFrame:
			if s.discarded {
				// An earlier frame in this packet completed the handshake and
				// retired this level's reassembly; the rest can only be duplicates.
				continue
			}
			if !c.config.Server {
				c.versionChosen = true
				c.candidateInitial = candidateInitialKeys{}
			}
			if err := c.tls.HandleCrypto(levels[space], f.Offset, f.Data); err != nil {
				return true, c.fail(err)
			}
			if err := c.syncTLS(); err != nil {
				return true, err
			}
		case wire.AckFrame:
			if err := c.handleAck(space, f, now); err != nil {
				return true, err
			}
		case wire.HandshakeDoneFrame:
			if !c.complete {
				return true, c.fail(&TransportError{Code: 0xa, FrameType: f.Type(), Reason: "HANDSHAKE_DONE before TLS completion"})
			}
			c.confirmed = true
			c.discard(handshakeSpace)
			c.notifyHandshakeProgress()
		case wire.NewTokenFrame:
			c.receiveAddressToken(f.Token, now)
		case wire.NewConnectionIDFrame:
			if c.peerIDs == nil {
				return true, c.fail(errors.New("quic: connection IDs before peer Initial"))
			}
			retired, err := c.peerIDs.add(f)
			if err != nil {
				return true, c.fail(err)
			}
			c.peerCID = clone(c.peerIDs.active[c.peerIDs.current].cid)
			for _, seq := range retired {
				if err := c.enqueue(applicationSpace, wire.RetireConnectionIDFrame{Sequence: seq}, 16); err != nil {
					return true, err
				}
			}
		case wire.RetireConnectionIDFrame:
			changed, err := c.localIDs.retire(f.Sequence, h.DCID)
			if err != nil {
				return true, c.fail(err)
			}
			if changed {
				if err := c.issueLocalIDs(); err != nil {
					return true, err
				}
			}
		case wire.ConnectionCloseFrame:
			return true, c.beginDraining(peerCloseError(f), now)
		default:
			return true, c.fail(fmt.Errorf("%w: 0x%x", ErrUnsupportedFrame, f.Type()))
		}
	}
	if c.config.Server && space == handshakeSpace {
		c.budget.Validate()
		c.discard(initialSpace)
	}
	return true, nil
}

func (c *Conn) replayDeferred() error {
	for {
		pending := c.deferred
		c.deferred = nil
		c.deferredBytes = 0
		progress := false
		for _, p := range pending {
			h, err := c.parseHeader(p.data)
			if err != nil {
				continue
			}
			ready, err := c.receiveECN(p.data, h, p.at, false, p.marking)
			if err != nil {
				return err
			}
			if ready {
				progress = true
			} else {
				c.deferred = append(c.deferred, p)
				c.deferredBytes += len(p.data)
			}
		}
		if !progress {
			return nil
		}
	}
}
func (c *Conn) handleAck(space int, f wire.AckFrame, now time.Time) error {
	if space == applicationSpace && c.earlyRejectedPackets != 0 {
		for _, r := range f.Ranges {
			if r.Smallest < c.earlyRejectedPackets {
				return c.fail(&TransportError{Code: 0xa, FrameType: f.Type(), Reason: "acknowledgment of rejected early data"})
			}
		}
	}
	s := &c.spaces[space]
	limited := !c.hasQueuedData() && c.recovery.BytesInFlight < c.congestion.Window()
	exponent := uint8(3)
	maxDelay := 25 * time.Millisecond
	if c.peerParams != nil {
		exponent = uint8(c.peerParams.AckDelayExponent)
		maxDelay = time.Duration(c.peerParams.MaxAckDelay) * time.Millisecond
	}
	events, err := c.recovery.OnACK(recovery.Space(space), f, now, recovery.DecodeACKDelay(f.Delay, exponent), maxDelay, c.confirmed)
	if err != nil {
		return c.fail(&TransportError{Code: 0xa, FrameType: f.Type(), Reason: err.Error()})
	}
	c.loss.recovered(events)
	if space == applicationSpace {
		c.noteKeyPhaseWriteACK(f, now)
	}
	if space == handshakeSpace {
		c.handshakeACK = true
	}
	c.timer.Progress(events, c.config.Server || c.handshakeACK || c.confirmed)
	if events.ECNCongestion {
		c.congestion.OnECN(events.ECNSentAt, now)
	}
	c.onCongestionLoss(events.Lost, now)
	c.congestion.OnACK(events.Acked, limited)
	if len(events.Acked) > 0 {
		c.probeAllowance[space] = false
	}
	if err := acknowledgeItems(s, f, func(item *sendItem) error {
		if space == applicationSpace {
			if err := c.ackStreamFrame(item.frame); err != nil {
				return err
			}
		}
		c.queuedBytes -= item.bytes
		c.recycleStreamItem(item)
		return nil
	}); err != nil {
		return c.fail(err)
	}
	pruneAcknowledged(s)
	c.queueLost(space, events.Lost)
	return nil
}
func (c *Conn) queueLost(space int, lost []recovery.Packet) {
	s := &c.spaces[space]
	queueLostItems(s, s, lost)
}

// LossDeadline returns the earliest ACK-based time-threshold loss deadline.
// This is not a PTO deadline; unacknowledged tail packets still need probes.
func (c *Conn) LossDeadline() time.Time { at, _, _ := c.recovery.LossTime(); return at }
func (c *Conn) DetectLoss(now time.Time) error {
	if err := c.check(); err != nil {
		return err
	}
	for space := range c.spaces {
		if !c.spaces[space].discarded {
			lost := c.recovery.DetectLoss(recovery.Space(space), now)
			c.onCongestionLoss(lost, now)
			c.queueLost(space, lost)
		}
	}
	c.armTimer(now)
	return nil
}
func (c *Conn) RTTStats() recovery.RTT { return c.recovery.RTT }

// NextDatagram returns at most one complete packet/datagram. Initial packets
// and ordinary packets use the configured original-path limit.
// Returning bytes commits a send: packet numbers and anti-amplification budget
// are consumed. The caller MUST immediately transmit it or close the engine on
// a write failure. Returned packets must never be edited or resealed.
// Nil means no output, no write keys, or insufficient path credit.
func (c *Conn) NextDatagram(now time.Time) ([]byte, error) {
	if c.pendingWrite != nil && !c.Closing() && !c.Draining() && !c.closed {
		return nil, nil
	}
	c.datagramECN = recovery.NotECT
	c.pacingAt = time.Time{}
	if c.Closing() {
		return c.nextClosingDatagram(now)
	}
	if err := c.check(); err != nil {
		return nil, err
	}
	if err := c.checkHandshakeTimeout(now); err != nil {
		return nil, err
	}

	if err := c.expirePathValidation(now); err != nil {
		return nil, err
	}
	if c.baseMTUNeedsValidation {
		switch c.validation.state {
		case PathValidated:
			c.baseMTUNeedsValidation = false
		case PathValidationFailed:
			return nil, c.fail(&TransportError{Code: 0x10, Reason: "minimum path size validation failed"})
		}
	}
	for space := range c.spaces {
		s := &c.spaces[space]
		keys := c.writeKeys(space)
		if keys == nil {
			continue
		}
		haveQueued := c.selectPublishedQueue(s)
		for haveQueued {
			item := s.queue[0]
			if !item.acked {
				frame, datagram := item.frame.(wire.DatagramFrame)
				if space != applicationSpace || c.sendingEarly() || !datagram || datagramFitsPath(frame, c.basePacketPayloadLimit(), len(c.peerCID)) {
					break
				}
				c.queuedBytes -= item.bytes
				c.pendingDatagrams--
				item.acked, item.queued, item.frame = true, false, nil
			}
			s.queue[0] = nil
			s.queue = s.queue[1:]
			haveQueued = c.selectPublishedQueue(s)
		}
		if c.forwardingBlocked && c.probeAllowance[space] && !haveQueued {
			if err := c.enqueue(space, wire.PingFrame{}, 32); err != nil {
				return nil, err
			}
			haveQueued = c.selectPublishedQueue(s)
		}
		if space == applicationSpace {
			c.prioritizeStreams(s)
		}
		var f wire.Frame
		var item *sendItem
		early := space == applicationSpace && c.sendingEarly()
		s.expireACKDelay(now) // a send at or after the deadline carries the ACK
		if s.ackPending && !early && !(c.probeAllowance[space] && haveQueued) {
			ack, err := s.received.ackView(now, uint8(c.config.Parameters.AckDelayExponent))
			if err != nil {
				return nil, c.fail(err)
			}
			if space != applicationSpace {
				ack.Delay = 0
			}
			f = ack
		} else if space == applicationSpace && c.baseMTUChallenge != nil {
			f = *c.baseMTUChallenge
		} else if haveQueued {
			item = s.queue[0]
			f = item.frame
		} else {
			continue
		}
		if c.baseMTUNeedsValidation {
			switch f.(type) {
			case wire.StreamFrame, wire.DatagramFrame:
				continue
			}
		}
		if frame, ok := f.(wire.StreamFrame); ok && !early && len(frame.Data) > portableStreamPayload+c.basePacketPayloadLimit()-minimumQUICMTU {
			if err := c.splitQueuedStream(s, 0, portableStreamPayload); err != nil {
				return nil, c.fail(err)
			}
			item, f = s.queue[0], s.queue[0].frame
		}
		limit, _ := keys.UsageLimits()
		used := c.sealed[space]
		if early {
			used = c.earlySealed
		}
		if used >= limit {
			if early {
				return nil, c.fail(aeadLimitError())
			}
			if space != applicationSpace {
				return nil, c.fail(aeadLimitError())
			}
			if err := c.InitiateKeyUpdateAt(now); err != nil {
				return nil, c.fail(aeadLimitError())
			}
			keys = c.writeKeys(space)
		}
		if len(s.sent) >= maxSentPackets && item != nil {
			return nil, c.fail(ErrSendBuffer)
		}
		if s.nextPN >= wire.MaxVarint {
			return nil, c.exhaustPacketNumbers()
		}
		payload, bundledLargest, bundled, err := c.bundleACK(s, space, early, item, f, now)
		if err != nil {
			return nil, c.fail(err)
		}
		if early && wire.ValidateFrame(f, wire.FrameContext{PacketType: wire.ZeroRTT, ResetStreamAtEnabled: c.peerAllowsPartialReset(), MaxDatagramFrameSize: c.streams.peerLimits.MaxDatagramFrameSize}, len(payload)) != nil {
			continue
		}
		header, err := c.makeHeader(space, s.nextPN, len(payload)+16)
		if err != nil {
			return nil, c.fail(err)
		}
		packetLimit := c.basePacketPayloadLimit()
		if len(header)+len(payload)+16 > packetLimit {
			return nil, c.fail(errors.New("quic: frame exceeds initial packetization limit"))
		}
		if space == initialSpace {
			// Recompute the length field after padding. Its varint width can grow.
			for len(header)+len(payload)+16 < packetLimit {
				payload = append(payload, make([]byte, packetLimit-len(header)-len(payload)-16)...)
				header, err = c.makeHeader(space, s.nextPN, len(payload)+16)
				if err != nil {
					return nil, c.fail(err)
				}
			}
			// A width change from a very short ACK can overshoot by one byte.
			if excess := len(header) + len(payload) + 16 - packetLimit; excess > 0 {
				payload = payload[:len(payload)-excess]
				header, err = c.makeHeader(space, s.nextPN, len(payload)+16)
				if err != nil {
					return nil, c.fail(err)
				}
			}
		}
		if (f.Type() == 0x1a || f.Type() == 0x1b) && c.budget.CanSend(1200) {
			payload = append(payload, make([]byte, 1200-len(header)-len(payload)-16)...)
		}
		size := len(header) + len(payload) + 16
		if size > packetLimit {
			return nil, c.fail(errors.New("quic: frame exceeds initial packetization limit"))
		}
		inFlight := space == initialSpace || wire.InFlightFrame(f)
		probe := c.probeAllowance[space] && wire.AckEliciting(f)
		if !c.congestion.CanSend(c.recovery.BytesInFlight, size, inFlight, probe) {
			continue
		}
		if inFlight {
			delay := c.pacer.Delay(now, c.congestion.Window(), c.recovery.RTT.Smoothed, size)
			if !probe && delay > 0 {
				at := now.Add(delay)
				if c.pacingAt.IsZero() || at.Before(c.pacingAt) {
					c.pacingAt = at
				}
				continue
			}
		}
		if !c.recovery.CanRecord(recovery.Space(space), inFlight) {
			continue
		}
		if !c.budget.CanSend(size) {
			continue
		}
		out, err := keys.SealInto(c.sealStorage, header, payload, s.nextPN)
		if err != nil {
			return nil, c.fail(err)
		}
		if inFlight {
			c.pacer.Sent(size)
		}
		if early {
			c.earlySealed++
		} else {
			c.sealed[space]++
		}
		if err = c.budget.Sent(len(out)); err != nil {
			return nil, c.fail(err)
		}
		if c.config.enableECN {
			c.recovery.EnableECN()
		}
		marking := c.recovery.ECNMarkingAt(recovery.Space(space), now)
		if err := c.recovery.OnSent(recovery.Space(space), recovery.Packet{ECN: marking, Number: s.nextPN, SentAt: now, Bytes: len(out), AckEliciting: wire.AckEliciting(f), InFlight: inFlight}); err != nil {
			return nil, c.fail(err)
		}
		c.lastBaseApplication = space == applicationSpace && !early
		_, c.lastBaseDatagram = f.(wire.DatagramFrame)
		c.datagramECN = marking
		c.datagramECNCommit = ecnCommit{c.recovery, recovery.Space(space), s.nextPN}
		if space == applicationSpace && !early {
			c.noteKeyPhaseACK(f)
			if bundled {
				c.noteKeyPhaseACKLargest(0, bundledLargest)
			}
		}
		if c.peerIDs != nil {
			id := c.peerIDs.active[c.peerIDs.current]
			id.used = true
			c.peerIDs.active[c.peerIDs.current] = id
		}
		s.sentAny = true
		if probe {
			c.probeAllowance[space] = false
		}
		s.largestSent = s.nextPN
		if item != nil {
			switch item.frame.(type) {
			case wire.DatagramFrame:
				c.queuedBytes -= item.bytes
				c.pendingDatagrams--
			case wire.PathChallengeFrame:
				c.sentPathChallenge(item.frame.(wire.PathChallengeFrame), len(out), now)
				c.queuedBytes -= item.bytes
			case wire.PathResponseFrame:
				c.queuedBytes -= item.bytes
			default:
				s.sent[s.nextPN] = item
			}
			if cid, ok := item.frame.(wire.NewConnectionIDFrame); ok {
				c.localIDs.markSent(cid.Sequence)
			}
			c.commitStreamPriority(item)
			item.queued = false
			s.queue[0] = nil
			s.queue = s.queue[1:]
			if bundled {
				s.clearACK()
			}
		} else if challenge, ok := f.(wire.PathChallengeFrame); ok {
			c.sentPathChallenge(challenge, len(out), now)
			c.baseMTUChallenge = nil
		} else {
			s.clearACK()
		}
		s.nextPN++
		if !c.config.Server && space == handshakeSpace {
			c.discard(initialSpace)
		}
		c.sentIdleActivity(now, wire.AckEliciting(f))
		c.armTimer(now)
		c.sentTotals.emitted(len(out))
		c.publishStatistics()
		return out, nil
	}
	return nil, nil
}
func (c *Conn) makeHeader(space int, pn uint64, payloadLen int) ([]byte, error) {
	if space == applicationSpace {
		if c.sendingEarly() {
			return wire.AppendLongHeader(nil, c.originalVersion, wire.ZeroRTT, c.peerCID, c.config.LocalCID, nil, pn, 4, payloadLen)
		}
		first := c.greaseQUICBit(0x43)
		if c.applicationKeys.phase {
			first |= 4
		}
		// The short header has a fixed-size packet number. Allocate its full
		// size once; incremental appends can grow twice for common CID sizes.
		b := make([]byte, 1+len(c.peerCID)+4)
		b[0] = first
		copy(b[1:], c.peerCID)
		binary.BigEndian.PutUint32(b[1+len(c.peerCID):], uint32(pn))
		return b, nil
	}
	var token []byte
	if space == initialSpace {
		token = c.retryToken
	}
	header, err := wire.AppendLongHeader(nil, c.config.Version, packetTypes[space], c.peerCID, c.config.LocalCID, token, pn, 4, payloadLen)
	if err == nil {
		header[0] = c.greaseQUICBit(header[0])
	}
	return header, err
}

// QueueProbe schedules up to two outstanding frame items from the earliest
// active space, using fresh packet numbers on transmission. This is an explicit
// loss-test hook, NOT the recovery timer entry point. Network drivers should
// use RecoveryDeadline and HandleTimeout for RFC 9002 timer-driven probing.
func (c *Conn) QueueProbe() error {
	if err := c.check(); err != nil {
		return err
	}
	for space := range c.spaces {
		s := &c.spaces[space]
		if s.discarded {
			continue
		}
		// Select by packet number for deterministic replay (map iteration is random).
		count := 0
		for count < 2 {
			var oldest *sendItem
			pn := ^uint64(0)
			for number, item := range s.sent {
				if number < pn {
					visitSendLeaves(item, func(part *sendItem) bool {
						if !part.queued {
							pn, oldest = number, part
							return true
						}
						return false
					})
				}
			}
			if oldest == nil {
				break
			}
			oldest.queued = true
			s.queue = append(s.queue, oldest)
			count++
		}
		if count > 0 {
			return nil
		}
	}
	return nil
}

func (c *Conn) issueLocalIDs() error {
	if c.peerParams == nil {
		return nil
	}
	limit := c.peerParams.ActiveConnectionIDLimit
	if c.sendingEarly() {
		// New transport parameters can arrive before application write keys.
		// CID frames emitted in 0-RTT must stay within remembered credit.
		limit = c.streams.peerLimits.ActiveConnectionIDLimit
	}
	frames, err := c.localIDs.issue(limit)
	if err != nil {
		return c.fail(err)
	}
	for _, f := range frames {
		if err := c.enqueue(applicationSpace, f, 64); err != nil {
			return err
		}
	}
	return nil
}

// RotatePeerID uses an unused peer CID on the same path. This is CID rotation,
// not address migration. It becomes available after handshake confirmation.
func (c *Conn) RotatePeerID() error {
	if err := c.check(); err != nil {
		return err
	}
	if !c.confirmed {
		return errors.New("quic: handshake not confirmed")
	}

	destination := &c.spaces[applicationSpace]
	if c.multipath != nil {
		if c.multipath.abandoned[0] != nil {
			return errPathAbandoned
		}
		id, ok := c.survivingPath(^uint64(0))
		if !ok {
			return errNoPathForControl
		}
		destination = c.multipath.paths[id].application.space
	}
	if destination.discarded {
		return errNoPathForControl
	}
	// Stage the complete retirement batch. Rejection must leave the connection
	// usable, including its CID manager shared with multipath path zero.
	staged := *c.peerIDs
	staged.active = maps.Clone(staged.active)
	retired, err := staged.rotate()
	if err != nil {
		return err
	}
	if len(retired) > c.availableSendBytes()/16 {
		return ErrSendBuffer
	}
	items := make([]*sendItem, 0, len(retired))
	for _, seq := range retired {
		items = append(items, &sendItem{frame: wire.RetireConnectionIDFrame{Sequence: seq}, bytes: 16, queued: true})
	}
	*c.peerIDs = staged
	c.peerCID = clone(c.peerIDs.active[c.peerIDs.current].cid)
	destination.queue = append(destination.queue, items...)
	c.queuedBytes += 16 * len(items)
	return nil
}

// HasOutstanding reports queued frames awaiting transmission, plus reliable
// frames awaiting an ACK. DATAGRAM, PATH_CHALLENGE and PATH_RESPONSE frames
// count only until emitted, so this is not a reliable-delivery predicate.
// It does not include an ACK-only packet waiting to be emitted.
func (c *Conn) HasOutstanding() bool { return c.queuedBytes != 0 }

func (c *Conn) CanRotatePeerID() bool {
	if c.check() != nil || !c.confirmed || c.peerIDs == nil {
		return false
	}
	if c.multipath != nil && c.multipath.abandoned[0] != nil {
		return false
	}
	_, ok := c.peerIDs.nextUnused()
	return ok
}

func (c *Conn) Version() wire.Version { return c.config.Version }
func (c *Conn) SupportsDatagrams() bool {
	return c.peerParams != nil && c.peerParams.MaxDatagramFrameSize > 0
}

// Err reports a terminal failure on the serialized connection owner.
func (c *Conn) Err() error { return c.check() }

// AdapterData returns immutable adapter metadata. Call on the connection owner.
func (c *Conn) AdapterData() any { return c.config.AdapterData }
