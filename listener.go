package quic

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/transport"
	"github.com/getlantern/photon/internal/wire"
)

type Version = wire.Version

const (
	Version1 = wire.Version1
	Version2 = wire.Version2
)

type ApplicationErrorCode = transport.ApplicationErrorCode

// ApplicationError describes a local or remote application connection close.
type ApplicationError = transport.ApplicationError

// PeerCloseError describes an authenticated connection close sent by the peer.
type PeerCloseError = transport.PeerCloseError

// Config is the initial experimental connection configuration. Zero values select
// defaults. This API does not yet provide quic-go configuration parity.
type Config struct {
	// GetConfigForClient selects settings after Initial/token checks and before TLS.
	// ClientInfo.AddrVerified reports whether token evidence validated the source.
	// An error refuses the connection. A nil result selects default settings.
	// It runs on the listener owner and must not block or call listener methods.
	// Returned settings must not be modified concurrently. Nested selectors are
	// ignored. Rejected attempts can invoke it again on subsequent Initials.
	// Versions and compatible version policy, preferred sockets and
	// admission capacity remain listener-wide. Multipath may be disabled per
	// connection, but enabling it requires listener-level support.
	GetConfigForClient func(*ClientInfo) (*Config, error)

	// InitialStreamReceiveWindow sets initial per-stream receive credit. Zero uses
	// 512 KiB. Values above 16 MiB are rejected. Storage is allocated lazily.
	// Growth is bounded by MaxStreamReceiveWindow.
	InitialStreamReceiveWindow uint64
	// InitialConnectionReceiveWindow sets initial connection-wide receive credit.
	// Zero uses 768 KiB. Values above the QUIC varint maximum are rejected.
	InitialConnectionReceiveWindow uint64
	// MaxStreamReceiveWindow bounds consumption/RTT-driven stream window growth.
	// Zero selects at least 6 MiB or the initial window, whichever is larger.
	// Maximum supported: 16 MiB.
	MaxStreamReceiveWindow uint64
	// MaxConnectionReceiveWindow bounds connection window growth. Zero selects
	// at least 15 MiB or the initial window, whichever is larger.
	// Explicit maxima must be at least their initial values.
	MaxConnectionReceiveWindow uint64
	// AllowConnectionWindowIncrease may veto a proposed connection window increase.
	// It receives this connection and the additional credit, and runs on the
	// connection owner. It must not block or call connection/stream methods.
	// Nil allows growth up to MaxConnectionReceiveWindow.
	AllowConnectionWindowIncrease func(conn *Conn, delta uint64) bool

	// Allow0RTT permits servers to accept replayable early application data.
	// Applications must tolerate replay. It does not change Listener.Accept timing.
	Allow0RTT bool
	earlyDial bool
	// TokenStore caches client address-validation tokens. Nil disables storage.
	// Share a store across DialAddr calls to avoid unnecessary Retry exchanges.
	TokenStore TokenStore

	// EnableCompatibleVersionNegotiation permits v1/v2 switching within a handshake.
	// Versions orders preference; clients normally start with its first version.
	EnableCompatibleVersionNegotiation bool
	// InitialVersion overrides the client first-flight version without changing
	// preference. Zero selects Versions[0] (v1 by default). It must be supported
	// by Versions. Listeners do not use it to select a connection version.
	InitialVersion Version
	Versions       []Version
	// HandshakeIdleTimeout limits inactivity before TLS completion. Zero uses
	// 5 seconds. Authenticated receives and the first ack-eliciting send after
	// a receive restart it. Retransmissions alone cannot keep it alive.
	HandshakeIdleTimeout time.Duration
	// HandshakeTimeout bounds time to TLS completion regardless of
	// activity. Zero uses twice HandshakeIdleTimeout (saturated on overflow).
	HandshakeTimeout time.Duration
	MaxIdleTimeout   time.Duration
	// KeepAlivePeriod sends PINGs on otherwise idle confirmed connections. Zero
	// disables keepalive. The period is capped at half the negotiated idle timeout;
	// probes then use a minimum of 1.5 times the selected path's base PTO.
	KeepAlivePeriod time.Duration
	MaxConnections  int
	EnableDatagrams bool
	// EnableStreamResetPartialDelivery advertises support for receiving reliable
	// stream prefixes followed by a reset. Sending uses the peer's capability.
	EnableStreamResetPartialDelivery bool
	// EnableQUICBitGreasing advertises willingness to receive either QUIC bit.
	// Sending is randomized only after the peer advertises support, independently
	// of this receive setting. Pre-handshake remembered-token greasing is not used.
	EnableQUICBitGreasing bool
	// MaxIncomingStreams limits concurrent peer-initiated bidirectional streams.
	// Zero selects 100; negative values disable them. Values above the protocol
	// maximum of 2^60 are clipped to that maximum.
	MaxIncomingStreams int64
	// MaxIncomingUniStreams applies the same policy to unidirectional streams.
	MaxIncomingUniStreams int64
	// RequireStreamCreditRelease keeps a finished peer-initiated stream counted
	// against MaxIncomingStreams or MaxIncomingUniStreams until the application
	// calls ReleaseStreamCredit on it. By default the credit returns on cleanup.
	RequireStreamCreditRelease bool
	// InitialPacketSize sets the original path's starting UDP payload limit.
	// Zero selects 1280. Nonzero values are clipped to 1200 through 1452.
	// Peer limits can reduce it. New or reset paths start again at 1200.
	InitialPacketSize uint16
	// DisablePathMTUDiscovery disables upward size probes. The original path
	// starts at InitialPacketSize; write failure or migration can reduce it.
	// Discovery is supported on concrete-address Linux/macOS sockets up to 1452.
	DisablePathMTUDiscovery bool
	// CongestionControl selects the congestion controller for the
	// connection's paths. The zero value is BBR.
	CongestionControl CongestionControl
	// EnableMultipath opts into RFC 10069 multipath, with path IDs 0–3.
	// Both endpoints must opt in. ListenAddr requires a concrete bind address.
	// Servers validate client address rebinding within the existing socket family,
	// reserving an unused peer
	// CID for probes when the client changes its destination CID.
	EnableMultipath bool
	// LocalAddr is the client UDP bind address; ListenAddr ignores it.
	// DialAddr requires a concrete IP here when EnableMultipath is set.
	LocalAddr *net.UDPAddr
	// PreferredAddresses configures server sockets advertised after the handshake,
	// at most one concrete IP per family. Port zero selects an ephemeral port.
	// ListenAddr requires a concrete primary address when this is set. DialAddr
	// ignores this server-only option. Listener.Close closes all these sockets.
	PreferredAddresses []netip.AddrPort
}

// ClientInfo describes the source of an incoming connection attempt.
type ClientInfo struct {
	RemoteAddr net.Addr
	// AddrVerified reports valid Retry or NEW_TOKEN address evidence. Without
	// such evidence RemoteAddr may be spoofed.
	AddrVerified bool
}

// Listener accepts incoming QUIC connections. Closing a ListenAddr listener also
// closes its socket and accepted connections. Closing a Listen listener stops
// accepted connections but preserves the borrowed socket. Closing a Transport
// listener leaves accepted connections and the socket running. Full quic-go
// parity is unfinished.
type Listener struct {
	configurable                  bool
	allowConnectionWindowIncrease func(*Conn, uint64) bool
	server                        *transport.UDPServer
	generation                    uint64
	shared                        bool
	datagrams                     bool
	preferred                     []netip.AddrPort
}

// EarlyListener exposes connections before client authentication and handshake
// completion. Applications must wait for HandshakeComplete when those guarantees
// are required. Allow0RTT independently controls acceptance of replayable data.
type EarlyListener struct{ *Listener }

func ListenAddrEarly(addr string, tlsConfig *tls.Config, config *Config) (*EarlyListener, error) {
	l, err := listenAddr(addr, tlsConfig, config, true)
	if err != nil {
		return nil, err
	}
	return &EarlyListener{Listener: l}, nil
}

func ListenAddr(addr string, tlsConfig *tls.Config, config *Config) (*Listener, error) {
	return listenAddr(addr, tlsConfig, config, false)
}

func listenAddr(addr string, tlsConfig *tls.Config, config *Config, early bool) (*Listener, error) {
	cfg, params, err := prepareConfig(config)
	if err != nil {
		return nil, err
	}
	engineConfig, tokens, err := makeServerConfig(tlsConfig, cfg, params, early)
	if err != nil {
		return nil, err
	}

	sockets, preferred, err := listenPreferredSockets(addr, cfg.PreferredAddresses)
	if err != nil {
		return nil, err
	}
	transferred := false
	defer func() {
		if !transferred {
			for _, socket := range sockets {
				socket.Close()
			}
		}
	}()
	engineConfig.PreferredAddresses = preferred

	endpoint, err := transport.NewServerEndpoint(engineConfig, tokens, cfg.MaxConnections)
	if err != nil {
		return nil, err
	}
	server, err := transport.NewUDPServerWithSockets(sockets, endpoint)
	if err != nil {
		endpoint.Close()
		return nil, err
	}
	transferred = true
	return &Listener{configurable: cfg.GetConfigForClient != nil, allowConnectionWindowIncrease: cfg.AllowConnectionWindowIncrease, server: server, datagrams: cfg.EnableDatagrams, preferred: preferred}, nil
}

func makeServerConfig(tlsConfig *tls.Config, cfg Config, params wire.TransportParameters, early bool) (transport.Config, *token.RetryTokens, error) {
	if tlsConfig == nil || len(tlsConfig.NextProtos) == 0 || (len(tlsConfig.Certificates) == 0 && tlsConfig.GetCertificate == nil && tlsConfig.GetConfigForClient == nil) {
		return transport.Config{}, nil, errors.New("quic: server certificate and ALPN configuration required")
	}
	if err := validateTLS(tlsConfig); err != nil {
		return transport.Config{}, nil, err
	}
	version := wire.Version(0)
	if len(cfg.Versions) == 1 {
		version = cfg.Versions[0]
	}
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		return transport.Config{}, nil, err
	}
	tokens, err := token.NewRetryTokens(key, nil, time.Minute)
	if err != nil {
		return transport.Config{}, nil, err
	}
	var addressKey [32]byte
	if _, err := rand.Read(addressKey[:]); err != nil {
		return transport.Config{}, nil, err
	}
	addressTokens, err := token.NewAddressTokens(addressKey, nil, 24*time.Hour)
	if err != nil {
		return transport.Config{}, nil, err
	}

	engineConfig := transport.Config{MaxStreamReceiveWindow: cfg.MaxStreamReceiveWindow, MaxConnectionReceiveWindow: cfg.MaxConnectionReceiveWindow, AcceptEarly: early, EnableEarlyData: cfg.Allow0RTT, CompatibleVersions: compatibleVersions(cfg), PreferredAddresses: cfg.PreferredAddresses, AddressTokens: addressTokens, DisablePathMTUDiscovery: cfg.DisablePathMTUDiscovery, CongestionControl: cfg.congestionAlgorithm(), InitialPacketSize: cfg.InitialPacketSize, EnableMigration: true, EnableMultipath: cfg.EnableMultipath, RequireStreamCreditRelease: cfg.RequireStreamCreditRelease, Version: version, TLS: tlsConfig, Parameters: params, HandshakeTimeout: cfg.HandshakeTimeout, HandshakeIdleTimeout: cfg.HandshakeIdleTimeout, KeepAlivePeriod: cfg.KeepAlivePeriod}
	engineConfig.AdapterData = acceptedConfig{cfg.EnableDatagrams, cfg.AllowConnectionWindowIncrease}
	if cfg.GetConfigForClient != nil {
		engineConfig.ConfigureClient = serverConfigSelector(cfg)
	}
	return engineConfig, tokens, nil
}
func (l *Listener) Addr() net.Addr { return l.server.Addr() }

// PreferredAddresses returns the concrete advertised sockets, including allocated ports.
// The returned slice is owned by the caller.
func (l *Listener) PreferredAddresses() []netip.AddrPort {
	return append([]netip.AddrPort(nil), l.preferred...)
}
func (l *Listener) Close() error {
	if !l.shared {
		return l.server.Close()
	}
	err := l.server.Do(context.Background(), func(e *transport.ServerEndpoint) error {
		e.CloseListenerGeneration(l.generation, time.Now())
		return nil
	})
	if errors.Is(err, transport.ErrEndpointClosed) {
		return nil
	}
	return err
}
func (l *Listener) Accept(ctx context.Context) (*Conn, error) {
	var raw *transport.Conn
	var peer netip.AddrPort
	var err error
	if l.shared {
		raw, peer, err = l.server.AcceptListener(ctx, l.generation)
	} else {
		raw, peer, err = l.server.Accept(ctx)
	}
	if errors.Is(err, transport.ErrListenerClosed) || errors.Is(err, transport.ErrEndpointClosed) {
		return nil, ErrServerClosed
	}
	if err != nil {
		return nil, err
	}
	// Acceptance has consumed the queue entry. Finish constructing its handle
	// even if the caller cancels now, so cancellation cannot lose a connection.
	lifetime, err := l.server.ConnectionContext(context.Background(), raw)
	if err != nil {
		return nil, err
	}
	c := &Conn{server: l.server, raw: raw, ctx: lifetime, local: l.Addr(), remote: net.UDPAddrFromAddrPort(peer), datagrams: l.datagrams}
	if err := l.server.Do(context.Background(), func(*transport.ServerEndpoint) error {
		if err := raw.Err(); err != nil {
			return err
		}
		policy := l.allowConnectionWindowIncrease
		if selected, ok := raw.AdapterData().(acceptedConfig); ok {
			c.datagrams = selected.datagrams
			policy = selected.allowWindowIncrease
		}
		c.installReceiveWindowPolicy(policy)
		return c.observeHandshake()
	}); err != nil {
		return nil, err
	}
	return c, nil
}

// Conn is a dialed or accepted connection. Its exported methods are safe to call
// concurrently. The engine remains confined to the UDP owner's goroutine.
type Conn struct {
	earlyStreamsRecovered bool // connection owner only
	stoppedReadsMu        sync.Mutex
	server                *transport.UDPServer
	raw                   *transport.Conn
	ctx                   context.Context
	local, remote         net.Addr
	state                 atomic.Pointer[ConnectionState]
	handshakeComplete     chan struct{}
	datagrams             bool
}
type ConnectionState struct {
	Used0RTT                           bool
	TLS                                tls.ConnectionState
	Version                            Version
	SupportsDatagrams                  bool
	SupportsMultipath                  bool
	SupportsStreamResetPartialDelivery struct{ Remote, Local bool }
	// RequireStreamCreditRelease is true if Config.RequireStreamCreditRelease was set.
	RequireStreamCreditRelease bool
}

func (c *Conn) Context() context.Context { return c.ctx }

// LocalAddr reports the last committed primary-path local address.
func (c *Conn) LocalAddr() net.Addr {
	if c.raw != nil {
		if local, _, ok := c.raw.CurrentPathAddresses(0); ok {
			return net.UDPAddrFromAddrPort(local)
		}
	}
	return copyAddress(c.local)
}

// RemoteAddr reports the primary path peer address, including committed port
// rebinding. It remains available after shutdown and returns an independent copy.
func (c *Conn) RemoteAddr() net.Addr {
	if c.raw != nil {
		if remote := c.raw.CurrentRemoteAddr(); remote.IsValid() {
			return net.UDPAddrFromAddrPort(remote)
		}
	}
	return copyAddress(c.remote)
}

func copyAddress(address net.Addr) net.Addr {
	if udp, ok := address.(*net.UDPAddr); ok {
		copied := *udp
		copied.IP = append(net.IP(nil), udp.IP...)
		return &copied
	}
	return address
}
func (c *Conn) ConnectionState() ConnectionState {
	if state := c.state.Load(); state != nil {
		return *state
	}
	return ConnectionState{}
}

// HandshakeComplete is closed after successful TLS completion. Handshake failure
// closes Context instead. Completion does not indicate application-data delivery.
func (c *Conn) HandshakeComplete() <-chan struct{} { return c.handshakeComplete }

// observeHandshake runs on the transport owner before publishing the Conn.
func (c *Conn) observeHandshake() error {
	c.handshakeComplete = make(chan struct{})
	c.publishConnectionState()
	if err := c.raw.WatchHandshakeComplete(func() {
		c.publishConnectionState()
		close(c.handshakeComplete)
	}); err != nil {
		return err
	}
	// Confirmation can make additional negotiated capabilities usable.
	return c.raw.WatchHandshakeConfirmed(c.publishConnectionState)
}
func (c *Conn) publishConnectionState() {
	raw := c.raw
	state := &ConnectionState{TLS: raw.ConnectionState(), Version: raw.Version(), SupportsDatagrams: raw.SupportsDatagrams(), SupportsMultipath: raw.SupportsMultipath(), Used0RTT: raw.EarlyDataAccepted(), RequireStreamCreditRelease: raw.RequireStreamCreditRelease()}
	state.SupportsStreamResetPartialDelivery.Local, state.SupportsStreamResetPartialDelivery.Remote = raw.PartialResetCapabilities()
	c.state.Store(state)
}
func (c *Conn) CloseWithError(code ApplicationErrorCode, reason string) error {
	return c.server.Do(context.Background(), func(*transport.ServerEndpoint) error { return c.raw.CloseWithError(uint64(code), reason, time.Now()) })
}
func (c *Conn) SendDatagram(data []byte) error {
	return c.SendDatagramContext(context.Background(), data)
}

// SendDatagramContext waits for send-queue capacity until ctx is canceled.
// Success means queued for transmission, not acknowledged delivery.
func (c *Conn) SendDatagramContext(ctx context.Context, data []byte) error {
	err := c.server.Wait(ctx, func(*transport.ServerEndpoint) (bool, error) {
		err := c.raw.SendDatagram(data)
		if errors.Is(err, stream.ErrWouldBlock) {
			return false, nil
		}
		return true, err
	})
	return c.connectionError(err)
}

var ErrDatagramsDisabled = errors.New("quic: receiving datagrams is disabled")

func (c *Conn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	if !c.datagrams {
		return nil, ErrDatagramsDisabled
	}
	var data []byte
	err := c.server.Wait(ctx, func(*transport.ServerEndpoint) (bool, error) {
		var err error
		data, err = c.raw.ReceiveDatagram()
		if errors.Is(err, stream.ErrWouldBlock) {
			return false, nil
		}
		return true, err
	})
	return data, c.connectionError(err)
}

func prepareConfig(config *Config) (Config, wire.TransportParameters, error) {
	cfg := Config{}
	if config != nil {
		cfg = *config
	}
	if _, ok := cfg.CongestionControl.algorithm(); !ok {
		return Config{}, wire.TransportParameters{}, errors.New("quic: unknown congestion control")
	}
	if cfg.HandshakeIdleTimeout < 0 || cfg.HandshakeTimeout < 0 || cfg.MaxIdleTimeout < 0 || cfg.KeepAlivePeriod < 0 {
		return Config{}, wire.TransportParameters{}, errors.New("quic: negative timeout")
	}
	if cfg.InitialPacketSize == 0 {
		cfg.InitialPacketSize = 1280
	} else {
		cfg.InitialPacketSize = min(max(cfg.InitialPacketSize, 1200), 1452)
	}
	if cfg.HandshakeIdleTimeout == 0 {
		cfg.HandshakeIdleTimeout = 5 * time.Second
	}
	if cfg.HandshakeTimeout == 0 {
		cfg.HandshakeTimeout = cfg.HandshakeIdleTimeout + min(cfg.HandshakeIdleTimeout, time.Duration(1<<63-1)-cfg.HandshakeIdleTimeout)
	}
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = 1024
	}
	if cfg.MaxIdleTimeout == 0 {
		cfg.MaxIdleTimeout = 30 * time.Second
	}
	if len(cfg.Versions) > 2 {
		return Config{}, wire.TransportParameters{}, wire.ErrVersion
	}
	for i, v := range cfg.Versions {
		if !v.Supported() || (i > 0 && v == cfg.Versions[0]) {
			return Config{}, wire.TransportParameters{}, wire.ErrVersion
		}
	}
	if cfg.InitialVersion != 0 {
		allowed := cfg.InitialVersion.Supported() && len(cfg.Versions) == 0
		for _, v := range cfg.Versions {
			if v == cfg.InitialVersion {
				allowed = true
			}
		}
		if !allowed {
			return Config{}, wire.TransportParameters{}, wire.ErrVersion
		}
	}
	cfg.Versions = append([]Version(nil), cfg.Versions...)
	params := wire.DefaultTransportParameters()
	params.GreaseQUICBit = cfg.EnableQUICBitGreasing
	// Keep spare IDs for migration even when a peer does not replenish its
	// initial handshake CID. The wire default of two leaves only one spare.
	params.ActiveConnectionIDLimit = 4
	params.MaxIdleTimeout = uint64(max(cfg.MaxIdleTimeout/time.Millisecond, 1))
	if cfg.InitialStreamReceiveWindow == 0 {
		cfg.InitialStreamReceiveWindow = 512 << 10
	}
	if cfg.InitialConnectionReceiveWindow == 0 {
		cfg.InitialConnectionReceiveWindow = 768 << 10
	}
	if cfg.InitialStreamReceiveWindow > 16<<20 || cfg.InitialConnectionReceiveWindow > wire.MaxVarint {
		return Config{}, wire.TransportParameters{}, errors.New("quic: receive window exceeds supported capacity")
	}
	if cfg.MaxStreamReceiveWindow == 0 {
		cfg.MaxStreamReceiveWindow = max(6<<20, cfg.InitialStreamReceiveWindow)
	}
	if cfg.MaxConnectionReceiveWindow == 0 {
		cfg.MaxConnectionReceiveWindow = max(15<<20, cfg.InitialConnectionReceiveWindow)
	}
	if cfg.MaxStreamReceiveWindow < cfg.InitialStreamReceiveWindow || cfg.MaxStreamReceiveWindow > 16<<20 || cfg.MaxConnectionReceiveWindow < cfg.InitialConnectionReceiveWindow || cfg.MaxConnectionReceiveWindow > wire.MaxVarint {
		return Config{}, wire.TransportParameters{}, errors.New("quic: invalid maximum receive window")
	}
	params.InitialMaxData = cfg.InitialConnectionReceiveWindow
	params.InitialMaxStreamDataBidiLocal = cfg.InitialStreamReceiveWindow
	params.InitialMaxStreamDataBidiRemote = cfg.InitialStreamReceiveWindow
	params.InitialMaxStreamDataUni = cfg.InitialStreamReceiveWindow
	cfg.MaxIncomingStreams = min(cfg.MaxIncomingStreams, 1<<60)
	cfg.MaxIncomingUniStreams = min(cfg.MaxIncomingUniStreams, 1<<60)
	if cfg.MaxIncomingStreams == 0 {
		cfg.MaxIncomingStreams = 100
	}
	if cfg.MaxIncomingUniStreams == 0 {
		cfg.MaxIncomingUniStreams = 100
	}
	params.InitialMaxStreamsBidi = uint64(max(cfg.MaxIncomingStreams, 0))
	params.InitialMaxStreamsUni = uint64(max(cfg.MaxIncomingUniStreams, 0))
	if cfg.EnableMultipath {
		maximum := uint64(3)
		params.InitialMaxPathID = &maximum
	}
	if cfg.LocalAddr != nil {
		address := *cfg.LocalAddr
		address.IP = append(net.IP(nil), address.IP...)
		cfg.LocalAddr = &address
	}
	params.ResetStreamAt = cfg.EnableStreamResetPartialDelivery
	if cfg.EnableDatagrams {
		params.MaxDatagramFrameSize = 65535
	}
	if cfg.MaxConnections < 1 || cfg.MaxConnections > 65536 {
		return Config{}, wire.TransportParameters{}, errors.New("quic: invalid connection limit")
	}
	return cfg, params, nil
}

func validateTLS(cfg *tls.Config) error {
	if cfg == nil || len(cfg.NextProtos) == 0 {
		return errors.New("quic: TLS and ALPN configuration required")
	}
	if cfg.MinVersion > tls.VersionTLS13 || (cfg.MaxVersion != 0 && cfg.MaxVersion < tls.VersionTLS13) {
		return errors.New("quic: TLS 1.3 required")
	}
	return nil
}

// VersionNegotiationError reports unauthenticated rejection of the selected version.
type VersionNegotiationError = transport.VersionNegotiationError

// DatagramsEnabled reports whether local QUIC DATAGRAM reception was enabled at
// construction. Peer support is reported separately by ConnectionState.
func (c *Conn) DatagramsEnabled() bool { return c.datagrams }

// DatagramsEnabled reports whether DATAGRAM reception is enabled by default or
// can be selected by GetConfigForClient. A selector makes the listener's policy
// connection-dependent; use Conn.DatagramsEnabled for the actual selected policy.
func (l *Listener) DatagramsEnabled() bool { return l.datagrams || l.configurable }

func compatibleVersions(cfg Config) []wire.Version {
	if !cfg.EnableCompatibleVersionNegotiation {
		return nil
	}
	if len(cfg.Versions) == 0 {
		return []wire.Version{wire.Version1, wire.Version2}
	}
	return append([]wire.Version(nil), cfg.Versions...)
}

// Called on the engine owner before exposing the public connection handle.
func (c *Conn) installReceiveWindowPolicy(policy func(*Conn, uint64) bool) {
	if policy != nil {
		c.raw.SetConnectionWindowPolicy(func(delta uint64) bool { return policy(c, delta) })
	}
}
