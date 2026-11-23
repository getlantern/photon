package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/transport"
)

// StatelessResetKey is a secret key for reproducible stateless-reset tokens.
// Generate it with crypto/rand and preserve it across restarts when desired.
type StatelessResetKey [32]byte

// Transport manages QUIC connections and listeners on a caller-owned UDP socket. Configure Conn
// before first use and do not read, write, or change deadlines on it until Close
// returns. Use WriteTo and ReadNonQUICPacket for raw traffic while it is owned.
// A Transport must not be copied after first use.
//
// Conn must provide UDP IP/port addresses and implement deadline cancellation.
// Plain PacketConn wrappers use ReadFrom/WriteTo without ECN or kernel-dependent
// MTU probing; *net.UDPConn and OOBCapablePacketConn retain those capabilities. This API is experimental:
// other transport options and preferred-address socket registration are unfinished.
type Transport struct {
	Conn net.PacketConn
	// ConnContext selects the parent context for incoming TLS, connections and
	// streams. An error rejects admission. Its input is canceled on failure or
	// closure. Configure before first use; the callback must not block or reenter
	// this Transport. Dialed connections do not invoke it.
	ConnContext func(context.Context, *ClientInfo) (context.Context, error)
	// ConnectionIDLength selects 1–20 byte local CIDs. Zero defaults to four.
	// Configure before first use; short IDs increase collision risk.
	ConnectionIDLength int
	// ConnectionIDGenerator overrides ConnectionIDLength. Calls are serialized
	// per Transport; configure before first use.
	ConnectionIDGenerator ConnectionIDGenerator
	// StatelessResetKey enables reset responses for unknown connection IDs. Nil
	// disables sending resets. The key is copied at first use.
	StatelessResetKey *StatelessResetKey
	// TokenGeneratorKey protects address-validation tokens. Nil generates a
	// random key at first use. The key is copied and retained across listener
	// restarts. Configure before first use.
	TokenGeneratorKey *TokenGeneratorKey
	// MaxTokenAge limits NEW_TOKEN validity. Zero selects 24 hours; negative
	// values are rejected. Configure before first use. Retry lifetime is separate.
	MaxTokenAge time.Duration
	// VerifySourceAddress requests Retry for an unvalidated source. Nil skips
	// Retry; amplification limits remain in force until address validation.
	// The supplied address may be spoofed. Configure before first use; the
	// callback must not block or reenter this Transport.
	VerifySourceAddress func(net.Addr) bool
	// DisableVersionNegotiationPackets suppresses stateless Version Negotiation
	// responses to unsupported versions. Set it before first use. It does not
	// disable processing of Version Negotiation received by outgoing connections.
	DisableVersionNegotiationPackets bool

	mu            sync.Mutex
	runtime       *transport.UDPServer
	closed        bool
	retryTokens   *token.RetryTokens
	addressTokens *token.AddressTokens
}

func (t *Transport) init(capacity int) (*transport.UDPServer, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, ErrTransportClosed
	}
	if t.runtime != nil {
		select {
		case <-t.runtime.Done():
			return nil, ErrTransportClosed
		default:
		}
		return t.runtime, nil
	}
	cidLength := t.ConnectionIDLength
	var generate func() ([]byte, error)
	if generator := t.ConnectionIDGenerator; generator != nil {
		cidLength = generator.ConnectionIDLen()
		if cidLength < 0 || cidLength > 20 {
			return nil, errors.New("quic: generator CID length must be between 0 and 20")
		}
		var generationMu sync.Mutex
		generate = func() ([]byte, error) {
			generationMu.Lock()
			defer generationMu.Unlock()
			cid, err := generator.GenerateConnectionID()
			if err != nil {
				return nil, err
			}
			if cid.Len() != cidLength {
				return nil, errors.New("quic: generator returned inconsistent CID length")
			}
			return cid.Bytes(), nil
		}
	} else {
		if cidLength == 0 {
			cidLength = 4
		}
		if cidLength < 1 || cidLength > 20 {
			return nil, errors.New("quic: ConnectionIDLength must be between 1 and 20")
		}
	}
	if cidLength == 0 && t.StatelessResetKey != nil {
		return nil, errors.New("quic: reset key requires nonzero connection IDs")
	}
	retries, addresses, err := newTransportTokens(t.TokenGeneratorKey, t.MaxTokenAge)
	if err != nil {
		return nil, err
	}
	endpoint, err := transport.NewClientOnlyEndpoint(cidLength, capacity)
	if err != nil {
		return nil, err
	}
	if hook := t.ConnContext; hook != nil {
		endpoint.ConnContext = func(ctx context.Context, peer netip.AddrPort, verified bool) (context.Context, error) {
			return hook(ctx, &ClientInfo{RemoteAddr: net.UDPAddrFromAddrPort(peer), AddrVerified: verified})
		}
	}
	endpoint.GenerateConnectionID = generate
	if t.StatelessResetKey != nil {
		key := transport.ResetKey(*t.StatelessResetKey)
		endpoint.ResetKey = &key
	}
	endpoint.DisableVersionNegotiationPackets = t.DisableVersionNegotiationPackets
	verify := t.VerifySourceAddress
	endpoint.VerifySourceAddress = func(peer netip.AddrPort) bool {
		return verify != nil && verify(net.UDPAddrFromAddrPort(peer))
	}
	runtime, err := transport.NewPacketServerBorrowed(t.Conn, endpoint)
	if err != nil {
		endpoint.Close()
		return nil, err
	}
	t.retryTokens, t.addressTokens = retries, addresses
	t.runtime = runtime
	return runtime, nil
}

// Listen starts a listener. Closing it preserves accepted connections and permits
// a replacement listener on this Transport. Only one listener can run at a time.
func (t *Transport) Listen(tlsConf *tls.Config, conf *Config) (*Listener, error) {
	return t.listen(tlsConf, conf, false)
}

// ListenEarly exposes incoming connections when application keys are available.
// Wait for each connection's HandshakeComplete before relying on authentication.
func (t *Transport) ListenEarly(tlsConf *tls.Config, conf *Config) (*EarlyListener, error) {
	listener, err := t.listen(tlsConf, conf, true)
	if err != nil {
		return nil, err
	}
	return &EarlyListener{Listener: listener}, nil
}

func (t *Transport) listen(tlsConf *tls.Config, conf *Config, early bool) (*Listener, error) {
	cfg, params, err := prepareConfig(conf)
	if err != nil {
		return nil, err
	}
	if len(cfg.PreferredAddresses) > 0 {
		return nil, errors.New("quic: Transport preferred-address sockets are not yet supported")
	}
	engine, tokens, err := makeServerConfig(tlsConf, cfg, params, early)
	if err != nil {
		return nil, err
	}
	runtime, err := t.init(cfg.MaxConnections)
	if err != nil {
		return nil, err
	}
	engine.AddressTokens = t.addressTokens
	tokens = t.retryTokens
	var generation uint64
	err = runtime.Do(context.Background(), func(e *transport.ServerEndpoint) error {
		var err error
		generation, err = e.StartListenerWithCapacity(engine, tokens, cfg.MaxConnections)
		return err
	})
	if errors.Is(err, transport.ErrEndpointClosed) {
		return nil, ErrTransportClosed
	}
	if err != nil {
		return nil, err
	}
	return &Listener{server: runtime, generation: generation, shared: true, configurable: cfg.GetConfigForClient != nil, allowConnectionWindowIncrease: cfg.AllowConnectionWindowIncrease, datagrams: cfg.EnableDatagrams}, nil
}

// Close terminates QUIC activity and waits for socket readers to exit. The
// caller's socket remains open with cleared deadlines. Connections are disposed
// immediately; applications wanting a wire close should close them first.
func (t *Transport) Close() error {
	t.mu.Lock()
	t.closed = true
	runtime := t.runtime
	t.mu.Unlock()
	if runtime != nil {
		return runtime.Close()
	}
	return nil
}

// ErrTransportClosed reports an operation on a stopped Transport.
var ErrTransportClosed = &errTransportClosed{}

type errTransportClosed struct{}

func (*errTransportClosed) Error() string { return "quic: transport closed" }
func (*errTransportClosed) Is(target error) bool {
	return target == net.ErrClosed || target == transport.ErrEndpointClosed
}

// ErrServerClosed reports acceptance after listener closure.
var ErrServerClosed = errServerClosed{}

type errServerClosed struct{}

func (errServerClosed) Error() string { return "quic: server closed" }
func (errServerClosed) Is(target error) bool {
	return target == net.ErrClosed || target == transport.ErrListenerClosed
}

// OOBCapablePacketConn lets UDP wrappers retain socket configuration and ancillary
// message support. Implementations must forward these operations consistently
// with their PacketConn methods. Plain PacketConn implementations remain usable
// without ECN or kernel-dependent MTU probing.
type OOBCapablePacketConn = transport.OOBCapablePacketConn

// WriteTo sends a raw datagram on the transport's primary socket. It can coexist
// with QUIC connections and does not affect their packet accounting.
func (t *Transport) WriteTo(b []byte, addr net.Addr) (int, error) {
	runtime, err := t.init(1024)
	if err != nil {
		return 0, err
	}
	n, err := runtime.WriteTo(b, addr)
	if errors.Is(err, transport.ErrEndpointClosed) {
		err = ErrTransportClosed
	}
	return n, err
}

// ReadNonQUICPacket reads an unrouted datagram whose two leading bits are clear.
// Known QUIC CIDs and stateless reset tokens retain QUIC routing precedence,
// including when QUIC-bit greasing is enabled. Reception begins on first use;
// the bounded queue drops new packets when full. A small b truncates the packet.
func (t *Transport) ReadNonQUICPacket(ctx context.Context, b []byte) (int, net.Addr, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	runtime, err := t.init(1024)
	if err != nil {
		return 0, nil, err
	}
	n, addr, err := runtime.ReadNonQUICPacket(ctx, b)
	if errors.Is(err, transport.ErrEndpointClosed) {
		err = ErrTransportClosed
	}
	return n, addr, err
}
