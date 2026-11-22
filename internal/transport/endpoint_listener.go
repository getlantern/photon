package transport

import (
	"errors"
	"net/netip"
	"time"

	"github.com/getlantern/photon/internal/token"
)

// CloseListener stops admission and acceptance without closing the socket owner
// or accepted/outgoing connections. Incoming connections not yet returned by
// Accept are refused, retaining their routes until close/drain disposal. Calls
// are serialized by the endpoint owner; repeated calls are harmless.
func (e *ServerEndpoint) CloseListener(now time.Time) {
	if e.closed || e.listenerClosed {
		return
	}
	e.listenerClosed = true
	// syncAt can remove failed connections, so keep iteration independent from
	// the endpoint's compacted live slice.
	for _, c := range append([]*Conn(nil), e.conns...) {
		if !c.config.Server || e.accepted[c] {
			continue
		}
		e.removeReady(c)
		if c.Closing() || c.Draining() {
			_ = e.syncAt(c, now)
			continue
		}
		if err := c.CloseTransportError(0x2, 0, "listener closed", now); err != nil {
			e.Remove(c)
			continue
		}
		_ = e.syncAt(c, now)
	}
}

// ListenerGeneration identifies the current listener. Zero means this endpoint
// has never listened. Read it on the endpoint owner when constructing a handle.
func (e *ServerEndpoint) ListenerGeneration() uint64 { return e.listenerGeneration }

// StartListener installs a new admission policy after the previous listener has
// closed, or on a client-only endpoint. Existing connection routes, settings and
// runtime capabilities survive. Capacity belongs to the endpoint, not a listener.
// The returned generation must accompany subsequent accept and close operations.
func (e *ServerEndpoint) StartListener(config Config, tokens *token.RetryTokens) (uint64, error) {
	return e.StartListenerWithCapacity(config, tokens, e.router.capacity)
}

// StartListenerWithCapacity also replaces the endpoint admission capacity. Live
// connections above a reduced limit survive; admission resumes below the limit.
func (e *ServerEndpoint) StartListenerWithCapacity(config Config, tokens *token.RetryTokens, capacity int) (uint64, error) {
	if e.closed {
		return 0, ErrEndpointClosed
	}
	if !e.clientOnly && !e.listenerClosed {
		return 0, errors.New("quic: listener already active")
	}
	if e.closeWhenIdle {
		return 0, errors.New("quic: dedicated endpoint cannot start listener")
	}
	if e.listenerGeneration == ^uint64(0) {
		return 0, errors.New("quic: listener generation exhausted")
	}
	if e.router.cidLength == 0 && e.GenerateConnectionID == nil {
		return 0, errors.New("quic: listener requires nonzero routing CIDs")
	}
	if e.GenerateConnectionID != nil {
		config.GenerateConnectionID = e.GenerateConnectionID
	}
	config.AdmissionCIDLength = e.router.cidLength
	candidate, err := NewServerEndpoint(config, tokens, capacity)
	if err != nil {
		return 0, err
	}
	config = candidate.config
	if (config.EnableMultipath || config.multipathCapacity > 0) && e.networks == nil {
		return 0, errors.New("quic: multipath listener requires address-aware endpoint")
	}
	for _, address := range config.PreferredAddresses {
		if !e.locals[canonicalPeer(address)] {
			return 0, errors.New("quic: preferred address has no registered socket")
		}
	}
	config.preferredSocketsReady = len(config.PreferredAddresses) > 0
	config.allowActiveMigration = config.EnableMigration && e.networks != nil
	config.enableECN = e.config.enableECN
	config.enablePathMTUDiscovery = e.config.enablePathMTUDiscovery
	e.config, e.tokens = config, tokens
	e.router.capacity = capacity
	e.clientOnly, e.listenerClosed = false, false
	e.listenerGeneration++
	return e.listenerGeneration, nil
}

// AcceptListener prevents an old listener handle from consuming a replacement
// listener's queue, even if shutdown and restart occur before a waiter wakes.
func (e *ServerEndpoint) AcceptListener(generation uint64) (*Conn, netip.AddrPort, error) {
	if e.closed {
		return nil, netip.AddrPort{}, ErrEndpointClosed
	}
	if generation == 0 || generation != e.listenerGeneration || e.listenerClosed {
		return nil, netip.AddrPort{}, ErrListenerClosed
	}
	return e.Accept()
}

// CloseListenerGeneration is idempotent, including after a replacement starts.
func (e *ServerEndpoint) CloseListenerGeneration(generation uint64, now time.Time) {
	if generation != 0 && generation == e.listenerGeneration {
		e.CloseListener(now)
	}
}
