package handshake

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

const CryptoWindow = 64 * 1024

// Flight is contiguous TLS output for one encryption level. The transport must
// fragment it into CRYPTO frames and retain unacknowledged bytes for recovery.
type Flight struct {
	Level  tls.QUICEncryptionLevel
	Offset uint64
	Data   []byte
}

// TLS owns a Go QUICConn and CRYPTO offsets. A single connection goroutine must
// serialize calls. It validates parameter encoding, ranges, and sender role.
// The connection must additionally validate observed connection IDs using
// wire.TransportParameters.ValidateConnectionIDs. This adapter does not provide
// a transport, discard keys, or confirm the QUIC handshake.
// Session tickets support 1-RTT resumption. Early data is opt-in for adapters
// that implement the early packet and stream lifecycle.
type TLS struct {
	allowEarly                                 bool
	earlyOffered, earlyAccepted, earlyRejected bool
	selectVersion                              VersionSelector
	localVersions                              *wire.VersionInformation
	pendingParams                              []byte
	versionSelected                            bool
	conn                                       *tls.QUICConn
	version                                    wire.Version
	originalVersion                            wire.Version
	server                                     bool
	localTransport                             wire.EarlyTransport
	resumedTransport                           wire.EarlyTransport
	haveResumedTransport                       bool
	read, write                                map[tls.QUICEncryptionLevel]*packet.Keys
	recv                                       map[tls.QUICEncryptionLevel]*reassembler
	sent                                       map[tls.QUICEncryptionLevel]uint64
	out                                        []Flight
	peerParams                                 []byte
	peerTransport                              wire.EarlyTransport
	gotParams                                  bool
	complete                                   bool
	started, closed                            bool
	fatal                                      error
}

func New(server bool, v wire.Version, config *tls.Config, localParams []byte) (*TLS, error) {
	return newTLS(server, v, config, localParams, nil)
}

func newTLS(server bool, v wire.Version, config *tls.Config, localParams []byte, selector VersionSelector) (*TLS, error) {
	return newTLSWithEarlyData(server, v, config, localParams, selector, false)
}

// NewWithEarlyData enables TLS early-data capability. The caller must implement
// early packet processing, rejection/reset semantics and application replay
// policy. New retains the established-connection-only behavior.
func NewWithEarlyData(server bool, v wire.Version, config *tls.Config, localParams []byte) (*TLS, error) {
	return newTLSWithEarlyData(server, v, config, localParams, nil, true)
}

func newTLSWithEarlyData(server bool, v wire.Version, config *tls.Config, localParams []byte, selector VersionSelector, allowEarly bool) (*TLS, error) {
	if !v.Supported() {
		return nil, wire.ErrVersion
	}
	if config == nil {
		return nil, errors.New("quic: TLS configuration is required")
	}
	if len(config.NextProtos) == 0 {
		return nil, errors.New("quic: ALPN is required")
	}
	local, err := wire.ParseTransportParameters(localParams, server)
	if err != nil {
		return nil, fmt.Errorf("local parameters: %w", err)
	}
	c := config.Clone()
	if c.MinVersion != 0 && c.MinVersion < tls.VersionTLS13 {
		return nil, errors.New("quic: TLS 1.3 is required")
	}
	if c.MaxVersion != 0 && c.MaxVersion < tls.VersionTLS13 {
		return nil, errors.New("quic: TLS 1.3 is required")
	}
	c.MinVersion = tls.VersionTLS13
	if server {
		bindServerTickets(c, config, v)
	} else if c.ClientSessionCache != nil {
		c.ClientSessionCache = resumeCache{ClientSessionCache: c.ClientSessionCache, version: v, allowEarly: allowEarly}
	}
	qconfig := &tls.QUICConfig{TLSConfig: c, EnableSessionEvents: true}
	var conn *tls.QUICConn
	if server {
		conn = tls.QUICServer(qconfig)
	} else {
		conn = tls.QUICClient(qconfig)
	}
	// A non-nil empty slice marks parameters as supplied in crypto/tls.
	params := make([]byte, len(localParams))
	copy(params, localParams)
	if selector == nil {
		conn.SetTransportParameters(params)
	}
	return &TLS{allowEarly: allowEarly, selectVersion: selector, localVersions: local.VersionInformation, conn: conn, version: v, originalVersion: v, server: server, localTransport: local.EarlyTransport(), read: make(map[tls.QUICEncryptionLevel]*packet.Keys), write: make(map[tls.QUICEncryptionLevel]*packet.Keys), recv: make(map[tls.QUICEncryptionLevel]*reassembler), sent: make(map[tls.QUICEncryptionLevel]uint64)}, nil
}

func (h *TLS) Start(ctx context.Context) error {
	if h.closed || h.started {
		return errors.New("quic: TLS already started or closed")
	}
	h.started = true
	if err := h.conn.Start(context.WithValue(ctx, QUICVersionContextKey, h.version)); err != nil {
		h.fatal = err
		return err
	}
	return h.events()
}

// HandleCrypto accepts authenticated CRYPTO frame data. Separate encryption
// levels have independent offsets and independent bounded reassembly windows.
func (h *TLS) HandleCrypto(level tls.QUICEncryptionLevel, offset uint64, data []byte) error {
	if h.fatal != nil {
		return h.fatal
	}
	if !h.started || h.closed {
		return errors.New("quic: TLS is not running")
	}
	if level != tls.QUICEncryptionLevelInitial && level != tls.QUICEncryptionLevelHandshake && level != tls.QUICEncryptionLevelApplication {
		return errors.New("quic: CRYPTO at invalid encryption level")
	}
	r := h.recv[level]
	if r == nil {
		r = newReassembler(CryptoWindow)
		h.recv[level] = r
	}
	if err := r.push(offset, data); err != nil {
		h.fatal = err
		return err
	}
	if b := r.pop(); len(b) > 0 {
		if err := h.conn.HandleData(level, b); err != nil {
			h.fatal = err
			return err
		}
	}
	return h.events()
}

func (h *TLS) events() error {
	for {
		e := h.conn.NextEvent()
		switch e.Kind {
		case tls.QUICNoEvent:
			return nil
		case tls.QUICSetReadSecret, tls.QUICSetWriteSecret:
			version := h.version
			// RFC 9369 section 4.1 retains original-version protection for 0-RTT.
			if e.Level == tls.QUICEncryptionLevelEarly {
				version = h.originalVersion
			}
			k, err := packet.TrafficKeys(version, e.Suite, e.Data)
			if err != nil {
				h.fatal = err
				return err
			}
			if e.Level == tls.QUICEncryptionLevelEarly {
				if !h.allowEarly {
					h.fatal = errors.New("quic: unexpected early traffic secret")
					return h.fatal
				}
				if e.Kind == tls.QUICSetReadSecret {
					h.earlyAccepted = true
				} else {
					h.earlyOffered = true
				}
			}
			if e.Kind == tls.QUICSetReadSecret {
				h.read[e.Level] = k
			} else {
				h.write[e.Level] = k
			}
		case tls.QUICWriteData:
			offset := h.sent[e.Level]
			if uint64(len(e.Data)) > wire.MaxVarint-offset {
				h.fatal = wire.ErrVarintRange
				return h.fatal
			}
			h.out = append(h.out, Flight{Level: e.Level, Offset: offset, Data: append([]byte(nil), e.Data...)})
			h.sent[e.Level] += uint64(len(e.Data))
		case tls.QUICTransportParameters:
			peer, err := wire.ParseTransportParameters(e.Data, !h.server)
			if err != nil {
				h.fatal = err
				return err
			}
			if h.selectVersion != nil {
				if err := h.selectServerVersion(peer); err != nil {
					h.fatal = err
					return err
				}
			}
			h.peerTransport = peer.EarlyTransport()
			h.peerParams = append([]byte(nil), e.Data...)
			h.gotParams = true
		case tls.QUICTransportParametersRequired:
			if h.selectVersion == nil || !h.versionSelected {
				h.fatal = errors.New("quic: transport parameters requested before version selection")
				return h.fatal
			}
			h.conn.SetTransportParameters(h.pendingParams)
			h.pendingParams = nil
		case tls.QUICHandshakeDone:
			if h.conn.ConnectionState().NegotiatedProtocol == "" {
				h.fatal = errors.New("quic: TLS did not negotiate ALPN")
				return h.fatal
			}
			if !h.server && h.earlyOffered && !h.earlyRejected && (!h.haveResumedTransport || !h.peerTransport.Supports(h.resumedTransport)) {
				h.fatal = wire.ErrEarlyLimits
				return h.fatal
			}
			h.complete = true
			if !h.server {
				h.earlyAccepted = h.earlyOffered && !h.earlyRejected
			}
			if h.server {
				record, err := ticketState(h.version, h.localTransport)
				if err != nil {
					h.fatal = err
					return err
				}
				if err := h.conn.SendSessionTicket(tls.QUICSessionTicketOptions{EarlyData: h.allowEarly, Extra: [][]byte{record}}); err != nil {
					h.fatal = err
					return err
				}
			}
		case tls.QUICResumeSession:
			// Offered tickets belong to the original first flight, even if the
			// server selected a compatible version before this event arrived.
			h.resumedTransport, h.haveResumedTransport = restoredTicketTransport(e.SessionState.Extra, h.originalVersion)
			// Never upgrade a ticket that did not authorize early data. Missing or
			// incompatible remembered limits reject 0-RTT without rejecting PSK.
			enabled := h.allowEarly && h.haveResumedTransport
			if h.server {
				enabled = enabled && h.localTransport.Supports(h.resumedTransport)
			}
			e.SessionState.EarlyData = e.SessionState.EarlyData && enabled
		case tls.QUICStoreSession:
			if !h.gotParams {
				h.fatal = errors.New("quic: session ticket before peer transport parameters")
				return h.fatal
			}
			record, err := ticketState(h.version, h.peerTransport)
			if err != nil {
				h.fatal = err
				return err
			}
			// This client-local Extra is separate from the opaque server ticket.
			// Snapshot the issuing connection, never a previous cached session.
			e.SessionState.Extra = append(e.SessionState.Extra, record)
			e.SessionState.EarlyData = e.SessionState.EarlyData && h.allowEarly
			if err := h.conn.StoreSession(e.SessionState); err != nil {
				h.fatal = err
				return err
			}
		case tls.QUICRejectedEarlyData:
			h.earlyRejected = true
			delete(h.write, tls.QUICEncryptionLevelEarly)
		case tls.QUICErrorEvent:
			h.fatal = e.Err
			return e.Err
		default:
			h.fatal = fmt.Errorf("quic: unexpected TLS event %d", e.Kind)
			return h.fatal
		}
	}
}

// Drain transfers ownership of queued output to the caller.
func (h *TLS) Drain() []Flight                                      { out := h.out; h.out = nil; return out }
func (h *TLS) ReadKeys(level tls.QUICEncryptionLevel) *packet.Keys  { return h.read[level] }
func (h *TLS) WriteKeys(level tls.QUICEncryptionLevel) *packet.Keys { return h.write[level] }
func (h *TLS) Complete() bool                                       { return h.complete }
func (h *TLS) PeerParameters() ([]byte, bool) {
	return append([]byte(nil), h.peerParams...), h.gotParams
}
func (h *TLS) ConnectionState() tls.ConnectionState { return h.conn.ConnectionState() }
func (h *TLS) Close() error {
	if h.closed {
		return nil
	}
	h.closed = true
	return h.conn.Close()
}

// DiscardKeys makes an encryption level unavailable after the engine reaches
// the RFC 9001 key-retirement milestone. The engine must also discard packets,
// ACK state, and recovery state at that level.
func (h *TLS) DiscardKeys(level tls.QUICEncryptionLevel) {
	delete(h.read, level)
	delete(h.write, level)
	delete(h.recv, level)
}

// EarlyDataRejected reports client-side TLS rejection. The transport must reset early stream
// and application state; this flag does not perform that reset.
func (h *TLS) EarlyDataRejected() bool { return h.earlyRejected }

// EarlyDataAccepted reports the server's early-secret installation or the
// client's completed handshake acceptance. It is not handshake confirmation.
func (h *TLS) EarlyDataAccepted() bool { return h.earlyAccepted }
