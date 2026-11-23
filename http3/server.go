// Package http3 provides experimental HTTP/3 endpoints over the independent QUIC
// transport. It includes explicit early-data and resource replay policies;
// full protocol and reference API parity remain unfinished.
package http3

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/http3conn"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

var ErrServerClosed = http.ErrServerClosed

// Server serves HTTP handlers on accepted QUIC connections negotiated with h3.
// Configure fields before serving and do not mutate them while in use. Do not
// copy a Server after use. Handlers must honor request cancellation; Shutdown
// cannot forcibly stop arbitrary application code.
type Server struct {
	// Addr is the UDP listening address for ListenAndServe methods. Empty uses :https.
	Addr string
	// Port overrides the port advertised by SetQUICHeaders, for example behind
	// a UDP port forwarder. Valid advertised ports are 1–65535. Zero uses
	// listening ports, then the port in Addr.
	Port int
	// TLSConfig supplies credentials and callbacks to ListenAndServe.
	TLSConfig *tls.Config
	// QUICConfig is cloned before listener setup. Nil enables QUIC early-data
	// acceptance; HTTP request replay remains governed by AllowEarlyData.
	QUICConfig *quic.Config

	// ConnContext customizes each admitted connection's context before HTTP/3
	// setup. The supplied context contains ServerContextKey, RemoteAddrContextKey
	// and http.LocalAddrContextKey. Return a non-nil context, normally derived
	// from it. The callback must return promptly and must not synchronously call
	// Shutdown, which joins the connection being initialized.
	ConnContext func(ctx context.Context, c *quic.Conn) context.Context

	// AdditionalSettings advertises up to 59 application-defined HTTP/3 settings.
	// Core/QPACK, HTTP/2-reserved, Extended CONNECT and DATAGRAM IDs are managed
	// separately and cannot be supplied here. Configure before use.
	AdditionalSettings map[uint64]uint64

	// AllowEarlyData authorizes replayable processing for a resource. It is
	// called before a handler when TLS is incomplete or Early-Data is present.
	// It must only inspect request metadata, honor cancellation, and not read
	// the body or perform resource actions. Nil waits for TLS for direct early
	// requests and returns 425 for requests marked by an earlier hop.
	AllowEarlyData func(*http.Request) bool

	// CapsuleProtocols selects capsule header/status policy for tokens registered
	// in ExtendedConnectHandlers or DatagramHandlers. Configure before serving.
	// Handlers still own extension handshakes and capsule parsing/error handling.
	CapsuleProtocols map[string]bool

	// DatagramHandlers declares Extended CONNECT protocols with HTTP datagram
	// semantics. Nonempty maps enable HTTP datagrams and Extended CONNECT.
	// Their handlers validate extension handshakes before AcceptTunnel.
	DatagramHandlers   map[string]http.Handler
	EnableDatagrams    bool // also advertise/track datagrams without registered extensions
	MaxDatagramQueue   int  // per request; zero selects 16
	MaxDatagramPayload int  // received payload bytes; zero selects 1200, maximum 65535
	// ExtendedConnectHandlers enables RFC 9220 when nonempty and routes each
	// :protocol to its handler. Unknown protocols receive 501. Configure before
	// serving; keys must be valid tokens and handlers non-nil. Handlers validate
	// their extension handshake before calling AcceptTunnel.
	ExtendedConnectHandlers map[string]http.Handler
	Handler                 http.Handler // nil uses http.DefaultServeMux
	MaxConnections          int          // zero selects 256
	MaxConcurrentRequests   int          // per connection; zero selects 64
	MaxHeaderBytes          int          // per encoded/decoded field section; zero selects 65536
	// PermitsTrailer must explicitly permit outgoing trailer field names, in
	// lowercase. Nil prohibits outgoing trailers. A Trailer declaration alone
	// does not establish that a field's definition permits use in trailers.
	PermitsTrailer func(string) bool
	// ErrorHandler observes request failures and must return on ctx cancellation.
	// It must not synchronously call Shutdown, which would join itself.
	ErrorHandler func(ctx context.Context, streamID uint64, err error)

	mu            sync.Mutex
	stopping      bool
	connections   map[*quic.Conn]*serverConnection
	listeners     map[*serverListener]struct{}
	listenerOrder uint64
	altSvcHeader  string
}
type serverConnection struct {
	conn        *quic.Conn
	cancel      context.CancelCauseFunc
	drain, done chan struct{}
}

// ServeQUICConn takes ownership of an accepted connection after validation and
// admission. It initializes HTTP/3, serves requests and joins protocol workers
// before returning. No other code may accept streams on conn. Validation or
// admission failure leaves ownership with the caller. Peer closure remains a
// typed transport error, including a normal H3_NO_ERROR peer close.
// Connections from an EarlyListener dispatch before TLS completion only when
// AllowEarlyData approves the resource. Otherwise direct requests wait for TLS,
// and forwarded Early-Data requests receive 425. Request.TLS snapshots dispatch.
func (s *Server) ServeQUICConn(parent context.Context, conn *quic.Conn) (err error) {
	requests, headers, connections, err := s.limits()
	if err != nil {
		return err
	}
	if parent == nil || conn == nil {
		return fmt.Errorf("http3: nil serving context or connection")
	}
	if parent.Err() != nil {
		return context.Cause(parent)
	}
	if (s.EnableDatagrams || len(s.DatagramHandlers) != 0) && !conn.DatagramsEnabled() {
		return fmt.Errorf("http3: QUIC datagram reception is disabled")
	}
	if conn.ConnectionState().TLS.NegotiatedProtocol != "h3" {
		return fmt.Errorf("http3: connection did not negotiate h3")
	}
	handler := s.Handler
	if handler == nil {
		handler = http.DefaultServeMux
	}
	ctx, cancel := context.WithCancelCause(parent)
	entry := &serverConnection{conn: conn, cancel: cancel, drain: make(chan struct{}), done: make(chan struct{})}
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		cancel(ErrServerClosed)
		return ErrServerClosed
	}
	if len(s.connections) >= connections {
		s.mu.Unlock()
		cancel(nil)
		return fmt.Errorf("http3: server connection limit reached")
	}
	if _, exists := s.connections[conn]; exists {
		s.mu.Unlock()
		cancel(nil)
		return fmt.Errorf("http3: connection already served")
	}
	if s.connections == nil {
		s.connections = make(map[*quic.Conn]*serverConnection)
	}
	s.connections[conn] = entry
	s.mu.Unlock()
	defer func() { cancel(err); s.mu.Lock(); delete(s.connections, conn); close(entry.done); s.mu.Unlock() }()
	// Startup stream writes must also be interruptible, before Session exists.
	closed := make(chan struct{})
	stopClose := context.AfterFunc(ctx, func() { defer close(closed); conn.CloseWithError(0x100, "HTTP3 server stopped") })
	peerDone := make(chan struct{})
	stopPeer := context.AfterFunc(conn.Context(), func() { defer close(peerDone); cancel(context.Cause(conn.Context())) })
	defer func() {
		if !stopPeer() {
			<-peerDone
		}
		if !stopClose() {
			<-closed
		}
		conn.CloseWithError(0x100, "HTTP3 server finished")
	}()
	ctx, cleanupContext := s.connectionContext(ctx, conn)
	defer cleanupContext()
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	settings := s.settings(headers)
	transport := http3conn.QUICTransport{Conn: conn}
	var datagramConfig *http3conn.DatagramSessionConfig
	if s.EnableDatagrams || len(s.DatagramHandlers) != 0 {
		queue, payload, _ := datagramLimits(s.MaxDatagramQueue, s.MaxDatagramPayload)
		dispatch, err := http3conn.NewDatagramDispatch(requests, queue, payload)
		if err != nil {
			return err
		}
		datagramConfig = &http3conn.DatagramSessionConfig{Dispatch: dispatch, Source: transport, Sender: transport, AbortRequest: dispatch.AbortRequest}
	}
	local, err := http3conn.OpenLocal(ctx, transport, settings)
	if err != nil {
		return err
	}
	priorities, err := http3conn.NewRequestPriorities(requests)
	if err != nil {
		return err
	}
	session, err := http3conn.StartSession(ctx, transport, local, http3conn.SessionConfig{
		Datagrams: datagramConfig, LocalSettings: settings,
		Decoder:          qpack.DecoderLimits{MaxTableCapacity: 4096, MaxBlockedStreams: 8, MaxBufferedBytes: headers * 8, MaxEncodedSection: headers, MaxDecodedSection: uint64(headers), MaxFields: 1024, MaxInstructionLiteralBytes: headers},
		Encoder:          http3conn.EncoderLimits{Capacity: 4096, MaxBlocked: 8, MaxSections: 1024, MaxReferences: 4096, MaxEncodedSection: headers, MaxFields: 1024, MaxInstruction: headers, MaxFieldSection: uint64(headers)},
		MaxActiveStreams: 16, MaxSettingsBytes: 65536, MaxSettingsEntries: 64, MaxEncoderInstruction: headers,
		// This server never allocates a push ID, so every CANCEL_PUSH refers
		// to an unpromised push. Other events are validated by ControlReader.
		Control: func(_ context.Context, event h3.ControlEvent) error {
			if event.Type == h3.PriorityUpdatePush {
				return &h3.Error{Code: h3.IDError, Reason: "PRIORITY_UPDATE for unpromised push"}
			}
			if event.Type == h3.CancelPush {
				return &h3.Error{Code: h3.IDError, Reason: "CANCEL_PUSH for unpromised push"}
			}
			return priorities.Update(event)
		},
	})
	if err != nil {
		return err
	}
	defer func() { session.Stop(err) }()
	return session.ServeRequests(ctx, transport, http3conn.ServeRequestsConfig{
		MaxActive: requests, Drain: entry.drain, Report: s.ErrorHandler,
		Request: http3conn.ServeRequestConfig{Priorities: priorities, HandlerError: httpError, HandlerPolicy: func(next http.Handler) http.Handler {
			withSettings := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				next.ServeHTTP(&settingsResponseWriter{HTTPResponseWriter: w.(*http3conn.HTTPResponseWriter), settingsView: settingsFrom(sessionSettings{session})}, r)
			})
			return replayHandler(withSettings, s.AllowEarlyData, conn.HandshakeComplete(), func() tls.ConnectionState { return conn.ConnectionState().TLS })
		}, CapsuleProtocols: s.CapsuleProtocols, DatagramHandlers: s.DatagramHandlers, ExtendedConnectHandlers: s.ExtendedConnectHandlers, Handler: handler, MaxEncodedHeaders: uint64(headers), PermitsTrailer: s.PermitsTrailer, RemoteAddr: conn.RemoteAddr().String(), TLSState: func() tls.ConnectionState { return conn.ConnectionState().TLS }},
	})
}

// Shutdown permanently stops admission and initiates GOAWAY on owned connections.
// It waits for handlers, delivery and protocol cleanup, bounded by ctx. Expiry
// does not abort active connections; use Close for abortive shutdown. Listeners
// passed to Serve stop accepting immediately and close after their workers join.
// Listeners passed to ServeListener or used independently with ServeQUICConn
// remain caller-owned and are not closed.
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("http3: nil shutdown context")
	}
	entries, listeners := s.stopAdmission()
	for _, entry := range entries {
		select {
		case <-entry.done:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	for _, listener := range listeners {
		select {
		case <-listener.done:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return nil
}
func (s *Server) stopAdmission() ([]*serverConnection, []*serverListener) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := make([]*serverConnection, 0, len(s.connections))
	for _, entry := range s.connections {
		entries = append(entries, entry)
		if !s.stopping {
			close(entry.drain)
		}
	}
	listeners := make([]*serverListener, 0, len(s.listeners))
	for listener := range s.listeners {
		listeners = append(listeners, listener)
		listener.stop()
	}
	s.stopping = true
	return entries, listeners
}

// Close aborts all owned connections and cancels handlers without waiting for
// application code. ServeQUICConn returns only after its workers have joined.
func (s *Server) Close() error {
	entries, listeners := s.stopAdmission()
	for _, listener := range listeners {
		if listener.owned {
			listener.listener.Close()
		}
	}
	for _, entry := range entries {
		entry.cancel(ErrServerClosed)
		entry.conn.CloseWithError(0x100, "HTTP3 server closed")
	}
	return nil
}

func (s *Server) limits() (requests, headers, connections int, err error) {
	if _, err := copyAdditionalSettings(s.AdditionalSettings); err != nil {
		return 0, 0, 0, err
	}
	if _, _, err := datagramLimits(s.MaxDatagramQueue, s.MaxDatagramPayload); err != nil {
		return 0, 0, 0, err
	}
	for protocol, handler := range s.DatagramHandlers {
		if !http3conn.ValidExtendedConnectProtocol(protocol) || handler == nil || s.ExtendedConnectHandlers[protocol] != nil {
			return 0, 0, 0, fmt.Errorf("http3: invalid or duplicate datagram protocol handler")
		}
	}
	for protocol, handler := range s.ExtendedConnectHandlers {
		if !http3conn.ValidExtendedConnectProtocol(protocol) || handler == nil {
			return 0, 0, 0, fmt.Errorf("http3: invalid Extended CONNECT handler")
		}
	}
	for protocol, enabled := range s.CapsuleProtocols {
		if enabled && (!http3conn.ValidExtendedConnectProtocol(protocol) || s.ExtendedConnectHandlers[protocol] == nil && s.DatagramHandlers[protocol] == nil) {
			return 0, 0, 0, fmt.Errorf("http3: capsule protocol requires a registered Extended CONNECT handler")
		}
	}
	requests, headers, connections = s.MaxConcurrentRequests, s.MaxHeaderBytes, s.MaxConnections
	if requests == 0 {
		requests = 64
	}
	if headers == 0 {
		headers = 65536
	}
	if connections == 0 {
		connections = 256
	}
	if requests < 1 || headers < 1 || headers > int(^uint(0)>>1)/8 || connections < 1 {
		return 0, 0, 0, fmt.Errorf("http3: invalid server configuration")
	}

	return requests, headers, connections, nil
}
