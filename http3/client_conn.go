package http3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/http3conn"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

var ErrClientClosed = errors.New("http3: client connection closed")
var ErrPeerDraining = http3conn.ErrPeerDraining
var ErrRequestRejected = http3conn.ErrRequestRejected

// ClientConfig supplies immutable policy for one established client connection.
type ClientConfig struct {
	// AdditionalSettings advertises up to 59 application-defined HTTP/3 settings.
	// Core/QPACK, HTTP/2-reserved, Extended CONNECT and DATAGRAM IDs are managed
	// separately and cannot be supplied here. Configure before use.
	AdditionalSettings map[uint64]uint64

	peerSettings        func(http3conn.SettingsSnapshot)
	responseHeaderBytes int // transport receive-only override; zero uses MaxHeaderBytes
	// CapsuleProtocols selects capsule header/status validation for Extended
	// CONNECT tokens. The map is copied at client creation. It does not enable
	// QUIC datagrams or infer capsule use from the Capsule-Protocol header.
	CapsuleProtocols map[string]bool

	EnableDatagrams       bool // requires QUIC receive support; Dial enables it automatically
	MaxDatagramQueue      int  // per request; zero selects 16, newest payloads drop on overflow
	MaxDatagramPayload    int  // per received payload; zero selects 1200, maximum 65535
	MaxConcurrentRequests int  // zero selects 64, including waiting stream allocation
	MaxHeaderBytes        int  // zero selects 65536
	MaxInformational      int  // zero selects 16
	PermitsTrailer        func(string) bool
	Information           func(status int, header http.Header) error // may run concurrently across requests
}

// ClientConn implements RoundTripper for one explicitly bound HTTPS origin.
// It does not dial, pool, retry, decompress or coalesce origins. Close cancels
// active work; callers must still close every returned response Body. Construct
// it exactly once on a connection before opening any HTTP/3 streams.
type ClientConn struct {
	earlySession       bool
	recoveryLock       chan struct{}
	replacement        *ClientConn
	recoveryErr        error
	ownership          *clientOwnership
	requestsCloseCause error
	conn               *quic.Conn
	session            *http3conn.Session
	ctx                context.Context
	lifetime           context.Context
	cancel             context.CancelCauseFunc
	origin             string
	config             ClientConfig
	slots              chan struct{}
	closeOnce          sync.Once
	monitorDone        chan struct{}
	requestsMu         sync.Mutex
	requests           map[*clientRequestLifetime]struct{}
	requestsClosed     bool

	// The Transport constructors set up the session in the background. ready is
	// nil for a connection that was set up before it was returned.
	ready     chan struct{} // closed once session is set or setupErr says why not
	setupDone chan struct{}
	setupErr  error
	settings  chan struct{}            // ReceivedSettings before the session exists
	uni       chan *quic.ReceiveStream // raw: streams handed over by the application
	autoGzip  bool
	logger    *slog.Logger
	streams   sync.WaitGroup // raw request streams still releasing their state
}

var _ http.RoundTripper = (*ClientConn)(nil)

// LocalAddr returns the local address of the underlying QUIC connection.
func (c *ClientConn) LocalAddr() net.Addr { return c.conn.LocalAddr() }

// RemoteAddr returns the remote address of the underlying QUIC connection.
func (c *ClientConn) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }

// NewClientConn initializes HTTP/3 on conn, taking ownership after validation.
// The caller must have authenticated conn for origin (https://host[:port]) using
// its chosen TLS policy. ALPN must be h3. ctx controls the connection lifetime,
// not just initialization. Origin is pinned; cross-origin requests are rejected.
func NewClientConn(ctx context.Context, conn *quic.Conn, origin string, config *ClientConfig) (*ClientConn, error) {
	return newClientConn(ctx, conn, origin, config, nil)
}

// remembered is supplied only by DialEarly's per-connection TLS cache.
func newClientConn(ctx context.Context, conn *quic.Conn, origin string, config *ClientConfig, remembered *[]h3.Setting) (client *ClientConn, err error) {
	defer func() { err = httpError(err) }()
	cfg, err := clientOptions(config)
	if err != nil {
		return nil, err
	}
	if ctx == nil || conn == nil {
		return nil, fmt.Errorf("http3: nil client context or connection")
	}
	key, err := clientOrigin(origin)
	if err != nil {
		return nil, err
	}
	if cfg.EnableDatagrams && !conn.DatagramsEnabled() {
		return nil, fmt.Errorf("http3: QUIC datagram reception is disabled")
	}
	if protocol := conn.ConnectionState().TLS.NegotiatedProtocol; protocol != "h3" && !(remembered != nil && protocol == "") {
		return nil, fmt.Errorf("http3: connection did not negotiate h3")
	}
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	client = newClientShell(ctx, conn, key, cfg, remembered != nil, false)
	cancel, ownership, monitorDone := client.cancel, client.ownership, client.monitorDone
	defer func() {
		if err != nil {
			if remembered != nil && errors.Is(err, quic.Err0RTTRejected) {
				_ = ownership.release()
			}
			cancel(err)
			ownership.close("HTTP3 client initialization failed")
			<-monitorDone
		}
	}()
	if err = client.start(remembered); err != nil {
		return nil, err
	}
	return client, nil
}

// newClientShell builds a client without a session and starts its monitor. A
// raw client leaves every accept loop to the application.
func newClientShell(lifetime context.Context, conn *quic.Conn, origin string, cfg ClientConfig, early, raw bool) *ClientConn {
	ctx, cancel := context.WithCancelCause(lifetime)
	client := &ClientConn{recoveryLock: make(chan struct{}, 1), earlySession: early, conn: conn, ctx: ctx, lifetime: lifetime, cancel: cancel, origin: origin, config: cfg, slots: make(chan struct{}, cfg.MaxConcurrentRequests), monitorDone: make(chan struct{})}
	client.ownership = &clientOwnership{closeConn: func(code uint64, reason string) error {
		return conn.CloseWithError(quic.ApplicationErrorCode(code), reason)
	}}
	if raw {
		client.uni = make(chan *quic.ReceiveStream)
	}
	// Connection loss cancels settings waits and request allocation. Parent
	// cancellation also closes transport, including during critical-stream setup.
	monitorDone := client.monitorDone
	ownership := client.ownership
	go func() {
		defer close(monitorDone)
		// No supported extension lets a server open bidirectional streams
		// (RFC 9114 section 6.1); unread, they would pin connection credit.
		// A raw client's application accepts them and decides instead.
		var accepted chan error
		if !raw {
			accepted = make(chan error, 1)
			go func() { _, err := conn.AcceptStream(ctx); accepted <- err }()
			defer func() { <-accepted }()
		}
		select {
		case <-conn.Context().Done():
			cancel(context.Cause(conn.Context()))
		case <-ctx.Done():
		case err := <-accepted:
			accepted <- err
			if err == nil {
				// Abort first: a failing initialization closes with H3_NO_ERROR
				// as soon as it observes cancellation.
				_ = ownership.abort(h3.StreamCreationError, "server-initiated bidirectional stream")
				cancel(&h3.Error{Code: h3.StreamCreationError, Reason: "server-initiated bidirectional stream"})
				return
			}
			// Connection loss or early-data rejection: the cases below and the
			// session report those.
			select {
			case <-conn.Context().Done():
				cancel(context.Cause(conn.Context()))
			case <-ctx.Done():
			}
		}
		ownership.close("HTTP3 client stopped")
	}()
	return client
}

// start opens the critical streams and the session. remembered is supplied
// only by DialEarly's per-connection TLS cache.
func (c *ClientConn) start(remembered *[]h3.Setting) error {
	cfg, ctx := c.config, c.ctx
	headers := cfg.MaxHeaderBytes
	receiveHeaders := cfg.responseHeaderLimit()
	settings := []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 4096}, {ID: h3.QPACKBlockedStreams, Value: 8}, {ID: h3.MaxFieldSectionSize, Value: uint64(receiveHeaders)}}
	owned := ownedClientTransport{QUICTransport: http3conn.QUICTransport{Conn: c.conn}, ownership: c.ownership}
	var transport clientTransport = owned
	if c.uni != nil {
		transport = handedClientTransport{ownedClientTransport: owned, streams: c.uni}
	}
	var datagramConfig *http3conn.DatagramSessionConfig
	if cfg.EnableDatagrams {
		dispatch, e := http3conn.NewDatagramDispatch(cfg.MaxConcurrentRequests, cfg.MaxDatagramQueue, cfg.MaxDatagramPayload)
		if e != nil {
			return e
		}
		datagramConfig = &http3conn.DatagramSessionConfig{Dispatch: dispatch, Sender: owned, Source: owned, AbortRequest: dispatch.AbortRequest}
		settings = append(settings, h3.Setting{ID: h3.H3Datagram, Value: 1})
	}
	settings = appendAdditionalSettings(settings, cfg.AdditionalSettings)
	local, err := http3conn.OpenLocal(ctx, transport, settings)
	if err != nil {
		return err
	}
	session, err := http3conn.StartSession(ctx, transport, local, http3conn.SessionConfig{
		EarlyPeerSettings: remembered, PeerSettings: cfg.peerSettings, Datagrams: datagramConfig, PeerIsServer: true, MaxRequests: cfg.MaxConcurrentRequests, LocalSettings: settings,
		Decoder:          qpack.DecoderLimits{MaxTableCapacity: 4096, MaxBlockedStreams: 8, MaxBufferedBytes: receiveHeaders * 8, MaxEncodedSection: receiveHeaders, MaxDecodedSection: uint64(receiveHeaders), MaxFields: 1024, MaxInstructionLiteralBytes: receiveHeaders},
		Encoder:          http3conn.EncoderLimits{Capacity: 4096, MaxBlocked: 8, MaxSections: 1024, MaxReferences: 4096, MaxEncodedSection: headers, MaxFields: 1024, MaxInstruction: headers, MaxFieldSection: uint64(headers)},
		MaxActiveStreams: 16, MaxSettingsBytes: 65536, MaxSettingsEntries: 64, MaxEncoderInstruction: headers,
		// No push credit is advertised. PUSH_PROMISE is rejected by response policy;
		// unsolicited push streams are rejected by the critical-stream dispatcher.
		Control: func(context.Context, h3.ControlEvent) error { return nil },
	})
	if err != nil {
		return err
	}
	c.session = session
	return nil
}

// RoundTrip closes request.Body on every outcome. The response Body owns the
// request reservation until Close, even after EOF. Concurrent calls are safe;
// request objects themselves must not be mutated except for EOF trailer values.
func (c *ClientConn) RoundTrip(req *http.Request) (*http.Response, error) {
	if !c.autoGzip || req == nil || !requestsGzip(req) {
		return c.roundTrip(req, nil)
	}
	response, err := c.roundTrip(acceptGzip(req), nil)
	if err != nil {
		return nil, err
	}
	response.Request = req
	decodeGzipResponse(response)
	return response, nil
}

// rejected records protocol evidence, separately from errors a caller-supplied
// body or callback might return. It is only consumed by the pooled transport.
func (c *ClientConn) roundTrip(request *http.Request, rejected *bool) (*http.Response, error) {
	return c.roundTripOwned(request, rejected, false, nil)
}
func (c *ClientConn) roundTripOwned(request *http.Request, rejected *bool, tunnel bool, datagrams **http3conn.DatagramStream) (response *http.Response, err error) {
	defer func() { err = httpError(err) }()
	if request == nil {
		return nil, fmt.Errorf("http3: nil client request")
	}
	ownsBody := true
	defer func() {
		if ownsBody && request.Body != nil {
			request.Body.Close()
		}
	}()
	originalRequest := request
	early := request.Method == MethodGet0RTT || request.Method == MethodHead0RTT
	if early {
		request = request.WithContext(request.Context())
		if request.Method == MethodGet0RTT {
			request.Method = http.MethodGet
		} else {
			request.Method = http.MethodHead
		}
	}
	if _, err := http3conn.HTTPRequestHeaders(request); err != nil {
		return nil, err
	}
	key, err := originKey(request.URL)
	if err != nil {
		return nil, err
	}
	// Only a connection created for an origin is pinned to it.
	if c.origin != "" && key != c.origin {
		return nil, fmt.Errorf("http3: request origin differs from connection origin")
	}
	if err := c.await(request.Context()); err != nil {
		return nil, err
	}
	// Closing the transport and cancellation of an in-flight exchange can
	// race. Preserve the explicit client-close cause at this API boundary,
	// while retaining a request cancellation that has already occurred.
	defer func() {
		if err != nil && errors.Is(context.Cause(c.ctx), ErrClientClosed) {
			if cause := context.Cause(request.Context()); cause != nil {
				err = cause
			} else {
				err = ErrClientClosed
			}
		}
	}()
	if !early {
		select {
		case <-c.conn.HandshakeComplete():
		case <-request.Context().Done():
			return nil, context.Cause(request.Context())
		case <-c.ctx.Done():
			return nil, context.Cause(c.ctx)
		}
	}
	var allocated bool
	var streamID uint64
	defer func() {
		if rejected == nil || err == nil {
			return
		}
		if state, ok := c.session.Goaway(); ok && (!allocated || state.Rejects(streamID)) {
			*rejected = true
		}
	}()
	ctx, cancel := context.WithCancelCause(request.Context())
	interrupted := make(chan struct{})
	go func() {
		defer close(interrupted)
		select {
		case <-c.ctx.Done():
			cancel(context.Cause(c.ctx))
		case <-c.session.GoawayReceived():
			cancel(ErrPeerDraining)
		case <-c.session.Done():
			_, cause := c.session.WaitEncoder(context.Background())
			cancel(cause)
		case <-ctx.Done():
		}
	}()
	defer func() { cancel(context.Canceled); <-interrupted }()
	if c.ctx.Err() != nil {
		return nil, context.Cause(c.ctx)
	}
	select {
	case c.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
	transferred := false
	defer func() {
		if !transferred {
			<-c.slots
		}
	}()
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	if _, draining := c.session.Goaway(); draining {
		return nil, http3conn.ErrPeerDraining
	}
	lifetimeCtx, finishLifetime, lifetimeErr := c.requestLifetime(request.Context())
	if lifetimeErr != nil {
		return nil, lifetimeErr
	}
	lifetimeTransferred := false
	defer func() {
		if !lifetimeTransferred {
			finishLifetime()
		}
	}()
	effective := request.WithContext(lifetimeCtx)
	stream, err := c.conn.OpenStreamSync(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		return nil, err
	}
	allocated, streamID = true, uint64(stream.StreamID())
	var bound *http3conn.DatagramStream
	if c.config.EnableDatagrams {
		bound, err = c.session.BindDatagramStream(stream, datagrams != nil)
		if err != nil {
			stream.CancelRead(0x10c)
			stream.CancelWrite(0x10c)
			return nil, err
		}
		defer func() {
			if !transferred {
				bound.DetachDatagrams()
			}
		}()
	}
	ownsBody = false // session exchange owns Body and resets every failed admission
	var tracked *rejectionTrackingStream
	var exchangeStream http3conn.ClientExchangeStream = stream
	if bound != nil {
		exchangeStream = bound
	}
	if rejected != nil {
		tracked = &rejectionTrackingStream{ClientExchangeStream: exchangeStream}
		exchangeStream = tracked
	}
	response, err = c.session.ExchangeRequest(effective, exchangeStream, http3conn.ClientExchangeConfig{Capsules: c.config.CapsuleProtocols[http3conn.ExtendedConnectProtocol(effective)], RequireHTTPDatagrams: datagrams != nil, AllowConnect: tunnel, RejectedBeforeSend: func() {
		if rejected != nil {
			*rejected = true
		}
	}, MaxEncodedHeaders: uint64(c.config.responseHeaderLimit()), MaxInformational: c.config.MaxInformational, PermitsTrailer: c.config.PermitsTrailer, Information: c.config.Information})
	if err != nil {
		if rejected != nil && (tracked.rejected.Load() || peerRejected(context.Cause(stream.Context()))) {
			*rejected = true
		}
		return nil, err
	}
	state := c.conn.ConnectionState().TLS
	response.TLS = &state
	response.Request = originalRequest
	response.Body = &reservedResponseBody{settingsView: settingsFrom(c), ReadCloser: response.Body, request: request.Context(), lifetime: c.lifetime, release: func() {
		if bound != nil {
			bound.DetachDatagrams()
		}
		finishLifetime()
		<-c.slots
	}}
	if datagrams != nil {
		*datagrams = bound
	}
	transferred = true
	lifetimeTransferred = true
	return response, nil
}
func (c *ClientConn) Close() error {
	c.closeOnce.Do(func() {
		c.stopRequests(ErrClientClosed)
		c.cancel(ErrClientClosed)
		c.ownership.close("HTTP3 client closed")
		if c.ready != nil {
			<-c.ready
		}
		if c.session != nil {
			c.session.Stop(ErrClientClosed)
		}
		if c.ready != nil {
			<-c.setupDone
		}
		<-c.monitorDone
		c.streams.Wait()
	})
	return nil
}

// await waits for background setup. A connection set up before it was returned
// never waits.
func (c *ClientConn) await(ctx context.Context) error {
	if c.ready == nil {
		return nil
	}
	select {
	case <-c.ready:
		return c.setupErr
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

type reservedResponseBody struct {
	settingsView
	io.ReadCloser
	request, lifetime context.Context
	once              sync.Once
	release           func()
	err               error
}

// Observe caller cancellation synchronously even if its interruption callback
// has not run yet. Peer connection cancellation is deliberately not consulted.
func (b *reservedResponseBody) Read(p []byte) (n int, err error) {
	defer func() { err = httpError(err) }()
	if b.request != nil && b.request.Err() != nil {
		return 0, context.Cause(b.request)
	}
	if b.lifetime != nil && b.lifetime.Err() != nil {
		return 0, context.Cause(b.lifetime)
	}
	return b.ReadCloser.Read(p)
}

func (b *reservedResponseBody) Close() error {
	b.once.Do(func() { b.err = b.ReadCloser.Close(); b.release() })
	return httpError(b.err)
}
func (b *reservedResponseBody) CloseWithError(code quic.StreamErrorCode, reason string) error {
	b.once.Do(func() {
		if closer, ok := b.ReadCloser.(interface {
			CloseWithError(quic.StreamErrorCode, string) error
		}); ok {
			b.err = closer.CloseWithError(code, reason)
		} else {
			b.err = b.ReadCloser.Close()
		}
		b.release()
	})
	return httpError(b.err)
}
func originKey(target *url.URL) (string, error) {
	if target == nil || !strings.EqualFold(target.Scheme, "https") || target.Opaque != "" || target.Host == "" {
		return "", fmt.Errorf("http3: HTTPS origin required")
	}
	parsed, err := url.Parse("//" + target.Host)
	if err != nil || parsed.Host != target.Host || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return "", fmt.Errorf("http3: invalid origin authority")
	}
	for i := 0; i < len(target.Host); i++ {
		if target.Host[i] <= 0x20 || target.Host[i] >= 0x7f {
			return "", fmt.Errorf("http3: origin requires ASCII authority")
		}
	}
	if target.Hostname() == "" {
		return "", fmt.Errorf("http3: empty origin host")
	}
	port := target.Port()
	if port == "" {
		port = "443"
	}
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return "", fmt.Errorf("http3: invalid origin port")
	}
	return net.JoinHostPort(strings.ToLower(target.Hostname()), strconv.FormatUint(number, 10)), nil
}

func (cfg ClientConfig) responseHeaderLimit() int {
	if cfg.responseHeaderBytes > 0 {
		return cfg.responseHeaderBytes
	}
	return cfg.MaxHeaderBytes
}

func clientOptions(config *ClientConfig) (ClientConfig, error) {
	cfg := ClientConfig{}
	if config != nil {
		cfg = *config
	}
	var err error
	cfg.AdditionalSettings, err = copyAdditionalSettings(cfg.AdditionalSettings)
	if err != nil {
		return cfg, err
	}
	if cfg.CapsuleProtocols != nil {
		selected := make(map[string]bool, len(cfg.CapsuleProtocols))
		for protocol, enabled := range cfg.CapsuleProtocols {
			if enabled && !http3conn.ValidExtendedConnectProtocol(protocol) {
				return cfg, fmt.Errorf("http3: invalid capsule protocol")
			}
			selected[protocol] = enabled
		}
		cfg.CapsuleProtocols = selected
	}
	queue, payload, err := datagramLimits(cfg.MaxDatagramQueue, cfg.MaxDatagramPayload)
	if err != nil {
		return cfg, err
	}
	cfg.MaxDatagramQueue, cfg.MaxDatagramPayload = queue, payload
	if cfg.MaxConcurrentRequests == 0 {
		cfg.MaxConcurrentRequests = 64
	}
	if cfg.MaxHeaderBytes == 0 {
		cfg.MaxHeaderBytes = 65536
	}
	if cfg.MaxInformational == 0 {
		cfg.MaxInformational = 16
	}
	if cfg.responseHeaderBytes < 0 || cfg.responseHeaderBytes > int(^uint(0)>>1)/8 || cfg.MaxConcurrentRequests < 1 || cfg.MaxHeaderBytes < 1 || cfg.MaxHeaderBytes > int(^uint(0)>>1)/8 || cfg.MaxInformational < 1 {
		return cfg, fmt.Errorf("http3: invalid client configuration")
	}
	return cfg, nil
}
func clientOrigin(origin string) (string, error) {
	target, err := url.Parse(origin)
	if err != nil || target.User != nil || target.RawQuery != "" || target.ForceQuery || target.Fragment != "" || target.Path != "" && target.Path != "/" {
		return "", fmt.Errorf("http3: invalid client origin")
	}
	return originKey(target)
}

// Request lifetime remains independently cancelable after the session records a
// graceful peer-close cause. Context cancellation cannot replace an earlier cause.
type clientRequestLifetime struct{ cancel context.CancelCauseFunc }

func (c *ClientConn) requestLifetime(parent context.Context) (context.Context, func(), error) {
	c.requestsMu.Lock()
	defer c.requestsMu.Unlock()
	if c.requestsClosed {
		return nil, nil, c.requestsCloseCause
	}
	if c.lifetime != nil && c.lifetime.Err() != nil {
		return nil, nil, context.Cause(c.lifetime)
	}
	ctx, cancel := context.WithCancelCause(parent)
	interrupted := make(chan struct{})
	stop := func() bool { return true }
	if c.lifetime != nil {
		stop = context.AfterFunc(c.lifetime, func() { defer close(interrupted); cancel(context.Cause(c.lifetime)) })
	}
	request := &clientRequestLifetime{cancel: cancel}
	if c.requests == nil {
		c.requests = make(map[*clientRequestLifetime]struct{})
	}
	c.requests[request] = struct{}{}
	var once sync.Once
	return ctx, func() {
		once.Do(func() {
			c.requestsMu.Lock()
			delete(c.requests, request)
			c.requestsMu.Unlock()
			if !stop() {
				<-interrupted
			}
			cancel(context.Canceled)
		})
	}, nil
}
