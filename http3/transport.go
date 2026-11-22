package http3

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/http3conn"
)

var ErrTransportClosed = errors.New("http3: transport closed")
var ErrConnectionLimit = errors.New("http3: transport connection limit reached")

// Transport is a concurrent HTTP/3 RoundTripper pooling one accepting connection
// per HTTPS origin. Configure it before use and do not copy it after use. It does
// not coalesce origins or fall back to TCP.
// Response bodies must be closed, even after EOF, to release pool reservations.
type Transport struct {
	// EnableDatagrams enables HTTP DATAGRAM support. An explicit QUICConfig
	// must also enable datagrams; a nil QUICConfig is configured automatically.
	EnableDatagrams bool
	// MaxResponseHeaderBytes limits encoded and decoded response field sections.
	// Positive values override ClientConfig.MaxHeaderBytes. Otherwise an explicit
	// ClientConfig limit is retained, or the default is 10 MiB.
	MaxResponseHeaderBytes int

	// AdditionalSettings advertises up to 59 application-defined HTTP/3 settings.
	// Core/QPACK, HTTP/2-reserved, Extended CONNECT and DATAGRAM IDs are managed
	// separately and cannot be supplied here. Configure before use.
	// Entries merge with ClientConfig.AdditionalSettings; duplicate IDs are errors.
	AdditionalSettings map[uint64]uint64

	// Enable0RTT uses DialEarly. Only explicit early GET/HEAD methods send before TLS completion.
	Enable0RTT         bool
	DisableRetries     bool // disables the single retry for protocol rejection or an eligible 425
	DisableCompression bool // disables automatic gzip negotiation and decoding
	TLSClientConfig    *tls.Config
	QUICConfig         *quic.Config
	// Dial optionally establishes the QUIC connection for an origin. It receives
	// host:port, cloned TLS settings with h3 ALPN and a default ServerName, and a
	// cloned QUIC configuration. It must honor ctx. The transport owns returned
	// connections and initializes HTTP/3 on them. An early connection is supported;
	// early HTTP requests additionally require Enable0RTT and remembered SETTINGS.
	Dial            func(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error)
	ClientConfig    *ClientConfig
	MaxConnections  int           // zero selects 256, including dialing and draining connections
	DialTimeout     time.Duration // zero selects 10 seconds; negative values are invalid
	IdleConnTimeout time.Duration // zero selects 90 seconds; negative disables idle expiry

	mu     sync.Mutex
	closed bool
	pool   map[string]*pooledClient
	all    map[*pooledClient]struct{}
}

type pooledClient struct {
	origin      string
	ctx         context.Context
	cancel      context.CancelCauseFunc
	ready, done chan struct{}
	confirmed   chan struct{} // closes after handshake/session recovery; client then stops changing
	idleChanged chan struct{}
	client      *ClientConn // protected by Transport.mu after ready
	err         error       // protected by Transport.mu after ready
	refs        int         // protected by Transport.mu, includes dial waiters and response bodies
	idleSince   time.Time   // protected by Transport.mu
	retired     bool        // protected by Transport.mu
}

var _ http.RoundTripper = (*Transport)(nil)

// RoundTrip retries at most once for proven protocol rejection or an eligible
// early-request 425 response. Bodies must be recreatable and have no upload trailers.
func (t *Transport) RoundTrip(request *http.Request) (*http.Response, error) {
	return t.RoundTripOpt(request, RoundTripOpt{})
}

// RoundTripOpt applies options to the initial attempt and any retry. Cached-only
// requests may join a dial already started by another request, but never start
// a new connection, including when a cached connection becomes unusable.
func (t *Transport) RoundTripOpt(request *http.Request, opt RoundTripOpt) (response *http.Response, err error) {
	defer func() { err = httpError(err) }()
	var rejected bool
	response, err = t.roundTrip(request, &rejected, opt)
	tooEarly := err == nil && t.Enable0RTT && retryTooEarly(request, response)
	if t.DisableRetries || (!tooEarly && (err == nil || !rejected)) {
		return response, err
	}
	if tooEarly {
		// Join the first exchange (including its uploader) and release its pool
		// reservation before recreating a body or reserving another attempt.
		response.Body.Close()
	}
	if request.Context().Err() != nil {
		return nil, context.Cause(request.Context())
	}
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return nil, ErrTransportClosed
	}
	retry, replayErr := replayRequest(request)
	if replayErr != nil {
		return nil, replayErr
	}
	if retry == nil {
		if tooEarly {
			return nil, fmt.Errorf("http3: early request is no longer replayable")
		}
		return nil, err
	}
	if tooEarly {
		if retry.Method == MethodGet0RTT {
			retry.Method = http.MethodGet
		} else {
			retry.Method = http.MethodHead
		}
	}
	response, err = t.roundTrip(retry, nil, opt)
	if response != nil {
		response.Request = request
	}
	return response, err
}

func (t *Transport) roundTrip(request *http.Request, rejected *bool, opt RoundTripOpt) (*http.Response, error) {
	if request == nil {
		return nil, fmt.Errorf("http3: nil client request")
	}
	ownsBody := true
	defer func() {
		if ownsBody && request.Body != nil {
			request.Body.Close()
		}
	}()
	if _, err := http3conn.HTTPRequestHeaders(request); err != nil {
		return nil, err
	}
	key, err := originKey(request.URL)
	if err != nil {
		return nil, err
	}
	if _, err := t.clientOptions(); err != nil {
		return nil, err
	}
	if t.MaxConnections < 0 || t.DialTimeout < 0 {
		return nil, fmt.Errorf("http3: invalid transport configuration")
	}
	if request.Context().Err() != nil {
		return nil, context.Cause(request.Context())
	}
	entry, err := t.acquireWithOptions(request.Context(), key, opt)
	if err != nil {
		return nil, err
	}
	transferred := false
	defer func() {
		if !transferred {
			t.release(entry)
		}
	}()
	select {
	case <-request.Context().Done():
		return nil, context.Cause(request.Context())
	case <-entry.ready:
	}
	if request.Context().Err() != nil {
		return nil, context.Cause(request.Context())
	}
	t.mu.Lock()
	client, entryErr := entry.client, entry.err
	t.mu.Unlock()
	if entryErr != nil {
		return nil, entryErr
	}
	original := request
	automaticGzip := !t.DisableCompression && requestsGzip(request)
	if automaticGzip {
		copy := new(http.Request)
		*copy = *request
		copy.Header = request.Header.Clone()
		if copy.Header == nil {
			copy.Header = make(http.Header)
		}
		copy.Header.Set("Accept-Encoding", "gzip")
		request = copy
	}
	ownsBody = false
	response, err := client.roundTrip(request, rejected)
	if err != nil {
		// Trust the session outcome, not an error supplied by a user callback.
		if t.Enable0RTT && errors.Is(err, quic.Err0RTTRejected) {
			select {
			case <-entry.confirmed:
			case <-request.Context().Done():
				return nil, context.Cause(request.Context())
			case <-entry.ctx.Done():
				return nil, context.Cause(entry.ctx)
			}
			if rejected != nil && client.rejectedEarlySession() {
				*rejected = true
			}
		}
		return nil, err
	}
	response.Body = &reservedResponseBody{settingsView: settingsFrom(response.Body), ReadCloser: response.Body, release: func() { t.release(entry) }}
	response.Request = original
	if automaticGzip {
		decodeGzipResponse(response)
	}
	transferred = true
	return response, nil
}

func (t *Transport) acquire(ctx context.Context, key string) (*pooledClient, error) {
	return t.acquireWithOptions(ctx, key, RoundTripOpt{})
}

func (t *Transport) acquireWithOptions(ctx context.Context, key string, opt RoundTripOpt) (*pooledClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		if t.closed {
			return nil, ErrTransportClosed
		}
		if entry := t.pool[key]; entry != nil {
			unusable := false
			select {
			case <-entry.ready:
				select {
				case <-entry.confirmed:
					if entry.client != nil {
						_, draining := entry.client.session.Goaway()
						unusable = draining || entry.client.ctx.Err() != nil
					}
				default: // Keep the slot while its worker recovers early rejection.
				}
			default:
			}
			if !unusable {
				entry.refs++
				t.idleChangedLocked(entry)
				return entry, nil
			}
			t.retireLocked(entry)
		}
		if opt.OnlyCachedConn {
			return nil, ErrNoCachedConn
		}
		limit := t.MaxConnections
		if limit == 0 {
			limit = 256
		}
		if len(t.all) >= limit {
			var oldest *pooledClient
			for candidate := range t.all {
				if candidate.refs == 0 && (oldest == nil || candidate.idleSince.Before(oldest.idleSince)) {
					oldest = candidate
				}
			}
			if oldest == nil {
				return nil, ErrConnectionLimit
			}
			t.retireLocked(oldest)
			// Keep the closing connection charged until its worker has joined.
			// No pool lock is held while waiting on network cleanup.
			t.mu.Unlock()
			select {
			case <-oldest.done:
			case <-ctx.Done():
			}
			t.mu.Lock()
			continue
		}
		if t.pool == nil {
			t.pool = make(map[string]*pooledClient)
			t.all = make(map[*pooledClient]struct{})
		}
		lifetime, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
		entry := &pooledClient{origin: key, ctx: lifetime, cancel: cancel, refs: 1, ready: make(chan struct{}), confirmed: make(chan struct{}), done: make(chan struct{}), idleChanged: make(chan struct{}, 1)}
		t.pool[key] = entry
		t.all[entry] = struct{}{}
		go t.run(entry)
		return entry, nil
	}
}

// retireLocked prevents new reservations but keeps existing responses alive.
func (t *Transport) retireLocked(entry *pooledClient) {
	entry.retired = true
	if t.pool[entry.origin] == entry {
		delete(t.pool, entry.origin)
	}
	if entry.refs == 0 {
		entry.cancel(ErrClientClosed)
	}
}

func (t *Transport) release(entry *pooledClient) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry.refs--
	if entry.refs != 0 {
		return
	}
	entry.idleSince = time.Now()
	t.idleChangedLocked(entry)
	select {
	case <-entry.ready:
		if entry.retired {
			entry.cancel(ErrClientClosed)
		}
	default:
		// No request remains interested in this handshake.
		t.retireLocked(entry)
	}
}

// A buffered notification lets the connection worker own and stop its timer.
func (t *Transport) idleChangedLocked(entry *pooledClient) {
	select {
	case entry.idleChanged <- struct{}{}:
	default:
	}
}

func (t *Transport) run(entry *pooledClient) {
	var client *ClientConn
	defer func() {
		entry.cancel(ErrClientClosed)
		if client != nil {
			client.Close()
		}
		t.mu.Lock()
		t.retireLocked(entry)
		delete(t.all, entry)
		close(entry.done)
		t.mu.Unlock()
	}()
	timeout := t.DialTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	dialCtx, cancel := context.WithTimeout(entry.ctx, timeout)
	cfg, err := t.clientOptions()
	if err == nil {
		client, err = dialHTTPWith(dialCtx, "https://"+entry.origin, t.TLSClientConfig, t.QUICConfig, &cfg, t.Enable0RTT, t.Dial)
		err = httpError(err)
	}
	cancel()
	t.mu.Lock()
	entry.client, entry.err = client, err
	if err != nil {
		t.retireLocked(entry)
	}
	close(entry.ready)
	t.mu.Unlock()
	if err != nil {
		close(entry.confirmed)
		return
	}
	if t.Enable0RTT {
		confirmation, stop := context.WithTimeout(entry.ctx, timeout)
		fresh, recoveryErr := client.NextConnection(confirmation)
		stop()
		t.mu.Lock()
		if recoveryErr == nil {
			client, entry.client = fresh, fresh
		} else {
			entry.err = recoveryErr
			t.retireLocked(entry)
		}
		close(entry.confirmed)
		t.mu.Unlock()
		if recoveryErr != nil {
			return
		}
	} else {
		close(entry.confirmed)
	}
	idleTimeout := t.IdleConnTimeout
	if idleTimeout == 0 {
		idleTimeout = 90 * time.Second
	}
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	goaway := client.session.GoawayReceived()
	for {
		var expired <-chan time.Time
		t.mu.Lock()
		if idleTimeout > 0 && entry.refs == 0 && !entry.retired {
			timer.Reset(time.Until(entry.idleSince.Add(idleTimeout)))
			expired = timer.C
		}
		t.mu.Unlock()
		select {
		case <-entry.ctx.Done():
			return
		case <-client.ctx.Done():
			// Closing the client cancels every request lifetime. Responses the
			// session kept readable (a graceful peer close after GOAWAY) stay
			// alive until their reservations are released.
			t.mu.Lock()
			t.retireLocked(entry)
			t.mu.Unlock()
			<-entry.ctx.Done()
			return
		case <-goaway:
			t.mu.Lock()
			t.retireLocked(entry)
			t.mu.Unlock()
			goaway = nil
		case <-entry.idleChanged:
		case <-expired:
			t.mu.Lock()
			// A reservation or a later idle period can race the timer notification.
			if entry.refs == 0 && !entry.retired && time.Since(entry.idleSince) >= idleTimeout {
				t.retireLocked(entry)
			}
			t.mu.Unlock()
		}
		timer.Stop()
	}
}

// CloseIdleConnections closes currently unused connections. Active requests and
// unclosed response bodies retain their connections. Future requests may dial.
func (t *Transport) CloseIdleConnections() {
	t.mu.Lock()
	var idle []*pooledClient
	for entry := range t.all {
		if entry.refs == 0 {
			t.retireLocked(entry)
			idle = append(idle, entry)
		}
	}
	t.mu.Unlock()
	for _, entry := range idle {
		<-entry.done
	}
}

// Close permanently stops admission, interrupts handshakes and active requests,
// and joins connection workers. Callers still own returned response bodies.
func (t *Transport) Close() error {
	t.mu.Lock()
	t.closed = true
	entries := make([]*pooledClient, 0, len(t.all))
	for entry := range t.all {
		t.retireLocked(entry)
		entry.cancel(ErrTransportClosed)
		entries = append(entries, entry)
	}
	t.mu.Unlock()
	for _, entry := range entries {
		<-entry.done
	}
	return nil
}
