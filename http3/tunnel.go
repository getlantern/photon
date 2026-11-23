package http3

import (
	"context"
	"fmt"
	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/http3conn"
	"io"
	"net/http"
	"sync"
)

// Tunnel carries unframed CONNECT bytes over HTTP/3 DATA frames. One reader and
// one writer can operate concurrently. Close interrupts both directions and
// releases the request reservation; callers must close even after receive EOF.
// CloseWrite ends only sending, allowing the peer's remaining bytes to be read.
// Writes are handed to the bounded upload worker, not acknowledged by the peer.
// OpenDatagramTunnel additionally enables HTTP datagram methods. CapsuleParser
// and WriteCapsule can frame this byte stream for explicitly selected extensions.
type Tunnel struct {
	settingsView
	datagrams *http3conn.DatagramStream
	reader    io.ReadCloser
	writer    *io.PipeWriter
	ctx       context.Context
	once      sync.Once
	err       error
}

// OpenTunnel sends an ordinary CONNECT and waits for final response headers.
// request.URL identifies this connection's HTTPS origin; request.Host identifies
// the tunnel target authority (host:port). Body, trailers and content length must
// be absent. The request context controls the entire tunnel lifetime.
//
// On a 2xx response, response.Body is the returned Tunnel; close either one.
// Other statuses return a nil Tunnel and a readable response whose Body the
// caller must close. Protocol/transport failures return an error. Extended
// CONNECT (:protocol), early tunnel writes, and retry are not provided here.
func (c *ClientConn) OpenTunnel(request *http.Request) (*Tunnel, *http.Response, error) {
	return c.openTunnel(request, false)
}
func (c *ClientConn) openTunnel(request *http.Request, wantDatagrams bool) (*Tunnel, *http.Response, error) {
	if request == nil || request.Method != "CONNECT" || request.Body != nil && request.Body != http.NoBody || len(request.Trailer) != 0 || request.ContentLength != 0 || len(request.TransferEncoding) != 0 {
		return nil, nil, fmt.Errorf("http3: tunnel requires CONNECT without body, length or trailers")
	}
	for _, name := range []string{"Content-Length", "Trailer", "Transfer-Encoding"} {
		for key := range request.Header {
			if http.CanonicalHeaderKey(key) == name {
				return nil, nil, fmt.Errorf("http3: tunnel prohibits %s", name)
			}
		}
	}
	input, output := io.Pipe()
	effective := request.Clone(request.Context())
	effective.Body, effective.GetBody = input, nil
	effective.ContentLength = -1
	var bound *http3conn.DatagramStream
	var binding **http3conn.DatagramStream
	if wantDatagrams {
		binding = &bound
	}
	response, err := c.roundTripOwned(effective, nil, true, binding)
	if err != nil {
		input.CloseWithError(err)
		output.CloseWithError(err)
		return nil, nil, err
	}
	response.Request = request
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		// End the unused upload cleanly; rejection response data remains readable.
		output.Close()
		return nil, response, nil
	}
	tunnel := &Tunnel{settingsView: settingsFrom(response.Body), datagrams: bound, reader: response.Body, writer: output, ctx: request.Context()}
	response.Body = tunnel
	return tunnel, response, nil
}
func (t *Tunnel) Read(p []byte) (int, error) {
	n, err := t.reader.Read(p)
	return n, httpError(err)
}
func (t *Tunnel) Write(p []byte) (n int, err error) {
	defer func() { err = httpError(err) }()
	if t.ctx.Err() != nil {
		return 0, context.Cause(t.ctx)
	}
	n, err = t.writer.Write(p)
	if err != nil && t.ctx.Err() != nil {
		err = context.Cause(t.ctx)
	}
	return n, err
}

// CloseWrite queues EOF to the upload worker, which sends FIN after prior DATA.
// It does not wait for network delivery or prevent further response reads.
func (t *Tunnel) CloseWrite() error {
	if t.datagrams != nil {
		t.datagrams.CloseDatagramSend()
	}
	return httpError(t.writer.Close())
}
func (t *Tunnel) Close() error {
	t.once.Do(func() {
		if t.datagrams != nil {
			t.datagrams.DetachDatagrams()
		}
		t.err = t.reader.Close()
		t.writer.Close()
	})
	return httpError(t.err)
}

// CloseWithError aborts both request directions with an extension-selected code,
// joins upload cleanup and releases the request reservation. It leaves the QUIC
// connection open. The first request failure determines the transport reset.
func (t *Tunnel) CloseWithError(code quic.StreamErrorCode, reason string) error {
	t.once.Do(func() {
		if closer, ok := t.reader.(interface {
			CloseWithError(quic.StreamErrorCode, string) error
		}); ok {
			t.err = closer.CloseWithError(code, reason)
		} else {
			t.err = t.reader.Close()
		}
		t.writer.Close()
	})
	return httpError(t.err)
}

// ErrExtendedConnectNotNegotiated means the server did not advertise support.
var ErrExtendedConnectNotNegotiated = http3conn.ErrExtendedConnectNotNegotiated

// OpenExtendedTunnel opens a negotiated Extended CONNECT byte stream. The URL
// supplies scheme, authority and path; protocol is the :protocol token. The
// caller owns extension-specific handshake headers and validation of the response
// (for example WebSocket version/subprotocol). This does not implement WebSocket
// frames or HTTP datagrams. Response ownership follows OpenTunnel.
func (c *ClientConn) OpenExtendedTunnel(request *http.Request, protocol string) (*Tunnel, *http.Response, error) {
	effective, err := http3conn.WithExtendedConnectProtocol(request, protocol)
	if err != nil {
		return nil, nil, err
	}
	tunnel, response, err := c.OpenTunnel(effective)
	if response != nil {
		response.Request = request
	}
	return tunnel, response, err
}

// ExtendedConnectProtocol returns the validated :protocol on an incoming request,
// or empty for ordinary HTTP/CONNECT. Request.Proto remains the HTTP version.
func ExtendedConnectProtocol(request *http.Request) string {
	return http3conn.ExtendedConnectProtocol(request)
}

// ErrHTTPDatagramsNotNegotiated indicates missing HTTP/QUIC datagram permission.
var ErrHTTPDatagramsNotNegotiated = http3conn.ErrHTTPDatagramsNotNegotiated
var ErrDatagramSendClosed = http3conn.ErrDatagramSendClosed
var ErrDatagramRequestClosed = http3conn.ErrDatagramRequestClosed

// OpenDatagramTunnel opens Extended CONNECT with HTTP datagrams. EnableDatagrams
// must be set in ClientConfig, both peers must advertise HTTP datagrams, and QUIC
// DATAGRAM support must be negotiated. The chosen extension must define datagram
// semantics; the caller validates its handshake response. Payloads exclude the
// HTTP quarter-stream ID. No capsule fallback is attempted.
func (c *ClientConn) OpenDatagramTunnel(request *http.Request, protocol string) (*Tunnel, *http.Response, error) {
	effective, err := http3conn.WithExtendedConnectProtocol(request, protocol)
	if err != nil {
		return nil, nil, err
	}
	if !c.config.EnableDatagrams || !c.conn.ConnectionState().SupportsDatagrams {
		return nil, nil, ErrHTTPDatagramsNotNegotiated
	}
	tunnel, response, err := c.openTunnel(effective, true)
	if response != nil {
		response.Request = request
	}
	return tunnel, response, err
}
func (t *Tunnel) SendDatagram(ctx context.Context, payload []byte) (err error) {
	defer func() { err = httpError(err) }()
	if t.ctx.Err() != nil {
		return context.Cause(t.ctx)
	}
	if t.datagrams == nil {
		return ErrHTTPDatagramsNotNegotiated
	}
	return t.datagrams.SendDatagram(ctx, payload)
}
func (t *Tunnel) ReceiveDatagram(ctx context.Context) (payload []byte, err error) {
	defer func() { err = httpError(err) }()
	if t.ctx.Err() != nil {
		return nil, context.Cause(t.ctx)
	}
	if t.datagrams == nil {
		return nil, ErrHTTPDatagramsNotNegotiated
	}
	return t.datagrams.ReceiveDatagram(ctx)
}
