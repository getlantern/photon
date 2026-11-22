# Experimental HTTP/3 endpoints

`Server.ServeQUICConn(ctx, conn)` serves ordinary `net/http` handlers on an
accepted connection from this repository's QUIC listener. `Server.Serve(ctx, listener)`
owns the listener and accepts connections itself. `Server.ServeListener(listener)`
accepts a `QUICListener` (including an early listener) while leaving listener
closure to the caller. Configure TLS ALPN as
`h3`. The server initializes and owns HTTP/3 control/QPACK streams and request
acceptance; the caller must not also accept streams on that connection.

Handler requests store authority in `Request.Host`. Non-CONNECT and extended CONNECT
requests have empty `URL.Scheme` and `URL.Host`, with path/query in `RequestURI`.
Ordinary CONNECT instead uses authority-form `RequestURI` and `URL.Host`.
`ClientConn.LocalAddr()` and `RemoteAddr()` return the underlying QUIC addresses.

```go
server := &http3.Server{
    Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "text/plain")
        _, _ = w.Write([]byte("hello\n"))
    }),
    MaxConnections:        256,
    MaxConcurrentRequests: 64,
    MaxHeaderBytes:        65536,
}
// conn is an accepted *quic.Conn with negotiated ALPN "h3".
err := server.ServeQUICConn(ctx, conn)
// Alternatively, pass an existing *quic.Listener to server.Serve(ctx, listener).
```

Connections accepted by `quic.ListenAddrEarly` can also be passed to
ServeQUICConn. Allow0RTT permits transport-level early acceptance. Handlers run
before client Finished only if Server.AllowEarlyData approves the resource; by
default direct requests wait for TLS completion. Each request gets a TLS snapshot at
dispatch, so later requests reflect completed TLS without mutating earlier
requests. `Request.TLS.HandshakeComplete` describes dispatch-time state; it does
not identify whether a request arrived in early packets before dispatch. Requests
with an Early-Data field receive 425 unless the resource policy approves them,
even if this connection has already completed TLS. An upstream-client
interop gate covers an early GET followed by an established GET in both versions.
Our client supports explicit early GET/HEAD and rejected-session recovery as described below; use Server.ConfigureTLSConfig before creating the early listener to bind server HTTP SETTINGS to tickets.

Call ServeQUICConn in separate goroutines for multiple accepted connections.
Validation and admission failures leave the connection with the caller, who
must close or otherwise handle it. After admission the server owns connection
closure, and ServeQUICConn joins its protocol and handler workers before returning.
Serve bounds connection tasks per listener, and MaxConnections caps admitted
HTTP/3 connections across the Server. Configure the QUIC listener's connection
limits separately to bound handshake and not-yet-accepted transport state.
Configuration must remain immutable while serving. Zero limits use the values
shown above; a nil Handler uses `http.DefaultServeMux`.

`Shutdown(ctx)` permanently stops new admission, sends GOAWAY, and waits for
existing requests and connection cleanup. Its deadline does not abort handlers;
`Close()` cancels owned connections and handlers without waiting for application
code to return. For listeners passed to Serve, Shutdown stops acceptance and
closes the listener after connection workers finish; Close closes it
immediately. Closing a listener from `quic.ListenAddr` also closes its UDP
socket. Closing one from `Transport.Listen` or the PacketConn `quic.Listen`
only stops admission: the socket and the Transport's reader stay with the
caller, who must close the Transport to stop them. Listeners passed to ServeListener or used independently with
ServeQUICConn remain caller-owned. ServeListener returns when its accept loop
stops, without waiting for active handlers; Shutdown still joins those workers.
An accept error from a custom listener leaves existing requests running.
Serve joins its connection workers and returns ErrServerClosed after shutdown.
It continues after individual connection failures; use ServeQUICConn when the
caller needs individual connection return errors. Handlers and error
callbacks must honor cancellation, and must not synchronously call Shutdown.
A normal peer-initiated close is still returned as `*quic.PeerCloseError`; it does
not prove acknowledgment of all queued transport bytes. Shutdown itself waits
for cleanup without aggregating per-connection serving errors.

Outgoing trailers require `PermitsTrailer`, a predicate over lowercase field
names whose definitions permit trailers. Setting the Trailer header alone is
insufficient. Request trailers are populated when the request body is consumed.
The implementation bounds QPACK tables, blocked streams, buffered sections and
field counts in addition to the configured header-byte bound.

This API is experimental and does not yet provide quic-go API parity. TLS-file/listen-address convenience
helpers, general retry policy, push, automatic Expect
handling, small-response Content-Length buffering, response deadline APIs and
full early-data integration remain unfinished. See `../interop/KNOWN_FAILURES.md` for unresolved peer gates,
including large QUIC v2 exchanges against the pinned upstream peer.

## Client on an established connection

`NewClientConn(ctx, conn, origin, config)` initializes HTTP/3 on a locally dialed
QUIC connection authenticated for `origin`, with ALPN `h3`. After validation it
owns the connection; do not also open HTTP/3 streams on it. The context controls
the whole connection lifetime. `ClientConn` implements `http.RoundTripper` and
can be assigned to `http.Client.Transport`.

```go
client, err := http3.NewClientConn(ctx, conn, "https://example.com", nil)
if err != nil {
    // Validation failures leave conn with the caller.
    return err
}
defer client.Close()
response, err := client.RoundTrip(request)
if err != nil {
    return err
}
defer response.Body.Close()
```

Request URL origins are pinned, with case-insensitive ASCII hosts and normalized
ports. Request.Host overrides still follow Go HTTP semantics. Cross-origin
redirects require another connection/transport. The default concurrent-request
bound is 64, retained until response Body.Close, including after EOF. Queued
requests and stream allocation honor request cancellation, client closure and
peer GOAWAY. `ErrPeerDraining` prohibits new requests; `ErrRequestRejected` means
GOAWAY rejected an allocated request. Neither error triggers an automatic retry.
All request bodies are closed; callers must close every returned response body.
Response TLS metadata is populated from the connection.

ClientConfig can set request/header/informational limits, trailer permissions
and informational-response callbacks. Close cancels active work and joins
connection-level processing, but callers remain responsible for response-body
closure. ClientConn itself does not dial. There is no pooling, origin coalescing, decompression,
Expect/continue upload scheduling yet. CONNECT byte tunnels are available through
OpenTunnel and OpenExtendedTunnel (documented below). The large v2
upstream interoperability failure remains unresolved.

## Dialing a client

`Dial(ctx, origin, tlsConfig, quicConfig, clientConfig)` establishes QUIC and
initializes ClientConn. For example:

```go
client, err := http3.Dial(ctx, "https://example.com", nil, nil, nil)
if err != nil {
    return err
}
defer client.Close()
```

Unlike NewClientConn's lifetime context, Dial's context bounds setup only.
Canceling it after success leaves the client usable. TLS settings are cloned,
ALPN is set to h3, and an empty ServerName defaults to the origin host. Nil TLS
configuration uses system roots and certificate verification; explicitly supplied
TLS policy is preserved. Origin and client limits are validated before dialing.
Setup failures close partially initialized connections. QUIC still selects the
first resolved address; multi-address racing remains unfinished.

## Pooled transport

`Transport` implements `http.RoundTripper` with one accepting connection per
normalized HTTPS origin. Concurrent requests share a dial; cancellation of one
waiter leaves the others intact. The last departing waiter cancels an unfinished
dial. Different origins authenticate and connect separately.

```go
transport := &http3.Transport{}
defer transport.Close()
client := &http.Client{Transport: transport}
response, err := client.Get("https://example.com/")
if err != nil {
    return err
}
defer response.Body.Close()
```

Configure TLSClientConfig, QUICConfig and ClientConfig before use. The default
pooled response-header limit is 10 MiB. Set MaxResponseHeaderBytes to a positive
value to override it; an explicit ClientConfig.MaxHeaderBytes is used otherwise.
Direct ClientConn construction retains its 64 KiB default. The transport response
limit governs encoded and decoded field sections and the advertised SETTINGS
limit, independently of the request encoder's budget. TLS cache scoping includes
this receive limit so changing it disables early use of incompatible metadata.
Existing field-count and aggregate QPACK budgets still apply.

EnableDatagrams enables HTTP DATAGRAM negotiation. With a nil QUICConfig, the
transport enables QUIC reception automatically. An explicit QUICConfig must also
have EnableDatagrams set; conflicting settings fail before dialing and close the
request body. The older ClientConfig.EnableDatagrams path retains its automatic
QUIC enabling behavior. Caller configurations are not mutated.

The default
DialTimeout is 10 seconds. IdleConnTimeout defaults to 90 seconds; negative
values disable idle expiry. The idle period starts when the last response body
or failed request releases its reservation. Reuse restarts that period, and
active requests and unclosed response bodies do not expire.
MaxConnections defaults to 256 and includes dialing
and draining connections. At capacity the least recently idle connection is
evicted, and admission waits for its cleanup with request cancellation support.
If every connection is reserved, new connections return ErrConnectionLimit;
there is no queue waiting for active requests to finish.
Use CloseIdleConnections to release all currently unused entries. Response Body.Close releases a
reservation; EOF alone does not. CloseIdleConnections leaves reserved connections
intact and permits future dialing. Close permanently rejects admission with
ErrTransportClosed, cancels pending dials and closes and joins owned connections.
Active exchanges can return ErrClientClosed when their underlying client closes.

GOAWAY removes a connection from reuse while earlier responses finish. New
requests can dial a replacement, subject to the connection limit. A request
racing GOAWAY, or refused with H3_REQUEST_REJECTED, can be retried once when
protocol state proves that the peer did
not process it. This applies to any method, including POST, but requires an empty
body or GetBody and no upload trailers. The first body is closed before GetBody
is called. Cancellation is checked before and after GetBody; if it occurs during
reconstruction, the cancellation cause wins and any returned body is closed.
GetBody must return promptly; it has no context argument and is not run in a
detached goroutine. Transport closure is checked before reconstruction and again
during admission, and any unused reconstructed body is closed.
DisableRetries turns this off. An unrepeatable body, a canceled request,
a second rejection, or upload trailers stop automatic replay. Response.Request
continues to identify the original request.

Retry evidence comes from the received GOAWAY boundary, local refusal before
sending, or a remote H3_REQUEST_REJECTED observed at the concrete QUIC stream
boundary. Errors returned by an application body or callback cannot supply that
evidence. Stream rejection may retry on the same connection; GOAWAY requires a
replacement. Errors while reading an already-returned response are never retried.
Generic connection failure retries, origin coalescing and HTTP fallback remain
unfinished.
The API is experimental. See [RFC 9114 section 5.2](https://www.rfc-editor.org/rfc/rfc9114.html#section-5.2)
for the GOAWAY processing guarantee and [section 4.1.1](https://www.rfc-editor.org/rfc/rfc9114.html#section-4.1.1)
for request rejection.

Transport automatically advertises `Accept-Encoding: gzip` when no Accept-Encoding
or Range header key is present and the method is neither HEAD nor CONNECT.
`DisableCompression` disables this behavior. Explicit encoding choices (including
an empty or nil header entry) are preserved. Only automatically negotiated,
single-gzip response encodings are decoded; bodyless 204/205/304 metadata and
other or stacked encodings are left intact. Direct ClientConn and Dial clients
continue to expose encoded bodies.

Automatic decoding sets Response.Uncompressed, removes Content-Encoding and
Content-Length, and sets ContentLength to -1. Wire length validation and HTTP/3
trailers remain active beneath the streaming gzip reader. Request headers and
Response.Request identity are preserved. Decoding begins on the first body read;
malformed headers, truncation and checksum errors surface through Read. Always
close the response body, including after decoding errors, to release the request
and pool reservations.


### Ordinary CONNECT client tunnels

`ClientConn.OpenTunnel(request)` now exposes unframed bidirectional bytes over
HTTP/3 DATA frames. Use a CONNECT request with URL pointing at the authenticated
proxy origin and Host containing the destination host:port. Leave Body, length
and trailers absent. The request context covers the tunnel lifetime.

On a 2xx response, the returned Tunnel is also response.Body. Read and Write may
run concurrently; CloseWrite ends only sending, and Close releases the request
reservation and interrupts both directions. Close is required even after EOF.
Writes hand bytes to a bounded upload worker; they do not prove peer delivery.
Non-2xx responses return a nil tunnel with a readable response body that must be
closed. RoundTrip still rejects CONNECT without this explicit tunnel owner.

The live upstream fixture covers echo, both half-close directions, rejection,
request cancellation and reuse of a single request reservation. It runs v1/v2 over
IPv4/IPv6. Default v2 transfer size is 12,000 bytes; v1 uses 66,000 bytes. Setting
QUIC_INTEROP_KNOWN_FAILURES=1 retains the 66,000-byte v2 gate, which stalled in
both address families, consistent with the separately documented pinned upstream
v2 key-update defect. The larger gate is not claimed passing.

Capsule integration,
pooled Transport tunnel opening and deadline methods remain unfinished. This API
is an initial public ordinary-CONNECT client path, not complete tunnel parity.


### Ordinary CONNECT server tunnels

A handler can call `http3.AcceptTunnel(w)` before reading the request body or
committing final response headers. Set headers and optionally a 2xx status first;
the default is 200. Successful acceptance returns a ServerTunnel carrying
unframed DATA bytes. Response Content-Length and trailers are prohibited. A writer
wrapper must expose `Unwrap() http.ResponseWriter` to allow handoff.

After acceptance, use the tunnel for I/O. Read and Write can run concurrently.
CloseWrite queues FIN and preserves reading; Close interrupts and joins current
I/O. The handler must join any workers it starts before returning. Handler return
finishes sending and closes receiving, unless Close already aborted the tunnel.
This retains server request accounting and cancellation rather than handing off
ownership beyond the handler lifetime. Writes on the original ResponseWriter
fail after acceptance.

TestHTTPConnectTunnelIndependentClient verifies an upstream client against this
public server API for v1/v2 and IPv4/IPv6, including echo, both half-close
directions and an ordinary request after tunnel cleanup. Unit tests cover invalid
handoffs, read interruption and forbidden HEADERS after acceptance. Capsules still require public integration.


### Extended CONNECT byte tunnels

`ClientConn.OpenExtendedTunnel(request, protocol)` sends an Extended CONNECT with
`:scheme`, `:authority`, `:path` and a validated `:protocol` token. Request.URL
identifies the HTTPS origin and target path; Request.Host can override authority.
Body, content length and trailers remain absent. The session waits for server
SETTINGS, then checks ENABLE_CONNECT_PROTOCOL before writing request headers. An
absent/zero advertisement returns ErrExtendedConnectNotNegotiated; cancellation
interrupts a settings wait. Status handling and tunnel ownership match OpenTunnel.

A server enables the setting by configuring a nonempty
`ExtendedConnectHandlers map[string]http.Handler`. Each token selects its handler;
unknown protocols receive 501 instead of reaching the ordinary Handler. Nil maps
do not advertise support. Invalid tokens and nil handlers fail configuration
validation. Handlers read `http3.ExtendedConnectProtocol(request)`; Request.Proto
continues to report HTTP/3.0. A selected handler validates its extension-specific
handshake, then calls AcceptTunnel for the successful byte stream.

This supplies RFC 9220 negotiation and routing, not WebSocket framing/handshake
validation or datagram semantics. Extension implementations own those requirements.
Independent tests cover both roles, ordinary and Extended CONNECT, v1/v2 and
IPv4/IPv6, preserving path/query/protocol fields and both half-close directions.
The reverse fixture also verifies unknown-protocol 501 and subsequent connection
reuse. Unit tests verify missing/zero advertisement, canceled settings waits,
metadata validation, routing and disabled receive permission.


### Client HTTP datagram tunnels

Set ClientConfig.EnableDatagrams and call
`ClientConn.OpenDatagramTunnel(request, protocol)` for an Extended CONNECT
extension that defines HTTP datagram semantics. HTTP Dial enables QUIC receiving
on a copied QUIC configuration. NewClientConn requires the supplied connection to
have local QUIC datagram reception enabled. QUIC Conn.DatagramsEnabled reports
that local capability separately from peer support.

The open call requires peer QUIC support, negotiated Extended CONNECT and both
HTTP H3_DATAGRAM settings. On success, Tunnel.SendDatagram(ctx, payload) queues an
unreliable payload and ReceiveDatagram(ctx) waits for one. HTTP quarter-stream IDs
are encoded/removed by the implementation. Send success does not imply delivery;
QUIC enforces the peer/path send-size limit. No capsule fallback is provided.
Extensions still own handshake validation and payload semantics.

MaxDatagramQueue defaults to 16 per request; MaxDatagramPayload defaults to 1200
received payload bytes (maximum configurable value 65535). Overflow drops newest
payloads and oversized receive payloads are dropped. Stream limits bound registered
requests. All ordinary requests are registered as lacking datagram semantics, so
an illegal datagram aborts that request with 0x33 while keeping the connection
usable. Unopened and receive-closed IDs are dropped.

CloseWrite cancels/joins datagram sends before submitting upload EOF. Receive FIN
closes datagram reception even before application EOF, independently of sending.
Close and response-body cleanup unregister the request and wake receivers. Error
aliases ErrHTTPDatagramsNotNegotiated, ErrDatagramSendClosed and
ErrDatagramRequestClosed describe these states. The public upstream-server fixture
now covers this API, empty/small/900-byte echoes, FIN/close gates and copied dial
configuration across v1/v2 and IPv4/IPv6. Separate live tests cover missing HTTP or
QUIC support, illegal datagrams on GET, and subsequent connection reuse.

Public server datagram configuration is described below. Capsule integration
remains unfinished.


### Server HTTP datagram tunnels

Register `Server.DatagramHandlers map[string]http.Handler` for protocols that
explicitly define HTTP datagram semantics. A nonempty map enables Extended CONNECT
and H3_DATAGRAM advertisement. Handler keys must be valid tokens; handlers must be
non-nil and must not duplicate entries in ExtendedConnectHandlers. After validating
its extension handshake, a handler calls AcceptTunnel and uses ServerTunnel's
SendDatagram/ReceiveDatagram alongside byte I/O. Request context and handler
ownership still govern cleanup. CloseWrite preserves receiving; Close interrupts
both directions. Peer receive FIN ends datagram reception before application EOF.

The QUIC listener must have EnableDatagrams set. Serve and ServeQUICConn reject
missing local QUIC reception before ownership transfer. Server.EnableDatagrams
can advertise/track datagrams even with no registered extensions. Queue/payload
limits share client defaults (16 and 1200 bytes). HTTP sends still require both
advertisements; QUIC enforces peer support and send size.

Accepted requests buffer datagrams within those limits while headers are decoded.
Declared semantics release supported queues; unsupported requests are reset with
0x33, and their handlers are canceled even without a body read. Ordinary request
handling remains usable after that request error. Unknown extension tokens get 501
when no datagram violation has already required abort.

The reverse independent fixture now uses this public server API. Both public roles
cover v1/v2 and IPv4/IPv6; negative tests cover handler cancellation, connection
reuse and preserving listener ownership on invalid configuration. Capsules,
impaired-network HTTP datagram validation and performance comparisons remain open.

### Capsule policy on the server

Set `Server.CapsuleProtocols[protocol] = true` for a token already registered in
`ExtendedConnectHandlers` or `DatagramHandlers` to enforce capsule request/response
header and status restrictions. Incoming violations are rejected before the handler
runs; outgoing violations reset the request. The handler still validates the chosen
extension handshake, calls `AcceptTunnel`, and uses `NewCapsuleParser` and
`WriteCapsule` for payload I/O. When constructed directly on either tunnel type, the parser automatically aborts
malformed capsules with the request error code. Wrappers must forward
`CloseWithError` to retain this behavior; plain readers leave error handling to
their owner. The header alone does not activate capsule policy.

For clients, set `ClientConfig.CapsuleProtocols[protocol] = true` before dialing
and use `OpenExtendedTunnel` (or `OpenDatagramTunnel` when QUIC datagrams are also
required). The client copies the map and validates both outbound and inbound
capsule header/status policy. Extension-specific handshake checks and payload
parsing remain the caller's responsibility.

HTTP Dial now attaches validated peer SETTINGS and origin/local-advertisement scope
to client-local session metadata. Tickets arriving before SETTINGS are held until
that connection's settings are available. Missing or mismatched metadata cannot
authorize early HTTP use; ordinary TLS resumption remains possible. Ordinary Dial
still disables early data.

## Explicit early client requests

`DialEarly` uses a suitable TLS session ticket and its bound peer SETTINGS to
initialize HTTP before TLS completion. With no suitable ticket it waits for TLS.
Use a shared `tls.Config.ClientSessionCache` across calls. As with Dial, canceling
the dial context after success does not close the client.

Only `MethodGet0RTT` and `MethodHead0RTT` allow requests before TLS completion;
they encode ordinary GET and HEAD without modifying the caller's request method.
Other methods wait. The caller must assess replay safety for the resource, as
required by [HTTP early-data policy](https://www.rfc-editor.org/rfc/rfc8470.html#section-4).
Early CONNECT and HTTP DATAGRAM permission still require live peer SETTINGS.

If RoundTrip returns `quic.Err0RTTRejected`, call `client.NextConnection(ctx)`.
It waits for TLS, retires the rejected HTTP/QPACK state and returns a fresh client
on the same QUIC connection. It never resends requests. Explicitly construct a
new request if application policy permits retrying. Close the returned client;
closing the retired client does not close its replacement. Concurrent/repeated
NextConnection calls share the replacement. An accepted early connection returns
the original client. Cancellation before handoff leaves recovery available;
failure after handoff closes the connection rather than reusing partial HTTP state.

Setup rejection before DialEarly returns is handled internally because no request
has been exposed. The pool implements bounded HTTP 425 retries as described below;
the full early/multipath combination remains unfinished. Independent server ticket-policy tests now cover
compatible and incompatible settings changes in both QUIC versions. Public UDP tests cover accepted/rejected GET and HEAD, ordinary
method waiting, explicit reconstruction, concurrent recovery and unchanged caller
requests against upstream quic-go for v1 and v2. Accepted cases withhold every
resumed server datagram until the handler receives the request and verify the
peer handshake is incomplete, proving delivery before TLS completion. Canceled
NextConnection waits before handoff preserve their cause and leave recovery usable.
Reduced-credit peers also verify cancellation while fresh critical streams are
being opened, independent cancellation of queued recovery callers, connection
closure after partial setup, and retention of terminal recovery causes. One-byte
stream-window tests verify rejection during initial DialEarly SETTINGS setup
returns a usable reconstructed client, and cancellation of a blocked SETTINGS
write closes the partial connection while retaining the caller cause.


## Server ticket SETTINGS policy

Call `server.ConfigureTLSConfig(tlsConfig)` before passing TLS configuration to a
QUIC listener with `Allow0RTT`. The returned clone selects h3 and adds an HTTP
SETTINGS record to authenticated server tickets. On restoration, incompatible,
missing or malformed HTTP metadata disables early data while preserving ordinary
resumption. This is separate from client-local cached SETTINGS. Settings acceptance
follows [RFC 9114](https://www.rfc-editor.org/rfc/rfc9114.html#section-7.2.4.2) and
[QPACK's nonzero-capacity rule](https://www.rfc-editor.org/rfc/rfc9204.html#section-3.2.3).

The wrapper retains application ticket callbacks, opaque Extra records and dynamic
GetConfigForClient selection. Its default callbacks use the supplied config's
EncryptTicket/DecryptTicket methods, keeping ticket-key ownership across clones.
Keep Server fields unchanged while using the result, and rebuild from the original
TLS configuration when changing HTTP policy. Pass the configured result to the
listener: ServeQUICConn receives a connection too late to change TLS acceptance.
The helper does not enable QUIC early data or implement replay protection.

Tests cover callback/state ownership, default authenticated encryption, compatible
and incompatible policy changes, malformed/duplicate/missing records and selected
TLS configuration wrapping. Live upstream v1/v2 tests verify equal/increased
limits accept early data while reduced limits, missing metadata and removed
CONNECT/DATAGRAM permissions reject it without losing TLS resumption. They exclude
Retry, require fresh HTTP success and check that rejected probes are not delivered
late. An early-GET gate with the wrapper enabled separately proves handler dispatch
before client Finished.


## Early requests through the pool

Set `Transport.Enable0RTT` before use to dial pooled connections with DialEarly.
A shared TLS session cache is still required for resumption. Ordinary methods wait
for TLS; only MethodGet0RTT and MethodHead0RTT opt into replayable early requests.
Early HEAD follows ordinary HEAD's automatic-compression policy.

The pool worker owns handshake confirmation and any rejected-session replacement.
Recovery keeps its existing connection slot, including with MaxConnections set to
one; replacement publication is synchronized with admission. A request receiving
Err0RTTRejected waits for that result. Actual session rejection supplies evidence
for the existing single retry, subject to DisableRetries, body recreation and
upload-trailer restrictions. Merely returning that error from application code
does not prove rejection. Requests are never retried indefinitely.

With DisableRetries, the rejected request returns its error, but the recovered
connection remains available for later requests. Close and idle closure still
join the owning worker. The confirmation/reconstruction phase is bounded by
DialTimeout independently of the initial dial. Independent v1/v2 tests cover
acceptance/rejection, both retry settings, one-slot recovery, unchanged caller
methods, HEAD compression, idle closure and concurrent follow-up requests. A
wrapped Err0RTTRejected from an informational callback causes no replay and leaves
the connection usable. Closing the pool during a gated early handshake joins the
worker and rejects later admission. Simultaneous rejection/handoff/closure stress
and additional body-error evidence cases remain to be expanded.


## HTTP 425 retries

With Enable0RTT, the pool retries an explicit MethodGet0RTT/MethodHead0RTT request
once after a 425 (Too Early) response when its body can be recreated and it has no
upload trailers. It closes the original response and joins upload cleanup before
calling GetBody. The retry uses ordinary GET/HEAD, which waits for TLS completion,
including if it needs a new connection. This follows
[RFC 8470 section 5.2](https://www.rfc-editor.org/rfc/rfc8470.html#section-5.2).

The attempt shares the existing one-retry budget with transport-level rejection;
a second 425 is returned with its body intact. DisableRetries, an ordinary request
method, missing GetBody for a real body, or upload trailers leave the first 425
response available to the caller. Any Early-Data request header also suppresses
this automatic retry, preserving intermediary forwarding requirements. The pool
does not add or remove that header. GetBody failure after cleanup returns an error.
Direct ClientConn callers continue to own status-based retry decisions.

Independent v1/v2 tests cover GET/HEAD, repeated 425, disabled retries, forwarded
Early-Data, body recreation, nonreplayable bodies, trailers, original Request
identity, completed peer TLS on retry, and interruption of a blocked original
uploader before body recreation. GetBody errors, request cancellation during
GetBody, pool closure during GetBody and nil replacement bodies have live tests
for exact body cleanup, no extra request, preserved causes and usable pool
reservations. Additional response-time cancellation cases still need broader
coverage.


Early-Data field handling follows
[RFC 8470 section 5.1](https://www.rfc-editor.org/rfc/rfc8470.html#section-5.1).
Handlers receive a single value of 1 when any structurally valid request instance
is present, including invalid values or duplicates. Outgoing requests preserve
that bit as one canonical field without mutating caller storage; the field is
never added merely because a connection supports early data. Responses,
informational responses, trailers and trailer declarations cannot contain it.
Normal HTTP field-octet rules still apply. This field can describe an earlier hop;
it does not establish transport-level provenance for the current request.

Server.AllowEarlyData is an optional metadata-only resource policy. It must not
read the request body or perform resource actions, and must honor cancellation.
It is consulted before ordinary and extension handlers when TLS is incomplete or
Early-Data is present. True authorizes replayable processing; false or nil waits
for local TLS on direct requests and returns 425 for forwarded markers. A completed
local handshake cannot remove replay risk inherited from an earlier hop. Deferred
requests refresh their TLS snapshot before handler dispatch. Policies execute
inside the request's panic and cancellation boundary.

Unit tests cover policy combinations and cancellation. Upstream v1/v2 tests prove
approved dispatch before Finished, denied dispatch after Finished, and 425 without
handler invocation for duplicate/invalid forwarded markers on established TLS.
Independent v1/v2 response/header/trailer misuse tests verify MessageError, no
invalid informational callback or trailer publication, and a healthy follow-up
request on the same single-slot connection. Extended CONNECT and datagram handler
routing have upstream-client denial/approval tests for forwarded replay markers,
followed by working stream/datagram tunnels on that same single-slot connection.
Request-trailer misuse, direct early extension dispatch and concurrent extension
cancellation remain open.


Client exchange errors and response-body reads/closes expose `*http3.Error` for
QUIC stream cancellation and peer application closure. Use `errors.As` to read
`Remote`, `ErrorCode` and `ErrorMessage`, or `errors.Is` with an `*http3.Error`
containing the expected code and endpoint. `Unwrap` preserves the original error
chain, including `*quic.StreamError` or `*quic.PeerCloseError`. Ordinary transport
failures and context cancellation retain their types. Tunnel return boundaries also apply this conversion. Server request-body reads/closes, response writes/flushes and accepted tunnel
operations also apply conversion while preserving the concrete response writer.
Constructor, dial, pooled request and early recovery return boundaries apply
the same conversion. Application closes now expose `*quic.ApplicationError`; legacy peer-close
inspection remains available through its error chain. Exhaustive setup/recovery
failure coverage remains incomplete. Upload failure cleanup forwards its cause
to bodies supporting `CloseWithError(error)`, preserving peer resets in tunnel
pipe writes. Ordinary request bodies retain their exactly-once `Close` contract.

## Inspecting peer settings

`ClientConn` and the response writer supplied to server handlers implement
`Settingser`. Response bodies returned by this implementation, including pooled
and automatically decompressed responses and client tunnels, also implement it.
For example, a handler can inspect a custom peer setting without consuming the
request body:

```go
settings := w.(http3.Settingser)
select {
case <-settings.ReceivedSettings():
    value, advertised := settings.Settings().Other[0xface]
    // Use value and advertised according to your extension's protocol.
    _, _ = value, advertised
case <-r.Context().Done():
    return
}
```

Receipt means validated live SETTINGS and successful local encoder setup.
Remembered 0-RTT settings do not signal receipt; connection shutdown before
receipt leaves the channel open, so waits need a cancellation case. `Settings`
returns a private snapshot (nil before receipt). The flags describe peer
advertisements; sending HTTP datagrams still requires negotiation at both layers.
`Other` includes explicitly advertised QPACK and unknown settings, but excludes
the maximum field-section size. Each snapshot remains readable after closure.
For a rejected early session, inspect the replacement returned by
`NextConnection`; old response views remain bound to the old session.

`Server.AdditionalSettings`, `Transport.AdditionalSettings`, and direct-client
`ClientConfig.AdditionalSettings` advertise application-defined values. For example:

```go
transport := &http3.Transport{
    AdditionalSettings: map[uint64]uint64{0xface: 1},
}
```

Configure these maps before use. Client setup copies the map; transport entries
merge with ClientConfig entries and duplicate IDs are errors. At most 59 extras
are accepted, leaving room for five managed settings in the 64-entry budget.
Identifiers and values must fit QUIC varints. Core/QPACK, HTTP/2-reserved,
Extended CONNECT and DATAGRAM IDs are controlled by the implementation and are
rejected here. Advertisement alone does not implement an extension.

Additional values are bound into HTTP ticket policy. Changing, adding or removing
one disables 0-RTT with the old ticket while preserving ordinary resumption;
`Server.ConfigureTLSConfig` must be applied to the server's listener TLS config
as described above. An empty map preserves existing ticket compatibility.

`ClientConn.Context()` tracks the underlying QUIC connection and retains its
close cause through `context.Cause`. `CloseWithError(code, message)` initiates an
application close; `Close()` joins the HTTP session's workers. Invalid codes leave
the connection usable. After rejected 0-RTT recovery, the old and replacement
clients share the transport context, but closing the old client cannot close the
replacement connection. Request contexts remain independent of this lifetime.

Use `Transport.RoundTripOpt(request, http3.RoundTripOpt{OnlyCachedConn: true})`
to prohibit dialing for a request. It can use a cached connection or share an
already-running dial, and returns `http3.ErrNoCachedConn` if none is available.
The restriction also applies to automatic retries. Cancellation releases this
request's reservation without canceling another caller's interest in the same
dial. As with RoundTrip, close every successful response body.

`Server.ConnContext` customizes the context inherited by requests on each admitted
connection. Its input includes `http3.ServerContextKey` (the `*Server`),
`http.LocalAddrContextKey` (local `net.Addr`), and `http3.RemoteAddrContextKey`
(remote `net.Addr`). Addresses reflect connection setup. Return a context derived
from the input to preserve those values; returning nil panics. The callback runs
before HTTP/3 setup and must return promptly. Do not call `Shutdown` synchronously
inside it. Parent, peer, and server shutdown still terminate the connection if a
callback returns a context detached from its input. Handler cancellation causes
can reflect whichever shutdown or transport event takes effect first.
