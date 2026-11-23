# Raw HTTP/3 streams and connections

This note covers the quic-go v0.65.0 `http3` "raw" cluster: `Stream`,
`RequestStream`, `HTTPStreamer`, `RawClientConn`, `RawServerConn`, their
constructors, and the `IdleTimeout`/`Logger` fields. Declarations match
upstream; the implementation is this package's own and sits on the existing
session, message and datagram internals.

## Mapping

| Public type | Built from | Owns |
| --- | --- | --- |
| `Stream` | `http3conn.RawStream` | DATA framing in both directions of one request stream after takeover |
| `RequestStream` | `http3conn.ClientStream` (a `RawStream` plus request/response state) | one client request slot, GOAWAY lease and datagram registration |
| `HTTPStreamer` | `settingsResponseWriter.HTTPStream` → `HTTPResponseWriter.TakeOver` | nothing; it moves the response side into a `Stream` |
| `RawServerConn` | the `ServeQUICConn` session and request loop, fed by the application | an admitted `Server` connection slot |
| `RawClientConn` | a `ClientConn` whose session reads application-fed streams | a `ClientConn` |

`RawStream` reads through the request's `MessageBody`/`MessageReader` and writes
through its `MessageWriter`, so frame order, unknown-frame skipping, trailers,
Content-Length accounting and QPACK are the same code that serves ordinary
requests. Nothing parses frames a second time. Both public types hold the same
`RawStream`; `Stream` embeds it through an unexported interface because the
reference declares those methods with value receivers.

## Server: `HTTPStreamer`

The handler's `http.ResponseWriter` implements `HTTPStreamer`. `HTTPStream`
follows upstream's rules: it fails once response body bytes were written; if
`WriteHeader` set a final status the header is sent first; otherwise no response
is sent and `Stream.Write`/`TryWriteAll` fail. Afterwards `WriteHeader` panics
and `Write` returns `http.ErrHijacked`. Repeated calls return the same `Stream`.

The request task does not end when the handler returns. It waits until the
application has finished both directions (send: `Close`, `CancelWrite` or the
peer's STOP_SENDING; receive: `Read` returned EOF or a terminal error, or
`CancelRead`), or until the connection ends, and only then releases QPACK
state, the datagram registration and the request slot. The QUIC receive
context cannot stand in for the second condition: it ends when FIN arrives,
before the application has read what is buffered. A `Write` or `Close` that
fails local validation, such as a declared Content-Length, does not end
sending: the application must call `CancelWrite`. A receive direction the
handler finished before `HTTPStream`, by reading the request body to its end or
closing it, counts as finished. `Close` succeeds when the
peer has already stopped the direction with H3_NO_ERROR, which a server may do
once it has responded. Until then the request
context stays live. Peer resets no longer abort the other direction; they are
returned from `Read`/`Write`. Protocol violations still close the connection
and malformed messages still reset the request. When the request ended first
(connection closed, handler panic), calls return that cause rather than the
local reset it produced.

Who owns a request's failure handling is one piece of state on its task,
guarded by one lock: an ordinary request, or a stream the application owns,
plus whether the request has failed and whether receiving is over. Every
decision that depends on it is made and recorded under that lock: `HTTPStream`
taking the stream over, a body read acting on its result, the observers of a
peer reset or STOP_SENDING, and the report that receiving is over. So a
takeover is either before such an event, which then gets raw-stream treatment
(only that direction is affected), or after it, and then sees its effect: a
request that already failed refuses the takeover, and a receive direction
that already finished is carried into the new stream. This holds for a body
read that is waiting when `HTTPStream` is called. Protocol violations and
malformed messages fail the request whoever owns it. The lock is not held
across I/O or application code; resets and cancellation follow a recorded
decision.

`ServerTunnel` (`AcceptTunnel`) is unchanged and remains handler-scoped and
CONNECT-only. The two are exclusive per request.

## Client: `RequestStream`

`ClientConn.OpenRequestStream` waits for the handshake, takes a
`MaxConcurrentRequests` slot, opens a stream and registers a GOAWAY lease.
`SendRequestHeader` waits for the server's SETTINGS (the QPACK encoder and the
Extended CONNECT check need them) and writes HEADERS. As upstream, a CONNECT
request whose `Proto` is not `HTTP/1.1` is an Extended CONNECT with that
`:protocol`. `ReadResponse` returns the final response; `Read` then reads its
body. One goroutine per stream waits for both directions to finish as above,
or for the lease or the client to end, and then releases the slot, lease and
datagram registration; `ClientConn.Close` joins it. Closing `Response.Body`
cancels reading, as upstream.

## Raw connections

`Transport.NewClientConn` and `NewRawClientConn` return immediately and set up
the session in the background, after the handshake. Setup failure closes the
connection with H3_INTERNAL_ERROR (or the specific protocol code) and every
later call returns that error. These connections are not pinned to an origin.

A raw connection owns no accept loop. The application accepts streams and hands
over the ones that are HTTP/3's:

- unidirectional: `HandleUnidirectionalStream`, type prefix unread. It feeds the
  session's existing dispatcher, so control, QPACK and unknown-type handling,
  the active-stream budget and critical-stream errors are unchanged. Every
  control and QPACK stream must be handed over: this package uses the QPACK
  dynamic table, so a withheld encoder stream stalls header decoding. The call
  returns when the dispatcher has taken the stream, not when the stream ends.
- client bidirectional (server): `HandleRequestStream`, which blocks until the
  request, including a taken-over stream, is finished. It feeds the same
  request loop as `ServeQUICConn`. Applications call it from their own
  goroutines, so priority registration accepts stream IDs out of order there.
- server bidirectional (client): the application keeps the streams its
  extension defines and passes the rest to `ClientConn.HandleBidirectionalStream`,
  which closes the connection with H3_STREAM_CREATION_ERROR. Non-raw clients
  still accept and reject such streams themselves.

`Server.NewRawServerConn` validates and admits the connection synchronously
(configuration, ALPN, `MaxConnections`) and returns an error for those, leaving
the connection with the caller. `ConnContext` runs after admission, as for
`ServeQUICConn`: if it returns a canceled context the error is returned and the
connection is closed, and a nil context panics. Critical-stream setup continues in the background and fails by closing
the connection, so the call never blocks on the peer. `ServeQUICConn` is the
same admission followed by the same serving code, run inline.

Both handlers call `ReleaseStreamCredit`, as upstream's do, for connections
using `quic.Config.RequireStreamCreditRelease`. The built-in accept loops now
do the same; without that they would stop receiving stream credit under that
setting.

## Lifecycle and shutdown

- GOAWAY: a raw client lease is rejected or kept exactly like a `RoundTrip`
  request; `OpenRequestStream` returns `ErrPeerDraining` after GOAWAY. A raw
  server connection is registered with its `Server`: `Shutdown` sends GOAWAY,
  rejects later `HandleRequestStream` calls with H3_REQUEST_REJECTED, waits for
  running requests and taken-over streams, then closes the connection with
  H3_NO_ERROR. `Close` aborts it.
- Concurrency: taken-over server streams and open `RequestStream`s hold their
  `MaxConcurrentRequests` slot for their whole life. `HandleRequestStream`
  blocks while the server limit is reached.
- `Server.IdleTimeout` runs in the request loop for both served and raw
  connections: armed while no request stream is active, it closes the
  connection with H3_NO_ERROR. A separate period of the same length bounds
  critical-stream setup, which a peer can stall by withholding credit while
  its PINGs keep QUIC alive.
- Early data: raw request streams never use 0-RTT. Remembered SETTINGS are
  bound to tickets only through `DialEarly`, so `OpenRequestStream` and the
  `Transport` constructors wait for the handshake. `AllowEarlyData` gates
  handler dispatch before `HTTPStream` can be called, as for any handler.
- Priority: `SetPriority` sets the QUIC stream's scheduling directly. A later
  PRIORITY_UPDATE from the client still applies.

## Datagrams

A raw stream uses the request's existing `DatagramStream` registration, so
routing, queue and payload limits are those of tunnels. A `RequestStream` is
registered as having datagram semantics when datagrams are enabled. A server
`Stream` has them when its request was routed through `Server.DatagramHandlers`;
otherwise `SendDatagram`/`ReceiveDatagram` return
`ErrHTTPDatagramsNotNegotiated` and a datagram from the peer resets the request
with H3_DATAGRAM_ERROR, as before. Sending requires the peer's SETTINGS.

## Deadlines

A stream deadline that expires (`os.ErrDeadlineExceeded`) leaves the stream
usable, with one exception.

| Call | After an expired deadline |
| --- | --- |
| `Read` | repeat it; the frame reader keeps a partial frame header, HEADERS block or skipped frame |
| `ReadResponse` | repeat it; the interim-response count carries over |
| `Write` | repeat it; the next `Write` or `TryWriteAll` first finishes the interrupted DATA frame |
| `TryWriteAll` | repeat it; nothing was queued |
| `Close` | repeat it; FIN was not queued, the message is as open as before (`Write` still works), and datagram sending resumes unless the stream was reset while `Close` waited (a `CancelWrite` or request abort does not wait for a `Close` that is waiting for room to queue FIN, and ends datagram sending for good). A datagram send that was in progress when `Close` was called has failed. `Close` also fails, retryably, while a DATA frame is unfinished |
| `SendRequestHeader`, and the response header sent by `HTTPStream` | terminal for the send direction: a HEADERS frame is tied to QPACK state already committed and cannot be abandoned or re-encoded |

Every other error a writer caches is terminal: a failed stream write, and
local message validation (a declared Content-Length, frame order, trailers).

### What a receive call can wait on

`Read`, `ReadResponse`, `Response.Body.Close`, `CancelRead` and
`SetReadDeadline` must not be held by the peer past the application's own
controls. The receive path can wait in these places:

| Wait | Peer-controlled | Ended by |
| --- | --- | --- |
| QUIC stream read | yes | the QUIC read deadline and `CancelRead` |
| a dynamic-table insertion the section references | yes | read deadline, `CancelRead`, body close, request end |
| the shared QPACK decoder's lock | no: held only for in-memory work | immediately |
| QPACK feedback (acknowledgment, cancellation) | not waited for: queued | n/a |
| `RawStream`'s read lock | no: held only by this stream's own `Read`/`ReadResponse` | that call returning |
| `FieldStream`'s and `RawStream`'s state mutexes | no | immediately |
| `Information` and trailer callbacks | no: application code | the application |

A section waiting for an insertion reads nothing from the request stream, so
the QUIC stream's controls cannot reach it. `FieldStream` applies the read
deadline to that wait and keeps the undecided section for the repeated call;
`CancelRead` cancels the stream's QPACK work, which ends the wait; a body close
or the request's end reach it through its context.

Decoder feedback used to be written under the shared decoder's lock, which put
the peer's decoder-stream credit on this path three ways: a section's own
acknowledgment, another stream's write holding the lock, and the cancellation
that releases an abandoned request. It is now queued and written by one
goroutine outside the lock ([HTTP3.md](HTTP3.md), QPACK decoder feedback), so
none of them remains, and an abandoned request gives its slot back whether or
not the peer accepts the cancellation.

`ReadResponse` keeps its count of interim responses across calls repeated
after a deadline, so `MaxInformational` bounds the response rather than each
attempt; the `MessageReader` already keeps everything else a repeated call
needs (a partial frame header, HEADERS block, skipped frame or section).

## Known differences from upstream

- Server extension routing is unchanged: Extended CONNECT reaches a handler
  only through `ExtendedConnectHandlers`/`DatagramHandlers`.
- `ReadResponse` returns the final response; interim responses go to
  `ClientConfig.Information`. A second call is an error.
- A `RequestStream` on a client pinned to an origin rejects requests for other
  origins; upstream has no pinning.
- `SendRequestHeader` sends Content-Length only for a positive
  `ContentLength`, where upstream announces zero for a bodiless POST, PUT or
  PATCH and then lets `Write` contradict it.
- A handler that panics after `HTTPStream` resets the stream; upstream leaves
  it with the application.
- `RequestStream` does not negotiate gzip. `Transport.NewClientConn` clients
  do, for `RoundTrip`, unless `DisableCompression` is set.
- `SendDatagram` fails until datagrams are negotiated instead of sending
  optimistically.
- An empty `Write` after `Close`, or after the send direction failed, returns
  that error, as `TryWriteAll` does. Upstream `Write` returns `0, nil` for an
  empty slice whatever the stream's state.
- A failed response header write in `HTTPStream` is terminal rather than
  retryable.
- With a response header sent, `Stream` enforces message rules upstream leaves
  to the caller: a declared Content-Length, and no DATA after trailers.
- `Server.Logger` nil logs nothing; upstream logs handler panics to
  `slog.Default()`.
- `Stream.QlogTrace` and `RequestStream.QlogTrace` are absent: no qlog here.
