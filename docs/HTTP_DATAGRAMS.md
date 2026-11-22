# HTTP datagrams and capsules

Target: [RFC 9297](https://www.rfc-editor.org/rfc/rfc9297.html), checked
2026-09-25. Public HTTP/3 client and server datagrams are implemented for explicitly
declared Extended CONNECT extensions. Capsule integration and the validation work
listed below remain unfinished; this is not complete extension conformance.

## Public endpoints

ClientConfig.EnableDatagrams enables HTTP datagrams and bounded request queues.
HTTP Dial enables QUIC reception on a copy of the supplied configuration;
NewClientConn requires an already-enabled QUIC connection. OpenDatagramTunnel
requires peer QUIC support, Extended CONNECT permission and both H3_DATAGRAM
advertisements before sending request headers. Tunnel.SendDatagram and
ReceiveDatagram expose payload bytes without the HTTP quarter-stream-ID prefix.

Server.DatagramHandlers declares protocol tokens with datagram semantics and
enables both HTTP datagram and Extended CONNECT settings. Each selected handler
validates its extension handshake, calls AcceptTunnel, and uses ServerTunnel's
SendDatagram/ReceiveDatagram methods. Ordinary ExtendedConnectHandlers declare
byte-stream semantics only; overlapping registrations are rejected. Unknown
protocols receive 501 unless an unsupported datagram has already required request
abort. EnableDatagrams also allows advertisement without registered extensions.

Servers require a QUIC listener/connection configured with EnableDatagrams. Serve
and ServeQUICConn reject missing local reception before taking ownership.
Listener.DatagramsEnabled and Conn.DatagramsEnabled report local capability;
ConnectionState.SupportsDatagrams reports peer support. Send enforces HTTP and
QUIC permissions and transport size limits. No oversized datagram is converted
into a capsule automatically, and successful enqueue does not promise delivery.

Both public endpoint configurations default to 16 queued payloads per request and
1200 receive payload bytes; the configurable maximum payload limit is 65535.
Oversized payloads and newest arrivals to a full queue are dropped. Empty payloads
occupy a queue slot. Request admission bounds registered requests. Extensions own
handshake and payload semantics; ordinary CONNECT has no datagram semantics.

## Dispatch and lifetime

Session owns and joins the datagram receive loop. DatagramDispatch parses quarter
stream IDs and copies accepted payloads into per-request queues. Unopened and
receive-closed IDs are dropped. A datagram on a request without supported semantics
aborts that request with 0x33; malformed quarter IDs abort the connection with the
same numeric code. Transport calls and request-abort callbacks run outside locks.

Clients register known semantics before writing headers. Servers register pending
requests before parsing headers and buffer within the same queue/payload bounds.
Pending data is not exposed until initial headers identify a registered datagram
extension. Unsupported semantics abort even if an early payload was dropped for
its size. Closed receive sides discard pending payloads. A dedicated abort context
cancels server handlers even when they are not reading the stream body.

DatagramStream binds dispatch to transport lifetime. Close/CancelWrite interrupt
and join admitted datagram sends before transport FIN/reset. Tunnel.CloseWrite
also handles asynchronously queued upload EOF. Close/body cleanup unregisters the
request and wakes receivers. Receive closure does not prohibit sending. Caller
cancellation can interrupt send admission, transport queue waits and receive waits;
a canceled receive does not consume a queued payload.

ReceiveStream.ReceiveContext reports complete FIN data, remote reset, local
cancellation or connection termination independently of application Read. FIN
with gaps completes only after missing bytes arrive, and buffered stream bytes
remain readable. The datagram observer wakes waiters; synchronous checks during
registration, delivery and dequeue prevent delayed callbacks from admitting late
payloads or aborting already-closed unsupported requests.

## Wire primitives and capsules

internal/http3wire implements quarter-stream-ID encoding/decoding (including empty
payloads and nonminimal varints), alignment/range checks, malformed/truncated ID
errors, and boolean SETTINGS_H3_DATAGRAM validation. SettingsState publishes
immutable snapshots and gates HTTP sends on both advertisements; absent/zero,
malformed or repeated settings cannot grant permission.

The capsule reader streams arbitrary type/length/value sequences without allocating
from declared lengths. It accepts all capsule types, maps truncation to request
H3_MESSAGE_ERROR and requires each payload to be consumed before reading another
header. DATAGRAM capsules omit the quarter-stream ID. The owner must skip unknown
capsules (or forward them when acting as an intermediary). Reads are serialized;
underlying stream cancellation/deadlines interrupt I/O.

Public CapsuleProtocol parses the Capsule-Protocol Boolean Item, treating false,
repeated, non-boolean and malformed values as absent. Unknown parameters are
validated using RFC 9651, including dates and UTF-8 display strings, then ignored.
The header alone does not activate capsules. Internal MessageConfig.Capsules
provides explicit header/status validation for selected capsule extensions, with
sticky request-scoped errors. Server.CapsuleProtocols enables this policy for
registered ExtendedConnectHandlers or DatagramHandlers. Incoming fields are checked
before handler dispatch; outgoing fields/statuses are checked before commitment,
including informational/rejection responses and responses completed on handler
return. Registrations are validated before serving. Capsule-Protocol alone never
selects the policy. ClientConfig.CapsuleProtocols selects the same policy for outgoing Extended
CONNECT tokens. Its map is copied on client creation. Outgoing headers are checked
before concurrent exchange starts; incoming headers are checked before response
adaptation can discard successful CONNECT content metadata. Both ordinary and
HTTP-datagram tunnels use this selection; it does not enable QUIC datagrams.

Public NewCapsuleParser / CapsuleParser.Next expose bounded CapsuleReader values
with Read, ReadByte, Remaining and Discard. Readers share payload position and
expire after a successful advance; an unconsumed payload prevents advance without
invalidating its reader. Truncation returns CapsuleError (request code 0x10e), and
clean inter-capsule EOF remains EOF. WriteCapsule accepts an io.Writer and writes
header/payload separately, rejecting out-of-range types before output and detecting
short writes. Owners serialize calls and reset/abandon streams after write errors.
Tunnel.CloseWithError and ServerTunnel.CloseWithError let extensions abort both
request directions with
an explicit stream error code, cancel its handler context and join tunnel I/O. The
first request failure is retained. Normal close and error close share once-only
cleanup; a late abort cannot change an already-closed tunnel outcome. Concurrent
client response-body cleanup releases its reservation exactly once. This does not
close the QUIC connection.
NewCapsuleParser detects sources implementing CloseWithError (including both
public tunnel types). Malformed capsule headers/payloads then automatically abort
with 0x10e and release tunnel resources. Plain readers retain report-only behavior;
wrappers must forward CloseWithError to preserve automatic abort. Underlying
connection errors keep their original scope. Clean EOF and pending-payload errors
do not reset requests. Capsule negotiation and header/status policy remain separate.

## Evidence and remaining work

TestHTTPDatagramsIndependentServer now exercises the public client against pinned
upstream quic-go v0.62.0: empty/small/900-byte echoes, send FIN and receive cleanup,
and copied dial configuration. TestHTTPDatagramsIndependentClient exercises the
public server with two upstream request streams, reverse-order echoes and FIN
closing reception without application EOF. Both roles cover v1/v2 and IPv4/IPv6.

Live negative tests cover absent HTTP/QUIC support, unsupported GET datagrams in
both roles, handler cancellation without body reads, subsequent connection/request
slot reuse, and validation preserving a listener without QUIC datagram support.
Unit tests cover pending semantics, bounded queue ownership, overflow/oversize,
abort/detach races, negotiation directionality and transport close notifications.
Wire fuzzers cover HTTP datagram and capsule parsing. Public codec interoperability
covers both directions against quic-go v0.62.0 with varint boundaries, reserved
HTTP/2 type numbers, byte-at-a-time reads, and empty/69KB payloads. A live independent
client test exercises capsule echoes over Extended CONNECT on v1/v2, unknown-type
discard and clean FIN. The server-role test covers the same capsule exchanges and
malformed capsule payloads: the parser automatically resets with 0x10e, followed by a
successful GET using the sole request slot on the same connection. Both fixtures
explicitly own extension handshake validation. The server fixture enables CapsuleProtocols; the handler invokes the streaming parser, which now owns malformed-input abort.
See PROGRESS.md for run results.

Live negative client policy tests verify remote 0x10e and subsequent connection/
single-slot reuse for forbidden response metadata/statuses and invalid header
placement. Isolated cases pass on v1/v2; the sequential v1 group passes. The longer
sequential v2 group rejects its final informational response, then times out on
the subsequent GET after upstream enters key phase 1. It
remains under QUIC_INTEROP_KNOWN_FAILURES=1 as an unresolved limitation; see PROGRESS.

Live client truncation tests also verify automatic reset and slot/connection reuse
without manual tunnel Close. Generic-reader tests check exactly-once abort, sticky
errors, clean EOF and preservation of unrelated connection errors. Direct
io.ErrUnexpectedEOF also triggers capsule truncation handling. A public parser
fuzzer compares normal, byte-at-a-time and data-plus-EOF reads for identical
capsule sequences, errors and abort counts.

Remaining work includes resolving the sequential v2 key-update interoperability
gate, extension-specific
capsule semantics, impaired-network datagram tests, broader API parity and load/
performance evidence. Remembered settings and 0-RTT validation are not implemented.
No universal performance or complete RFC/extension coverage claim is made here.
