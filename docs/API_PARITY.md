# Public API parity inventory

Initial Config inventory, 2026-09-28. Current independent peer:
`github.com/quic-go/quic-go v0.65.0` (pin moved from v0.63.0 on 2026-11-22; dated
v0.63.0 and v0.62.0 statements below were not re-verified). The initial behavior inspection used v0.62.0
`interface.go`, `config.go`, `connection.go` and `internal/protocol/params.go`.
The exported interface.go is unchanged in v0.63.0; other behavior comparisons
below retain their v0.62.0 evidence unless explicitly refreshed. This is not
a full API parity claim.
The upstream dependency remains confined to the interop module. This inventory
records contracts; production implementation remains independent.

The export-data audits, regenerated against v0.65.0 on 2026-11-22, supply reproducible symbol-level baselines:
[root package](API_ROOT_INVENTORY.md) and [HTTP/3](API_HTTP3_INVENTORY.md).
The root has 4 missing entries out of 253 reference entries; HTTP/3 has 2 out
of 163. Entries include types, fields, methods and functions, so these counts are
not counts of independent features. Matching declarations do not establish
behavior. Differences also include harmless parameter names, internal alias
paths and the local Stream's promoted value-method availability.

HTTP/3 now exports NextProtoH3, CapsuleProtocolHeader and the package-level
ConfigureTLSConfig helper. The helper selects h3 on clones, recursively wraps
GetConfigForClient selections, preserves fallback/error results and application
callbacks, and initializes source ticket keys before cloning. Two configured
clones can decrypt each other's tickets. This helper does not bind HTTP SETTINGS
to tickets; Server.ConfigureTLSConfig retains that richer early-data policy.
Direct v0.63.0 contract checks cover nested selection, clone isolation and callback
fallback/error behavior. Live upstream clients require the helper to fix a selected
configuration's deliberately non-h3 ALPN in both v1/v2. Disabling that wrapping
fails both handshakes; omitting ticket initialization fails cross-clone decryption.
HTTP/3 inventory totals are 2 missing, 7 review differences and 154 identical
entries out of 163. No full HTTP/3 feature parity is claimed. The two missing
entries are Stream.QlogTrace and RequestStream.QlogTrace, which need qlog.

The raw HTTP/3 cluster is implemented with matching declarations: Stream,
RequestStream, HTTPStreamer, RawClientConn, RawServerConn, FrameType, StreamType,
Transport.NewClientConn/NewRawClientConn, Server.NewRawServerConn,
ClientConn.OpenRequestStream/HandleBidirectionalStream, Server.IdleTimeout and
the Server and Transport Logger fields. [HTTP3_RAW_STREAMS.md](HTTP3_RAW_STREAMS.md)
records how they map onto the session internals and where behavior knowingly
differs: extension routing stays with ExtendedConnectHandlers/DatagramHandlers,
raw streams never use 0-RTT, ReadResponse returns the final response, a raw
stream holds its request slot until both directions finish, and a response
header that was sent brings message validation with it. Interop tests run a raw
request stream against the reference server and the reference client against
HTTPStreamer, each with DATA and HTTP datagrams, over v1 and v2, from served and
raw connections.

HTTP/3 Transport.Dial now accepts an application QUIC dialer. The callback gets
host:port, cloned TLS/QUIC settings, h3 ALPN and an origin-derived ServerName
unless one was explicitly supplied. Returned connections enter the existing
origin pool, SETTINGS-bound session cache, early rejection recovery and shutdown
ownership. Callbacks must honor cancellation; nil connections fail explicitly.
Independent v1/v2 exchanges use a concrete application UDP Transport, then
AddPath/Probe/Switch to another socket and continue HTTP on the same pooled
connection. The upstream handler observes the new source address. Independent
custom-dial early tests cover acceptance/rejection, optional retries, cached-only
requests, concurrent requests and closure during the handshake. These tests do
not establish arbitrary multipath combinations or full callback contract parity.
A gated-handshake fixture also checks callbacks returning early connections while
HTTP early data is disabled or the callback bypasses the HTTP SETTINGS cache.
Both v1/v2 and accepting/rejecting peers must resume and serve exactly once after
the gate opens; no early wire packet or completed HTTP setup is observed during
the bounded gate interval. Removing the handshake wait fails all eight cases.
Transport.NewClientConn and the raw connection APIs are described above.

HTTP/3 Server.Port, SetQUICHeaders and ErrNoAltSvcPort now support Alt-Svc
advertisement. The cached value follows active listener registration order,
preserves duplicate port entries, honors Port overrides and falls back to Addr
only when listeners provide no usable ports. Calls append to existing headers;
no listener means no advertisement. Independent reference comparisons cover
registration, removal, duplicate ports, override/fallback and preservation of
existing values. Live upstream-client v1/v2 startup tests read the advertised
bound port from our response. Local advertised ports are restricted to 1–65535;
invalid-number behavior is not part of the reference parity claim.

HTTP/3 Server now has Addr, TLSConfig and QUICConfig plus ListenAndServe and
ListenAndServeTLS; package ListenAndServeQUIC delegates to the file-based method.
Startup clones caller configuration, defaults an empty address to :https, enables
QUIC early-data acceptance only when QUICConfig is nil, and enables transport
DATAGRAM reception for HTTP datagrams. HTTP replay policy remains separate.
File credentials use a fresh TLS configuration, matching the pinned convenience
API. Owned sockets close on shutdown or failed registration. Own-client and
independent v1/v2 tests cover both credential methods, graceful/abortive closure,
configuration isolation and socket reuse. Independent tests also preserve an active
response across a timed-out graceful shutdown before draining and releasing the
socket. The package helper has missing-file coverage; simultaneous TCP/TLS+QUIC
ListenAndServeTLS remains missing.

StreamID.StreamNum now returns the one-based ordinal for its initiator/direction
class. Its inferred return value also reconstructs IDs through StreamID, including
the zero-count invalid-ID sentinel. External-package tests cover all four classes
and the 62-bit boundary. The isolated v0.63.0 comparison checks 1,031 input IDs,
signed extremes, and valid/invalid reverse-conversion arguments. These conversions
do not authorize invalid stream IDs on the wire. The return type is an independent
internal/transport.StreamNum, so the inventory records a declaration difference
rather than an identical internal/protocol type. Root totals are 4 missing, 58
review differences and 191 identical declarations. Remaining missing entries are
Config.Tracer, Transport.Tracer, Conn.QlogTrace and ConnectionState.GSO, which
need qlog and GSO.

The seven root symbols v0.65.0 added are implemented. With
Config.RequireStreamCreditRelease, a peer-initiated stream's MAX_STREAMS credit
returns only after the engine has reclaimed the stream and the application has
called ReleaseStreamCredit, in either order; repeated calls, calls on locally
opened streams and calls without the setting do nothing. Config.Clone copies the
setting, GetConfigForClient can select it per connection, and
ConnectionState.RequireStreamCreditRelease reports it. Own-peer tests cover both
roles and stream directions; an interop test runs one script with each
implementation withholding credit from the other, in both TLS roles.
ConnectionID.Valid distinguishes the zero value, now printed as "(invalid)" and
no longer equal to ConnectionIDFromBytes(nil), from a zero-length ID; a generator
may still return either for zero-length IDs. ConnectionStats.CongestionWindow
publishes the sender window of the path whose RTT the snapshot reports.
ErrStreamClosed is the error Write, WriteWithLimit and TryWriteAll return after
Close, and, as in the reference, the send Context's cancellation cause after a
normal Close (previously context.Canceled; Context().Err() is unchanged).

Conn.AddPath, Path.Probe, Path.Switch, ErrPathClosed and ErrPathNotValidated now
have matching declarations. Handles bind a separate Transport, validate without
switching ordinary traffic, and commit on explicit Switch. Cancellation, closure,
repeated own-peer switching and bounded upstream sequences have race coverage.
One uncommitted candidate per QUIC path, concrete same-family addresses and
upstream repeated-probe CID exhaustion remain limitations; see
[MIGRATION_API_PLAN.md](MIGRATION_API_PLAN.md). This is not full behavior parity.

SupportedVersions now returns independent v1/v2 slices in default preference
order. Version.String supplies supported, historical and hexadecimal names;
historical formatting does not enable those wire versions. An isolated v0.63.0
comparison covers the historical numeric interval and 10,000 deterministic random
values. QUICVersionContextKey now exposes the version at TLS startup through
ClientHelloInfo.Context. Parent metadata remains available, and a stale parent
value under this key is replaced. Compatible negotiation can subsequently change
the version; ConnectionState remains the negotiated-version API. Own-peer v1/v2
switching and upstream-client callback tests cover this distinction and normal
metadata propagation. Reference contract: interface.go / internal/protocol/version.go
and internal/handshake/crypto_setup.go in pinned v0.63.0, checked 2026-09-30.

Package-level Dial, DialEarly, Listen and ListenEarly now use dedicated runtimes
on caller-owned PacketConn sockets. Listener Close terminates accepted connections;
failed setup joins readers; a successful dial releases its reader after terminal
closing/draining. The borrowed socket remains open with cleared deadlines. Native,
plain and OOB wrapper v1/v2 lifecycle tests cover local/peer close and raw socket
reuse. Cached early-data acceptance/rejection and compatible switching are tested;
upstream exchanges cover both roles and both helper modes (without cached 0-RTT
in that independent fixture). These convenience calls require exclusive socket
use; Transport remains the shared-socket API. Reference helper contracts checked
2026-09-30: https://pkg.go.dev/github.com/quic-go/quic-go@v0.63.0#Listen and
https://pkg.go.dev/github.com/quic-go/quic-go@v0.63.0#Dial .

Transport.ConnContext now derives incoming TLS, connection and send-stream
contexts from an admission callback. Callback input and returned lifetime are
canceled on rejection, setup failure or terminal connection events. The hook
survives listener replacement and does not run for dialing. Rejection uses a
fixed CONNECTION_REFUSED response without private callback error text. Callback
execution is synchronous on the endpoint owner and must not block or reenter.
Qlog context propagation awaits the missing qlog API. Outgoing lifetime contexts
now retain dial context values while dropping its deadline and cancellation;
normal and early dialing cover connection and stream propagation.

SendStream.SetPriority (also promoted by Stream) now clips urgency to 0–7,
with default urgency 3/incremental true. New and retransmitted STREAM frames
respect urgency within their respective classes; retransmissions precede new
data. Incremental streams share committed packet turns with the smallest-ID
non-incremental stream. Reprioritization applies to queued and future work.
Non-STREAM queue slots remain fixed. Source-queue selection for path borrowing
uses the same priority rules. Global priority across separately queued paths,
scalable queue indexing and broad priority fairness validation remain
unfinished. This adds two declarations, not a complete scheduling parity claim.

HTTP/3 server request Priority headers now feed SetPriority before handler
dispatch, including datagram wrappers. The independent RFC 8941 Dictionary
parser implements RFC 9218 defaults (urgency 3, incremental false), duplicate-key
replacement, ignored unknown/wrong-type values and whole-field failure fallback.
Repeated header lines are combined. Control/QPACK send streams receive urgency
zero/incremental true before their first write to avoid starvation by urgent
response data. PRIORITY_UPDATE wire parsing and role/placement/ID checks now feed a bounded
request-priority tracker. Latest valid updates override request headers, including
updates received before acceptance, and apply to active requests. Both pending
and active maps are bounded by MaxConcurrentRequests; completed requests need
no per-ID tombstones. Live QUIC stream-limit checks and response-side overrides
remain.

ConnectionStats now exposes all ten fields through an owner-published value
snapshot. Live v1/v2 tests cover concurrent reads, owner-side callbacks, transfer
and shutdown retention. Accounting policies and remaining differential/lifecycle
gaps are recorded in [the statistics implementation plan](CONNECTION_STATS_PLAN.md).
This removes twelve missing declarations, without claiming identical upstream
behavior for every malformed-input, loss-history or multipath case.

ConnectionID and ConnectionIDFromBytes now provide comparable immutable values,
length, independent byte copies and lowercase hexadecimal/empty formatting.
Root tests cover all lengths 0–20, equality/map-key identity, byte ownership and
oversized-input panics. An isolated upstream comparison covers 5,376 values and
mutation independence. This removes five missing declarations. The representation
uses an immutable string; no allocation/performance parity is claimed. Application
CID generators and shared-transport routing integration remain missing.

Transport now exposes Conn, Listen, ListenEarly, Dial, DialEarly and Close, with ErrServerClosed
and ErrTransportClosed. Borrowed *net.UDPConn lifetime, generation-bound listener
restart and survival of accepted connections have live v1/v2 tests; independent
upstream peers also transfer streams in both directions after restart. Generic PacketConn
wrappers now work when they expose UDP addresses and honor deadlines. Plain
wrappers disable ECN/kernel MTU probing; UDP sockets retain the specialized path.
OOBCapablePacketConn wrappers retain socket controls and ancillary I/O; preferred
socket registration and other Transport options remain unsupported. Nine declarations were
added; the Transport type is not a claim of complete behavior or option parity.

Shared Dial/DialEarly tests cover simultaneous incoming/outgoing connections,
multiple dials to one peer, cancellation without closing other connections,
reusable capacity after failures, and authenticated version fallback. Existing
cached-ticket 0-RTT acceptance/rejection and compatible-version tests now also run
on the shared adapter. Own-peer multipath and independent v1/v2 simultaneous-role
stream transfers pass. This does not establish full independent extension or
migration combinations on shared sockets.

Generic PacketConn tests include non-comparable wrapper values, typed nils,
borrowed socket reuse, listener restart, shared incoming/outgoing connections,
own-peer multipath and independent v1/v2 stream transfers. Arbitrary non-UDP
address families are outside this UDP transport adapter. These behavior additions
do not change the export inventory count.

OOBCapablePacketConn now exposes the reference capability interface and enables
wrapper-preserving ReadMsgUDP/WriteMsgUDP plus SyscallConn socket configuration.
IPv4/IPv6 tests verify incoming ECN and per-datagram outgoing markings without
leakage; root and independent v1/v2 tests cover wrapped shared transports. Twelve
inventory entries (the interface and its inherited/declared methods) are added;
this is one capability interface, not twelve independent transport features.
Plain PacketConn wrappers remain usable with reduced socket capabilities.

Transport.WriteTo and ReadNonQUICPacket now support raw packet I/O on the primary
socket alongside QUIC. Reception starts on first read, retains at most 32 packets,
drops new packets on overflow and consumes a truncated packet in one read. The
reference's documented two-clear-leading-bits predicate is applied only after
known QUIC routes and reset tokens take precedence, preserving negotiated QUIC-bit
greasing. This precedence is an intentional extension rather than exact first-byte
classification parity. Concurrent readers, shutdown, socket wrappers, live greased
streams and independent bidirectional raw/QUIC traffic are tested.
Reference contract: https://pkg.go.dev/github.com/quic-go/quic-go#Transport.ReadNonQUICPacket
(checked 2026-09-29). Two more method declarations are supplied.

Transport.DisableVersionNegotiationPackets now snapshots an endpoint-wide policy
before first use. It suppresses stateless unsupported-version responses across
listener replacement, while supported handshakes, accepted connections and client
processing of received Version Negotiation stay active. V1/v2 live tests and an
isolated reference-policy comparison pass. Contract checked against
https://pkg.go.dev/github.com/quic-go/quic-go#Transport on 2026-09-29.

The largest missing API groups are remaining Transport/socket capabilities, qlog,
path operations and additional stream I/O. SetPriority now has STREAM scheduling
semantics and HTTP request/update integration; broader independent scheduling
comparisons remain unfinished.

Regenerate from the repository root with `go run ./scripts/api-inventory` and
`go run ./scripts/api-inventory -package=http3`, redirecting to the linked files.
The tool loads the reference through interop/go.mod and resolves aliases,
promoted fields/methods and pointer-only method sets using Go export data.
Build tags/platform follow the environment; tags, generic constraints and
behavioral contracts still require manual audit. Other reference packages,
including qlog, qlogwriter and quicvarint, are not covered by these two baselines.

| Reference Config field(s) | Current implementation | Remaining difference or evidence gap |
| --- | --- | --- |
| GetConfigForClient | Present with ClientInfo; selected settings reach accepted handles | V1/v2 live selection/default/refusal gates; listener-wide policy, combined extensions, callback scheduling and lifecycle comparison remain |
| Versions | Present | Complete preference/default/negotiation behavioral comparison remains open |
| HandshakeIdleTimeout | Present; zero selects 5 seconds | Engine activity/expiry and public silent-peer tests; independent stalled-flight gate still needed |
| MaxIdleTimeout | Present; zero selects 30 seconds | Full reference behavior audit remains open; existing tests cover negotiated limit and PTO floor |
| TokenStore | Present | Complete interface, key ownership and eviction behavior audit remains open |
| InitialStreamReceiveWindow | Present; zero selects 512 KiB | Default matches reference; ours rejects values above 16 MiB, reference clips at varint bound |
| MaxStreamReceiveWindow | Present; zero selects at least 6 MiB | Normal default matches reference; growth algorithm and large-window memory design remain open |
| InitialConnectionReceiveWindow | Present; zero selects 768 KiB | Default matches reference code; ours rejects above varint bound rather than clipping |
| MaxConnectionReceiveWindow | Present; zero selects at least 15 MiB | Normal default matches reference; clipping and tuning behavior differ |
| AllowConnectionWindowIncrease | Present with matching signature | V1/v2 denial/retry and public binding tests; total memory behavior unproven |
| MaxIncomingStreams, MaxIncomingUniStreams | Present | Matching zero=100, negative=disabled and 2^60 clipping; complete lifetime/behavior audit remains open |
| KeepAlivePeriod | Present | Saturating 1.5-base-PTO floor; multipath uses the eligible idle path with smallest PTO. Connection liveness only, not per-path middlebox retention |
| InitialPacketSize | Present; default 1280, nonzero clipped to 1200–1452 | Migration policy parity and constrained-path/kernel validation |
| DisablePathMTUDiscovery | Present | Platform/runtime and probe policy comparison remains open |
| Allow0RTT | Present | Independent accepted/rejected early tests exist; combined-extension and replay/lifecycle coverage incomplete |
| EnableDatagrams | Present | Complete API/error behavior audit remains open |
| EnableStreamResetPartialDelivery | Present, with asymmetric connection-state flags and public boundary/cancel methods | Sender/receiver interop passes v1/v2 and both TLS roles; selected receive policy covered. Both early roles, selected policy and one-packet loss covered; broader impairment/concurrency audit remains (see PARTIAL_STREAM_RESET.md) |
| Tracer | Missing | Compatible observability hooks and qlog output |

The reference interface comment says 512 KiB for the initial connection window,
but its actual default is 1.5 × 512 KiB = 768 KiB; the table follows code.

HandshakeIdleTimeout uses the existing authenticated receive / first
ack-eliciting send activity accounting, with no negotiated PTO floor on this
local resource limit. The idle limit ends at TLS completion. HandshakeTimeout
is an additional local field: zero derives a saturated twice-idle duration;
explicit values override that absolute duration. Both handshake limits now end
at TLS completion, matching the v0.63.0 connection timer's completion boundary.
QUIC confirmation remains distinct: handshake keys and PTO are retained until
confirmation, while ordinary negotiated idle expiry still applies. V1/v2 engine
tests complete real TLS, delay confirmation beyond the absolute limit, and check
PTO output, eventual confirmation/key discard, and idle expiry. A live v0.63.0
peer test holds server short-header packets for 750 ms after server TLS
completion while the independent client's absolute handshake limit is 500 ms;
Dial succeeds after release in both versions. This does not establish complete
timer-policy parity (including all pre-completion stalls and idle floors).
Exported IdleTimeoutError and HandshakeTimeoutError implement net.Error and
unwrap to net.ErrClosed. Their text remains local. Handshake inactivity is
classified as idle expiry; absolute expiry takes precedence when both limits
have passed before TLS completion.

The completion/confirmation distinction follows
[RFC 9001 sections 4.1.1–4.1.2](https://www.rfc-editor.org/rfc/rfc9001.html#section-4.1.1).
The choice of handshake timeout boundary is a reference implementation policy,
not a timeout duration mandated by that RFC.

Other local configuration extensions include compatible-version negotiation,
initial-version override, QUIC-bit greasing, multipath, preferred addresses,
client bind address and connection admission count. Their presence does not
compensate for missing reference capabilities.

Next inventory sections must cover package-level functions and types, Transport,
Listener/EarlyListener, Conn, streams, connection state, errors, HTTP/3 and qlog.
Nonblocking `Conn.OpenStream` and `Conn.OpenUniStream` now return
`*StreamLimitReachedError` when peer stream-count credit is exhausted. Its message
matches v0.63.0's `too many open streams`, and callers can use `errors.As` or
`errors.Is` with the public type. The wrapper also preserves the historical local
engine would-block cause. Synchronous opens keep waiting for credit and return
context cancellation/deadline errors; stream-count exhaustion is not a connection
failure. Public v1/v2 tests cover both roles, disabled/positive limits and renewed
credit. `TestStreamsBlockedUpstream` compares the typed nonblocking errors against
the pinned peer while retaining protected STREAMS_BLOCKED and synchronous
cancellation checks. Complete error/API parity remains unproven.

`DatagramTooLargeError` now exposes `MaxDatagramPayloadSize int64` and matches
errors with the same limit via `errors.Is`. SendDatagram and SendDatagramContext
return this public type for oversized messages, preserving the previous engine
sentinel through unwrapping. The limit is the smaller of peer frame credit minus
the type byte and the selected path's packet payload budget. Early sends retain
remembered credit and long-header overhead; established sends follow current
path size. Tests cover peer credit, configured size, path shrink and early-data
limits. Independent v1/v2 peers in both roles receive a boundary-sized payload
after an oversized call reports that boundary. This is a current admission
snapshot, not a delivery guarantee: subsequent path changes can cause queued
DATAGRAMs to be discarded. Conservative CID/header reservation can produce a
smaller reported limit than the reference, so exact payload-size parity remains
open.

`SendStream.TryWriteAll` (also promoted on Stream) now queues a whole slice or
returns public `ErrWouldBlock` without consuming credit or appending a prefix.
It ignores the write deadline and uses a nonblocking writer lock. The owner
preflights both flow limits, all per-frame queue charges and any partial-reset
reservation before committing portable chunks in one serialized operation.
It can wait briefly for the owner, but never waits for flow credit or queue space.
Engine tests cover refusal at either flow limit, a one-byte queue shortfall,
retry, loss recovery and caller-buffer ownership. Public and independent tests
cover expired deadlines and exact delivery, with v1/v2 and both roles; independent
delivery covers both stream directions. The bounded local queue and portable
chunk policy differ from the reference's buffer strategy. Reservation-boundary
tests now cover established sends, accepted TLS resumption and a synthetic
multipath survivor after primary abandonment, in both versions/directions. They
verify one-byte-short rejection, exact-fit admission, reservation reuse and FIN
ACK cleanup. A real TLS rejection matrix verifies transmitted atomic early
frames with delivered/dropped packets, retirement of sent references and reset
reservations, and fresh same-ID writes. Public adapter tests separately verify
that stale TryWriteAll/WriteWithLimit/Peek handles cannot access a reused stream
or invoke a limiter. Full-size/fairness and independent early/multipath atomicity
validation remain open.

`SendStream.WriteWithLimit` (also promoted on Stream) now applies an external
allowance to each newly admitted reliable frame, after stream/connection credit
and local queue admission checks. Results are clamped to the offered range; a
restricted prefix returns `ErrWriteLimitReached` and its committed byte count.
Retransmission retains the committed frame without invoking the limiter again.
The callback runs on the owner and must not block or call QUIC methods. A nil
limiter uses the ordinary Write path. Engine tests check flow-control offers,
queue pressure, zero/negative/oversized results, loss recovery and accounting;
independent v1/v2 peers in both roles/directions receive exact bytes after a
limited write and explicit suffix retry. A public 24-case v1/v2 matrix covers
both roles/directions with a three-byte stream and connection window: deadline
expiry, changing a live deadline and cancellation preserve the admitted count
and single external charge. After expiry, peer reads permit an explicitly limited
retry and a nil-limiter suffix; canceled retries never invoke the limiter.
Independent deadline/cancellation and early/multipath limiter-specific matrices
remain open.

`ReceiveStream.SetReceiveFinalSizeCallback` now observes validated FIN, RESET_STREAM
and RESET_STREAM_AT final offsets, including when receive gaps remain. A pending
callback can be replaced or removed with nil; duplicate frames do not notify it
again. Later registrations report a known size synchronously outside the owner,
including after stream release/runtime shutdown. Deferred callbacks execute on
the owner and must not block or call QUIC methods. Closure before learning the
final size does not invoke a callback. Engine tests cover incomplete FIN/reset
prefixes and invalid final sizes; public tests cover registration lifecycle and
shutdown; independent v1/v2 FIN/partial-reset transfers check one correct callback
in both roles/directions. Early/multipath callback-specific matrices and complete
concurrent registration/closure ordering remain open.

`ReceiveStream.Peek` (also promoted on Stream) now fills a caller buffer without
consuming bytes, advancing receive credit, or marking terminal data as read. It
waits for the requested size, with a short result only at EOF or a partial-reset
reliable boundary, and honors the mutable read deadline. Exact-sized terminal
prefixes return successfully; a larger request returns the prefix plus terminal
error. Read and Peek serialize through the same mutex. Empty Peek returns
immediately. Tests cover gaps/wrap, deadline retry, repeated peeks, unchanged
credit, read-after-peek and retained data after runtime shutdown. Independent
v1/v2 peers cover both roles/directions and FIN/partial-reset endings. The
absolute-offset receive-storage oracle now validates random Peek requests and
snapshots all receive storage/accounting to detect mutation. A bounded campaign
completed 28,292 executions; near-varint-limit and wrapped-ring seeds also pass
under the race detector. Broader cancellation, early-data, concurrent deadline
and sustained fuzz matrices remain open.

`VersionNegotiationError` now includes the reference's `Ours` and `Theirs` version
lists and unwraps to `net.ErrClosed`. Existing `ChosenVersion` and `PeerVersions`
fields remain available. The engine records the attempted version; DialAddr's
no-common-version return snapshots the complete configured preference list.
Peer offers are unauthenticated and must not be treated as proof of server
capabilities. A synthetic UDP rejection test covers both preference orders and
slice ownership; the independent public dial rejection gate checks the offered
version and error identity. Error text remains local. Post-return early fallback
and complete version-negotiation API parity remain open.

Config.Clone now matches the v0.63.0 shallow-copy contract: scalar fields and
slice headers are copied; referenced storage, pointers, callbacks and TokenStore
remain shared. No defaults or validation are applied. The receiver must be
non-nil and may not be modified concurrently with cloning. A comparison test
runs scalar mutation, shared Versions storage, and slice reassignment against
both implementations. Local extensions (LocalAddr and PreferredAddresses) follow
the same shallow-copy rule. HTTP/3 uses Clone when enabling QUIC datagrams for a
dial without changing the caller's scalar configuration. Signatures alone do
not establish broader behavior parity.


InitialPacketSize implementation gates:

- The engine now accepts 1200–1452 for Initial padding, zero=1200, and clamps
  output to already-known peer UDP limits. V1/v2 handshakes cover 1200/1280/1452.
  Public Config now forwards its normalized value through both Dial and Listen.
- The reference actually defaults to 1280 and clamps nonzero inputs to
  1200–1452 in config.go (despite the interface comment describing small values
  as invalid). Public normalization now follows that contract.
- Original-path STREAM packetization and initial congestion sizing now use the
  selected size. A fixed MTU state permits fallback while discovery is disabled.
  DATAGRAM admission now uses the selected path limit; queued oversized messages
  are dropped after shrink, and smaller paths cannot borrow them. DATAGRAM local
  EMSGSIZE now drops the committed message, removes flight accounting and validates
  the baseline without retransmission. Wildcard runtimes use the configured size
  in the base builder and now recover oversized writes through baseline validation,
  reserved challenge capacity, STREAM splitting and unreliable-message dropping.
  They still lack address-aware discovery; kernel error and constrained-link gates
  remain incomplete. Path-zero adoption now preserves any prior base fallback,
  pending validation and cooldown rather than restoring the configured size.
- Current engine policy resets migrated/new paths to 1200, regardless of the
  original configured size. Tests cover peer UDP clamping, reset and discovery
  policy, and configured STREAM EMSGSIZE fallback/validation/splitting. Complete
  migration/rebinding, black-hole and independent transfer coverage. Inspection
  of the pinned reference connection.go migration handler confirms it resets MTU
  and congestion using InitialPacketSize, so our 1200 reset is a known behavioral
  difference, not an unverified match.
- Independent pinned-peer qlog verifies Initial sizes and maximum DATAGRAM
  delivery for defaults, 1200 and 1452, v1/v2 in both roles. Failed-path and
  migrated-path independent behavior remain open. No full parity claim.


HTTP/3 v0.63.0 compatibility refresh (2026-09-28):

- Non-CONNECT and extended CONNECT server requests now use origin-form URL metadata:
  URL.Scheme and URL.Host are empty, Request.Host carries authority, and RequestURI
  retains the path/query. Ordinary CONNECT retains authority-form RequestURI and
  URL.Host. Adapter and handler tests cover these distinctions, including escapes
  and OPTIONS `*`; live v1/v2 ordinary requests are checked in both peer roles.
- ClientConn.LocalAddr and RemoteAddr expose underlying connection addresses,
  checked during a live independent-server exchange.
- Public http3.Error, ErrCode constants, code/endpoint matching and Unwrap now
  exist. ClientConn request failures and response-body read/close boundaries
  convert stream resets and peer application closes while preserving original
  errors. Live reset/retry gates verify both HTTP and QUIC error discovery.
  Tunnel read/write/close and datagram return boundaries now also convert
  eligible errors. Synthetic partial-write, cancellation and cleanup tests and
  existing live tunnel/capsule/datagram gates pass. The upload worker now forwards
  failures through optional CloseWithError(error), retaining the original cause
  in tunnel pipes. A live v1/v2 peer-reset regression failed before the fix and
  passes ten race repetitions afterward; body closure remains exactly once.
  Server handler body read/close, response write/flush/error inspection and
  accepted tunnel operations now convert eligible errors. An internal hook
  preserves concrete writer interfaces and stored lifecycle errors. Live v1/v2
  request cancellation covers ordinary handlers and accepted CONNECT read/write/
  CloseWrite. Constructor, Dial/DialEarly, NextConnection and pooled RoundTrip
  return boundaries now convert eligible failures after cleanup. Live blocked
  critical-stream setup tests cover peer application close through constructor,
  both dial modes and pooled requests in v1/v2. Failed early reconstruction and
  exhaustive exported API/error-path coverage remain incomplete.
  Server datagram peer-failure and broader connection-close ordering gates are
  still incomplete. The root connection-close
  type now includes ApplicationError for local and remote application closes,
  with matching public fields and code/endpoint identity. Its chain retains
  legacy PeerCloseError or ErrLocalClosed, and Is matches net.ErrClosed. This
  differs from the reference direct Unwrap-to-net.ErrClosed chain. Local reasons
  retain full text while wire reasons keep the existing 900-byte bound. Broader
  error and close-lifecycle parity remains unfinished. Unknown ErrCode.String
  retains all 64 bits, unlike the reference fallback formatting.
- The isolated peer dependency is upgraded; production has no upstream dependency.
  Historical performance records retain their v0.62.0 reference; a separate
  v0.63.0 checkpoint now appears in docs/benchmarks/stream-echo.md. The strict v2 bulk/key-update failure also reproduces
  against v0.63.0; see interop/KNOWN_FAILURES.md.


Stream/transport error audit (2026-09-28):

- StreamError.Is now compares StreamID, ErrorCode and Remote, matching the
  v0.63.0 contract; it previously matched every StreamError. Type discovery
  continues through errors.As. Tests reject differing IDs/codes/endpoints and
  typed nil targets, with v1/v2 public local-write and peer-read cancellation.
- Public TransportError/TransportErrorCode and the reference's standard transport
  code constants now exist. Local protocol, TLS alert, transport-parameter,
  early-limit and CRYPTO reassembly failures are normalized when recorded as a
  terminal connection failure. Original local engine errors and TLS diagnostics
  remain in the chain, alongside net.ErrClosed. Remote transport closes preserve
  PeerCloseError in their chain. Application closes and unrelated failures are
  not reclassified. Matching uses endpoint, code and triggering frame.
- TLS close packets still omit local diagnostics; local errors preserve them.
  Crypto-code helpers cover 0x100–0x1ff, and unknown code formatting retains the
  full value. Public certificate/downgrade failures and protected peer close
  tests cover the new type. Exhaustive failure-source and network-owner mapping,
  formatting parity and every public operation remain to audit. Wire close-code
  selection now consumes the normalized error, sharing its code/frame mapping.
  Receive and network-owner close paths return the recorded normalized cause.
  V1/v2 tests compare public code/frame to protected Initial close packets for
  protocol, parameter, early-limit, CRYPTO buffer/conflict and TLS failures.
  Remote/unclassified errors do not elicit a protocol close.


Stateless reset error contract (2026-09-28):

- Public StatelessResetError now implements net.Error (Timeout=false,
  Temporary=true) and unwraps to net.ErrClosed. Existing engine sentinel
  matching remains intact. Temporary describes the reference API contract;
  it does not permit reuse of a reset connection.
- V1/v2 active-used-token detection now verifies the typed/net.Error contract,
  alongside existing unused/retired-token exclusion and silent draining tests.
  Public wrapping/type inspection is tested, and HTTP/3 preserves the reset
  type rather than converting it to an application error. A live v1/v2
  independent-server restart gate now retains the peer reset key/socket while
  dropping orderly close output and replacing connection state. Peer-generated
  resets reach the public context, subsequent stream writes and stream opening.
  Reverse-role generation, migration/multipath restart and all public-operation
  propagation remain open.


Admission selection groundwork (2026-09-28): internal ConfigureClient runs after
Initial integrity checks and Retry/NEW_TOKEN address validation, before creating
a connection or TLS state. It receives a shallow template copy and the verified
source tuple. Routing identity, Initial version and address-token policy remain
admission-owned. The callback is removed from the admitted connection and is not
invoked recursively. A callback error produces a fixed CONNECTION_REFUSED Initial
close without transmitting its local diagnostic. Tests cover both wire versions,
compatible switching, selected receive credit, Retry/NEW_TOKEN validation gates,
bounded deterministic refusal packets and client-observed transport error code.

This groundwork has since been wired to the public callback as described below.
Transport.VerifySourceAddress now controls Retry for unvalidated sources, and
ClientInfo.AddrVerified reflects actual token evidence. Nil skips Retry on shared
Transport listeners. ListenAddr retains its legacy mandatory-Retry policy.


Public GetConfigForClient wiring (2026-09-28): ClientInfo exposes RemoteAddr and
AddrVerified. The callback runs synchronously on the listener owner after address-token
processing; it must not block or reenter listener operations. An error or invalid
selected configuration refuses admission. Returning nil selects normalized
public defaults, matching the inspected v0.63.0 nil-return behavior; it does not
inherit the listener's connection defaults. Nested callbacks are ignored.
Stateless rejection can invoke the callback again for subsequent Initials.

Selected receive limits, stream counts, idle/handshake/keepalive timers, initial
packet size, PMTU policy, early-data policy, QUIC-bit greasing and DATAGRAM
negotiation feed the connection template. Immutable adapter metadata carries the
selected DATAGRAM reception and receive-window callback policies to the accepted
public handle. V1/v2 tests against v0.63.0 verify verified source address/port,
selected versus default stream limits, real DATAGRAM delivery and disabled local
reception, nested-selector suppression, callback error/invalid-config rejection,
and remote CONNECTION_REFUSED without local diagnostic text. The public window
policy test now exercises the selected server callback and its exact Conn handle.

Versions/compatible-version policy, admission capacity and preferred sockets
remain listener-wide. Selected preferred addresses must be absent or match the
listener's requested addresses; selection cannot open new sockets. Multipath
can be disabled for a connection, but enabling it requires listener opt-in;
early data plus multipath remains rejected. Client-only settings are irrelevant
to accepted connections. These policies are explicit local boundaries, not a
claim of complete reference parity. Further work includes combined early/migration/multipath coverage, callback
scheduling and version-policy audit.

Same-listener isolation and HTTP/3 integration (2026-09-28): v1/v2 tests keep
three upstream connections alive with distinct stream limits and DATAGRAM
policies, reject a fourth source, then transfer STREAM/DATAGRAM data over the
previously admitted connections. Listener.DatagramsEnabled now reports whether
DATAGRAM reception is enabled by default or may be selected by a callback;
Conn.DatagramsEnabled remains the actual connection policy. This lets HTTP/3
validate static disabled listeners early while allowing a selector to enable
DATAGRAMs. ServeQUICConn still rejects selected connections with reception off.
V1/v2 IPv4/IPv6 HTTP DATAGRAM echo and FIN-lifecycle tests cover selector-only
enablement, alongside static-policy coverage; a negative v1/v2 live test verifies
HTTP/3 closes selected disabled connections. Callback scheduling and combined
extensions remain separate open work.


Selected 0-RTT policy gate (2026-09-28): a v0.63.0 client resumes against the
same EarlyListener with valid NEW_TOKEN evidence. The selector enables early
DATAGRAMs for ticket issuance, then explicitly accepts or rejects them for the
resumed connection, opposing the listener default. Both v1/v2 gates observe an
actual 0-RTT packet, assert no Retry occurred, and hold the server flight so the
early accepted connection is exposed before client Finished. Accepted data is
delivered; rejected early data does not leak into the subsequent fresh DATAGRAM
read. Both sides report the selected Used0RTT value and successful TLS resumption,
including when early data is rejected. Selected DATAGRAM reception is enabled
while the listener default is disabled. Server responses before client Finished
and fresh post-handshake delivery remain covered. This does not cover all reduced
transport limits or combine early data with multipath, which remains unsupported.


Selected reduced-limit resumption (2026-09-28): v1/v2 independent-peer gates
keep Allow0RTT enabled but separately lower connection receive credit, stream
receive credit, bidirectional stream count, or unidirectional stream count after
ticket issuance. Each case has an unchanged-limit acceptance control. Actual
0-RTT wire packets and absence of Retry are checked; reduced limits reject early
DATAGRAMs while both peers report successful TLS resumption. The same resumed
connection delivers fresh DATAGRAMs and a 4 KiB unidirectional STREAM, exercising
credit updates with 1 KiB selected windows. Reduced stream-count cases also
check that an extra stream cannot open. The upstream client explicitly calls
NextConnection after rejection before using fresh streams.

This covers the public selector's four limit controls, not each of the three
stream-direction credit parameters independently; InitialStreamReceiveWindow
sets those together. Active CID limits and DATAGRAM-size reductions are not
selectable through these public controls. Broader remembered-parameter and
combined-extension coverage remains open. The required early-data rejection
boundary follows [RFC 9000 section 7.4.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-7.4.1).

`ClientConn.ReceivedSettings` now signals validated live SETTINGS, and
`ClientConn.Settings` exposes `Settings` (DATAGRAM/Extended CONNECT flags and
Other) through the new `Settingser` interface. Explicit QPACK settings are in
Other; maximum field-section size is excluded, matching the pinned peer's public
view. Unknown values are retained under the existing 64-entry / 65536-byte client
parser budgets. Snapshots are copied so caller mutation cannot affect negotiation
or later reads. The reference returns its shared settings pointer; pointer identity
and caller mutation are not reproduced. This API requires waiting for receipt;
the local implementation additionally returns nil safely before receipt.

Remembered 0-RTT settings do not close the receipt channel. Invalid settings,
incompatible early encoder promotion and shutdown without SETTINGS leave it open;
waiters must also observe their lifetime/cancellation context. Rejected early
sessions retain their own state; the client returned by NextConnection exposes
the replacement's settings. Publication follows successful encoder setup locally,
whereas the reference publishes immediately after parsing. Internal race tests
cover early promotion, invalid/over-budget/empty input, snapshot ownership and
shutdown. Independent v1/v2 tests compare snapshots with the pinned client against
the same upstream server, with DATAGRAM disabled/enabled and explicit unknown /
QPACK settings. Raw connections and full early-rejection/lifecycle parity remain open.

Reference inspection: [v0.63.0 HTTP/3 connection settings](https://github.com/quic-go/quic-go/blob/v0.63.0/http3/conn.go)
and the pinned module's frames.go / transport.go definitions.

As a local extension, response bodies from ClientConn.RoundTrip and
Transport.RoundTrip now implement Settingser. The binding survives pool reservation and automatic gzip wrappers,
and successful CONNECT Tunnel responses expose the same settings. Queries do not
read payload bytes, release request reservations or acquire body read locks.
The view remains tied to the session that produced the response, including after
body/connection close. Independent v1/v2 tests check direct responses against the
client's receipt channel, copied unknown settings after close, automatic and
explicit compression, HEAD/range/compression-disabled paths, and datagram tunnels
on IPv4/IPv6. Existing stream error/cancellation and close ownership behavior is
preserved by adding delegation to the existing wrappers. The pinned reference
exposes Settingser on its connection and server response writer, not these body
wrappers; the body extension is not counted as closing a missing reference API.

Public server handlers now receive a response writer implementing Settingser,
matching the reference response-writer contract. It uses the same bounded live
session view and private snapshots as ClientConn. Embedding the existing concrete
writer preserves Flush, FlushError and AcceptTunnel; Unwrap also supports response
controllers and extension helpers. Independent v1/v2 tests send custom client
settings with DATAGRAM disabled/enabled, inspect exact handler-visible values,
check snapshot isolation and flush/write the response. The full independent
suite exercises the wrapper through existing tunnel, cancellation, early-data
and handler error paths. Raw connection APIs, deadline methods and full callback /
closure ordering still require work.

`Server.AdditionalSettings` and `Transport.AdditionalSettings` now advertise
application-defined settings. Direct clients use the local ClientConfig field of
the same name. Client configuration is copied; pooled settings merge the transport
and ClientConfig maps, rejecting duplicate IDs even when values agree. Settings
are sorted before serialization. Up to 59 extras fit alongside five managed
settings under the existing 64-entry endpoint budget. IDs/values must fit QUIC
varints; core/QPACK, HTTP/2-reserved, Extended CONNECT and DATAGRAM identifiers
cannot override managed policy. These explicit resource and validation rules are
stricter than the reference's documented invalid-input contract / writer behavior.

Invalid configuration fails before dialing or server connection ownership.
Independent v1/v2 tests verify direct, pooled and server advertisements, including
zero values and maximum-varint IDs/values. Unit tests cover exact-fit and overflow
budgets, merging, duplicates and caller-map ownership. Configure fields before use;
concurrent mutation of Server/Transport configuration is unsupported.

Additional values are hashed canonically into the client-local and authenticated
server HTTP ticket scopes. Empty maps retain the existing scope. Because unknown
extension semantics cannot establish safe changes, any addition/removal/value
change disables early use of the old ticket while preserving ordinary resumption.
Unit ticket tests cover identical maps with different insertion order and changed,
added or removed values. Live independent v1/v2 tests now cover both directions.
On the server, an actual gated early QUIC datagram isolates the ticket decision:
unchanged extra values accept; changes/additions/removals reject while both peers
resume TLS. Retry is explicitly excluded, rejected probes do not leak into fresh
traffic, and subsequent HTTP sees current SETTINGS. On direct and pooled clients,
the independent server always allows early data: unchanged maps produce observed
0-RTT packets and accepted early data; changed/added/removed/empty maps produce
no early packets while TLS resumption and HTTP delivery succeed. Peer snapshots
verify each current advertisement. This does not establish extension-specific
replay safety or early application semantics for unknown settings. Additional
settings advertise values only; applications implement their extension semantics.
Reference field contract: [pinned transport](https://github.com/quic-go/quic-go/blob/v0.63.0/http3/transport.go).

`ClientConn.Context` now exposes the underlying QUIC connection context, including
its typed local/remote close cause. A rejected early HTTP generation and its
replacement share that context; retirement alone does not signal transport closure.
`ClientConn.CloseWithError` initiates an application close through the existing
ownership guard. Invalid varint codes fail before consuming ownership, and an
obsolete generation cannot close its replacement. Call Close to join HTTP workers
when cleanup must be complete. As with other local adapters, the stale-generation
behavior is an extension of the reference contract; the reference has no matching
NextConnection HTTP-generation abstraction.

Independent v1/v2 tests verify local and remote code/message/origin on both peers,
connection usability after an invalid close, stable context identity and causes,
and rejected-early replacement usability after stale closure. Ownership race tests
exercise both ordinary and explicit-code closure against transfer. Context remains
the transport lifetime, not a per-request or per-HTTP-generation context. Complete
closure/deadline ordering remains open.

`Transport.RoundTripOpt`, `RoundTripOpt.OnlyCachedConn` and `ErrNoCachedConn` now
support cached-only requests. RoundTrip uses the same path with default options.
A cached-only request can share a dial already started by another request or a
slot undergoing early recovery. It never starts a new connection, including
when a GOAWAY-triggered automatic retry finds the prior connection retired.
A miss returns ErrNoCachedConn before capacity eviction or pool allocation;
request bodies are still closed on failure. A canceled waiter releases only its
own reservation and cannot cancel another request's dial interest.

Unit tests cover empty/full-pool misses, body cleanup, pending-dial sharing and
waiter deadline expiry. Independent v1/v2 tests cover warm reuse, idle eviction,
and a cached second request rejected by GOAWAY after a successful warm-up. Its
retry returns ErrNoCachedConn instead of redialing. Existing ordinary GOAWAY retry
cases remain passing. Full independent validation includes default/early paths;
cached-only 425 tests now cover successful reuse and eviction during GetBody.
Both paths recreate the upload once, close original/recreated bodies once, preserve
caller request identity/method, and use exactly one peer connection. Eviction
returns ErrNoCachedConn without a second application attempt or dial. These tests
use the explicit replayable method on a warm connection; they do not prove actual
pre-handshake delivery. The pooled early matrix also covers cached-only admission while server TLS is
held, acceptance/rejection, and automatic retries enabled/disabled in v1/v2. An
unrestricted HEAD_0RTT request establishes the early pool slot, observed 0-RTT packets prove an
early attempt, and the cached upload's Read callback proves its HTTP stream and
headers were admitted before releasing TLS. Rejection retries use the recovered
slot; disabled retries return Err0RTTRejected and an explicit cached-only fresh
request succeeds. Exact handler/handshake counts exclude duplicate processing
and redial. Original/recreated body-close counts verify cleanup. This does not
identify the cached stream inside an individually decrypted 0-RTT packet or
prove all loss/cancellation interleavings during recovery.

`Server.ConnContext`, `ServerContextKey` and `RemoteAddrContextKey` now expose
per-connection request context customization. The callback sees server identity,
local net.Addr under http.LocalAddrContextKey, remote net.Addr, and the explicit
Serve/ServeQUICConn parent values. It runs once after admission, before HTTP/3
critical-stream setup, outside server locks. Normal and extension handlers inherit
the selected context. Address values are setup snapshots, not migration observers.
Returning nil panics with the reference message; deferred cleanup closes the
connection and releases admission even when the caller recovers that panic.

A returned context can supply values, deadlines and cancellation. The local owner
also enforces transport/server shutdown if the callback detaches from its input
context. Shutdown must not be called synchronously from this callback because it
joins the connection being initialized. Live v1/v2 tests check two requests on one
connection, callback count, server/address metadata, parent and custom values,
callback/parent cancellation causes, detached-context server closure, and nil
callback cleanup. Under Server.Close, listener closure can win the cancellation
race and expose a transport-close cause instead of ErrServerClosed; complete
ordering parity is not established. A further v1/v2 startup matrix holds stream
count, stream-byte or connection-byte credit at the independent peer, observes
the corresponding blocked frame, and then exercises callback cancellation,
detached owner cancellation and selected-context deadline expiry. It checks
startup completion, admission cleanup and closed QUIC ownership without request
dispatch. Owner cancellation can close QUIC before the detached-context bridge
runs, so these cases do not require a particular error cause ordering. Broader
deadline, selected-config, early-data and migration combinations remain open.


`QUICListener` and `Server.ServeListener` accept application-owned regular, early,
and custom listeners. Close/Shutdown cancel acceptance without closing the
listener; the application can reuse it with a new server. The new method returns
when acceptance ends, while Shutdown still joins active connections. An accept
error does not cancel existing requests. The older local Serve(ctx, *Listener)
retains its owning/joining contract. Per-connection datagram capability checks
remain authoritative when a custom listener has no capability query.

Live v1/v2 tests cover regular and early listener reuse after Close or graceful
shutdown, handler cancellation, preservation of active responses across shutdown
timeout, and the http.ErrServerClosed result. A custom accept-failure test proves
an active response survives ServeListener returning and remains owned by Shutdown.
Unit tests include non-comparable listener values and error propagation. The
independent resumed-request matrix now also covers ServeListener in v1/v2. Holding
the server flight proves the connection callback runs before TLS completion and
resource policy controls pre-completion dispatch. Both peers report resumption,
the server reports accepted 0-RTT, and a later established request gets a fresh
TLS snapshot without mutating the first. The listener ownership tests themselves
still use ordinary handshakes. Rejection, cancellation and changed-policy
combinations through this entry point need further coverage.


Transport.EnableDatagrams now enables HTTP DATAGRAM negotiation; nil QUICConfig
gets automatic QUIC support, while explicit disabled QUIC support fails before
pool admission. The existing ClientConfig opt-in retains automatic enabling.
Transport.MaxResponseHeaderBytes supplies a receive-only field-section budget,
including advertised SETTINGS, decoder bounds and the response frame reader.
Positive values override the receive side of ClientConfig.MaxHeaderBytes; without
either explicit limit, pooled clients now default to 10 MiB, matching v0.63.0.
Direct clients retain their local 64 KiB default. Request encoder limits remain
independent. The effective receive limit is included in the TLS session-cache scope.
The implementation also retains its 1024-field limit and aggregate QPACK budgets;
matching the byte default does not establish complete resource-policy parity.

Independent tests verify peer-observed HTTP DATAGRAM settings, automatic enabling
with nil QUICConfig in v1, and explicit configuration in both versions. Default
version fallback against the v2-only pinned peer remains an opt-in known failure.
Header tests cover a 1 KiB request under a 512-byte response limit, distinct encoded
and decoded field-section failures, and accepted 70 KiB headers in v1/v2. Local
HTTP protocol failures now convert to public *http3.Error with the original cause
preserved; the limit tests exposed this missing conversion. A further independent
v1/v2 resumption matrix verifies that equal effective receive limits preserve
early use across explicit/default, legacy/new and direct/pooled configuration.
Increased or decreased limits suppress early traffic while both peers retain TLS
resumption. Boundary responses after resumption enforce the new receive budget.
Changing only the request encoder budget does not change the receive-limit cache
scope. Broader resumption interleavings, aggregate-memory and maximum-size
coverage remains open.

Transport.VerifySourceAddress (2026-09-29) is captured at first use and retained
across listener generations. Nil/false admits under anti-amplification limits;
true requests Retry. Valid token evidence bypasses the callback. Own-peer and
independent v1/v2 tests cover nil/false/true, callback source ports, validation
metadata and listener restart. Multiple unvalidated Initial packets can invoke
the policy before the client receives Retry. Public contract checked against
https://pkg.go.dev/github.com/quic-go/quic-go#Transport .

Transport.TokenGeneratorKey and TokenGeneratorKey (2026-09-29) now configure
address-token protection. Initialization copies a configured key or generates a
random one; HMAC-SHA256 derives separate Retry and NEW_TOKEN keys. Listener
restarts preserve both token protectors. New transports with the same configured
key can validate earlier tokens; changed/random replacement keys cannot. Token
formats remain implementation-specific. MaxTokenAge now configures the address-token lifetime; zero defaults to 24 hours. Own-peer and independent v1/v2 tests
cover listener and transport replacement with shared, changed and random keys.

Transport.MaxTokenAge (2026-09-29) supports positive Go durations with nanosecond
precision; zero selects 24 hours and negative durations are rejected before
socket ownership. This negative-value rejection is a local API choice, not an
independently established reference contract. Format 2 NEW_TOKEN values use a
74-byte envelope with timestamp seconds/nanoseconds and duration, while legacy
54-byte format 1 values remain readable. Both sizes are disjoint from Retry.
Validation applies the shorter of the issued lifetime and current policy. The
local client token store still discards tokens after 24 hours; configuring longer
server validity does not extend local client storage. Reference field/default:
https://pkg.go.dev/github.com/quic-go/quic-go#Transport .

Transport.StatelessResetKey / StatelessResetKey (2026-09-29) now configure stable
CID-bound reset tokens for incoming and outgoing connections. The key is copied
at first use. Unknown fixed-bit short headers can elicit a reset after CID and
peer-token routing; live CIDs are excluded even when peer-address routing fails.
Replies are 21–42 bytes and strictly smaller than the trigger, with random
prefixes and a 100-per-second endpoint-wide limit. Nil keys disable sending.
Long headers and clear-fixed-bit unknown traffic are ignored. Own-peer v1/v2
restart gates cover both roles, including client-issued CIDs; independent
upstream clients recognize resets from restarted servers. Broader cross-extension
reset/restart coverage and per-source fairness remain open.

Transport.ConnectionIDLength (2026-09-29) supports lengths 1–20, with zero
selecting the reference's four-byte default. The setting is captured at startup
and retained in routing across listener generations. Server admission and shared
dialing generate local IDs at that length; subsequent CID issuance inherits it.
Legacy ListenAddr/DialAddr retain eight-byte defaults. Retry source IDs and
client-selected original destination IDs still use eight bytes independently of
this local routing length. ConnectionIDGenerator is integrated below.
Own-peer tests inspect both roles' emitted Initial SCIDs and transfer stream data
for all 21 settings across v1/v2. Reference field/default checked at
https://pkg.go.dev/github.com/quic-go/quic-go#Transport .

ConnectionIDGenerator and Transport.ConnectionIDGenerator (2026-09-29) now override
ConnectionIDLength and supply initial/shared-dial/replacement/multipath IDs.
Calls are serialized per Transport; returned IDs are length-checked and copied.
Generator length 0 selects address-based routing, disables migration and cannot
combine with a reset key or multipath. Multiple connections to the same peer
cannot be demultiplexed with empty local IDs. Retry source IDs and client-chosen
original destination IDs remain independent random eight-byte values. Preferred
CID generation has the internal hook, but shared-Transport preferred-address
registration remains unsupported. Own-peer and independent-client tests cover all
0–20 lengths across v1/v2; concurrency/error gates pass. Cross-transport generator
synchronization and globally unique/unlinkable output are application obligations.
Broader duplicate/routing-collision, restart and migration combinations remain.
Reference contract: https://pkg.go.dev/github.com/quic-go/quic-go#ConnectionIDGenerator .

CID generator follow-up (2026-09-29): independent coverage now spans both roles,
v1/v2 and all lengths 0–20 (84 cases). Public collision tests preserve the first
connection after a colliding second dial, including empty-ID address routing.
Custom 8/12-byte CID tests combine additional-path migration, primary retirement,
listener restart and transfers on both existing and new connections. Incoming
admission and later-issued cross-connection collision cases remain to expand.
