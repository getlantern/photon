# Independent-peer handshake interoperability

This test-only module pins `github.com/quic-go/quic-go` to v0.65.0; `go.sum`
records dependency checksums. The implementation under test is imported from
the parent directory through a local module replacement. Upstream is not a
dependency of the parent implementation.

Run `go test -race ./...` from this directory.

Migration address diagnostics are opt-in: set `QUIC_INTEROP_MIGRATION_TRACE=1`
to retain a bounded upstream packet/frame/loss and relay-address timeline on
failure, or `QUIC_INTEROP_MIGRATION_TRACE=all` with `go test -v` to print successful
runs too. The trace holds at most 512 formatted events, without packet payloads
or keys. Challenge/response values, packet numbers, injected drops and application
milestones help diagnose the intermittent upstream address assertion. Zero qlog
checksums mean unavailable, not a valid packet correlation key; short-header
correlation currently relies on event ordering/time rather than exact identity.
Tracing adds timing overhead; the unchanged untraced gate remains relevant.

`TestHTTP3CustomDialMigration` uses our HTTP Transport.Dial callback to dial from
an application-owned concrete UDP transport, then probes and switches to a second
transport. Upstream HTTP/3 observes both source addresses across v1/v2; the client
must retain one pooled connection, invoke the dialer once, and close its returned
connection on HTTP transport shutdown. `TestHTTP3CustomDialEarlyRecovery` runs
the pooled early-data matrix through a supplied DialAddrEarly callback, covering
acceptance/rejection, cached-only requests, retry policy, concurrent requests,
application error isolation and closure during a gated handshake.

`TestHTTP3CustomDialWaitsForHandshake` holds the resumed server handshake behind
a gate after the custom callback returns an early connection. Across v1/v2 and
accept/reject policies, it checks disabled HTTP early data and a callback bypassing
the HTTP SETTINGS cache. A 20 ms observation window must see neither completed
HTTP setup nor a 0-RTT packet. After release the request must finish once with
resumption intact. A temporary overlay removing the wait fails all eight cases.

`TestHTTP3AltSvcCompatibility` compares listener registration/removal against
upstream using blocking listeners with declared addresses. It checks port override,
address fallback, missing ports, insertion order, repeated ports and appending to
existing headers. The live startup fixture also checks the advertised bound port
in responses from our server to the independent v1/v2 client.

`TestHTTP3ListenAndServeIndependent` exercises our owned-listener HTTP startup
with the pinned upstream client across v1/v2, configured TLS/file credentials and
graceful/immediate shutdown. It verifies negotiated transport datagrams, response
bytes, unchanged caller configuration and UDP address reuse after shutdown. The
graceful cases hold a response open through a shutdown deadline and require it
to finish before the owned listener returns. The convenience API requires reserving
and releasing an ephemeral address before startup; a competing bind is reported
as a test failure. This fixture does not test cached early-data acceptance.

`TestStatelessResetIndependentRestart` exchanges stream bytes with the pinned
server, suppresses orderly close output, then replaces its transport using the
same caller-owned UDP socket and reset key. Sending on the old connection causes
that unmodified peer to generate a stateless reset. Both versions require the
public context, subsequent stream writes and stream opening to expose the typed
reset error with net.ErrClosed semantics. This covers the client receiving a
reset after peer state loss. TestStatelessResetMigratedIndependentRestart adds
public AddPath/Probe/Switch before the restart, exercising reset forwarding from
an established foreign socket in v1/v2. It requires a concrete original local
binding. TestStatelessResetProbingIndependentRestart restarts upstream before
Probe and blocks both directions on the original socket, requiring the typed reset
through the candidate Transport. Reverse roles, preferred-address and broader
multipath combinations remain separate work.

`TestStatelessResetCIDCollisionIndependent` shares our Transport between two
upstream peers. It restarts one peer and rewrites the generated reset's random
prefix to collide with a CID observed on the other connection's live traffic,
preserving the reset token.
Both v1/v2 cases require the typed reset on the intended connection and continued
bidirectional transfer on the other. TestStatelessResetForeignCIDCollisionIndependent
migrates the target onto the resident connection's Transport and disables its old
socket. This covers local-CID-first authentication followed by a foreign reset
handoff. TestStatelessResetForeignFirstLocalTargetIndependent and
TestStatelessResetForeignFirstForeignTargetIndependent migrate the first CID owner
onto the shared socket, disable its original socket, and target a local or second
foreign connection with the rewritten reset. Both require typed reset termination
and continued bidirectional traffic on the first connection. Disabling return
reservation makes all four v1/v2 cases time out; ten positive repetitions pass.

`TestStatelessResetLongHeaderCollisionIndependent` covers all eight combinations
of v1/v2 and local/foreign first CID owner and reset target. It preserves the reset
token generated by upstream after restart and substitutes a framing-valid Handshake
prefix with an observed CID. This verifies our receiver's required long-header
reset tolerance; upstream itself emits a short-header reset. Old migrated sockets
are muted, the target must report the typed reset, and the unrelated connection
must continue transferring data.

`TestClientResumptionUpstream` checks client 1-RTT resumption in both versions.
Each case performs an initial handshake, a resumed handshake using the caller's
session cache, and a full handshake with tickets explicitly disabled. Both peers'
DidResume values, verified certificate chains, exact application bytes/FIN and
stored tickets with early data disabled are checked. The upstream server permits
0-RTT, but its Used0RTT remains false. `TestServerResumptionUpstream` performs the reverse-role sequence against our
listener, covering server-issued tickets with the upstream client. These gates
do not establish 0-RTT support or transport-parameter binding for early data.

`TestKeepAliveUpstream` checks optional keepalive with our endpoint in both roles
and both QUIC versions. Upstream keepalive and both peers' MTU discovery are off.
Our five-second idle timeout and ten-second keepalive period must honor the
peer's one-second timeout: enabled cases stay open for 3.5 seconds with no app
traffic and then transfer exact bytes/FIN. Disabled controls require both peers
to report idle timeout. All eight cases pass with race instrumentation. This is
loopback coverage; it does not test dropped keepalives, migration or multipath
NAT-binding retention.

`TestIncomingStreamLimitsUpstream` verifies the independent implementation's
configured incoming stream credit using the pinned upstream sender. Its twenty
cases cover v1/v2, independent client and server roles, asymmetric bidi/uni
limits, disabled directions, and each direction's 64-stream boundary. It checks
upstream's nonblocking credit error and synchronous-open cancellation, then
exchanges payloads and FINs and verifies exactly one replenished credit for
both bidirectional and unidirectional completion. This validates our advertised
limits; it does not remove the implementation's documented stream-object bounds
or establish upstream configuration/default parity.

`TestQPACKStaticTableIndependentDecoder` checks all 99 static table entries
against the pinned `github.com/quic-go/qpack` v0.6.0 decoder using indexed field
sections, plus rejection of index 99. This dependency is test-only; the parent
implementation generates its own table from RFC 9204 Appendix A. This check
does not establish complete QPACK or HTTP/3 interoperability.

An optional [Picoquic multipath gate](picoquic/README.md) uses a pinned external
server binary. Set `PICOQUIC_DEMO` to include it. This checks v1/v2 negotiation,
opening an additional path, abandoning path zero, 1 MiB stream transfers, and
two additional-path migrations between transfers and a third during an
unfinished response. Clean and first-packet-loss cases run through
a relay that gives each client socket a distinct server-facing binding.
Without that variable it skips independently of the known upstream-peer
skips. No Picoquic source or library is linked into the implementation.
Set `PICOQUIC_CLIENT` to include reverse-role validation with our small C
application driver linked to the unmodified pinned library. Both versions
validate path 1, abandon path zero, and finish an exact 1 MiB response. The
reverse matrix includes first-packet loss in either or both directions during
path opening and a three-large-datagram response burst loss after abandonment.
Exact drop counts and distinct NAT bindings are asserted. The
original demo failure remains reproducible with `PICOQUIC_REVERSE_DIAGNOSTIC=1`
and is not counted as passing coverage. See the linked peer README.

`TestKeepAliveMigrationUpstream` adds idle intervals before migration and after
each of two local socket changes, with no application traffic during those
intervals. It checks v1/v2 against the upstream server, with upstream keepalive
and both MTU discovery mechanisms disabled. Clean cases use a one-second idle
timeout and 3.5-second quiet intervals; relay cases drop the first packet in
both directions at each new binding and use a five-second timeout with
5.5-second quiet intervals. Exact payload/FIN, changed local/observed addresses,
and drop counts remain checked. The larger loss-case timeout allows validation
retries to complete; an initial one-second loss case expired during validation.
The default keepalive period is 500 ms. Strict mode retains a 100 ms v2 case that
reached the v0.63.0 peer's key-update failure; see KNOWN_FAILURES.md.

`TestIndependentStagedProbeUpstreamServer` exercises public AddPath/Probe/Switch
against upstream in v1/v2, checking stream delivery and peer addresses before
and after two switches. `TestIndependentAbandonedProbeUpstreamServer` adds an
unused validated probe closed between those switches. Longer sequences exhaust
the pinned peer's probe CID supply, also reproduced using upstream in both roles;
the two opt-in reproduction tests are documented in KNOWN_FAILURES.md.

`TestIndependentClientMigrationUpstreamServer` exercises the public client's
`Migrate` operation against the unmodified upstream server in v1 and v2. It
exchanges small bidirectional streams before migration and after each of two
successive local-port changes, verifying the peer's observed address. Ten
race-enabled repetitions of the original clean cases passed. The matrix now
also includes a live UDP relay with a distinct server-facing NAT binding per
client source address. For each migrated address it drops the first client
packet, the first server packet, or both. Exact drop counters and bidirectional
streams verify each case; relay readers are joined during cleanup. All eight
v1/v2 clean/loss cases pass with the race detector. This does not cover arbitrary
loss patterns, live reordering, address-family changes, arbitrary numbers of
migrations.

The public API advertises an active CID limit of four. With the former limit of
two, the first migration passed but the second waited for a spare CID until its
context expired: upstream v0.62.0 `conn_id_generator.go` deliberately does not
issue a replacement when sequence zero is retired. The wire default and explicit
internal transport limits remain unchanged; no upstream patch is applied.

The matrix covers QUIC v1/v2 with:

- Our client and the upstream server, followed by CID rotation.
- Our server and upstream's default DialAddr client (zero-length client CID).
- Our server and upstream's shared UDP transport with an 8-byte client CID,
  followed by CID rotation.

Both clients verify the test server certificate against an explicitly trusted
test root. All packets use actual loopback UDP sockets. The driver waits for
handshake confirmation, flushes ACKs, and waits for reliable control frames to
be acknowledged. CID rotation cases also wait for spare IDs before switching.

This verifies handshakes and selected connection-ID behavior, not stream
interoperability, production transport readiness, or complete RFC conformance.
The UDP driver is test-only and triggers explicit engine probes after a 20ms
read timeout; it does not implement RFC 9002 recovery or congestion control.
No throughput or latency comparison is claimed from these tests.

`TestHuffmanIndependentCodec` compares all one- and two-byte encoded inputs
against `golang.org/x/net/http2/hpack`, plus byte-exact encoding across all byte
values. `FuzzHuffmanIndependentCodec` extends the comparison to arbitrary inputs.
The implementation itself uses an independently generated RFC codebook and codec.

`TestQPACKFieldLinesIndependentDecoder` uses our whole-section encoder and checks all static indices and literal
name-reference combinations, with Huffman and never-index flag combinations,
plus literal-name forms (503 cases). Upstream currently lacks dynamic table
references, so this check does not cover relative/post-base decoding or claim
full QPACK interoperability. Never-index preservation is checked locally because
the peer decoder does not expose that flag.

`TestQPACKSectionIndependentEncoder` decodes a 200-field section produced by
upstream's encoder and verifies ordering, values and exact decoded-size limits.
`FuzzQPACKSectionIndependentEncoder` varies arbitrary names/values and duplicates.
This upstream encoder does not exercise dynamic references or never-index flags.

`TestQPACKSelectedFieldsIndependentDecoder` checks our automatic representation
selection across 298 fields using all static entries, sensitive variants, custom
values and a literal name. Upstream validates decoded names/values; local tests
validate the never-index flag, which upstream does not expose.

`TestHTTP3LiveIndependentServer` assembles our public QUIC transport, HTTP/3 frame
and SETTINGS codecs, critical-stream registry, and QPACK encoder/decoder into a
live client test against the pinned upstream HTTP/3 server. Both QUIC versions
exchange a POST with a 11 KiB patterned body, request trailers, response headers,
an exact echoed body and response trailers. TLS uses the existing trusted test
certificate; the handler verifies HTTP/3. Client critical streams remain open
until connection teardown. The peer's initial control SETTINGS are parsed.
This is an explicitly assembled test harness, not a public HTTP/3 client API.
QPACK capacity is zero, so the test covers static/literal compression, not dynamic
compression. Later control-stream events, push, datagrams, malformed HTTP semantics
are not covered by this gate.

`TestHTTP3LiveIndependentClient` reverses the roles using upstream's HTTP/3
Transport and our server-side QUIC/framing/QPACK components. Both versions verify
an exact 11 KiB POST, custom headers and request trailers, then return a response
with trailers that upstream exposes through its net/http interface. The fixture
parses initial peer SETTINGS and retains local critical streams. The HTTP client
goroutine is canceled and joined on every exit. As with the forward test, this
is a test-only assembly with QPACK capacity zero, not a production endpoint.
Run both directions with `go test -race -run '^TestHTTP3LiveIndependent(Client|Server)$' -count=10 ./...`.

Both live HTTP/3 roles now initialize their local critical streams through
`internal/http3conn.OpenLocal`, the reusable production lifecycle component.
Request/response assembly and incoming control handling remain test fixtures.

Both live roles also classify the initial incoming control stream through
`http3conn.Incoming`, exercising cancellable prefix reading and critical identity
registration. Later stream acceptance/dispatch remains outside these fixtures.

The live HTTP/3 roles now run `RunIncoming` with reusable control/QPACK handlers
through connection teardown. Zero-capacity compression policy remains fixture
code. `TestHTTP3LiveCriticalFailure` tests control FIN and duplicate control
streams over both versions and asserts that the independent peer observes the
expected HTTP/3 application close code, in addition to checking the local error.

Independent dynamic QPACK codec tests are available through the optional pinned
[ls-qpack peer](lsqpack/README.md). Set `LSQPACK_BIN` to its build/bin directory
to include both encoding directions in this suite.


`TestConcurrentClientResumptionSharedEarlyTicket` obtains an actual early-data
capable ticket using the upstream client/server, then freezes that cache entry.
Six independent clients concurrently resume from the same object in each QUIC
version, retaining verified identity, transferring exact bytes/FIN and never
using 0-RTT at the upstream peer. Serialized source session state and ticket bytes
must remain unchanged, including EarlyData=true on the source. This deliberately
reuses a ticket to stress ownership; it is not an application ticket-reuse policy.
The test passes under race instrumentation; nested-module vet passes.


`TestServerResumptionTicketPolicy` reuses one frozen client ticket across listener
restarts in v1/v2, with default and custom TLS wrapping. A retained old key permits
resumption, removing it forces a full handshake, and disabling server tickets
rejects resumption even after restoring the valid old key. Custom wrapping carries
an Extra marker; unwrap restores it only on the resumed connection. Wrap/unwrap
call counts are checked, including no calls in the disabled case, and every
connection transfers exact bytes/FIN. This checks explicit key sets across
listener restarts, not live rotation or the clock-driven automatic key lifecycle.
The matrix also selects each configuration through GetConfigForClient while the
base listener configuration has unrelated keys and the opposite disabled policy.
It requires one selection callback and zero base wrapping callbacks. All thirty-two
connections pass under race instrumentation; interop vet passes.


`TestServerLiveTicketKeyRotation` keeps a listener running while rotating the
original TLS configuration with SetSessionTicketKeys. It freezes the initial
client ticket, verifies acceptance while its key is retained, then requires a
full handshake after retirement. All established connections must still exchange
exact DATAGRAM payloads after rotation. Both v1/v2 cases initially resumed using
the retired key, exposing the listener clone's isolated key state; default ticket
callbacks now bind to the caller's shared configuration. The focused race gate
and existing server resumption/selected-policy gates pass. Clock-driven automatic
rotation and callback failure behavior remain separate coverage gaps.


`TestServerRejectsExternalEarlyTicket` obtains a real 0-RTT-capable ticket from
an upstream server, then presents it to our server using the same test ticket
key. A PacketConn gate withholds server input until an actual v1/v2 0-RTT packet
has been written, eliminating the early-write/handshake race. Both peers resume,
but upstream reports rejection and Used0RTT=false. After NextConnection resets
the rejected stream state, only the exact new 1-RTT payload/FIN reaches our
application. Both versions pass under race instrumentation; interop vet passes.
This verifies rejection, not support for accepting or replaying 0-RTT data.


`TestServerTicketCallbackFailureIsolation` injects WrapSession and UnwrapSession
errors against an upstream client in v1/v2. Each case first obtains a ticket,
then requires a remote transport close from the injected failure, exactly one
failing callback invocation, and no accepted application connection. Disabling
the fault on the same listener must allow another resumed handshake and exact
DATAGRAM delivery. All four cases pass under race instrumentation; interop vet
passes. These checks cover failure isolation, not every callback policy or
TLS alert-code mapping.

`TestEarlyClientDatagramsUpstream` sends actual 0-RTT DATAGRAMs to the upstream
EarlyListener, covering acceptance, rejection and deliberate early-packet loss
for both versions. It checks empty and 1130-byte payloads, copied send buffers,
fresh post-handshake messages and reverse responses. `TestEarlyServerDatagramsUpstream`
uses cached NEW_TOKEN evidence on the same listener to avoid Retry, holds the
server TLS flight, and reads accepted data before client Finished. Its rejection
control disables TLS tickets on the second handshake without invalidating the
address token. A server response is queued before Finished in both cases. These
are deterministic local interoperability gates, not general delivery guarantees
for unreliable DATAGRAMs or a complete impairment matrix.

`TestStreamIDOrdinalCompatibility` compares public stream-number conversion and
the inferred ordinal's reverse conversion against v0.63.0 for 1,031 stream IDs,
including signed extremes and all four stream classes. This is an API conversion
check, not acceptance of out-of-range IDs by either wire implementation.

`TestHTTP3TLSConfigCompatibility` compares package TLS-helper behavior with v0.63.0,
including nested selected configurations, unchanged application ALPN and callback
fallback/errors. `TestHTTP3LiveIndependentClient` uses our helper to configure a
selected TLS configuration for actual v1/v2 HTTP/3 exchanges. Removing the selected
ALPN wrapper makes both negotiations fail with no application protocol.
