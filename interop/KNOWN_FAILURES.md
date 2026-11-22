# Known interoperability failures

## Repeated staged migration exhausts upstream probe CIDs (2026-09-30)

The new public AddPath/Probe/Switch fixture against unmodified v0.63.0 completes
three switched paths, then its fourth probe expires. Peer debug output states
`skipping validation of new path ... since no connection ID is available`.
A control using upstream Transport and Conn in both roles also exhausts its CID
supply (observed on the third probed path), then reaches the six-second context.
This establishes a peer-side reproduction; it does not prove our complete CID
lifecycle or repeated-migration compatibility. No upstream code was changed.

Default `TestIndependentStagedProbeUpstreamServer` covers two switches;
`TestIndependentAbandonedProbeUpstreamServer` additionally probes and abandons one
unused association between them. Both versions pass twenty race repetitions.
Six successive switches pass with our own peer in base and multipath modes.
Longer independent and upstream-only sequences remain explicit failing controls:

```sh
QUIC_INTEROP_KNOWN_FAILURES=1 go test -race -run 'Test(IndependentRepeatedProbeUpstreamCIDExhaustion|UpstreamSelfRepeatedProbeCIDExhaustion)' -count=1
```

These two opt-in fixtures are exclusions, not successful interop coverage.


## Intermittent shared endpoint handshake timeout (2026-09-28)

A full default interop race run failed `TestSharedServerEndpoint/1` at
`endpoint_test.go:92`: the fixture's UDP read reached its five-second deadline
before both handshakes completed. Ten exact focused race repetitions passed
(1.454s). This followed a separate full-run v2 handshake-loss timeout documented
below. No packet trace established either failure's cause or whether they are
related. Ten repetitions of both complete affected test groups then passed
(7.160s). Neither fixture's deadlines nor enabled coverage were changed.


## Intermittent receive-window fixture handshake timeout (2026-09-28)

A three-repetition race run of `TestReceiveWindowsUpstream` failed once in
`1/server=true/large=true/remote-bidi` at upstream DialAddr (line 68), reporting
`timeout: no recent network activity` after five seconds. This occurs before
Accept returns or application reads can invoke the receive-growth policy. No
callback was configured in this fixture. Twenty focused race repetitions then
passed (1.656s), followed by ten full receive-window matrix repetitions
(4.415s). The cause is unresolved; a passing retry does not erase the
failure or establish a relationship to earlier ticket-policy handshake stalls.
The case remains enabled with its original deadlines.


## Configured receive windows and v2 bulk transfer (2026-09-28)

The new TestReceiveWindowsUpstream verifies advertised receive limits through
upstream qlog and transfers data into our implementation in both TLS roles and
all receive-stream directions. A focused v2 server/large-window/remote-uni bulk
run stalled after 121,989 bytes, with upstream reporting key phase 1. This is
consistent with the existing pinned-peer key-update mismatch, not proof of a
new receive-window defect or an independently re-established root cause.

A 96 KiB attempt also reached key phase 1 in one repeated small-window case
and stalled at 97,538 bytes. The default v2 payload is 64 KiB, while v1 uses 275,000 bytes. Thus default v2
covers parameter advertisement, successful data reception and small-window
turnover, but does not establish turnover of the 128 KiB stream window. Keep the
full v2 transfer available as a strict reproduction:

```sh
QUIC_INTEROP_KNOWN_FAILURES=1 go test -race -v -run '^TestReceiveWindowsUpstream$/^6b3343cf$/^server=true$/^large=true$/^remote-uni$' -count=1 .
```

Rechecked with the unmodified v0.63.0 peer on 2026-09-28: the strict command
above again failed, after 122,220 bytes with peer key phase 1 (5.273s). The peer
updatable_aead.go is unchanged from v0.62.0. The default race suite passes with
the existing reduced v2 payload; this does not close the bulk-transfer gap.

The engine-pair large-window loss/reorder tests cover both versions, but do not
replace this independent bulk-transfer gap.


## Picoquic early acceptance across compatible switching (2026-09-28)

`TestPicoquicEarlyCompatibleAcceptance` seeds a v1 early-capable ticket, sends an
actual original-version early request, and asks the unmodified pinned Picoquic
server to switch to v2. The peer rejects early data, so the strict acceptance
check fails with `quic: early data rejected`. It is opt-in under
`QUIC_INTEROP_KNOWN_FAILURES=1`; this skip is not an acceptance pass.

The same-version `TestPicoquicEarlyClientControl` succeeds while the relay discards
all client 1-RTT output until the exact response is read. This prevents reliable
1-RTT retransmission from concealing an early delivery failure. The normal
`TestPicoquicEarlyCompatibleRecovery` pins the observed rejection behavior, verifies
NextConnection and same-ID stale-handle isolation, then completes a fresh request
in v2 without a Version Negotiation packet. It is a rejection-recovery gate, not
a requirement that other peers reject early data when switching versions.

Pinned source `picoquic/tls_api.c` checks ticket version against the connection's
current `version_index` around lines 1407–1414 and derives traffic keys using that
index around line 1667, including early secrets. This is consistent with the
observed rejection; the test does not instrument the peer's TLS decision directly.
Peer revision: `8c6bbf82b6b6d98a413a69e6bf2265e7f6a10161`. No peer changes were made.
Acceptance in this direction and reverse-role combined early/version coverage
remain open. The existing peer limitation on v2-to-v1 switching also remains.

Reproduce from `interop` with the pinned binary:

```sh
PICOQUIC_DEMO=/absolute/path/to/picoquicdemo QUIC_INTEROP_KNOWN_FAILURES=1 \
  go test -race -run TestPicoquicEarlyCompatibleAcceptance -count=1
```

The reference peer is upstream quic-go v0.62.0, pinned in this module.

## Intermittent Retry loop under handshake loss (2026-09-28 diagnostics)

Subsequent lower-level logs captured the shared-endpoint variant with a client
bound to `[::]:60381`, while read-only socket inspection found an unrelated
IPv4 `cloudflared` socket at `*:60381`. It also found `*:53850`, matching the
earlier v2 loss failure's client port. A standalone probe reproduced simultaneous
IPv4 and IPv6 wildcard binding and IPv4 replies reaching only the IPv4 socket.
See [diagnostic artifacts](../docs/diagnostics/README.md). The affected IPv4
loss/shared-endpoint fixtures now use explicit udp4 sockets with upstream Dial.
This addresses the demonstrated address-family port collision without relaxing
timeouts or changing QUIC behavior. Fifty full repetitions of the two affected
test matrices passed (30.866s). A subsequent full run failed the still-wildcard
`TestInitialPacketSizeUpstream/1/server=true/size=1200` at Dial, with a five-second
context timeout (75.439s); it was not separately traced. All direct reference
DialAddr fixture calls now share explicit destination-family binding through
upstream Dial, including benchmark setup. The full default interop race suite
then passed (71.261s), and vet passed. Other historical stalls remain unclassified.


A 50-repetition race run with bounded relay tracing reproduced the v2
independent-server/drop-server=true/packet=2 timeout (34.871s for the run).
The trace recorded 20 events: two client Initials, two server Retries (one
intentionally dropped), then repeated Initial/Retry exchanges at about 204 ms,
606 ms, 1.408 s and 3.010 s. Initial DCID length stayed 15; all observed server
packets had the v2 Retry header and were 96 bytes. No TLS server flight appeared.

After adding the reference peer's connection-level PacketDropped events,
300 exact v2 race repetitions passed (3.509s). A second 50-repetition full
handshake-loss/shared-endpoint matrix failed two v1 cases: independent server,
drop-server=false, packet=1 and packet=2 (39.456s). Both traces showed the same
repeated Initial/Retry pattern. Neither peer connection trace recorded a
PacketDropped event. That absence does not prove packets reached the peer socket
or identify why Retry was not accepted. This narrows the investigation to
admission/Retry reception before TLS, without establishing a root cause.

The fixtures retain the last 128 packet events, header prefixes, direction,
peer and timing, printing only on failure. The shared-endpoint fixture also
reports completion/confirmation/deadline state and joins/closes every reference
client during cleanup, including after failure. No deadlines were relaxed.

## Intermittent v2 handshake under single-datagram loss (2026-09-20)

A 2026-09-28 full default race run also failed the opposite drop direction:
`TestPublicHandshakeSingleDatagramLoss/6b3343cf/independent-server=true/drop-server=true/packet=2`.
Upstream DialAddr reported the same five-second idle timeout after the relay
confirmed one deliberate drop. Ten exact focused race repetitions passed
(1.441s). Partial-reset advertisement was disabled; no root cause or fix is
claimed. The case remains enabled with its original deadlines.


One full race run failed
`TestPublicHandshakeSingleDatagramLoss/6b3343cf/independent-server=true/drop-server=false/packet=2`
with upstream DialAddr reporting `timeout: no recent network activity` after
one relay drop and five seconds. The fixture drops the second client-to-server
UDP datagram. Five immediate focused race repetitions passed (1.489 seconds
total). A subsequent full race rerun passed (59.004 seconds). The cause is unresolved;
passing repetitions do not establish a fix.
This occurred while validating preferred-candidate selection changes, which
are inactive during the failed handshake. Keep this failure distinct from the
known v2 post-handshake key-update mismatch below.

## QUIC v2 key updates

### HTTP/3 one-byte setup window (2026-09-29)

Adding STREAM_DATA_BLOCKED emission exposes this same boundary during round zero
of TestHTTP3DialEarlySetupRejection with a one-byte stream window. The first full
suite failed (83.912s); three isolated runs reproduced v2 timeouts while v1 passed.
A received-event trace then showed peer key phase 1 at the RoundTrip timeout.
An isolated temporary copy of v0.63.0 with only getNextTrafficSecret selecting
`quicv2 ku` for Version2 passed the unchanged one-byte case (1.434s). This is
causal diagnostic evidence using a modified peer, not canonical-peer success.
The module cache and pinned interop dependency remain unchanged.

Default v2 coverage now uses a two-byte remembered window, which still blocks
initial SETTINGS, waits for early traffic before releasing server TLS, rejects
early setup, and checks a fresh usable HTTP generation. V1 keeps one byte. Three
focused default repetitions passed (1.422s). This narrower default does not
validate v2 key-update interoperability. The strict one-byte case remains:

```sh
QUIC_INTEROP_KNOWN_FAILURES=1 go test -race -run '^TestHTTP3DialEarlySetupRejection$/^6b3343cf$' -count=1 -v .
```

Run from `interop`; the trace prints the remembered window and reports round and
peer key phase on request failure. Our v2 key derivation was not changed to match
the defective peer label.


### Abandoned-probe migration fixture (2026-11-22)

TestUpstreamAbandonedProbeIndependentServer/v2 now exceeds 100 1-RTT packets in
its final iteration, so the pinned client initiates a key update and every later
packet fails authentication at our server; the fixture then reaches its
six-second context. Instrumented runs placed the first failed open at server
packet numbers 101-103, and a temporary diagnostic that derived our v2 update
secret with the peer's `quic ku` label passed thirty repetitions. That diagnostic
was reverted. V1 and the shorter non-abandoning variant are unaffected. Upstream
v0.65.0 selects `quicv2 ku` for Version2; the pin has not been moved. The strict
case remains:

```sh
QUIC_INTEROP_KNOWN_FAILURES=1 go test -run '^TestUpstreamAbandonedProbeIndependentServer$/^v2$' -count=1 -v .
```

Skipping this subtest removes default v2 coverage of abandonment between
switches against the upstream client; it is an exclusion, not a pass.


The incoming stream-concurrency fixture now also reaches this boundary at
300 bidi plus 300 uni streams. Both TLS roles time out accepting uni streams
with upstream qlog reporting key phase 1 (2026-09-25). The v1 equivalents and
v2 100/100 cases pass. The 300/300 v2 cases are retained behind the existing
strict-reproduction flag; they are not counted as passing coverage. Reproduce:

```sh
QUIC_INTEROP_KNOWN_FAILURES=1 go test -race -v -run '^TestIncomingStreamLimitsUpstream$/^6b3343cf$/server=/^300$/^300$' -count=1
```

This is consistent with the previously diagnosed key-label mismatch below;
these new packet-event observations alone do not independently establish cause.


The capsule client policy sequence also crosses this boundary. Upstream qlog
counters show phase 0 before its seventh malformed-response case and phase 1 at
the timeout. The peer confirms the expected request reset; the subsequent healthy
GET stalls. The equivalent v1 sequence completes through phase 1. Isolated v1/v2
cases each verify reset plus healthy GET on the same connection. No peer source
changes or production compatibility workarounds are used. Reproduce the longer
unresolved sequence, including key-phase and packet-count diagnostics, with:

```sh
QUIC_INTEROP_KNOWN_FAILURES=1 go test -race -v -run '^TestCapsuleClientResponseRejection/6b3343cf/sequential$' -count=1
```


The ECN active-migration sender test also reproduced this gate with 24
DATAGRAM echoes per path round. All five v2 repetitions stalled over both
IPv4 and IPv6; v1 passed. A diagnostic IPv4 run timed out receiving echo
round 1 / packet 17 after upstream qlog reported key phase 1. Default v2
coverage uses 12 echoes on each of three paths, requiring more than ten
ECT(0) observations per round. `QUIC_INTEROP_KNOWN_FAILURES=1` retains the
longer failing case. No key-derivation compatibility workaround was added.


Large bidirectional stream exchanges stall after upstream's first key update.
All three role/CID configurations reproduced this failure. v2 handshake/CID
checks pass, and the equivalent v1 stream exchanges pass.

The pinned upstream source `internal/handshake/updatable_aead.go:133` derives
updated traffic secrets with the label `quic ku` regardless of version. RFC
9369 section 3.3 replaces this label with `quicv2 ku` for v2. Our packet layer
uses the version-specific label and passes the RFC 9369 Appendix A updated-
secret vector. Engine tests also cover two successive v2 key updates.

The default suite explicitly skips the three original v2 **stream** subtests
and the large public stream test, keeping handshake and small public stream
checks enabled. The public large-transfer test independently stalls after roughly
100 received packets, while the equivalent v1 transfer passes. This is an unresolved interoperability gate, not a passing v2
stream result. To reproduce the failure against the unmodified pinned peer:

```sh
QUIC_INTEROP_KNOWN_FAILURES=1 go test -run '^TestUpstreamHandshake$/^version-6b3343cf$/^client$' -v ./...
```

The public API reproduction is:

```sh
QUIC_INTEROP_KNOWN_FAILURES=1 go test -run '^TestPublicStreams$/^6b3343cf$' -v
```

The concurrent HTTP/3 server lifecycle test also stalled on all three v2 runs
with six requests of 11,264 bytes each, while v1 passed. This is consistent with
the same key-update gate; this run did not independently trace the update packet.
The default concurrent v2 lifecycle check uses six 704-byte requests. Set strict
mode to retain the larger unresolved exchange:

```sh
QUIC_INTEROP_KNOWN_FAILURES=1 go test -race -run '^TestHTTP3LiveIndependentClient$/^6b3343cf$'
```

The keepalive migration test also reproduces a v2 stall with a 100 ms
keepalive period across two migrations. A diagnostic run recorded eight failed
authentications, no accepted key-phase transition, and application next packet
number 105 before idle expiry. This is consistent with the known first-update
failure; that diagnostic did not independently decode the upstream update.
The default migration keepalive gate uses 500 ms and stays below that traffic
volume. The faster unresolved case remains available in strict mode:

```sh
QUIC_INTEROP_KNOWN_FAILURES=1 go test -race -run '^TestKeepAliveMigrationUpstream$/6b3343cf/clean$' -v
```

No production derivation workaround or upstream source patch is applied.
Future work must verify a corrected independent peer and reassess upstream
compatibility. A full key-update side-channel/lifecycle audit remains outstanding in this
implementation; usage limits are now enforced.

## PATH_CHALLENGE on the current path

Fixed-path validation succeeds against the upstream client (default zero-length
CID and shared-socket CID configurations) in both versions. Against the upstream
server, the validation attempt expires in both v1 and v2.

The pinned source `connection.go:2049` queues PATH_RESPONSE directly only in the
client role. The server path in `connection.go:1262` returns immediately when the
remote address equals the current peer, bypassing the path manager that otherwise
creates a response. RFC 9000 section 8.2.2 requires responding on the incoming
path; section 8.2 also permits checking the current path after quiescence.

The default suite explicitly skips these two **path-validation** subtests. The
handshake, DATAGRAM, and existing stream checks continue independently. Reproduce:

```sh
QUIC_INTEROP_PATH_FAILURES=1 go test -run '^TestUpstreamHandshake$/^version-1$/^client$/^path-validation$' -v ./...
```

The independent engine answers current-path probes and tests both versions.
No implementation workaround or upstream modification is applied. Validation
against another independent server remains an open interoperability gate.

## Version information after fallback to v2

`TestPublicDialUpstreamFallback/6b3343cf` receives Version Negotiation from the
v2-only upstream v0.62.0 server, starts a fresh v2 attempt, and fails with local
transport error 0x11 because the server's transport parameters omit
version_information. The pinned `internal/wire/transport_parameters.go` has no
version_information field or encoder. RFC 9368 sections 4 and 8 require this
metadata after fallback to v2; the absence exception applies only to v1. Fallback
to the upstream v1 server passes using that legacy exception. Our own listener
and dialer pass fallback in both directions with the parameter present.

This additional default skip is an unresolved interoperability gate. Reproduce:

```sh
QUIC_INTEROP_VN_FAILURES=1 go test -run '^TestPublicDialUpstreamFallback$/^6b3343cf$' -v
```

No bypass of the missing-metadata check or upstream source modification is applied.

The same missing-version-information failure was observed with the unmodified
v0.63.0 peer on 2026-09-29 in
`TestHTTP3TransportProtocolOptions/6b3343cf/automatic-datagrams`. That case uses a
nil QUICConfig and therefore the dialer's default version list against a v2-only
server. It remains opt-in with QUIC_INTEROP_VN_FAILURES. Explicit-v2 DATAGRAM
configuration remains enabled and passes independently; automatic configuration
passes against v1. The skipped fallback case is not evidence of v2 success.


## ls-qpack interactive dynamic compression

The optional ls-qpack peer at 91567706c41c0d97ab8dc576873ecd472d7869fa accepts
our raw decoder feedback and decodes all surviving sections, but its encoder
falls back to literals after three blocked sections with duplication enabled.
The 80-section fixture sustains dynamic references with `LSQPACK_ENC_OPT_NO_DUP`.
Both configurations run by default, including one-byte feedback fragmentation;
the duplicate-enabled case explicitly checks the observed reduced coverage.

Reproduce the unresolved sustained-compression gate from this directory:

```sh
LSQPACK_BIN=/tmp/quic-lsqpack-peer-build/build/bin QUIC_INTEROP_KNOWN_FAILURES=1 go test -run 'TestLSQPACKInteractiveFeedback/duplicates=true' -v
```

The first normal-mode feedback records are `01 80`, `01 84`, `01 88 01`, then
`01 01 01 01`. Our decoder accounts for Section Acknowledgment advancing Known
Received Count before generating subsequent increments, as described by
[RFC 9204 section 2.1.4](https://datatracker.ietf.org/doc/html/rfc9204#section-2.1.4).
The peer's `enc_proc_header_ack` updates `qpe_max_acked_id`, whereas `enc_proc_ici`
adds increments to the separate `qpe_last_ici` and can ignore them as duplicates.
A reduced reproduction now confirms the counter mismatch without using any of our
decoder code. After two insertions, ACK stream 0 sets Known Received Count to 2;
a third insertion followed by increment 1 leaves it at 2, violating the required
advance to 3. Run from this directory:

```sh
LSQPACK_BIN=/tmp/quic-lsqpack-peer-build/build/bin python3 lsqpack/check-feedback.py
```

The script exits 1 on the unmodified pin. Its read-only driver command observes
private struct counters; this diagnostic intentionally depends on the exact pin.
A separate temporary build with `lsqpack/accounting-experiment.patch` uses the
current Known Received Count for increment bounds and addition. The reduced probe
then passes, and all four duplicate-enabled interactive cases sustain 80 blocked
sections (27 cancellations where enabled), including one-byte feedback. This
isolates the fallback's cause for this fixture. It does not establish broader
correctness of that experimental patch or replace unmodified-peer interop.

The canonical build script does not apply the patch. Production decoder behavior
is unchanged, and the original strict gate still fails against the unmodified
peer. Our existing encoder reference tests independently pass ACK-followed-by-
increment scenarios and increment bounds under race.


## Historical GOAWAY active-response loss (synchronized regression now passes)

During the client-resumption full race run, the v1 case of
`TestHTTP3ClientGoawayWakesQueuedRequest` received only `start` of `startend`,
then a peer close with code 0x100 and reason `HTTP3 request loop stopped`.
Twenty immediate focused race repetitions passed. That test supplies no client
session cache, so it does not exercise the new resumed-handshake path; the cause
has not been established. It remains enabled in the default suite. A passing
repeat does not resolve this observed failure.

```sh
go test -race -run '^TestHTTP3ClientGoawayWakesQueuedRequest$' -count=20 .
```

## Intermittent server-resumption dial timeout

During the client ticket-version binding full race run,
`TestServerResumptionUpstream/1` failed at upstream DialAddr with its five-second
context deadline. The output did not identify the round; the fixture now includes
it. No production server resumption behavior changed in that turn. Five focused
race repetitions of client/server resumption, shared-cache resumption and the
GOAWAY case passed. This does not establish the timeout's cause or resolve it;
the test stays enabled with its original deadline. The same full run also
reproduced the GOAWAY active-response failure above.

## Buffered HTTP/3 response loss after graceful close: regression fixed

`TestHTTP3ClientGoawayBufferedResponseAfterClose` now reproduces the GOAWAY
response-loss symptom without sleeps. It finishes the active handler, waits for
server Shutdown to return successfully, then waits for the client's raw QUIC
connection context to observe peer close before reading the response body.
Both v1 and v2 failed in all three race repetitions: zero body bytes and peer
application close 0x100, reason `HTTP3 request loop stopped`. The ordinary timing
case passed ten race repetitions. Both endpoints in this fixture are ours;
placement in the interop module does not make this independent-peer evidence.

```sh
QUIC_INTEROP_KNOWN_FAILURES=1 go test -race -run '^TestHTTP3ClientGoawayBufferedResponseAfterClose$' -count=3 .
```

The new strict case is opt-in; the pre-existing intermittent test stays enabled.
Inspection identifies two relevant paths: transport `beginDraining` clears
received stream buffers and `ReadStream` returns the connection error before
reading; HTTP/3 response contexts also inherit session cancellation. A successful
server delivery barrier establishes packet acknowledgment, not application body
consumption. Preserving completed response delivery requires addressing both
buffer lifetime and HTTP/3 cancellation/cleanup, while bounding retained state.
No production fix has been made yet, and passing ordinary timing does not resolve
this failure.

The first production step retains accepted, completed transport receive buffers
across peer close, consumes them without sending credit updates and releases them
on EOF or cancellation. Engine v1/v2 tests pass, including after routing Close;
incomplete and unaccepted streams are discarded. The strict HTTP/3 case still
fails for both versions. Its remaining paths include ExchangeRequest's session-
cancellation callback (CancelRead), MessageBody's canceled-context check, and
ClientConn's monitor requesting transport closure. Engine retention alone
is not a public-stream or HTTP/3 delivery fix.

The synchronized peer-close regression now passes following completed receive
retention and HTTP/3 lease-cancellation changes. A peer H3_NO_ERROR close after
GOAWAY no longer cancels lower-ID active response contexts; caller cancellation,
rejected IDs and other failures still cancel them. The new test is enabled by
default, with an additional request-cancellation-after-close case. The earlier
failing output above is historical evidence, not the current test outcome.

A root race run exposed a related interrupted-read error race: io.ErrClosedPipe
could mask ErrRequestRejected. Response reads now normalize interruption errors
when Body.Close has not been called. Thirty managed-exchange race repetitions
pass after that fix. Extended unread-body lifetimes past UDP owner shutdown and
explicit ClientConn.Close after an already observed peer close still need audit;
the synchronized regression does not establish those broader guarantees.

Explicit ClientConn.Close after prior graceful peer close now has a default-suite
regression. Before the fix, the v1 fixture read the entire `startend` body with no
error after explicit Close. Independent request cancellation lifetimes now make
Close cancel active exchanges even when the session already has a peer-close
cause. Ten post-fix race repetitions of explicit close, GOAWAY and client-close
cases pass in both versions. The lifetime registry is bounded by existing
request slots and released on failed admission/exchange or Body.Close. Parent
lifetime cancellation after earlier peer close and reads after UDP owner shutdown
remain separate audit items.

Original ClientConn lifetime cancellation after earlier peer close is now covered
by a default-suite regression using a distinct cancellation cause. Before the
fix, v1 consumed the whole buffered response with nil error. Active request
lifetimes now observe the original client lifetime independently of the peer-
canceled session; Body.Read checks caller cancellation synchronously. Ten race
repetitions of lifetime cancellation, explicit Close, GOAWAY and client-close
cases pass in both versions. Callback cleanup stops or joins the lifetime
observer. Buffered reads after the UDP owner exits remain an outstanding item.

Completed public stream reads after UDP owner termination are now supported by a
serialized ownership handoff after UDPServer.Done closes. The pre-fix public
regression returned ErrEndpointClosed for all four streams in each version.
Post-fix public race repetitions verify exact retained payloads, EOF/release,
late read cancellation and deadlines across concurrent streams. The default
HTTP/3 test TestHTTP3ClientResponseAfterRuntimeStops polls actual endpoint closure
before reading the response, rather than assuming a fixed draining delay.
These fixtures cover the demonstrated buffered-body case; they do not establish
all trailer/QPACK and abnormal-close combinations.

## Intermittent peer-address observation after lossy migration

After the sparse ACK lookup optimization, one full macOS race-suite run failed
`TestIndependentClientMigrationUpstreamServer/6b3343cf/both`: the stream request
arrived, but the immediate upstream RemoteAddr check still reported another
relay binding. Five focused race repetitions then passed (21.580 seconds).
The cause and any relationship to the recovery change are unresolved. A focused
pass does not erase the full-suite failure; the address assertion remains active.

Follow-up: pinned upstream source exposes an address-publication race in this
fixture. `handleShortHeaderPacket` invokes `handleUnpackedShortHeaderPacket`
(and stream delivery) before calling `ChangeRemoteAddr`. The application can
therefore finish reading the request before publication. The fixture now waits
up to one second, bounded by its existing context, for the exact expected tuple;
connection closure or a persistently wrong address still fails. This corrects
the unsynchronized assertion without changing transport behavior.

Validation after the fixture correction: all migration cases passed three race
repetitions, and the full default independent-peer race suite passed afterward
(59.389 seconds). The separate strict v2 key-update gate remains unresolved.

## Picoquic IPv6-only preferred address

At pinned revision `8c6bbf82b6b6d98a413a69e6bf2265e7f6a10161`,
`picoquic/transport.c:117` encodes `preferred_address->ipv4Port` into the IPv6
port field. An IPv6-only advertisement consequently carries port zero.
The v1 IPv6-only diagnostic completes TLS but times out waiting for our client's
preferred-address switch; peer qlog shows the IPv6 loopback address and zero port.
Reproduce from this directory:

```sh
PICOQUIC_DEMO=/absolute/path/to/picoquicdemo QUIC_INTEROP_KNOWN_FAILURES=1 \
  go test -race -run '^TestPicoquicPreferredServer$/^1$/^::1$/^loss=false$' -count=1
```

The default optional gate advertises both families on the same preferred port,
which yields valid wire parameters and passes migration plus exact file transfer
for v1/v2 over IPv4/IPv6. No peer or production implementation patch is applied.
This is a restriction of that external test, not general IPv6-only interop proof.
Our own preferred-address server tests separately cover single-family sockets.

The initial fixture also supplied unsupported `IP:port` values to the peer's
`-4`/`-6` arguments. Despite the help text, the pinned parser accepts bare IPs.
The corrected fixture uses `-p original:preferred`; supplying just one port
leaves the preferred port zero, and using the same port twice fails socket bind.
The initial suspected byte-order bug was incorrect: the text-address helper
stores this particular port directly in host order. Those fixture failures must
not be counted as transport defects or passing interop results.

## Pinned Picoquic v2-to-v1 compatible switching (2026-09-25)

The external Picoquic pin `8c6bbf82b6b6d98a413a69e6bf2265e7f6a10161`
accepts a desired v1 version in its test application driver, but does not complete
its v2-to-v1 transition against our server. Our listener times out waiting for
handshake acceptance. The same fixture's v1-to-v2 cases succeed. Local engine and
public own-to-own tests pass both directions; they do not prove independent
reverse-direction interoperability.

The pinned source's `picoquic/quicctx.c` supported-version table gives the v1
entry a null `upgrade_from` list. The v2 entry permits upgrading from v1.
`picoquic_process_version_upgrade` rejects a different version unless that new
version's list includes the original. `picoquic/packet.c` calls it when a client
receives the desired alternate version. This explains the observed limitation;
the peer library has not been patched. The driver calls its public
`picoquic_set_desired_version` API before starting TLS.

The v2-origin branch of TestPicoquicClientCompatibleMultipath is skipped by
default and retained as an explicitly failing diagnostic:

```sh
PICOQUIC_CLIENT=/absolute/path/to/picoquic-client \
QUIC_INTEROP_KNOWN_FAILURES=1 \
  go test -race -count=1 \
  -run '^TestPicoquicClientCompatibleMultipath$/^127.0.0.1$/^6b3343cf$/^clean$'
```

The initial broad run observed clean, request-loss and response-loss timeouts
before being deliberately stopped to avoid repeating the same 20-second failure
in every impairment case. This is not a successful reverse-direction gate.

## Intermittent Picoquic client multipath transfer timeout (2026-09-25)

A combined reverse-role regression run failed
`TestPicoquicClientMultipath/127.0.0.1/1/both`: our server's response write timed
out after 1,019,160 bytes of its 1,047,552-byte remainder. The peer reported
`client ret=0 finished=0 opened=1 failed=1`. This case does not request version
switching or greasing. The relay drops the first additional-path request and
response. The cause is unresolved; do not attribute it to compatible negotiation
without further evidence. Preserve this separately from the deterministic
v2-to-v1 transition limitation.

Five focused race repetitions of the ordinary multipath timeout case passed
(3.457s). These repetitions do not establish a fix. The strict v2-to-v1 clean
diagnostic separately reproduced its timeout in 20.268s.

The final combined reverse-role ordinary/greased/compatible race rerun passed
(50 cases, 14.350s, with unsupported v2-origin switching branches skipped). The
intermittent transfer timeout remains unresolved despite this passing rerun.

## Intermittent ticket-policy handshake timeout (2026-09-28)

While validating public early dialing, the broader resumption/early race-test group
twice timed out in `TestServerResumptionTicketPolicy` during upstream `DialAddr`:
first v1/custom=true/selected=true, then v2/custom=true/selected=true. These are
ordinary resumption-policy cases. No protocol cause or attribution is established.
The case and new early acceptance tests passed on immediate rerun; five standalone
policy repetitions and three full selected-group repetitions subsequently passed.
Stage and remaining-deadline logs now accompany failures; timeouts and assertions
are unchanged. The case remains enabled. Further diagnosis requires reproducing
with packet/handshake evidence, not treating these reruns as a proven fix.
