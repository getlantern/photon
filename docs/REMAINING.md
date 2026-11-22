# Remaining requirements and evidence map

Summary refreshed 2026-10-09 against the current worktree. This is a gap map, not a
completion certificate or a reduction of the user's objective. Implementing a
feature, passing a local gate, and proving conformance are separate claims.
Chronological checkpoints are useful evidence but may describe superseded code.
The independent quic-go peer is now pinned to v0.65.0 (moved from v0.63.0 on
2026-11-22; v0.63.0 measurements below are historical). HTTP server URL metadata
and ClientConn address methods are aligned. Public HTTP/3 error wrapping now
covers client exchange/body and tunnel return boundaries, including peer-reset
upload cause propagation. Server handler body/response/tunnel I/O now also
converts eligible errors. Client setup/recovery return boundaries also convert
errors, with live peer-close setup gates. Complete failure ordering coverage and
broader close/error contract parity remain open. ApplicationError now exposes
local/remote application codes and reasons while retaining legacy error chains. The latest matched v0.63.0 loopback echo comparison still shows about 1.87x
slower exchanges and 2.55x allocated bytes after eliminating native ECN send allocations; older
records retain their earlier code, fixture and reference versions.

Partial stream reset now has public receive configuration, asymmetric capability
state, reliable-boundary cancellation, ticket memory and early-data integration.
Independent v1/v2 tests cover both TLS roles, selected server policy, rejection
and single-packet loss. Own-peer UDP tests combine negotiated partial resets and
multipath after primary-path retirement. Full impairment, concurrency, normative
and combined-extension audits remain; see [partial reset](PARTIAL_STREAM_RESET.md).

| Required outcome | Current evidence | What is not established |
| --- | --- | --- |
| Independent implementation | Root transport/crypto/recovery/HTTP packages; upstream peer isolated in interop/go.mod | Completed provenance/dependency/license audit for release |
| RFC 9000/9001/9002 coverage | Internal wire, handshake, packet, stream, recovery and transport implementations; public root race suite and independent gates | Exhaustive normative requirement-to-code/test map (started in CONFORMANCE.md); remaining early-data combinations; independent security review |
| QUIC v2 and compatible negotiation | v1/v2 vectors and own-peer tests; Picoquic compatible switching in both TLS roles | Independent v2-to-v1 switching; the former quic-go v2 key-update exclusions were removed with the v0.65.0 pin |
| Migration and multipath | Public migration and opt-in multipath; Picoquic loss/rebinding/bulk-transfer gates | Full draft audit, heterogeneous path scheduler/fairness evaluation, final RFC/code-point revalidation |
| Public/API feature parity | Root Config/Conn/stream APIs, HTTP server/client/RoundTripper, DATAGRAM/capsules and QPACK; generated root/HTTP3 export inventories in API_PARITY.md | 11 root and 62 HTTP/3 missing symbol entries, including remaining Transport capabilities, stream I/O and raw HTTP APIs; complete signature/behavior audit and other-package inventories; early API, receive-window tuning/large-window and observability parity |
| Performance objective | Matched v1 loopback echo plus focused allocation benchmarks | Current echo remains about 1.87x slower than upstream in a short run; WAN, tail latency, many connections, CPU/peak-memory/fairness and cross-platform comparisons are incomplete |
| Release-quality verification | Root/default interop race/vet, bounded fuzz campaigns, opt-in external fixtures | Sustained fuzzing, Linux independent interop completion, other-platform runtime gates, external audit and full independent matrix |

Separate migration probing/switching now has an internal candidate mode that
keeps old-path traffic active, sends only probing frames and defers binding commit.
Internal multi-Transport probe/switch/STREAM/close routing now runs over separately
owned UDP sockets with CID publication and pre-sealing capacity reservations.
Uncommitted foreign-probe write failures now preserve the established path, with
zero-byte recovery rollback and protection against stale completions affecting a
replacement candidate. Public AddPath/Probe/Switch now exposes staged migration.
Own-peer v1/v2 tests cover six switches and cancellation/closure; upstream v1/v2
covers two switches, including abandonment of an unused validated probe. Longer
upstream sequences exhaust peer probe CIDs, also reproduced with upstream in both
roles. Broader failure/cancellation coverage, multiple pending candidates,
address-family changes and full behavior parity remain open; see [migration API work](MIGRATION_API_PLAN.md).

Migration proof now rejects peer-retired CID reservations even while reliable
retirement queue pressure retains the candidate for cleanup. Owner-level v1/v2
regressions cover handle admission, endpoint servicing and output; existing send
and commit guards already rejected the revoked CID. Public own-peer tests also
cover a source Transport carrying a resident connection alongside the migrated
connection, including resident traffic after migrated-connection closure. These
checks do not establish arbitrary source-sharing races or independent combined
retirement/migration coverage.

Established cross-Transport migration now forwards otherwise-unroutable resets
using immutable tuple/token hints, without reading Conn state on the source.
Only used active tokens are published, including before first-use source output.
The destination retains current tuple/token and authentication checks. Independent
v1/v2 restarted-upstream tests verify the typed reset result after AddPath/Probe/
Switch. Resets during eligible locally initiated probes are also covered by an
independent restarted-upstream fixture that disables the original socket in both
directions. Expired, unused, retired and canceled candidates do not grant reset
authority. Ambiguous normal-CID/reset dispatch, independent preferred-address and
broader multipath/source-sharing combinations, and wildcard original bindings
remain gaps.

Owned-socket dispatch now handles resets whose random prefix matches a different
local connection's CID: authentication takes precedence, then a unique eligible
local reset target can receive the datagram. Unit and independent modified-prefix
upstream-reset tests cover v1/v2 while preserving the unrelated connection.
Initial unknown-CID routing now rejects multiple eligible local token owners,
including fixed-peer/migrated-tuple combinations. Endpoint v1/v2 tests preserve
both connections on ambiguity and accept the reset once only one remains eligible.
The stateful token lifecycle fuzzer also compares initial routing with live tuple
selection. UDP dispatch now also compares local matches with immutable foreign
hints before consuming an unknown-CID reset. Ambiguity within either domain or
across them drops the packet. A stale foreign hint conservatively suppresses a
local reset until publication catches up. Known-CID duplicate-token ambiguity
and final cross-owner state changes still need a unified audit.

Local-CID-first collisions now also reach a foreign reset owner after failed local
authentication, with one-hop buffer/provenance transfer and final live eligibility
checks. Independent v1/v2 coverage disables the migrated connection's old socket
and preserves the resident connection. Closing-state authentication and authenticated
reserved-bit/key-phase error classification now suppress reset fallback. The reverse
foreign-CID-first route now returns failed authentication through a bounded mailbox
and delivers only to the original, still-eligible reset target. Independent v1/v2
cases cover local and other-foreign targets while preserving the first connection.
Stale/replaced/ambiguous targets, queue pressure and shutdown have owner-level race
coverage. Full malformed-header/key-update combinations, combined multipath cases
and global reset ambiguity on other dispatch paths remain open.

Long-header reset recognition is now covered across local and foreign dispatch,
including coalesced authentication precedence. Independent fixtures transform only
the reset prefix while preserving upstream-generated tokens and unrelated traffic.
This closes the header-form filtering gap. Duplicate tokens among local owners
are now rejected by initial unknown-CID routing, and UDP dispatch includes
foreign hints in that decision. Known-CID duplicate tokens, final handoff state
changes and the broader combinations listed above still require audit.

Migration observation (2026-10-07): the full independent race gate fails in the
v1 response-loss case at the one-second upstream-address assertion. The same
run overlaps a fuzz campaign that ends in a harness context deadline error;
an isolated fuzz retry passes. Neither failure's cause is established, and no
timeout has been relaxed. Ten isolated repetitions of the full migration matrix
pass (162.909s), leaving the intermittent cause unverified. Opt-in bounded
migration tracing now records peer frame/loss events, relay tuples and application
milestones; ten traced v1 response/both-loss repetitions and a subsequent untraced
full migration matrix pass. The cause remains unverified; see the
[diagnostic report](diagnostics/migration-address-trace.md).

Migration observation (2026-09-30): one full independent race run timed out waiting
for the upstream peer's expected address in the v1 both-loss fixture. Ten repeat
runs of all migration versions/loss modes passed without reproducing it. The
cause remains unverified; the one-second address assertion was not relaxed.

HTTP/3 observation (2026-09-30): a full independent race run timed out in
TestHTTP3ResponseWriterSettingsUpstream/v1/datagrams=true while the upstream
client awaited RoundTrip (five-second context). Twenty initial repeats and 200 further instrumented repeats (800 v1/v2 ×
DATAGRAM-mode subcases) passed. The test now reports handler progress and any available handler result
on failure; deadlines and assertions are unchanged. A fresh full independent race run passed
(87.782s). The cause is unresolved,
and this is separate from the migration address-publication observation above.

The 2026-09-30 interoperability run also exposed a distinct HTTP/3 admission race:
a request released its concurrency slot before deleting active priority state,
allowing a replacement request to exceed the priority-state limit. Cleanup now
removes priority state before releasing admission. A mutex-gated regression failed
with the old order and passed after the fix; 100 independent-client repetitions
also passed. This does not diagnose either earlier timeout observation.

Stream SetPriority now schedules by retransmission status, urgency and
incremental/non-incremental turns. Tests cover packet order, dynamic updates,
clipping, split retransmissions, borrowed paths, concurrent writes, congestion
transactionality and a flow-blocked stream. A sorting-model fuzz oracle covers
queue preservation and selection with 123,789 bounded local executions. HTTP/3
request Priority fields now drive scheduling before handler dispatch, including
datagram wrappers. Critical control/QPACK streams use urgency zero and incremental
sharing. Structured-field parsing has vectors and a 1.92-million-execution fuzz
campaign. PRIORITY_UPDATE now has bounded wire parsing, role/placement/ID
validation and public unpromised-push rejection. Valid updates now apply to
active/future requests through bounded state, take precedence over headers and
are released after request cleanup. Checking live stream limits remains open,
alongside response overrides, priority across different
path queues, broad loss/flow-control
combinations and indexed queue scalability remain open. Current selection is
allocation-free but linear in queued items; microbenchmarks do not establish
end-to-end performance superiority.

Transport.ConnContext now propagates values into incoming TLS callbacks, public
connections and send streams, with cancellation on local/peer/socket closure,
admission rejection and setup failure. Local v1/v2 coverage includes Retry
metadata and listener replacement; independent-peer coverage checks TLS values
and peer-close causes. Outgoing contexts preserve dial values independently of
handshake cancellation, including 0-RTT acceptance/rejection and compatible
version switching. Qlog integration and broader context/early-data/admission
failure combinations remain open.

Package-level PacketConn Dial/DialEarly/Listen/ListenEarly helpers now manage
dedicated borrowed-socket runtimes, including setup failures and terminal reader
cleanup. Native/plain/OOB v1/v2 lifecycle, cached early-data and independent
normal exchanges pass. Broader independent cached early-data/lifetime comparisons
remain open; these additions do not establish full API behavior parity.

Transport now exposes Listen, ListenEarly, Dial, DialEarly and Close on caller-owned *net.UDPConn
sockets. Listener closure/restart preserves accepted connections; generations
prevent stale handles from operating on replacements. Borrowed runtime shutdown
joins readers and clears deadlines while preserving the socket. Own-peer v1/v2
lifecycle tests and upstream post-restart stream transfers pass. Shared dialing
has failure-isolation, 0-RTT, version fallback, own-peer multipath and independent
simultaneous-role transfer coverage. Generic PacketConn wrappers with UDP
addresses now have own-peer and independent v1/v2 coverage; they conservatively
omit ECN/kernel MTU features. OOBCapablePacketConn wrappers now preserve socket
controls and ancillary I/O, with IPv4/IPv6 ECN tests and independent v1/v2 shared
transport transfers. Raw WriteTo/ReadNonQUICPacket share the primary socket with
bounded capture and known-QUIC-route precedence, including live greasing tests.
DisableVersionNegotiationPackets is implemented with restart and reference-policy
coverage. Preferred-address registration and remaining Transport options still
need implementation and independent validation. Internal admission now supports
a policy-controlled no-Retry path with bounded amplification and accurate
configuration-selection validation metadata. Transport.VerifySourceAddress now exposes this policy, defaults to skipping Retry
and retains the configured callback across listener restarts. Own-peer and
independent v1/v2 tests verify nil/false/true and validation metadata. Legacy
ListenAddr still requires Retry; convenience-listener default alignment remains.
Transport token keys now persist across listener restart, with configurable
TokenGeneratorKey supporting transport replacement and shared server keys.
MaxTokenAge now supports positive Go durations, with nanosecond expiry and
legacy-token validation. Local client storage still caps token age at 24 hours;
broader storage/reference lifetime contract parity remains open.

Initial receive windows default to 512 KiB per stream and 768 KiB per connection.
Zero maxima enable growth to at least 6 MiB / 15 MiB, matching the reference's
normal defaults; explicit maxima equal to initial values keep windows fixed. A consumption/RTT policy
can grow stream buffers up to 16 MiB and connection credit up to its configured
maximum. Engine tests prove growth and emitted credit; independent tests exercise
fixed and growth-enabled configurations in both roles and all receive directions.
Full bulk v2 transfers run by default since the v0.65.0 pin. AllowConnectionWindowIncrease now
lets applications veto proposed connection-credit growth; v1/v2 tests cover both
public roles, denial/retry, maximum bounds and cleanup. Total memory-pressure
behavior, large-window/clipping parity and heterogeneous-path tuning remain unfinished.
Receive storage now grows with the buffered span: an in-order first byte uses
4.5 KiB for payload and bitmap, excluding other overhead. Far-ahead data can still
allocate the entire window; sparse storage remains a performance gap.

Important evidence locations:

- [Exact-sized short headers](benchmarks/short-header.md): profile-guided removal
  of one or two allocations per header; the matched echo measures 4.75% fewer
  allocations and 1.72% fewer allocated bytes without an established speedup.
- [Typed ACK-only retention](benchmarks/ack-only.md): one fewer allocation and
  32 fewer allocated bytes per retained send in a matched recovery microbenchmark;
  the independent echo follow-up measures 2.26% fewer allocations and 2.41% fewer
  allocated bytes against its control, without a demonstrated throughput gain.
- [Current plan](PLAN.md), [recent checkpoints](PROGRESS.md),
  [incremental conformance audit](CONFORMANCE.md).
- [Interop exceptions](../interop/KNOWN_FAILURES.md): never count opt-in failures
  or environment-skipped external fixtures as passing coverage.
- [Version negotiation](COMPATIBLE_VERSION.md), [multipath](MULTIPATH.md),
  [PMTU](MTU.md), [greasing](QUIC_BIT.md), [HTTP APIs](../http3/README.md).
- [Embedded recovery-history links](benchmarks/history-intrusive.md): another allocation
  removed per history send/ACK, about 4.24% fewer echo allocations in a matched
  short run, with no demonstrated throughput gain. The preceding
  [boxing reduction](benchmarks/history-boxing.md) retains its historical results.
- [End-to-end performance](benchmarks/stream-echo.md): ciphertext-buffer reuse
  reduces allocations; it does not establish overall performance superiority.

## Accepted 0-RTT: implemented foundation and remaining combinations

`DialAddrEarly` now opts clients into early sending and `Config.Allow0RTT` opts
servers into acceptance. Ordinary dialing and default server policy still disable
early data. Public UDP tests cover acceptance/rejection, FIN-closed stale handles,
stream-ID reuse and canceling the dial context after early return. Independent
upstream v1/v2 STREAM and DATAGRAM acceptance/rejection tests now cover both
roles with actual early wire-packet gates. DATAGRAM tests include client packet
loss, empty and boundary payloads, ownership, valid-token admission and responses
before client Finished. `ListenAddrEarly` exposes server connections and data before
client Finished; ordinary `Listener.Accept` still waits for confirmation.
`NextConnection` now waits for TLS completion and releases the rejected stream
gate on the same Conn; stale stream handles remain invalid. Post-return VN
fallback and combined extensions remain.

`ticket_state.go` restores version-bound transport metadata, and
`internal/wire/early_transport.go` encodes remembered values. Private cache copies
preserve early capability only for an enabled adapter and valid state. `DialAddr`
still returns an established connection; `DialAddrEarly` is the opt-in early API.
`interop/resumption_early_reject_test.go` remains a rejection gate.

Finish the following before treating early-data support as complete:

1. **TLS policy and events.** Endpoint opt-in, HTTP resource policy and transport
   key retirement are implemented with targeted tests; complete their combined
   coverage and clause-level audit. Keep
   tickets bound to version, ALPN and valid remembered state. Rejection resets
   stream and associated application state; accepting replayable data requires
   an explicit application contract. Test shared caches and ticket callbacks.
   [RFC 9001 sections 4.6 and 4.9.3](https://www.rfc-editor.org/rfc/rfc9001.html#section-4.6).
2. **Remembered transport state.** TLS acceptance checks now compare mandatory
   limits. Map failures to transport errors and apply cached credit only to early packets;
   validate accepted limits. Initial-limit promotion now updates all sending
   streams at client handshake completion, with role/direction and real resumed
   handshake tests. The private client sender now installs remembered credit and resets rejected
   stream state. Public stream and HTTP session invalidation now have dedicated tests; broader combined-extension invalidation remains open. Do not reuse
   connection-specific IDs, ACK policy or reset-token state. Test reduced limits,
   missing metadata, invalid schemas and extension parameters.
   [RFC 9000 section 7.4.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-7.4.1).
3. **Packet lifecycle.** Server receive now shares application packet numbers
   with distinct early keys, frame validation and 1-RTT acknowledgement. Private
   STREAM sending/loss/rejection and Retry now have engine tests. Complete
   endpoint Retry/NEW_TOKEN combinations and combined/independent gates. Early
   DATAGRAM sending now has size, rejection, loss and ownership tests; fixed-address
   endpoint routing has admitted-connection/delayed-packet coverage. Preserve packet numbers through Retry, update destination
   IDs correctly, and stop early sending once processing server 1-RTT.
   [RFC 9000 sections 12.3 and 17.2.3](https://www.rfc-editor.org/rfc/rfc9000.html#section-17.2.3).
4. **Public behavior.** Rejection-safe stream release now permanently invalidates
   public handles, including FIN-closed delivery waits and runtime-shutdown cases;
   adapter tests verify stale cancellation does not affect a live same-ID stream.
   Public UDP tests now also cover real early acceptance/rejection and same-ID
   handle isolation. Explicit NextConnection transitions now have public and independent v1/v2
   coverage; complete broader early API coverage. HandshakeComplete notification and atomic
   connection-state snapshots now exist, with completion/confirmation updates and
   concurrent-reader tests. Complete
   stream rejection errors and application recovery against the reference API.
   Test cancellation, concurrent stream operations and cache isolation. Exposing
   early handles must not silently replay application requests after rejection.
5. **Combined gates.** Basic accepted and rejected early STREAM/DATAGRAM
   transfers now have independent coverage in both TLS roles and v1/v2. Complete
   loss/reorder/Retry matrices, changed
   parameters and independent compatible switching. Local compatible-switching
   STREAM/DATAGRAM matrices now cover both directions, acceptance, rejection,
   reduced limits and loss; public UDP cases include Retry and explicit recovery.
   Picoquic now independently verifies v1 early delivery and v1-to-v2 rejection
   recovery; strict early acceptance during the switch is a pinned-peer failure.
   HTTP server early GET dispatch now has an upstream v1/v2 gate; request TLS
   metadata is refreshed per dispatch. Remembered SETTINGS compatibility now has
   an internal validation component, including exact nonzero QPACK capacity and
   omission rules. Client HTTP Dial now binds validated peer SETTINGS to each
   ticket and canonical origin/local advertisement policy. Public DialEarly now
   uses that metadata, with explicit GET/HEAD early methods and NextConnection
   reconstruction. Server.ConfigureTLSConfig now binds authenticated ticket SETTINGS;
   independent v1/v2 peers verify acceptance/rejection across header-limit and
   extension changes while retaining TLS resumption. Internal sessions now initialize
   early encoders and promote live limits in place, preserving table/reference
   state and supporting decoder feedback before SETTINGS.
   Independent UDP v1/v2 tests now verify rejected internal-session teardown,
   surviving QUIC and a fresh public HTTP client request after explicit recovery.
   Public GET/HEAD acceptance/rejection and concurrent reconstruction now have
   upstream v1/v2 gates, including harmless old-client Close, proven accepted HTTP
   delivery before TLS completion and canceled waits before handoff. Independent
   reduced-credit tests cover cancellation during fresh critical-stream setup and
   retain terminal recovery causes. One-byte stream windows now verify initial
   DialEarly setup rejection/reconstruction and cancellation of a blocked SETTINGS
   write. Transport.Enable0RTT now integrates worker-owned pooled recovery and
   bounded rejection retries. Callback-error spoofing and closure during a gated
   early handshake now have independent tests. Expand simultaneous rejection/handoff/closure
   and body-error evidence gates. Pooled explicit early requests now support bounded
   425 retries as ordinary GET/HEAD, with GetBody error/cancellation/closure ownership
   gates. RFC 8470 Early-Data normalization and field-placement rules now have
   component coverage. Server.AllowEarlyData now gates ordinary/extension resources,
   defaults to TLS deferral or forwarded-marker 425, and has independent dispatch
   and malformed-request-marker gates. Independent response/trailer misuse tests
   preserve the connection and request slot. Extended CONNECT and datagram routing
   now have independent forwarded-marker denial/approval and single-slot reuse
   gates. Add request-trailer misuse, direct early extension dispatch and concurrent
   extension cancellation gates; expand response-time cancellation coverage. Finish
   multipath handoff gates
   before claiming those combinations supported.

Do not enable TLS EarlyData alone: the packet engine and public stream state
must handle the same outcome. Until all dependent pieces are integrated, keep
the existing early-data rejection behavior explicit.

## Standards freshness

On 2026-09-30 the [IETF Datatracker](https://datatracker.ietf.org/doc/draft-ietf-quic-multipath/)
still identifies draft-21 as the latest multipath revision, in the RFC Editor
queue with first edit in progress. It does not yet show a published RFC.
Keep the draft label and recheck code points before release. This observation
only updates specification freshness; it is not a conformance result. A separate
[multipath audit slice](MULTIPATH.md#draft-21-control-frame-audit-2026-09-30) maps
path-limit and disposed-path handling to code and protected v1/v2 tests.

Migration validation finding (2026-09-29): stress reproduced the prior three-second
migration failure three times in 1,000 repetitions, across both versions and with
and without greasing. A deterministic test exposed blocked old-tuple PATH_RESPONSE
output during local migration. The sender now permits these probing responses
while holding ACK/application output, allowing the peer's independent validation
to finish. The deterministic regression and 1,000 post-fix live repetitions pass.
This fixes the demonstrated deadlock; broader migration impairment coverage remains.

Keyed stateless resets (2026-09-29): Transport.StatelessResetKey enables derived
initial, preferred-address, issued and multipath CID tokens and unknown-CID reset
responses. Root v1/v2 restart tests cover both roles; independent clients detect
restarted-server resets. Unknown clear-fixed-bit traffic is not reset. The
100-per-second endpoint-wide cap has no per-source fairness; broader impairment,
reset/migration/multipath combinations and independent-server coverage remain.

Transport.ConnectionIDLength now supports 1–20 bytes and defaults to four, with
own-peer v1/v2 routing tests for every setting. Custom ConnectionIDGenerator is now integrated;
convenience-function default alignment remains open; CID collision handling still
requires broader capacity/stress validation, especially for very short IDs.

CID generator groundwork now reaches admission, replacement, preferred-address
and multipath CID issuance with owned output, length checks and bounded duplicate
rejection. Public ConnectionIDGenerator now has per-Transport serialization, shared-dial
wiring, zero-length handshakes and independent-client verification. Broader
incoming/later-issued collision and impairment combinations remain. Independent
server coverage now joins independent client coverage for all 0–20 CID lengths.
Own-peer tests cover colliding second dials, generator-backed additional-path
migration, primary retirement and listener restart with continued old/new transfer.

Reset forwarding performance (2026-09-30): unchanged shared-peer hint publication
avoids repeated allocation and stale set retention. A lazy source-owned HMAC token
index now removes the local-connection scan from warmed foreign-CID alternate
selection: about 160 ns at 4096 connections versus 0.61 ms for the matched control.
[Measurements](benchmarks/foreign-reset-index.md) also record maintenance costs.
Cold bootstrap and unique established-tuple hits now have measurements: at 4096
connections, about 1.75 ms/3.24 MB allocated to initialize and 235 ns per validated
hit. Stateful differential fuzzing passes 20,026 race-enabled executions. Retained
heap, varied/probe hits, foreign-registration scans and end-to-end/upstream
comparisons remain work. Initial unknown-CID routing now reuses an already active
index and skips impossible zero-CID scans. At 4096 synthetic fixed-peer connections,
median miss/unique lookup falls from 437/416 microseconds to 143/219 nanoseconds;
duplicate detection falls from 47.8 microseconds to 395 nanoseconds. The subsequent
single-owner direct path reduces lookup to 146–151 ns in a separate run; parity
with the original roughly 131 ns baseline is not established. Routing without
an active index still scans. See [routing measurements](benchmarks/router-reset.md). The
[publication report](benchmarks/foreign-reset-publication.md) retains earlier
measurements and changed-snapshot costs; no universal speedup is inferred.
