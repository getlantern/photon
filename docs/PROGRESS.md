# Progress checkpoint

## Latest: 2026-10-09 HTTP/3 Alt-Svc advertisement

Added Server.Port, Server.SetQUICHeaders and ErrNoAltSvcPort. Cached headers
follow active accept-loop registration/removal, use an explicit advertised port
before listener ports, and fall back to Addr only when listeners provide no
usable ports. SetQUICHeaders appends to caller headers. No active listener means
no advertisement. Values preserve registration order and repeated ports, matching
the pinned v0.63.0 reference behavior. Invalid local advertised ports outside
1–65535 are rejected; invalid-number parity is not claimed.

Ten independent API/lifecycle comparison race repetitions pass (1.415s), covering
missing ports, override/fallback, multiple and duplicate listeners, existing
header preservation and listener removal. Live independent v1/v2 startup tests
now verify the advertised bound port in real responses for both TLS setup methods
and shutdown modes. Final full HTTP/3 race passes (1.608s); all independent
TestHTTP3-prefixed tests pass (15.208s); both module vet gates pass. Core QUIC code
is unchanged, so unrelated full root/interop suites were not repeated.

Regenerated HTTP/3 export inventory: 61 missing, 8 declaration differences,
92 identical / 161. Root remains 4 missing / 246. Combined TCP/TLS+QUIC startup,
raw HTTP streams, observability, full conformance/extension coverage and measured
whole-stack performance parity remain open. The intermittent migration-address
failure also remains unresolved. The full goal stays active.

## Latest: 2026-10-09 migration address diagnostics

Added opt-in, bounded independent-peer migration tracing. It records upstream
packet numbers/frame kinds/challenge-response/loss events, relay tuples and
injected loss, and local commit/stream/address milestones. Descriptions are
copied immediately under a mutex; at most 512 records are retained, without
payloads or keys. Disabled tracing skips checksum and address-formatting work.
No production transport change or timeout relaxation.

Ten traced v1 response/both-loss repetitions pass (61.911s). A successful detailed
response-loss sample passes (3.432s), demonstrating dropped first server probes,
client retries, fresh challenge responses and subsequent address confirmation.
The final untraced v1/v2 migration matrix also passes (17.454s), and interop vet
passes. The full independent race suite with failure tracing enabled passes
(91.305s).
The prior intermittent failure has not been reproduced or explained.

[Diagnostic report and captured logs](diagnostics/migration-address-trace.md)
state the evidence limits: upstream's ordinary short-header qlog checksums are
unset, so they cannot provide exact relay-to-event packet correlation. Its frame
processing precedes path/address-switch decisions; stream receipt alone is not
an address-publication barrier. This is a guide for further diagnosis, not a
root-cause finding or production qlog implementation. The full goal stays active.

## Latest: 2026-10-07 single-owner reset lookup

Routing now checks registered state directly with zero or one owner, avoiding
keyed hashing for the measured small-population case. Larger populations retain
the active-index path, and index publication/removal remain unconditional with
respect to population. New transition coverage replaces a token with one owner,
grows to an ambiguous pair, removes down to zero, then re-adds both owners.
A temporary overlay skipping single-owner index publication fails that regression
(0.400s); it is removed.

Five isolated 200 ms samples measure one-owner lookup at 146–151 ns, versus
173–252 ns in the previous indexed run. This reduces the observed regression;
it does not establish parity with the original roughly 131 ns baseline. Large
populations retain sub-microsecond lookups and zero allocations. The
[benchmark report](benchmarks/router-reset.md) records all nine cases, separate
measurement dates and limits. No whole-stack performance claim.

Full root race passes (root 16.853s, HTTP3 2.145s, transport 49.170s). Ten repetitions
of all independent TestStatelessReset-prefixed fixtures pass (4.730s), including
migration, probing, local/foreign CID collisions and long-header handling. Both
module vet gates pass. The broader independent suite was not repeated for this
lookup threshold change; its prior migration-address timeout remains unresolved,
not converted into a passing gate by focused reset tests. The full goal stays
active, with migration diagnosis and wider conformance/API/performance work open.

## Latest: 2026-10-07 indexed initial reset routing

Router.Route/RouteNetwork now reuse an already-active keyed reset-token index.
Every indexed candidate still requires current registration, nonterminal state,
tuple eligibility and a constant-time token match. Index activation policy is
unchanged, and the complete scan remains when no index exists. Normal routing
also avoids a zero-CID search for multi-entry peer buckets, relying on Sync's
exclusive zero-CID registration invariant.

Added warmed unknown-CID miss/unique/duplicate benchmarks and stale-index routing
coverage (fixed-peer and address-aware APIs). Five isolated 200 ms samples show
4096-owner miss/unique medians falling from 437401/415951 ns to 143/219.4 ns;
duplicate detection falls from 47798 ns to 394.9 ns. All cases allocate zero.
One-owner lookup regresses from about 131 ns to 173–252 ns because of keyed lookup
cost. [Report and reproducible baseline](benchmarks/router-reset.md) retain this
tradeoff. No end-to-end or upstream performance claim.

Ten focused router/index/reset race repetitions pass (3.763s); the subsequent
full root race includes the additional stale-index regression and passes (root
16.510s, HTTP3 1.974s, transport 48.773s). Both module vet gates pass. A concurrent
30-second fuzz campaign ends with a harness context deadline error after 10,448
executions; no failing input is reported. An isolated retry passes after 15,313
executions with 19 new coverage inputs (33.467s). The cause of the initial deadline
is unproven.

Full independent race fails (89.821s): v1 response-loss migration times out at the
one-second upstream-address assertion. Similar historical migration failures
remain unresolved; this is not counted as a passing gate or assumed unrelated.
Ten isolated repetitions of the complete migration matrix pass (162.909s),
without reproducing the timeout. Its cause remains unverified; the failed full
gate is retained as a failure. The full goal remains active, including this
diagnosis and the small-population tradeoff.

## Latest: 2026-10-06 unknown-CID reset selection across owners

Separated normal routing identity lookup from reset-token hints. On sockets with
foreign registrations, UDP dispatch now selects unknown-CID resets across local
live eligibility and immutable foreign publications before either owner can
consume them. Multiple local matches, multiple foreign matches or one of each
cause a drop. Unique foreign targets retain final owner-side validation and
buffer/provenance transfer; normal CID routes retain authentication precedence.
Foreign hints are conservative: stale publication can suppress a unique local
reset rather than authorize choosing one of two apparent owners.

New v1/v2 tests cover short and malformed-long reset packets, duplicate tokens
across owners, unique local/foreign matches and stale foreign hints. Ten focused
race repetitions, including existing CID collision/authentication/return tests,
pass (13.559s). Restoring the old dispatch via a temporary overlay fails all
eight ambiguous/stale cases by incorrectly terminating the local connection
(0.468s). The overlay is removed. Full root race passes (root 17.300s, HTTP3
1.714s, transport 48.579s); full default independent race passes (89.528s).
Both module vet gates pass.

No public API change or measured performance claim. The new ambiguity cases are
owner-level fixtures, not independent-peer duplicate-token tests. Known-CID
collisions involving duplicate tokens, state changes between selection and final
handoff, full normative/extension coverage and performance parity remain open.
The full goal stays active.

## Latest: 2026-10-06 unknown-CID local reset ambiguity

Initial Router.Route/RouteNetwork fallback now considers all eligible local token
owners instead of returning the first map match. Multiple matches reserve the
packet without selecting a connection or admitting a new one. Address-aware
routing considers migrated tuples in the same selection as fixed-peer owners,
ignores closed/draining reset candidates and retains explicit/zero-length CID
precedence for authentication. Short inputs skip token scans. Final endpoint
receive checks remain responsible for live tuple/probe expiry authority.

New v1/v2 endpoint tests cover fixed-peer and address-aware dispatch: repeated
ambiguous resets preserve both connections, then removing one token's eligibility
lets the unique reset terminate its target. A routing test combines fixed-peer
and migrated owners, wrong local tuples, closed owners and CID precedence. Ten
focused race repetitions pass (1.953s). Restoring first-match selection in a
temporary overlay fails the endpoint and mixed-tuple regression tests (0.376s);
the overlay is removed. Test failure messages now print identities rather than
entire connection internals.

The stateful reset-index fuzzer now also compares initial unknown-CID routing
against live endpoint selection after owner/path/token changes. A 30-second,
two-worker race campaign passes with 16,259 executions and 32 additional coverage
inputs (33.414s including shutdown). Full root race passes (root 15.684s, HTTP3
1.981s, transport 47.042s); full default independent race passes (91.436s). Both
module vet gates pass. No API declaration changes or performance claim.

This establishes local-owner ambiguity handling only. Duplicate-token selection
across local and foreign owners, known-CID reset collisions involving duplicate
tokens, wider extension combinations and initial reset-scan scaling still need
audit. The full implementation and performance goal remains active.

## Latest: 2026-10-05 custom dialer early-data policy regression

Added an independent gated-handshake fixture for custom dialers that return a
resumed early connection when HTTP early admission is disabled or the callback
bypasses the HTTP SETTINGS cache. The matrix covers v1/v2 and accepting/rejecting
server policies. It proves the callback actually returned before TLS completion,
observes no completed HTTP setup or 0-RTT packet during a 20 ms gate interval,
then requires a resumed response and exactly one handler invocation after release.

Ten race repetitions pass (4.363s). Removing the handshake wait in a temporary
Go overlay fails all eight cases (0.425s), predominantly with premature h3 ALPN
validation failure. The overlay is removed. Interop vet passes. This change adds
test evidence only; production is unchanged, so the preceding full HTTP/3 and
independent HTTP/3 race gates were not repeated. The fixture uses a bounded
observation window and is not exhaustive scheduling or replay-safety proof.
Root/HTTP3 inventories remain 4/64 missing entries. The full goal remains active.

## Latest: 2026-10-05 HTTP/3 custom dialing and migration

Implemented Transport.Dial with the pinned reference signature. Callbacks receive
normalized host:port, h3 ALPN, default/explicit SNI and cloned configurations.
Returned QUIC connections enter the existing HTTP pool and SETTINGS-bound cache
and recovery machinery. Early callbacks wait for handshake completion when HTTP
early requests are disabled or remembered SETTINGS are unavailable. Nil results
fail explicitly; setup/callback errors and transport closure release returned
connections. Callbacks must honor their context. TLS/QUIC clones retain their
normal shallow-copy semantics for referenced data.

Ten unit race repetitions cover argument preparation, scalar configuration
isolation, error/nil outcomes and request/transport/deadline cancellation (1.693s).
Ten independent v1/v2 migration repetitions pass (1.535s): a custom dialer uses an
application-owned concrete UDP Transport, HTTP requests continue after public
AddPath/Probe/Switch, the upstream handler observes the new source address, the
dialer runs exactly once, and HTTP Close terminates its supplied connection.
Three independent custom early-data matrix repetitions pass (2.659s), including
acceptance/rejection, cached-only requests, retry policy, concurrent requests,
application error isolation and closure during a gated handshake.

After the final setup change, full HTTP/3 race passes (1.595s), all independent
TestHTTP3-prefixed tests pass (14.669s), and both module vet gates pass. Core QUIC
code is unchanged; unrelated full transport/interop suites were not repeated.
The generated HTTP/3 inventory now has 64 missing, 8 declaration differences and
89 identical entries / 161. Root remains 4 missing / 246. NewClientConn method,
raw HTTP APIs, observability, wider migration/multipath combinations, complete
normative coverage and end-to-end performance parity remain open. No universal
performance or feature parity claim; the full goal remains active.

## Latest: 2026-10-05 HTTP/3 owned-listener startup

Added Server.Addr, TLSConfig and QUICConfig, Server.ListenAndServe and
ListenAndServeTLS, and package ListenAndServeQUIC. Startup uses the existing
SETTINGS-aware server TLS configuration, clones the QUIC config, and enables
transport DATAGRAM reception for HTTP datagrams. Nil QUIC config enables QUIC
0-RTT acceptance without bypassing HTTP replay policy. File credentials use a
fresh TLS config. Listener setup failures and concurrent shutdown rejection
release owned sockets; graceful shutdown retains workers until they finish.

Own-client v1/v2 tests cover both credential methods and both closure modes,
configuration isolation, socket reuse and invalid/closed startup. Ten focused
race repetitions passed (2.233s). The new independent v0.63.0 client fixture also
checks datagram negotiation and holds active responses across a timed-out graceful
shutdown before draining. Ten repetitions of the strengthened fixture pass
(2.833s). The full HTTP/3 race suite passes (1.511s); all TestHTTP3-prefixed
independent tests pass (14.421s). Both module vet gates pass. The broader gates
preceded the final test-only active-drain assertions; those assertions received
the ten focused repetitions. No core transport production code changed, and
unrelated full transport/interop suites were not repeated.

Regenerated HTTP/3 inventory: 65 missing, 8 declaration differences, 88 identical
out of 161. Root remains 4 missing out of 246. Package ListenAndServeQUIC has
missing-file coverage and delegates its successful path to the tested method;
its successful public entry point is not separately tested. Cached early data,
combined TCP/TLS+QUIC startup, Alt-Svc helpers, raw streams, observability and full
behavior parity remain open. No new end-to-end performance claim. The full goal
remains active.

## Latest: 2026-09-30 HTTP/3 TLS configuration helper

Added package-level ConfigureTLSConfig, NextProtoH3 and CapsuleProtocolHeader.
The helper clones ALPN configuration, wraps callback-selected configurations,
preserves nil/error results and existing certificate/ticket callbacks, and shares
initialized source ticket keys across clones. It does not add HTTP SETTINGS ticket
metadata or enable early data; Server.ConfigureTLSConfig keeps its existing policy.

Ten focused HTTP TLS/ticket race repetitions pass (1.719s), and the full HTTP3 race
suite passes (1.640s). The independent v0.63.0 helper comparison checks nested
selection and clone/fallback/error contracts. A live upstream-client fixture now
uses our helper around a selected configuration with incompatible original ALPN.
Ten v1/v2 live/helper repetitions pass (1.771s). Removing selected-config wrapping
fails both negotiations (0.799s); omitting source key initialization fails ticket
sharing (0.286s). Both temporary controls are removed. Both module vet gates pass.
Core transport code is unchanged; unrelated full transport/interop suites were
not repeated for this additive HTTP API change.

Regenerated HTTP/3 export inventory: 71 missing, 8 differences, 82 identical / 161.
Root remains 4 missing / 246. Raw stream APIs, server conveniences, observability,
GSO, wider migration/multipath combinations and measured performance parity remain
unfinished. The full implementation goal stays active.

## Latest: 2026-09-30 public stream ordinal conversion

Implemented StreamID.StreamNum and the inferred ordinal's reverse StreamID
conversion for all four stream classes. Zero count reconstructs the invalid -1
sentinel. Signed conversion edge behavior follows the pinned reference API;
wire stream-ID validation is unchanged. Production remains independent of upstream.

External-package boundary/class tests pass under race (1.262s). The isolated
upstream v0.63.0 comparison covers 1,031 IDs, signed extremes, all valid classes
and invalid reverse-conversion arguments, and passes under race (1.382s). Both
module vet gates pass. This adds conversion methods without changing packet or
stream state machines; full transport/interop traffic suites were not repeated.

Regenerated the root export inventory: 4 missing, 58 declaration differences,
184 identical declarations / 246 reference entries. StreamNum's internal return-
type path remains a review difference. HTTP3 remains 74 missing entries. Root
missing entries now comprise Config.Tracer, Transport.Tracer, Conn.QlogTrace and
ConnectionState.GSO; matching declarations still do not establish behavior parity.
The full goal remains active, including observability, other package APIs,
migration/multipath combinations and measured end-to-end performance work.

## Latest: 2026-09-30 stateful index fuzzing and cold/hit measurements

Added FuzzResetTokenIndexLifecycle, comparing exact indexed bucket membership and
indexed local selection against live state/full scans after synthetic owner events.
Sequences include activation, retirement, duplicate tokens, path abandonment,
local/remote tuple changes, removal/re-addition, closed state and path deletion.
All surviving index identities must disappear on repeated removal. Tests include
long/short header bits and undersized inputs; packet authentication and concurrent
owner mutation are outside this fuzzer's scope.

Three seeds pass under the race detector (1.296s). A 30-second, two-worker race
fuzz session passes after 20,026 executions and 61 new coverage inputs (33.354s
including shutdown). Retaining removed owners through a temporary overlay fails
all three seeds (0.389s). The overlay is removed. Transport vet passes. This turn
adds tests/benchmarks and documentation only; production and independent fixtures
are unchanged, so the prior full root/interop race gates were not repeated.

Cold initialization at 4096 one-token connections measures a five-sample median
1.751 ms, 3,241,267 allocated bytes and 16,491 allocations. Validated unique hits
measure 235.3 ns with zero allocation. These separate setup cost from warmed
selection; allocation traffic is not retained heap. No end-to-end performance
claim follows. See [methodology and raw results](benchmarks/foreign-reset-index.md).

Foreign-registration and initial unknown-CID scans, duplicate/probe hit scaling,
retained memory, global ambiguity, remaining APIs and wider migration/multipath
validation remain open. Full implementation goal stays active.

## Latest: 2026-09-30 indexed source-owned reset selection

Foreign CID dispatch now lazily bootstraps a source-owned reset-token index.
Lookup uses standard HMAC-SHA256 under a fresh random secret, with transformed
map keys and reusable owner-local scratch. Buckets retain all matching connection
identities. Live tuple and constant-time token checks still determine eligibility;
foreign registrations keep immutable hints and their existing scan.

Successful Router.Sync publishes one connection's current used/active tokens,
including the unchanged-CID fast path. Endpoint output synchronizes before packet
exposure. Router removal drops index references; stale hits cannot bypass current
eligibility. A test that directly adds a token now explicitly synchronizes it,
matching the router contract rather than relying on an implicit full scan.

Ten focused index/router/collision race repetitions pass (12.372s). Tests include
bootstrap, first use, replacement, retirement, duplicate buckets, stale/wrong-peer
rejection, path abandonment and removal. Disabling incremental Sync publication
makes both independent foreign-first/local-target restart cases time out (10.290s).
The temporary negative-control and matched-scan overlays are removed.

Five-sample warmed selection medians: 4096 local connections improve from 606017
ns under the matched scan control to 160.1 ns with zero allocations. One-token
unchanged maintenance costs 215.9 ns per Sync (eight tokens: 1142 ns), also without
allocation. Bootstrap, changed membership and retained storage have costs not
characterized here. No full transport or upstream performance claim follows.
See [methodology, tradeoffs and raw measurements](benchmarks/foreign-reset-index.md).

Full root race passes (root 14.901s, HTTP3 1.578s, transport 44.905s), full
independent race passes (88.087s), and both module vet gates pass. Earlier
intermittent independent timeout observations remain unresolved. Foreign-registration and initial
unknown-CID scans, global ambiguity, cold-start/memory measurements, broader
migration/multipath combinations, APIs and full performance parity remain open.
The complete implementation goal stays active.

## Latest: 2026-09-30 reset publication identity and measured scaling

Added reproducible microbenchmarks for alternate reset selection and immutable
hint publication. Unchanged multipath publication was allocating five times
(608 bytes) whenever paths shared a peer address: raw visit counts were compared
to deduplicated set sizes. It could also retain stale peer/token hints when two
distinct values became a shared value with the same visit count.

Publication now records per-path peer identities and eligible CID sequence/token
values. Unchanged snapshots are reused without allocation; changes rebuild an
immutable snapshot. Shared/collapsed peer and token tests all fail under the exact
old source (0.302s). Retirement, first use, abandonment and empty-state reuse are
also covered. Ten focused reset/publication/probe race repetitions pass (10.487s).
Temporary control overlays are removed; baseline source is retained as a benchmark
artifact. Full root race passes (root 16.125s, HTTP3 1.526s, transport 45.123s),
full independent race passes (88.165s), and both module vet gates pass. Earlier
intermittent independent timeout observations remain unresolved.

Five-sample medians: unchanged two-path publication improves 328.3 to 97.09 ns,
eight-path 807.3 to 319.9 ns; both drop 608 bytes/five allocations to zero. Changed
snapshots cost more: two-path rebuild rises 334.5 to 477.3 ns and 608 to 1880 bytes.
These are isolated local results, not end-to-end or upstream performance claims.
See [methodology and raw results](benchmarks/foreign-reset-publication.md).

The new return path's alternate-target scan remains an important scaling gap:
about 0.54 ms per foreign packet at 4096 source-owned connections sharing the tuple.
An indexed design must preserve eligibility updates, ambiguity, token-comparison
privacy and ownership boundaries. This turn fixes publication correctness/churn;
it does not resolve that scan. Full implementation goal remains active.

## Latest: 2026-09-30 long-header reset recognition and forwarding

The reset audit found that recent local fallback and foreign routing helpers
rejected long headers. RFC 9000 section 10.3 requires receivers to recognize a
valid reset token regardless of header form, even though senders must emit short
headers. Removed those filters and permitted authorized forwarded long datagrams.
Unknown-token-hint dispatch now explicitly marks reset fallback for final live
eligibility checks. Foreign CID selection parses long-header DCIDs before reset
hints, preserving authentication precedence for coalesced datagrams beginning with
a retired Handshake packet and ending with authenticated 1-RTT input.

Local v1/v2 tests cover malformed long-header token hints, known-CID long reset
collisions, valid coalesced suffix collisions and closing-state authentication,
with local and foreign alternate reset targets. Ten focused return/reset/receive
race repetitions pass (11.703s). A negative control disabling long-header CID
selection fails all four coalesced authentication-precedence cases (0.538s).
The affected test now reports missing dispatch immediately instead of blocking
on an empty handoff channel.

Independent TestStatelessResetLongHeaderCollisionIndependent preserves a restarted
upstream v0.63.0 peer's reset token and crafts a framing-valid v1/v2 Handshake
prefix with another connection's observed CID. The prefix is fixture-generated;
upstream still generates ordinary short-header resets. Eight local/foreign first-
owner and target combinations require typed reset receipt and continued traffic
on the unrelated connection, with old migrated sockets muted. Ten repetitions
pass (2.268s). Restoring long-header rejection in the local and foreign reset
selectors makes all eight independent cases time out (40.560s). Both negative
control overlays are removed. Full root race passes (root 14.935s, HTTP3 1.718s,
transport 44.946s), full independent race passes (87.501s), and both module vet
gates pass. Earlier intermittent independent timeout observations remain unresolved.

Duplicate-token ambiguity was the initial audit target, but the explicit header-
form requirement took priority. Global ambiguity, further multipath combinations,
malformed-header coverage, APIs and performance remain incomplete; goal active.

## Latest: 2026-09-30 foreign-CID-first reset dispatch

The return mailbox is now connected to runtime dispatch. Before a CID-selected
foreign handoff, the source selects a unique eligible reset target across its own
connections and immutable foreign hints. Only a distinct target gets a reservation.
The first owner reports authentication or reset acceptance; either suppresses
fallback. Otherwise the original buffer returns to the source loop, which checks
that the recorded target is still uniquely eligible. Local delivery bypasses CID
redispatch; a foreign target gets one final reset-fallback handoff without a return
ticket. There is no recursion or fan-out. Destination queue drainage cancels pending
return reservations, and source shutdown closes/drains its return mailbox.

TestForeignCIDResetReturn covers v1/v2 and local/foreign targets, authenticated
suffixes, closing and invalid key-phase authentication, retired tokens, removed
connections, changed tuples, replacement identities, late ambiguity, both owner
shutdowns and first/return/final queue pressure. The extended fixture initially
tried to reinstall network ownership on an already active connection when adding
a second local target; fixing fixture initialization resolves those failures.
Ten final extended race repetitions pass (7.223s). The earlier broader return,
reset and forwarded-receive group passes twenty repetitions (18.586s).

Independent upstream v0.63.0 tests migrate the first CID owner to a shared socket,
then restart another peer and rewrite its reset prefix to that observed live CID.
The reset target is either local or on a second foreign owner. Old sockets are
muted in both directions; reset termination and continued traffic on the first
connection are required. Ten repetitions of both versions/directions pass (1.846s).
Disabling only return reservation makes all four cases time out (20.278s).
The temporary overlay is removed. Both module vet gates pass.

Final full root race passes (transport 44.811s; unchanged root and HTTP3 results
reused from 15.006s / 1.610s passes). Full independent race passes (87.760s).
The first root run included the fixture initialization failures described above;
the final run uses the corrected fixture. Earlier intermittent interop timeout
observations remain unresolved. Broader reset ambiguity, multipath combinations,
malformed-header/key-update coverage, APIs and performance remain incomplete.
The goal stays active; no performance improvement is claimed for this change.

## Latest: 2026-09-30 bounded authentication-return mailbox foundation

Added a private reset-return mailbox for the remaining foreign-CID-first reset
collision path. Its 64-slot bound includes both outstanding reservations and
queued returns. A reservation is obtained before dispatch; completion can transfer
the original packet buffer without waiting for the source owner. Authenticated
packets, cancellation, invalid input and source closure suppress return. Tickets
are single-use, cancellation is idempotent, and shutdown drains queued buffers
while late completion retains caller ownership. The channel remains open.

Each ticket copies the exact local-connection or foreign-registration identity
chosen by the source. Foreign completion only handles the ticket; it does not
inspect connection state. The future consumer must revalidate that identity and
must not substitute a newer matching connection or registration.

This is tested infrastructure, not runtime collision support: dispatch, foreign
authentication reporting, destination shutdown cancellation, source-loop receipt
and final target revalidation are not wired yet. Existing one-hop behavior is
unchanged. The full implementation goal remains active.

TestResetReturn* passes twenty race repetitions (1.809s), covering combined
capacity accounting, buffer/provenance/identity preservation, authenticated-result
suppression, invalid inputs, single-use tickets and concurrent completion,
cancellation and closure. Full transport race passes (43.818s) and root vet passes.
A temporary overlay removing reservation accounting fails the capacity test with
the expected pending-reservation overflow (0.435s); the overlay is removed. No
runtime integration changed, so independent interop was not rerun for this step.

## Latest: 2026-09-30 local CID collision to a foreign reset owner

The socket owner now records authentication progress across routed receives. If
local CID dispatch fails authentication, a unique foreign tuple/token hint can
receive the original buffer through the bounded one-hop handoff. Metadata marks
this as reset fallback; the destination rechecks live tuple/token/registration
state before normal authentication/reset processing. A forwarded packet is never
rerouted through another source registration. Successful handoff transfers buffer
ownership; queue rejection keeps source-side release and suppresses reset output.
No foreign Conn state is read by the source.

TestOwnedCIDForeignResetHandoff covers v1/v2 authenticated suffix collisions,
actual reset delivery, stale retired-token/tuple/registration hints, provenance,
buffer ownership and the one-hop bound. The first route/receive/reset group passes
twenty race repetitions (4.505s). The independent collision fixture now also moves
the reset target onto a Transport carrying the CID-colliding resident connection.
Both directions of its original socket are disabled. Ten v1/v2 repetitions pass
(1.773s); removing only post-authentication foreign reset forwarding makes both
versions time out (10.498s).

The authentication audit also found that legacy closing-state packets and
post-decryption reserved-bit/key-phase errors were missing authentication progress.
Those now count as authenticated for reset precedence. A reserved-bit error under
next keys also now takes precedence over preceding key-candidate authentication
failures. Expanded handoff tests cover valid closing-state packets and an
AEAD-valid invalid key phase. Local protected reserved-bit fixtures test next-key
error classification; these are generated test fixtures, not published RFC vectors.
Restoring old authentication handling fails six v1/v2 handoff/key-classification
cases (0.988s). Ten focused expanded repetitions pass (3.222s); broader key/closing/
reset tests pass ten repetitions before the extra next-key fixture (14.980s).
Temporary fixtures and overlays are removed after verification.

This handles locally routed CID to foreign reset-owner collisions. The opposite
case, where a foreign CID selects the first authentication owner, still requires
an authenticated-result return before local or other-foreign fallback. Global
ambiguity, full migration/extension combinations, remaining APIs and performance
coverage remain incomplete. API counts stay 5 root / 74 HTTP3 missing; no new
performance claim. Full goal active.

Final full root race passes (root 15.264s, HTTP3 1.805s, transport 44.811s);
full default independent race passes (88.135s). Both final vet gates pass.
Earlier intermittent timeout observations remain unresolved.

## Latest: 2026-09-30 reset collisions among locally owned connections

Owned-socket endpoint receive now records whether normal CID dispatch authenticated
any packet. If it did not, a bounded fallback searches the same endpoint's other
connections for a unique active reset-token match on an eligible tuple. Fixed-peer
and network-aware owners retain their address policy, including local-probe expiry.
Authenticated datagrams never invoke fallback. Ambiguous matches between fallback
targets are ignored. Routed receive and synchronization use one shared helper.
This fixes reset prefixes colliding with another local connection's CID, including
reserved CIDs whose fixed-peer address differs from the reset target's address.

TestEndpointResetCIDCollision covers v1/v2 fixed-peer and network-aware endpoints,
valid authenticated suffix collisions, damaged-packet resets, distinct peers,
wrong peer/socket, retired tokens and ambiguous fallback matches. Twenty race
repetitions pass (6.075s). The independent fixture restarts one upstream v0.63.0
server and rewrites only its reset's unpredictable prefix to another live client's
CID on a shared Transport. The reset target reports the typed error and the other
connection continues bidirectional transfer. The first live fixture assumed an
initial generator CID and its negative control unexpectedly passed: that CID no
longer forced a live routing collision. The corrected fixture captures a CID
actually observed on the stable peer's short-header traffic. Ten corrected race
repetitions pass (2.197s); disabling fallback now makes both versions time out
(10.551s). The temporary overlay is removed.

This is owned-socket dispatch, not complete cross-owner collision handling.
Foreign CID forwarding still needs an authentication-result handoff before it can
try a different owner; token-first routing would incorrectly steal authenticated
packets with matching suffixes. Unknown-CID global ambiguity and full extension/
platform/security audits also remain work. API inventory stays 5 root / 74 HTTP3
missing; no new performance claim. The complete goal remains active.

Full root race passes (root 15.218s, HTTP3 1.392s, HTTP3 connection 1.465s,
transport 44.383s). Full default independent race passed (87.652s) before the live fixture was
strengthened; its corrected ten-repetition gate passes as recorded above. Both
vet gates pass, including a repeat interop vet after fixture correction. Earlier
intermittent timeout observations remain unresolved.

## Latest: 2026-09-30 stateless resets during local probing

Locally initiated migration candidates now grant reset tuple eligibility only
while validating or validated and while retaining a used, token-bearing peer CID
reservation. Unsent, unchecked, failed, detached, retired and abandoned attempts
do not qualify; the receive owner enforces absolute expiry for unfinished probes.
Remote server-side probing does not create this authority. Forwarding hints now
include eligible local probe tuples; first-use publication still precedes source
write submission. Authentication-before-reset and current-token checks remain
on the connection owner. Router fallback uses the same tuple policy as a routing
hint, with expiry authoritatively checked at receive time.

TestLocalProbeResetEligibility covers eleven v1/v2 cases including validated proof
retained beyond its initial probing deadline, wrong peer and server policy.
Twenty final race repetitions pass (3.447s); the broader reset-focused group
passed twenty before the extra unchecked/abandoned cases (4.205s).
TestStatelessResetProbingIndependentRestart creates an AddPath handle, restarts
upstream v0.63.0 before Probe, and requires typed reset errors from Probe, context
and stream APIs. Both directions on the original socket are muted, preventing an
old-path reset from satisfying the fixture. Ten independent repetitions pass
(1.540s). Disabling only candidate tuple eligibility makes both versions time out
(10.367s); the temporary overlay is removed after validation.

Both vet gates pass. Full root race passes (root 15.111s, HTTP3 1.663s,
HTTP3 connection 1.617s, transport 44.680s); full default independent race passes
(88.129s). Earlier intermittent timeout observations remain unresolved.
Normal-CID/reset collision
dispatch, independent preferred-address and broader multipath reset combinations,
multiple candidates, remaining APIs and performance targets remain unfinished.
API counts remain 5 root / 74 HTTP3 missing; no new performance claim. Goal active.

## Latest: 2026-09-30 stateless resets on established foreign sockets

Added immutable tuple/token routing hints to cross-owner registrations. The
connection owner publishes only used active peer reset tokens and established
foreign tuples; first-use output refreshes hints before source write submission.
Unchanged snapshots are reused. Source matching uses constant-time token
comparisons across bounded registrations, excludes malformed/wrong-tuple packets,
rejects ambiguous hints and preserves normal CID routing precedence. The source
never accesses foreign Conn state. Final tuple/token eligibility and authentication
remain with the destination, so stale hints cannot authorize a reset.

Routing tests cover v1/v2 unused/retired tokens, stale socket hints, malformed
packets, wrong tuples and ambiguity/revocation. A submission-order regression
checks that newly used tokens become visible before foreign output. Twenty final
focused reset repetitions pass (2.113s); broader forward/foreign cases pass ten
repetitions (4.789s). The new independent reset fixture migrates with public
AddPath/Probe/Switch, restarts unmodified upstream v0.63.0 on the same socket with
the same reset key, and requires typed reset errors from the public context and
stream APIs. Ten initial repetitions pass (1.430s). Disabling only source reset
forwarding makes both v1/v2 cases time out (10.303s); temporary overlay removed.

An initial fixture using a wildcard original DialAddr binding was rejected before
migration; the fixture now explicitly binds loopback, matching current migration
constraints. Wildcard original bindings remain a feature gap and are documented
on AddPath. Reset handling for uncommitted candidates, normal-CID/reset collision
dispatch and full multipath/shared-source combinations remain open. API counts
stay 5 root / 74 HTTP3 missing; no new performance claim, full goal remains active.

Final full root race passes (root 15.024s, HTTP3 1.455s, transport 43.835s);
full default independent race passes (88.050s). Both final vet gates pass.
Earlier intermittent timeout observations remain unresolved.

## Latest: 2026-09-30 invalidate retired migration proof and test source sharing

Migration handles now recheck peer CID reservation ownership and path abandonment
before reporting candidate availability. Endpoint servicing and candidate output
mark a revoked reservation failed, preserve its error, clear responses and retry
cancellation if reliable retirement is temporarily blocked by send-queue capacity.
This prevents a stale validated candidate from appearing usable while its cleanup
is pending. Existing packet construction and commit checks already rejected the
revoked CID; this corrects API proof state and cancellation lifecycle.

TestMigrationHandleRejectsRetiredProof uses v1/v2 peers with four issued CIDs,
revokes the validated candidate's reservation and keeps an unrelated spent
reservation pending to force retirement queue pressure. Handle, service and output
checks reject stale proof, preserve primary binding and later finish reliable
cleanup. A temporary overlay restoring the previous behavior failed all six cases
(0.389s); the overlay is removed after verification.

TestPublicAddPathSharedSource migrates onto a Transport already carrying another
connection, exercises bidirectional streams on both, then closes the migrated
connection and continues using the resident. Twenty focused race repetitions pass
(root 4.792s, transport 3.187s). This is own-peer socket-sharing coverage, not an
independent sharing or full concurrent-lifetime audit.

Both vet gates pass. Full root race passes (root 14.977s, HTTP3 1.409s,
HTTP3 connection 1.377s, transport 44.687s); full default independent race passes
(87.867s). Earlier intermittent timeout observations remain unresolved.
API inventory remains 5 root / 74 HTTP3 missing; no new performance claim. Multiple
candidates, broader migration races, standards/API coverage and the full goal
remain incomplete.

## Latest: 2026-09-30 public staged migration handles

Added Conn.AddPath, Path.Probe, Path.Switch, ErrPathClosed and
ErrPathNotValidated. The existing Path type now distinguishes caller-Transport
migration handles from owned-socket multipath handles. Migration state stays on
the original connection owner, with immutable atomic address snapshots and a
context-aware gate for concurrent Probe calls. Cross-owner registration happens
outside owner callbacks. AddPath does not probe or switch; Probe preserves the
primary binding and successful proof; Switch waits for pending output before
committing. Close cancels unused probes, rejects the active path and leaves the
Transport and caller socket open. A canceled probe can be retried; delayed CID
retirement cleanup remains eligible before reserving another candidate.

Public v1/v2 tests cover base and multipath modes, six consecutive switches,
bidirectional streams before/after switching, cancellation before and after probe
output, retry, a canceled waiting Probe, and path/source/owner closure. Dropped
candidate output makes pending-probe lifecycle checks deterministic. Primary
stream delivery remains usable after candidate cancellation or source closure.
A stale detached candidate cannot report fresh proof; route closure takes
precedence over its detached-state error. Focused public race tests passed twenty
repetitions (3.827s), before the final additional primary-transfer assertion.

Independent v0.63.0 tests cover two switches in v1/v2, with and without an unused
validated probe abandoned between switches; twenty race repetitions pass (2.068s).
The attempted longer sequence exposed peer CID exhaustion: upstream logs that
no connection ID is available for path validation. An upstream-only client/server
control reproduces the same diagnostic. Both longer cases remain opt-in failing
reproductions, explicitly excluded from default passing coverage; this is not a
claim of arbitrary repeated migration compatibility. See interop/KNOWN_FAILURES.md.

The refreshed root API inventory is 5 missing / 57 declaration differences /
184 same declarations out of 246. HTTP/3 remains 74 missing. Matching symbols do
not establish full API behavior parity. One uncommitted candidate per QUIC path,
concrete same-family sources, migrated stateless-reset forwarding, established
foreign-write failure policy, broader concurrency/extension combinations and full
normative coverage remain work. No new performance measurements or outperformance
claim; the overall goal remains active.

Validation: full root race passes (root 15.477s, HTTP3 1.438s, HTTP3 connection
1.513s, transport 44.044s); full default independent race passes (88.025s). Both
vet gates pass. Earlier intermittent timeout observations remain unresolved.


## Latest: 2026-09-30 isolate failed uncommitted foreign probes

Retained outbound descriptors now identify local migration probes by candidate,
path/recovery identity, packet number and source registration. A failed write on
an uncommitted alternative cancels that candidate and matching registration without
closing established output. Zero-byte rejection removes the latest probe from
flight/PTO/congestion history and records its packet number as unsent; future ACKs
for it fail validation. Partial results retain possible-transmission recovery state.
Retirement-queue pressure preserves retryable cancellation ownership. Older failure
results cannot cancel a replacement candidate or revoke a replacement registration.
Probe failure causes remain available on candidate state for the future controller.

Switching a validated candidate now waits for pending write completion; automatic
migration servicing treats that wait as retryable. Idle rollback retains its
activity-revision guard. Tests cover v1/v2 zero/partial results, primary-path output,
unsent ACK rejection, queue pressure, stale completions, equal-clock activity and
explicit/automatic switching waits. The stale-completion fixture replenishes the
peer CID consumed by the canceled probe before creating its replacement.

A temporary overlay removing only endpoint failure isolation failed all four
v1/v2 zero/partial primary-preservation cases (0.277s). Fixed focused transport/
recovery/live-output race tests passed twenty repetitions (transport 2.837s,
recovery 1.498s); the final migration failure/wait cases pass twenty repetitions
(2.371s). Full root race passes (root 14.997s, HTTP3 1.461s, HTTP3 connection 1.414s,
recovery 1.791s, transport 43.967s). Independent race passes (87.953s); both vet
gates pass. Earlier timeout observations remain unresolved.

Established foreign-path failures retain the existing removal fallback. Public
migration handles, broader source/connection/cancellation races, multiple candidates
and independent validation of the staged cross-Transport API remain incomplete.
API counts stay 10 root / 74 HTTP3 missing. No performance claim; full goal active.

## Latest: 2026-09-30 live foreign probe, switch, STREAM and close output

Connected endpoint foreign-route authorization, packet selection, capacity tickets
and asynchronous submission. The endpoint reserves before sealing, retains regular
output for owner-side completion, and submits cached close bytes through the same
bounded source queue without recovery rollback. Submission clears borrowed output
descriptors and metadata. Candidate servicing recognizes registered foreign sockets
without adding them to listener-local addresses. Source publication/completion and
capacity notifications wake the existing owner loop.

Established paths blocked by foreign output suppress output-dependent recovery,
pacing and MTU deadlines until selection retries. Absolute path validation and
connection lifetime deadlines remain active. This avoids a due-recovery timer loop
while retaining queue-capacity wakeups and normal recovery once sending resumes.
Source failure still uses ordinary connection-removal fallback; candidate-local
failure isolation is unfinished.

A live v1/v2 UDP regression runs separate source and destination owners, receives
an encrypted staged challenge from the foreign address, returns proof through CID
forwarding, observes validation without automatic switching, explicitly commits,
and receives STREAM data then application close through the foreign socket. This
uses our own packet engine as the peer, not an independent peer. The extended live
case passes twenty race repetitions (1.652s); the combined route/migration/foreign
scheduler tests pass ten repetitions (6.173s). Full root race passes (root 14.963s,
HTTP3 1.495s, transport 43.676s); full independent race passes (87.544s). Both vet
gates pass. Previous timeout observations remain unresolved.

Public AddPath/Probe/Switch, multiple alternatives, cancellation/failure isolation
and independent coverage of the new staged cross-Transport path remain incomplete.
API counts stay 10 root / 74 HTTP3 missing. No new performance measurement or claim.
Full objective remains active.

## Latest: 2026-09-30 endpoint foreign-route authorization registry

Added an endpoint-owned bounded registry of foreign writer registrations, separate
from owned/listener-local sockets. Binding verifies a live confirmed connection,
source writer/router identity, a concrete foreign local address and the exact
handoff connection binding. Identical binding is idempotent; conflicting local
registrations fail. Closed/revoked writers are pruned and unsubscribed; connection
removal clears registry entries. Capacity exhaustion first reclaims dead entries.

Per-selection route snapshots poll CID publication and expose only registrations
that acknowledged the current CID revision. Stale or failed publication cannot
authorize output. Each source must acknowledge its own registration, while source
closure after snapshot creation is rechecked by reservation/execution. Publication
errors now remain available to this controller instead of permitting use of an
older cached router snapshot. Removing a failed alternative preserves the primary.

Focused registry race tests pass ten repetitions (4.163s), covering v1/v2 CID
revision changes, invalid identity/address cases, collision, shutdown races,
per-source readiness and disposal. An additional source-side CID-collision test
passes ten repetitions (1.416s). Full root race passes (root 14.920s, HTTP3 1.407s,
HTTP3 connection 1.540s, transport 43.671s); independent race passes (87.783s).
Both vet gates pass. Earlier timeout observations remain unresolved.

The registry and scoped selector are not yet connected to the live output loop.
Foreign candidate lifetime, publication/capacity-wait deadlines, runtime submission,
terminal-output policy and public AddPath/Probe/Switch remain unfinished. API counts
stay 10 root / 74 HTTP3 missing. No performance claim; full goal active.

## Latest: 2026-09-30 scoped packet-route capacity reservation

Added a scoped selector that reserves foreign writer capacity before candidate or
ordinary path packet sealing. Owned sockets pass through; blocked foreign routes
produce no committed packet and allow the scheduler to try another route. Cached
close output reserves before emission. The selected packet returns its ticket
and immutable tuple to the completion owner. Reselection, construction failure,
empty output and mismatched transfer cancel unused reservations; selector state
is removed from the connection on return.

V1/v2 tests fill the foreign queue and verify candidate packet numbers, credit,
flight/emission accounting and proof state remain uncommitted. STREAM output on
the old path continues. After freeing a slot, the selected candidate packet
retains its reservation against competing offers, decrypts at the peer, and
submits/completes through the existing foreign writer. Additional cases cover
established-path output, revoked routes, repeated cancellation, packet-number
exhaustion after reservation and an empty cached-close emission. The error-path
fixture initially advanced beyond candidate expiry; scheduling it at the actual
retry deadline exercises the intended construction failure. Final focused race
tests pass ten repetitions (1.556s).

Full root race passes (root 14.899s, HTTP3 1.424s, HTTP3 connection 1.572s,
transport 43.511s), independent race passes (87.835s), and both vet gates pass.
The earlier timeout observations remain unresolved; no performance claim is made.

The selector is not yet called by the live endpoint route controller. It requires
a controller-supplied authorized/publication-ready route map; foreign candidate
lifetime, capacity-wait deadlines, runtime ticket submission and closing/error
policy still require integration. Public AddPath/Probe/Switch remain unfinished.
API counts remain 10 root / 74 HTTP3 missing. Full goal remains active.

## Latest: 2026-09-30 migration probe accounting and HTTP/3 admission cleanup

Probe-output inspection found that the migration sealer omitted emitted packets
from connection send statistics and could be called directly during a pending
foreign write. It now respects the pending hold and publishes packet/byte totals
once after successful emission. New v1/v2 regression cases cover peer/local/staged
challenges, responses, retries and credit-blocked attempts. The tests failed first
on bypassing the pending hold, then on missing counters after fixing that hold;
both sets of eight failures establish the regressions independently. Focused
migration/foreign-write race tests passed ten repetitions (3.533s), and the final
accounting test passed ten repetitions (1.715s).

The subsequent full independent race run failed (86.761s) with H3_EXCESSIVE_LOAD:
active priority state limit in TestHTTP3LiveIndependentClient/v1. Fifty focused
repeats did not reproduce it (2.976s), but inspection found an admission cleanup
race: LIFO defers released the request slot before deleting priority state. A new
mutex-gated cleanup test failed under the old order (calls=2 before cleanup), then
passed with priority cleanup preceding slot release. Twenty request/priority race
repetitions pass (1.803s); 100 independent-client repetitions pass (4.534s). Neither
concurrency nor priority limits were increased. Earlier HTTP/3 and migration
timeout observations remain separate and unresolved.

Final full root race passes (root 14.881s, HTTP3 1.553s, HTTP3 connection 1.306s,
transport 43.547s). Full independent race passes (88.048s), and both module vet
gates pass.

Foreign-route selection and reservation before packet commitment remain missing;
this turn fixed issues found while tracing that integration. Public migration
APIs and the broader conformance/performance objective remain incomplete. API
counts stay 10 root / 74 HTTP3 missing. No performance claim; full goal active.

## Latest: 2026-09-30 pending foreign write scheduling and lifetime timers

Connections now retain one outstanding foreign completion. The endpoint consumes
that result before packet selection; successful completion releases the hold and
zero-byte failures enter retained PMTU handling. Positive-count failures remove
the connection without rolling back a potentially transmitted packet. Direct
base/bound-multipath packet construction also respects the hold. Other connections
continue scheduling. Duplicate starts release their ticket and fail without
replacing the pending owner. Close, closing, draining and removal release retained
write state, so late results cannot change disposed recovery state.

Pending output suppresses loss/PTO, pacing, keepalive and path-validation deadline
work that cannot progress until completion. Handshake/idle expiry, previous/early
key retirement, and closing/draining deadlines remain active. This avoids timer
spin while preserving idle termination of stalled writes. Tests exercise both
legacy and multipath scheduling, v1/v2 send ordering and PMTU outcomes, another
connection's progress, key/idle expiry, duplicate starts and terminal transitions.

Focused race tests pass ten repetitions (2.531s). The unrelated-connection fixture
initially retained its one-connection client-router capacity; extending that test
fixture to admit two connections fixed its setup failure. Full root race passes
(root 14.855s, HTTP3 1.548s, HTTP3 connection 1.305s, transport 43.376s), and the full
independent race suite passes (87.659s). Both module vet gates pass. The previously
observed intermittent HTTP/3 and migration timeouts remain unresolved observations.

The live scheduler still does not select foreign routes or acquire reservations
before packet commitment. Candidate/socket lifetime, alternative-path failure
isolation and public AddPath/Probe/Switch remain unfinished. Delayed completion
also needs broader ACK/loss/path-retirement and queue-latency validation. API
counts remain 10 root / 74 HTTP3 missing. No new performance claim; full goal active.

## Latest: 2026-09-30 owner-side foreign write completion primitive

Added a retained, single-consumption completion state for reserved foreign writes.
Unsupported ECN triggers exactly one same-ciphertext retry only after a zero-byte
rejection and successful owner-side ECN rollback. A full retry queue waits for
capacity without recommitting the packet or repeating rollback. Submission errors
produce an owner wakeup and completion. Closed engines discard delayed results;
short successful writes report io.ErrShortWrite and positive byte counts remain
visible so future callers cannot mistake them for atomic unsent rejections.

Focused race tests pass ten repetitions (1.769s), including queue-pressure wakeup,
closure/revocation, submission failure, absent ECN accounting, partial results and
v1/v2 retained encrypted-packet metadata across all five PMTU rejection forms.
Full root race passes (root 14.944s, HTTP3 1.426s, transport 43.257s); both module
vet gates pass.

The full independent race run failed after 92.044s in the HTTP/3 response-writer
SETTINGS fixture (v1, DATAGRAM enabled), with a five-second upstream RoundTrip
timeout. Twenty focused repeats passed (1.863s). Added failure-only reporting of
handler phase/result, then 200 more repeats passed (9.676s, 800 subcases). No
deadline or assertion was relaxed; the cause remains unresolved. This observation
is separate from the earlier migration address-publication timeout. A fresh full
independent race run passed (87.782s); the diagnostic fixture and interop vet
also passed after instrumentation.

This is an internal completion primitive. It is not wired into the live scheduler:
pre-construction reservations, pending-send ordering, blocked-output timeout
handling and alternative-path failure policy still require integration. Foreign
candidate lifetime/output routing and public AddPath/Probe/Switch remain missing.
API counts remain 10 root / 74 HTTP3 missing. No new benchmark or performance
claim; the full goal remains active.

## Latest: 2026-09-30 retained outbound state and guarded idle rollback

Added Outbound.retain to copy borrowed wire bytes and all optional write metadata,
including reusable baseLarge storage. Engine identity pointers remain on the
connection owner; foreign socket requests still carry no mutable recovery state.
PMTU write metadata now captures an idle-activity revision. Error handling restores
pre-send idle state only if no intervening activity occurred, including events
with equal clock values; rollback itself consumes the revision.

New race tests pass (1.525s), covering v1/v2 MTU probes, large path STREAM/DATAGRAM,
base large STREAM/DATAGRAM, descriptor/buffer reuse and later authenticated peer
PINGs before local write rejection. A temporary pre-fix overlay failed all ten
receive-before-rejection cases (0.510s), confirming the old rollback erased later
activity. Existing write/keepalive/idle tests pass (1.506s). Full root race passes
(root 14.951s, HTTP3 1.434s, HTTP3 connection 1.480s, transport 43.243s). Both vet
gates pass.

Full default independent race passes (86.967s). The earlier intermittent migration
address-publication timeout remains an unresolved observation.

General asynchronous completion is still incomplete: latest-packet checks,
closure, ECN retry and pending-send ordering remain to be integrated with reserved
queue capacity and scheduling. Foreign candidate lifetime/output routing and
public AddPath/Probe/Switch remain unfinished. API counts stay 10 root / 74 HTTP3
missing. No performance claim is made; the full goal remains active.

## Latest: 2026-09-30 reserved foreign write capacity

Added single-use queue-capacity tickets for foreign writes. Queued datagrams and
outstanding reservations share the 64-slot limit; competing ordinary offers
cannot consume a reserved slot. Submit copies packet bytes and consumes the
ticket, including on invalid input. Cancel is idempotent and does not affect a
previously submitted write. Closure and revocation invalidate remaining tickets.

Blocked registration owners receive coalesced capacity wakeups on dequeue,
cancellation, invalid submission, revocation and writer shutdown. Waiter state
is bounded and prunes revoked routes. Runtime route-change servicing releases
revoked tickets without relying on the old requesting owner to cancel them.

Ten focused race repetitions pass (1.773s), covering competing producers,
exactly-once tickets, copied bytes, capacity reuse/wakeup, concurrent submit/cancel
and real destination shutdown while another registration waits. Full root race
passes (root 15.027s, HTTP3 1.443s, HTTP3 connection 1.323s, transport 43.580s).
Both vet gates pass.

Full default independent race passes (87.924s). The earlier intermittent migration
address-publication timeout remains documented and unresolved.

The scheduler does not yet acquire tickets before packet construction. Owned
Outbound metadata, delayed write completion, ECN/PMTU handling and foreign
candidate lifetime integration remain required before cross-Transport QUIC output.
Public path APIs and reset-token forwarding are still missing. API counts remain
10 root / 74 HTTP3 missing. No performance claim is made; full goal stays active.

## Latest: 2026-09-30 foreign socket write inbox

Added a lazy 64-request socket-owner inbox for copied wire datagrams. Offers are
bounded, reject closed/revoked handles and return one buffered byte-count/error
result with an optional wakeup. Closing the owner fails retained requests without
waiting for consumers. Execution rechecks registration, connection binding and
concrete local socket identity. The writer never reads or changes connection or
recovery state and performs exactly one socket write with the requested marking.
Generic sockets reject ECN-marked requests before writing; partial writes and
local errors remain exact results for the requesting owner.

Ten focused race repetitions pass (1.396s), covering copied ownership, queue
capacity, shutdown/notification pressure, concurrent producers/close, queued
revocation, unbound rejection, actual UDP source/payload and no retry on success,
partial writes, ECN rejection or EMSGSIZE. Full root race passes (root 15.182s,
HTTP3 1.446s, HTTP3 connection 1.325s, transport 44.039s). Both vet gates pass.
Full default independent race passes (87.454s). The earlier intermittent
migration address-publication timeout remains unresolved.

This is a write primitive, not yet Outbound scheduling or authenticated QUIC
cross-Transport output. Integration must preserve borrowed metadata/bytes, obtain
queue capacity before packet commitment, and serialize completion/ECN fallback
with connection state. Existing synchronous PMTU error handling needs changes
before delayed completion can safely restore idle/send state. Candidate lifetime,
public path APIs and reset-token forwarding remain open. API counts stay 10 root
/ 74 HTTP3 missing. No performance claim is made; the full goal remains active.

## Latest: 2026-09-30 foreign receive authorization

Forwarded datagrams now carry their source registration identity. Registration
captures the source runtime's primary socket address, and source dispatch refuses
to use it for another local socket. Reader failure revokes the affected routes.
The destination admits only a live subscription bound to the same connection
selected by the packet CID, with a matching concrete local address and a
confirmed address-aware owner. The normal engine still authenticates and applies
candidate/path validation. Foreign addresses never enter the endpoint-wide socket
set; rejected forwards have no raw/listener/stateless-response fallback.

Ten focused race repetitions pass (3.270s). Encrypted candidate PATH_RESPONSE
tests cover v1/v2 and base-only/multipath path zero, rejecting missing/unbound/
revoked/wrong-connection provenance, wrong tuples, corruption and closed owners.
Valid proof validates but does not switch the candidate. Other tests cover source
socket scope/failure, UDP provenance, no raw fallback and existing forwarding
lifecycle/queue behavior. Full root race passes (root 15.039s, HTTP3 1.427s,
HTTP3 connection 1.307s, transport 44.209s). Both vet gates pass.
Full default independent race passes (86.979s). The earlier intermittent
migration address-publication timeout remains an unresolved observation.

The encrypted fixture directly prepares a candidate and injects peer proof at
the endpoint; it is not a complete runtime cross-Transport exchange. Candidate
servicing still assumes owned sockets, and output routing/public AddPath/Probe/
Switch remain absent. Reset-token forwarding and wildcard-local support are
also pending. API counts remain 10 root / 74 HTTP3 missing. No performance claim
is made; the full goal remains active.

## Latest: 2026-09-30 CID advertisement scheduling gate

Connected snapshot publication to endpoint output preparation after path
activation/migration servicing and before packet construction. Pending/rejected
reservations hold NEW_CONNECTION_ID and PATH_NEW_CONNECTION_ID, including
retransmissions. Other queued traffic bypasses held advertisements; primary
connections stay usable on publication failure. Removing a failed alternative
releases its hold. Queue selection preserves unsent state and handles datagram
size drops after bypassing held items. Legacy and path PTOs can send PING instead.

Added an on-demand runtime publication wakeup for acknowledgements and binding
changes. Source processing also wakes other destinations sharing pending-ID
capacity, including after revocation, so rejected requests can retry without
another application command.

Ten scheduling race repetitions pass (2.413s). Tests cover v1/v2 legacy, base-path
and multipath output; activation-issued IDs in the first snapshot; packet-number
and sent-state preservation; ordinary STREAM progress; PTO; datagram shrink;
collision/removal; shared-capacity wakeup; and real UDP output resuming solely
from an acknowledgement. Final full root race passes (root 15.286s, HTTP3 1.497s,
transport 43.832s; other packages cached). Both vet gates pass.
Final full default independent race passes (88.225s). The earlier intermittent
migration address-publication timeout remains documented and unresolved.

Registration/binding remain explicit internal operations. The public path
controller still needs initial readiness/error/revocation checks before using
foreign local addresses, plus foreign-local authorization and output routing.
Reset-token forwarding and AddPath/Probe/Switch remain missing. No authenticated
cross-Transport exchange or performance improvement is claimed. API counts remain
10 root / 74 HTTP3 missing. Full goal remains active.

## Latest: 2026-09-30 forwarding CID snapshot publication

Added connection-owner publication state for every bound foreign registration.
Router commits advance a revision; unchanged Sync retains it. Readiness requires
acknowledgement of the current snapshot from every bound foreign source. Older
acknowledgements trigger publication of the latest snapshot without authorizing
its advertisement. Same-count retirement/replacement and new multipath path IDs
advance the snapshot. Repeated binding preserves publication progress. Collision
errors stay visible without removing the primary connection.

Ten focused race repetitions pass (2.228s), covering v1/v2 issuance while an older
snapshot is pending, independently delayed acknowledgements, unchanged polling,
retirement/replacement, collision failure, multipath issuance and live runtime
publication/wakeup/removal. Full root race passes (root 15.256s, HTTP3 1.657s,
HTTP3 connection 1.384s, transport 44.337s). Both vet gates pass; transport vet
also passes after the additional runtime test and its ownership cleanup.
Final publication race tests pass (1.420s); full default independent race passes
(88.142s). The earlier migration address-publication timeout remains unresolved
despite this passing run.

The helper is not yet called by packet scheduling. Pending/error handling and
requester wakeups must be integrated after network activation and before sealing.
Publication exclusively manages updates for its bound registrations; manual
updates must not be mixed with it. Foreign-path failure handling, reset-token
forwarding, foreign-local authorization, output routing and the public path APIs
remain open. API counts stay 10 root / 74 HTTP3 missing. The full feature and
performance goal remains active.

## Latest: 2026-09-30 asynchronous forwarding CID updates

Added a nonblocking cross-owner update request with copied CID bytes, one buffered
result and an optional coalesced requester wakeup. The source runtime applies
requests on its existing route wakeup, retaining old reservations on rejection.
One pending request per route and 4096 aggregate pending CID entries per source
bound queued state. Source/destination/connection revocation fails pending work
and returns capacity exactly once, including races with application of the update.
No owner waits for the requester to consume an acknowledgement.

Full root race passes (root 17.608s, HTTP3 1.454s, HTTP3 connection 1.432s,
transport 45.666s). Final focused race passes ten repetitions (1.954s), including
an additional concurrent destination-close test. Coverage includes copied request
ownership, pending dispatch, collision atomicity, empty replacement, notification
pressure, request limits, revocation, capacity reuse, single completion and
reciprocal owner updates. Both vet gates pass, with transport vet repeated after
the additional test.

The full independent race run failed (87.113s) in
TestIndependentClientMigrationUpstreamServer/1/both: the upstream peer address
remained 127.0.0.1:61184 instead of 127.0.0.1:64922 through the one-second
publication deadline. That fixture does not register forwarding routes. The
failure is retained as an unresolved observation; no timeout was relaxed.
Ten subsequent repetitions of all v1/v2 clean/request/response/both-loss migration
cases passed (162.901s). This did not reproduce or explain the failed full run.
A fresh full independent race run then passed (88.512s). The earlier observation
remains open in REMAINING.md despite these passing reruns.

Automatic connection-CID snapshots and send gating are still pending. Inspection
confirmed that initial multipath activation can issue CIDs during output
preparation: the gate must follow activation and precede packet construction.
Acknowledgement confirms installation only; callers must also check revocation.
This does not yet supply public AddPath/Probe/Switch, reset-token forwarding or
foreign-local/output routing. API counts remain 10 root / 74 HTTP3 missing. No
performance claim is made and the full goal remains active.

## Latest: 2026-09-30 connection-bound forwarding cleanup

Added an internal destination-owner operation to bind a forwarding registration
to a routed connection. Binding validates runtime identity, connection membership
and route lifetime; repeat binding is harmless and reassignment is rejected.
Router removal now revokes bound registrations and wakes source owners. Routes
remain active throughout closing/draining and disappear at final removal.
Explicit source removal releases the destination's connection reference.
Unrelated subscriptions survive. Cross-owner access uses connection pointers
only as identities under the inbox mutex, never to read mutable connection state.

Focused race tests pass (1.370s), covering v1/v2 real-handshake closing/draining
expiry, unrelated-subscription retention, unsubscribe, binding validation and
live runtime wakeup on connection removal. Full root race passes (root 15.056s,
HTTP3 1.417s, transport 43.897s; other packages cached). Full default independent
race passes (87.952s). Both vet gates pass.

The binding primitive is not yet called by a public path API or automatic CID
publisher. CID issuance/retirement must still be mirrored before advertisement,
and reset-token routing, foreign-local authorization and output routing remain
open. Public AddPath/Probe/Switch and full feature/performance parity remain
unfinished. API counts remain 10 root / 74 HTTP3 missing. Goal remains active.

## Latest: 2026-09-30 forwarding registration lifetime

Added destination subscriptions and atomic route revocation. Destination shutdown
marks registrations and posts coalesced source wakeups; each source removes its
own CID reservations. Source shutdown unsubscribes outgoing registrations without
waiting on the destination owner. Explicit removal is idempotent, invalidates the
handle and releases CIDs. Empty CID sets keep their registration until removal;
the registration count is bounded even when every CID set is empty.

CID updates now check revocation after pruning. This avoids reinstalling a route
that was removed by pruning after concurrent destination shutdown. Local CID
publication also prunes revoked reservations before collision checks.

Ten repetitions of focused race tests pass (1.697s), including both closure
directions, explicit/repeated removal, empty registrations, CID reuse, stale
updates, concurrent update/shutdown and reciprocal runtime closure. Final full
root race passes (transport 44.239s, other packages cached from the preceding
passing run). Full independent race passes (88.331s, final revision cached). Both vet
gates pass. No performance claim is made for the new wakeup case or pruning.

This is runtime-level cleanup, not connection-specific lifecycle publication.
Automatic CID issuance/retirement, connection-close cleanup, reset-token routes,
foreign-local authorization and output routing remain open. Public AddPath,
Probe and Switch remain unimplemented. API counts stay 10 root / 74 HTTP3 missing;
the full implementation and performance goal remains active.

## Latest: 2026-09-30 foreign CID receive dispatch

Added owner-serialized handoffRoute registration and atomic complete-set CID
updates/removal. Foreign reservations are bounded to 4096 CIDs and reject empty,
oversized or prefix-ambiguous identifiers. Local/foreign collision checks work
in both registration orders, including local CID Sync before advertisement.
Zero-CID local routing cannot coexist with foreign routes at this stage.

Short-header socket input now dispatches to a registered handoff target before
raw classification. Different CID lengths and greased QUIC bits are supported.
A one-handoff marker prevents loops. Full or closed targets drop the packet;
reserved foreign traffic cannot fall through to raw delivery or stateless
admission/reset. CID bytes are copied and failed updates leave routes unchanged.

Focused race tests pass (1.362s), covering collision atomicity, replacement,
removal, zero-CID exclusion, metadata-preserving real socket dispatch, queue
pressure/closed-target drops and loop prevention. The socket fixture is a marker
packet, not authenticated cross-owner QUIC. Full root race passes (root 15.450s,
HTTP3 1.416s, HTTP3 connection 1.348s, transport 44.947s); full default independent
race passes (89.245s). Both vet gates pass.

Automatic CID lifecycle publication, reset-token forwarding, connection-close
route cleanup, foreign-local authorization and output routing remain open.
Registration calls currently run outside owner callbacks, so no synchronous
owner-to-owner call is introduced. Public path APIs remain missing and no
cross-Transport migration completion or performance claim is made. API counts
stay 10 root / 74 HTTP3 missing. Full goal remains active.

## Latest: 2026-09-30 bounded cross-owner packet inbox

Added packetHandoff and an on-demand UDPServer receiver inbox as the first
cross-Transport routing primitive. Successful offers transfer pooled datagram
ownership; a full/closed 64-entry inbox leaves ownership with the sender. Socket
read errors and missing buffers are rejected. Shutdown orders against producers,
rejects late offers and drains retained buffers without closing the channel.
No inbox allocation occurs until requested on the receiver owner.

Socket input and handoff input now share one receive helper on the serialized
owner. FIFO metadata, bounded capacity, rejected-offer ownership, concurrent
producers/close, lazy initialization and live runtime delivery pass focused race
(1.409s before a test diagnostic cleanup; final full gate includes that cleanup).
The live inbox fixture uses raw marker delivery, not CID-routed QUIC traffic.
Full root race passes (root 15.290s, HTTP3 1.698s, HTTP3 connection 1.296s,
transport 44.243s); full default independent race passes (88.376s). Both vet gates
pass. No performance claim is made for the additional disabled select case.

Foreign CID registration, foreign local-address authorization, output routing
and the public path APIs remain unimplemented. The inbox alone is not a claim
of cross-Transport migration support. Next requirements are recorded in
MIGRATION_API_PLAN.md. API counts remain 10 root / 74 HTTP3 missing. Full goal
remains active.

## Latest: 2026-09-30 migration selection authorization fuzzing

Added FuzzMigrationSelectionAuthorization, which records external nomination,
candidate-birth and reachability-proof events independently of selection policy.
Across probing, reordered application packets, expiry and commit attempts, each
successful commit must have current candidate ownership, proof and a sufficiently
recent nomination for that tuple. Selection operations and failed commits must
leave the established binding unchanged. Inputs are bounded to 256 events over
three tuples; valid authentication/proof is assumed at this layer.

A two-worker 10-second fuzz campaign passed 12,259 executions (11.399s including
termination). No production change was required. Focused race seeds and selection
regressions pass; transport vet passes. Full suites were not repeated for this
test-only addition. Packet cryptography, token matching, preferred-address/CID
combinations and sustained fuzzing are outside this oracle's evidence. Public
path APIs and multi-Transport routing remain open; full goal remains active.

## Latest: 2026-09-30 abandoned-probe replacement

Extended the upstream migration fixture to six successive switches, which passed.
Adding validated-but-abandoned probes between switches exposed a real failure:
the first replacement probe timed out in both v1 and v2 (12.378s failing run).
The server retained the abandoned validated candidate indefinitely and refused
new probing tuples in its bounded single-candidate slot.

A newer authenticated ordinary probe now replaces an unselected ordinary
candidate. Preferred-address and non-probing-nominated candidates remain
protected. Construction publishes replacement only on success; existing owner
cleanup retires spent CID reservations. Old/reordered probe numbers cannot
replace current state. Local pending/validated/protected/replay tests pass
(focused race 1.337s). Upstream normal and abandonment variants pass in v1/v2
(1.456s), with six switches and five intervening abandoned probes per abandonment
case, plus old-path stream delivery before each switch.

Full root race passes (root 15.614s, HTTP3 1.602s, transport 44.966s), full default
independent race passes (88.420s), and both modules pass vet. Documentation now
records the replacement policy and its remaining one-candidate limitation.
Multi-candidate retention, local public AddPath/Probe/Switch and multi-Transport
routing remain unfinished. API counts stay 10 root / 74 HTTP3 missing. Full goal
remains active.

## Latest: 2026-09-30 server response to staged path probes

Found and fixed a gap exposed by the new staged client mode: probing-only input
from a new client tuple was dropped before PATH_RESPONSE queuing. Servers now
admit an authenticated PATH_CHALLENGE into bounded validation state without
nominating that tuple for application traffic. Generalized preferred-address
selection's proof/nomination separation; ordinary old-path traffic preserves
proof and clears older nomination, while a newer non-probing candidate packet
can authorize commit. Probe admission retains a replay watermark across expiry.

Real two-engine v1/v2 base/multipath exchanges validate both sides without
synthetic proof, keep STREAM traffic on the old tuple and then switch explicitly.
Selection tests cover fixed expiry, stale replay and reordered nomination
(focused combined race 1.353s). Pinned upstream's actual AddPath/Probe/Switch
passes against our server in v1/v2, including a stream between Probe and Switch
(1.408s). Existing migration/preferred race gates pass (15.072s). Full root race
passes (root 15.119s, HTTP3 1.611s, transport 44.124s), full default independent
race passes (88.452s), and both modules pass vet.

CONFORMANCE.md records the response/nomination audit slice. This still supports
one pending candidate per path, not every simultaneous probing scenario. Local
public AddPath/Probe/Switch and multi-Transport routing remain missing; no API
inventory changes. Full goal remains active.

## Latest: 2026-09-30 staged migration prerequisite

Added an internal explicit-switch candidate mode for the missing public path API.
It reserves a fresh CID and validates a new local tuple without adding PING to
probe packets, holding ordinary old-path output, or automatically committing a
validated candidate during endpoint service. Explicit commit retains existing
ownership/validation/CID gates. Cancellation, socket loss and CID revocation
invalidate stale candidates. Existing immediate migration behavior is unchanged.

Encrypted v1/v2 base-only and multipath tests verify probing-only output, STREAM
traffic on the old tuple before/after proof, repeated servicing without switching,
explicit commit and three cleanup paths. Peer proof is synthesized using the
other engine's keys, so this is not independent staged migration evidence.
Focused new/existing local migration race passes (1.830s); full root race passes
(root 15.353s, HTTP3 1.460s, transport 44.962s); full default independent race passes
(89.217s). Both modules pass vet. An initial fixture attempted to write an unopened
stream; it was corrected to open the stream before final gates.

The public AddPath/Probe/Switch APIs are still missing. Cross-Transport socket
routing and multiple pending candidates require additional implementation; the
concrete requirements are recorded in docs/MIGRATION_API_PLAN.md. No API inventory
count changes: 10 root and 74 HTTP3 missing. Full goal remains active.

## Latest: 2026-09-30 version APIs and TLS context

Added SupportedVersions (independent v1/v2 preference slices), Version.String
(supported/historical names and hexadecimal fallback), and QUICVersionContextKey.
TLS startup adds a typed version value without losing application context values.
It overrides a stale parent annotation. The value records TLS startup, while
ConnectionState reports the subsequently negotiated version after compatible
switching; no mutable context value or concurrent handshake-state read is needed.

V1/v2 normal and compatible-switching live callback tests pass under race
(1.425s). Independent callback checks and version API comparisons pass (1.365s).
Formatting comparison covers the historical numeric interval, explicit special
values and 10,000 deterministic random values against pinned v0.63.0. Historical
names do not imply wire support. Full root race passes (root 15.917s, transport
44.109s and all other packages); full default interop race passes (88.355s).
Both modules pass vet. An initial edit command used the wrong directory; the
independent callback assertion was then applied and the focused gate rerun.

Root inventory now has 10 missing, 57 differing and 179 matching declarations
out of 246 reference entries. HTTP3 remains 74 missing. Observability/qlog, path
API integration, wider normative audits and the performance objective remain
unfinished. Full goal remains active.

## Latest: 2026-09-30 PacketConn convenience APIs

Added package-level Dial, DialEarly, Listen and ListenEarly using the existing
independent Transport implementation. Each convenience call owns a dedicated
runtime borrowing the caller's socket. Failed setup closes/joins runtime work.
Successful dialing arms close-when-idle after the full handshake/fallback result,
preserving terminal closing/draining before reader release. Listener Close stops
its accepted connections and joins readers without closing the borrowed socket.
The Transport APIs retain their existing shared-socket lifetime semantics.

V1/v2 tests cover normal/early helpers, native/plain/OOB wrappers, local and peer
close, immediate accepted-connection cancellation, canceled/invalid setup and
raw socket reuse after reader shutdown. Focused race passes (4.136s). Reused
cached-ticket fixtures cover 0-RTT acceptance/rejection, both initial versions,
compatible switching and context values (1.547s). The independent fixture covers
bidirectional streams in both TLS roles and helper modes (1.501s); its early mode
has no cached ticket and is not evidence of independent 0-RTT acceptance.

Full root race passes (root 15.517s, HTTP3 1.572s; internal packages cached), and
full default interop race passes (88.731s); both modules pass vet.
An initial interop test invocation mistakenly used the
root module and ran no tests; it is excluded from evidence. A temporary test
format-string compile failure was corrected before final validation.
Root export inventory now reports 13 missing, 57 differences and 176 matching
entries of 246; HTTP3 remains 74 missing. Full goal remains active.

## Latest: 2026-09-30 native UDP ECN allocation

Fresh echo profiling found a 64-byte allocation per ECN send caused by sharing
an ancillary buffer between native UDP and generic socket branches. Split the
buffer lifetimes while keeping one encoder. Native IPv4/IPv6 send/drain benchmarks
now report zero allocations, down from one / 64 bytes. Wrapped sockets retain
normal interface dispatch and per-call buffer ownership.

An isolated overlay control and final matched echo benchmark show about 6.9%
lower whole-process allocated bytes and 4.7% fewer allocations. Timing does not
establish a speedup. Final comparison remains about 1.87x upstream time and
2.55x allocated bytes. See docs/benchmarks/ecn-send-allocation.md for raw data,
control patch, reproducibility and limits. Root race, focused ECN race, independent
name-filtered race and both vet gates pass; full default interop race also passes
(88.425s). Linux/amd64 cross-compilation passes.
API counts are unchanged. Full goal remains active.

## Latest: 2026-09-30 multipath control audit

Rechecked IETF/RFC Editor sources: draft-21 remains current and in first-editor
work; no published multipath RFC was found. Audited section 4 control code points,
path maximum validation and the distinction between retained abandoned paths and
fully disposed paths. The latter ignore reordered controls; retained paths can
still process late ACKs, per section 3.4.3.

Added encrypted v1/v2 tests for path-limit violations across all path-ID-bearing
control types (including ACK_ECN), invalid MAX_PATH_ID values, and controls after
path disposal. The disposal matrix asserts no recreated path, CID history growth,
response queue changes or memory-charge changes. Focused race gates pass (1.445s)
and transport vet passes. No production changes were necessary, so full suites
were not repeated. docs/MULTIPATH.md now records this clause-level audit slice
and its limits. Broader multipath conformance, independent impairment coverage
and performance evaluation remain incomplete; full goal remains active.

## Latest: 2026-09-29 live and pre-request priority updates

Connected PRIORITY_UPDATE to HTTP response scheduling with RequestPriorities.
The latest valid update takes precedence over header fields whether it arrives
before acceptance, during header decoding, or during handler/response delivery.
Updates replace the complete priority value; malformed dictionaries leave the
last valid signal unchanged. Header/update application is serialized through a
mutex so an older header cannot race and overwrite the newest update.

Pending and active state have separate MaxConcurrentRequests bounds. Replacing
an existing update consumes no additional capacity; new excess pending state
causes H3_EXCESSIVE_LOAD. Registration occurs in the ordered accept loop before
launching the handler. State remains active through stream Release and is then
removed, including failure/shutdown cleanup. An acceptance watermark permits
ignoring completed-request updates without an unbounded closed-ID set. The
tracker requires increasing acceptance IDs, matching the concrete QUIC API.

Lifecycle, replacement/defaulting, malformed signals, header/update race, capacity,
registration order, direct request dispatch and task cleanup tests pass. Full
root race passes (changed internal/http3conn 1.435s; unchanged packages cached).
Both modules pass vet. A raw HTTP/3 fixture over independent QUIC observes real
transport SetPriority calls for early, live and malformed updates in v1/v2
(1.439s). It is not an independent HTTP/QPACK implementation. Full default
independent race passes (87.060s). Pending-budget remote-error coverage was added
to the independent public-server matrix after that full run; the combined final
priority state/error matrix passes race (1.529s).

Live QUIC stream-limit validation remains unexposed to this HTTP layer (RFC 9218
7.2 acknowledges that practical barrier); bounded state is not claimed as that
validation. Response overrides, cross-path/global priority, indexed scalability
and broader fairness/performance work remain. API counts stay 17 root / 74 HTTP3
missing. Full goal remains active.

## Latest: 2026-09-29 PRIORITY_UPDATE wire validation

Added RFC 9218 request/push PRIORITY_UPDATE frame constants and bounded control
parsing. Only client control streams permit either frame; request/response/push
message streams reject them with H3_FRAME_UNEXPECTED. Request IDs must identify
client bidirectional streams. Missing/truncated IDs cause H3_FRAME_ERROR;
non-request IDs cause H3_ID_ERROR. The public server never promises push, so
push updates cause H3_ID_ERROR. A 4104-byte complete payload limit is checked
before allocation. The reader preserves frame boundaries and sticky errors.
Malformed priority dictionaries produce an event marked invalid, not a connection
error. Valid request updates are currently exposed but not applied by the public
server; active/future request state and stream-limit validation remain next work.

Wire tests cover one-byte fragmented reads, every varint width, placement/roles,
malformed fields, ID errors and oversized lengths. A two-worker 10-second fuzz
campaign passed 270,527 executions (11.179s including termination). HTTP wire,
connection and public HTTP packages pass race (1.153s/1.302s/1.564s). Independent
v1/v2 invalid request/push/missing-ID cases observe expected remote application
errors (1.365s). The full default independent race suite passes (86.960s),
including the prior critical-stream priority change. Both modules pass vet.
One mistaken test invocation ran no tests
before the file was created; it is excluded from validation evidence.
API counts remain 17 root / 74 HTTP3 missing. Full goal remains active.

## Latest: 2026-09-29 HTTP request Priority fields

Implemented independent RFC 8941 Dictionary parsing for RFC 9218 Priority
signals: urgency 3/incremental false defaults, last duplicate replacement,
ignored unknown/wrong-type/out-of-range values, and full-field malformed fallback.
Unknown members still undergo syntax validation (numbers, strings, tokens,
booleans, byte sequences, inner lists and parameters). Repeated HTTP field lines
are comma-combined. Existing field-section budgets bound parser input.

ServeRequest applies parsed/default priorities before handler dispatch.
DatagramStream forwards the scheduling method to its underlying transport.
Control/QPACK streams now receive urgency zero/incremental true before setup
writes so urgency-zero response data cannot exclude their fresh queued work.
Dynamic PRIORITY_UPDATE reception/state and response-side overrides remain open.

Header parsing and dispatch vectors pass race, including multiple field lines,
malformed dictionaries and unknown members. The parser fuzz campaign passed
1,920,828 executions in 10 seconds (two workers); this checks safety/range/failure
invariants, not equivalence with another full structured-field parser. Independent
v1/v2 HTTP exchanges cover missing/valid/malformed fields and datagrams on/off
(1.363s). Critical stream setup ordering is asserted by the local stream fixture.
Final affected HTTP packages pass race (internal/http3conn 1.394s, http3 1.673s),
and final independent HTTP priority/public/QPACK-focused gates pass (2.727s).
The full root race suite passed before critical-stream prioritization (transport
43.328s); the full default independent race suite likewise passed before that
last change (86.786s). Both modules pass vet. No throughput comparison was made.
API counts remain 17 root / 74 HTTP3 missing; the full goal remains active.

Contracts checked in RFC 9218 sections 4/5 and RFC 8941 section 4.2, downloaded
from the RFC Editor on 2026-09-29. No upstream production dependency was added.

## Latest: 2026-09-29 scheduler model fuzzing and blocked sends

Added a separate sorting/grouping reference model for STREAM priority selection.
A bounded fuzz target exercises changing priorities, canceled entries, transmitted
and fresh items, mixed scheduling modes and cursor wraparound. It verifies the
selected item, pointer multiset preservation, fixed non-STREAM slots, and no
cursor advancement before commit. A two-worker 10-second campaign passed
123,789 executions (11.365s including startup/shutdown), finding 84 additional
coverage inputs. This is bounded local evidence, not sustained fuzz certification.

New actual packet-building tests cover congestion-full retry and reprioritization
before the first successful send for v1/v2 base and path output. They verify no
packet-number, memory-charge or fairness-cursor consumption on blocked attempts.
A flow-control test confirms a high-priority blocked stream permits queued
low-priority work. Focused race tests including fuzz seeds pass (1.368s), and
transport vet passes. The full default independent race suite on the final
one-pass selector passes (86.738s), closing the previous checkpoint's revision
coverage gap. No production changes were needed. The broader scheduler,
HTTP priority and performance requirements remain open.

## Latest: 2026-09-29 STREAM scheduling priorities

Added SendStream.SetPriority and its promoted Stream method. Urgency clips to
0–7; defaults are 3/incremental true. Selection prefers retransmitted STREAM
work, then urgency. Incremental streams share committed turns with the lowest-ID
non-incremental stream at that urgency. Dynamic updates affect queued/future
frames. Selection does not advance fairness cursors until packet commitment.
Fragment splitting and reliable-prefix replacement retain retransmission status.
Base, 0-RTT and per-path output use the selector; idle-path borrowing selects
prioritized data from its source queue. Flow-control admission remains unchanged.

An initial control-preemption approach failed MTU fallback and early routing
tests. The corrected selector reorders only STREAM slots, preserving every
non-STREAM slot; those regressions now pass. Deterministic tests cover urgency,
clipping, incremental/mixed/non-incremental order, updates, uncommitted selection,
actual base/path packets, borrowed paths, split and real lost-packet work.
Public concurrent priority changes preserve payloads under race; independent
v1/v2 transfers with three prioritized streams pass race (1.374s). Focused
internal final tests pass (1.424s). Final full root race passes (root 12.241s,
internal transport 42.607s), both modules pass vet. Full default independent race
passed (88.212s) before the final equivalent one-pass selector optimization;
the focused independent priority gate above used the final selector.

The one-pass scan reduces the 4096-item microbenchmark from ~49us to ~27us with
zero allocations; selection remains linear and no end-to-end speedup is claimed.
See benchmarks/stream-priority-selection.md for fixture limits. Root inventory
is now 17 missing / 57 differing / 172 matching of 246 reference declarations;
HTTP3 stays 74 missing. HTTP priority signals, global priority across distinct
path queues, scalable indexing and broader scheduling/fairness evaluation remain.
The full implementation and performance goal remains active.

## Latest: 2026-09-29 outgoing context metadata preservation

Fixed outgoing Conn.Context and stream contexts losing values from the dial
context. Previously TLS received those values, but the public lifetime came from
context.Background. Conn.Start now establishes an owner-canceled lifetime from
context.WithoutCancel when admission has not already installed a selected
context. Failure/disposal use the same cancellation path for both TLS roles.
Dial deadlines and post-return cancellation do not terminate the connection;
terminal peer errors retain their application code, remote flag and cause.

A new v1/v2 regression for DialAddr and Transport.Dial fails in all four cases
without the fix. With the fix it verifies retained values, absent inherited
deadline, bidirectional stream metadata, data transfer after dial cancellation,
and peer-close causes. Existing early-dial tests now verify metadata propagation
through actual 0-RTT in both socket APIs, acceptance/rejection and compatible
version switching. Focused race tests pass (2.270s). Full root race passes
(root 12.242s, internal transport 42.273s), and both modules pass vet.
Independent v1/v2 tests add outgoing context values, post-cancellation transfer
and peer-close cause checks; the full default independent race suite passes
(87.012s). API counts remain 19 root / 74 HTTP3 missing;
no new performance measurement or overall completion claim is made.

## Latest: 2026-09-29 incoming connection context ownership

Added Transport.ConnContext with immutable startup capture and incoming-only
admission. The callback runs after Initial integrity and address-token checks,
before configuration selection and TLS allocation. It receives real token-based
AddrVerified metadata. Rejection and nil returned contexts produce a fixed
CONNECTION_REFUSED response; local callback details do not enter wire errors.
The callback is synchronous on the endpoint owner and must not block or reenter.

Returned context values reach TLS ClientHelloInfo, Conn.Context and send-stream
contexts. The engine owns cancellation on terminal errors and disposal, including
before public acceptance. Both callback input and selected lifetime are canceled
even when the returned context has an unrelated parent. Socket-loop cancellation
ordering cannot replace the connection's terminal cause. Listener closure leaves
accepted contexts alive; the hook persists through listener replacement and never
runs for dialed connections. Qlog propagation awaits its missing public API.

Focused v1/v2 race tests cover Retry/no-Retry metadata, TLS and stream values,
listener replacement, local/peer/transport shutdown and cause identity. Ten
repetitions pass (5.841s). Rejection, nil context, TLS failure, generator failure
and configuration refusal cleanup pass (1.610s). An internal test covers rejected
Initial integrity and independent returned-parent cancellation (1.296s).
Independent v1/v2 peer-close and TLS propagation tests pass (1.326s). Full root
race passes (root 12.158s, internal transport 42.535s); the full default
independent race suite passes (86.750s), and both modules pass vet.
Root inventory now has 19 missing, 56 differing and 171 matching declarations
out of 246; HTTP3 remains 74 missing. No new performance measurement was made.
The broader implementation and validation goal remains active.

## Latest: 2026-09-29 CID collision and lifecycle validation

Audited router synchronization: collision preflight precedes route mutation, and
failed registration removes only the candidate connection. New public v1/v2
tests force a generator to repeat the live initial CID and verify rejection of
the second dial, continued transfer on the original connection, and successful
subsequent unique dialing. Empty-ID second dials to the same peer are likewise
rejected without disrupting the first. Ten repetitions pass race (1.731s).

New generator-backed lifecycle tests combine additional-path validation/migration,
primary-path retirement, listener replacement and old/new stream transfers using
8- and 12-byte IDs. Ten v1/v2 repetitions pass race (1.694s). Independent-server
tests now exercise generated client IDs at all lengths 0–20; combined with the
existing independent-client tests, all 84 role/version/length cases pass race
(1.549s). Combined focused root gates pass (1.352s), and both modules pass vet.
No production change was required; full suites were not repeated for test-only
additions. API counts remain 20 root / 74 HTTP3 missing. Incoming admission
collisions, later-issued cross-connection collisions and broader impairment
combinations still merit additional live coverage. Full goal remains active.

## Latest: 2026-09-29 public CID generator and empty IDs

Added ConnectionIDGenerator and Transport.ConnectionIDGenerator. Generator length
is captured at startup and overrides ConnectionIDLength. A per-Transport mutex
serializes initial generation during concurrent dialing with owner-side admission
and later issuance. Output is copied and length-checked; callback errors propagate.
Endpoint admission retains generator policy across listener generations.

Custom length zero now selects empty IDs and address-based routing. Handshakes
work with migration disabled; reset keys and multipath cannot use empty IDs.
Retry source IDs and client-selected original destination IDs remain independent
eight-byte random IDs. Shared preferred-address registration remains unsupported,
though its internal generator hook is present. Applications must provide globally
unique, unlinkable IDs and synchronize a generator shared across Transports.

All 42 custom own-peer length/version cases pass; independent upstream clients
also pass 42 server-generator cases (race 1.529s). Public failures cover invalid
lengths, wrong output length, preserved callback errors and unsupported empty-ID
combinations. Eight simultaneous dials use an intentionally unsynchronized fixture
to verify Transport serialization (combined public race 1.553s). Full root race
passed (root 11.468s, transport 42.638s), and both modules passed vet. A mistaken
root-module invocation selected no independent tests; the separate interop-module
run above supplies the actual independent evidence. Full independent suite was
not repeated. Root inventory: 20 missing, 56 differences, 170 matching of 246;
HTTP3 remains 74 missing. Goal remains active, with broader collision/restart and
migration combinations outstanding.

## Latest: 2026-09-29 CID generator groundwork

Added internal GenerateConnectionID plumbing for admission, replacement issuance,
preferred addresses and multipath handoff/issuance. A common helper copies and
length-checks callback output. Per-connection uniqueness remains bounded; repeated
custom IDs fail after 16 attempts. Preferred CID generation now also has a bounded
attempt count. Admission retains generator policy across configuration selection,
and errors/invalid initial output return without allocating a connection.

Tests exercise reused callback storage, duplicates, wrong lengths, preserved
errors, protected v1/v2 admission and handshake, replacement issuance, preferred
IDs and subsequent multipath IDs. Focused generator/reset/preferred race gates
passed (13.833s); strengthened generator gates passed (1.303s). Full root race
passed (root 11.307s, transport 42.173s), and root vet passed. Independent tests
were not rerun for this unexposed internal hook. Public ConnectionIDGenerator is not exposed yet:
its zero-length contract, transport callback serialization and shared-dial initial
CID generation remain to integrate. Inventory remains 24 root / 74 HTTP3 missing;
no completed public generator parity claim is made. The full goal stays active.

## Latest: 2026-09-29 configurable connection-ID length

Added Transport.ConnectionIDLength for 1–20 byte local routing IDs; zero selects
four bytes. Startup validates and fixes the router length, which server admission
and shared dialing now use. Listener replacement retains the endpoint length;
selection callbacks cannot override it. Subsequent and preferred-address CID
issuance already inherit initial local length, and reset extraction reads the
router length. Legacy convenience listeners/dialers and Retry source IDs retain
eight-byte generation. Custom ConnectionIDGenerator remains outstanding.

All 42 own-peer cases pass race (1.555s), inspecting emitted Initial SCIDs from
both roles and verifying stream routing for every setting across v1/v2. Invalid
lengths fail before runtime initialization. Full root race passed (root 11.035s,
transport 42.137s), and both modules passed vet. Independent reset-restart tests
now exercise default/1/4/8/20-byte server CIDs across both versions. Full default
independent race passed (86.533s); documented opt-in exclusions remain unchanged.
Root inventory is 24 missing, 56 differences and 166 matching entries of 246;
HTTP3 remains 74 missing. The full goal remains active.

## Latest: 2026-09-29 stateless-reset transmission and public key

Added StatelessResetKey and Transport.StatelessResetKey with startup key copies.
Incoming admission and shared outgoing dialing inherit the endpoint key. After
normal CID/reset-token routing fails, unknown fixed-bit short packets can elicit
random-prefix resets carrying the derived CID token. Live CID ownership prevents
reset disclosure even if address policy rejects routing. Responses shrink
strictly, stay within 21–42 bytes and obey an endpoint-wide 100/second cap without
retaining attacker-selected source state. Nil keys disable sending; unknown long
headers and clear-fixed-bit traffic do not trigger resets. Listener closure does
not disable endpoint reset handling.

Engine tests cover every trigger length 0–1200, header filters, token contents,
live-CID suppression, nil policy, rate exhaustion and recovery (race 1.326s).
Own-peer v1/v2 tests prove restarted server and client detection, with explicit
selection of a client-issued CID for the latter (race 1.367s). Independent
upstream-client restart detection passed (1.283s). Full root race passed (root
10.982s, transport 42.376s); both modules passed vet. The full independent suite
was not repeated. Broader reset/extension combinations and rate-limit fairness
remain open. Root inventory: 25 missing, 56 differences, 165 matching of 246;
HTTP3 remains 74 missing. The full goal remains active.

## Latest: 2026-09-29 keyed reset-token groundwork

Added internal ResetKey configuration, copied at connection creation. Reset
tokens use HMAC-SHA256 over the locally issued CID, truncated to 16 bytes.
Servers advertise the derived initial-CID token; both roles derive issued-CID
tokens, and preferred-address and multipath CID ownership retain the same key.
Admission keeps this policy independent of per-client configuration selection.
Zero-length local CIDs cannot enable keyed resets. Without a key, existing
random issued-CID tokens and absent initial reset parameters are unchanged.

The RFC 4231 HMAC test vector passes. Protected v1/v2 handshakes verify initial,
preferred-address and additional CID token advertisement, client exclusion from
the server-only initial parameter, caller-key snapshot ownership, and subsequent
multipath CID issuance (focused race 1.307s). Full root race passed (root 10.880s, transport 42.262s),
and root vet passed. Independent interoperability was not rerun because this
internal policy is not yet exposed by the public transport.
This is internal groundwork: public StatelessResetKey configuration, unknown-CID
reset transmission, packet/rate limits and restart interoperability remain next.
No public API inventory reduction or completed reset-transmission claim is made.
Normative basis: [RFC 9000 section 10.3.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-10.3.2).
The full implementation goal remains active.

## Latest: 2026-09-29 migration response deadlock

Investigated the intermittent additional-path migration failure. An initial 150
stress repetitions passed, but 1,000 repetitions then failed three times after
about three seconds (v1/v2, greasing enabled/disabled). Local migration was
suppressing every old-tuple packet, including PATH_RESPONSE required for the
peer to finish its independent validation. The peer can reject migration probes
until that validation completes, so both sides can wait until migration expires.

The sender now permits queued old-tuple PATH_RESPONSE while continuing to hold
ordinary output. These packets contain only probing frames and padding, so they
do not cancel peer migration nomination. Congestion, pacing and amplification
gates remain in effect; a blocked response cannot fall back to an old-tuple ACK.
The v1/v2 deterministic regression failed before the fix and passes afterward.
It authenticates the delayed challenge/response, proves peer validation completes,
and checks retained migration/ACK state and blocked-send packet-number stability.
Normative basis: [RFC 9000 section 8.2.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-8.2.2).

Focused internal migration race tests passed (1.773s); the strengthened response
regression passed (1.276s). All 1,000 post-fix live stress repetitions passed
(49.519s). Full root race passed (root 11.133s, transport 43.368s), as did
independent migration/rebinding/path race gates (38.026s). Both modules passed
vet. The full independent suite was not repeated. API counts are unchanged; the full goal
remains active and broader migration impairment validation remains outstanding.

## Latest: 2026-09-29 configurable address-token expiry

Added Transport.MaxTokenAge, captured at first use: zero selects 24 hours,
positive durations retain nanosecond precision, and negative values fail before
socket ownership. Format 2 tokens contain timestamp seconds/nanoseconds and an
issued duration in a 74-byte envelope; legacy 54-byte format 1 tokens remain
readable. Both sizes stay outside Retry's classification range. Validation takes
the shorter issued/current lifetime and rejects future or malformed times.
Local client storage still has a separate 24-hour retention limit.

Tests cover one-nanosecond through maximum-duration boundary expiry, fractional
seconds, longer-than-seven-day lifetimes, current-policy shortening, legacy
format expiry, malformed authenticated fields and classification. Own-peer and
independent token-restart tests now include expired evidence; token/early-data
independent race gates passed (1.874s). The full default independent race suite
passed (86.328s), with documented opt-in exclusions unchanged. Both modules
passed vet. A 10-second
address-token fuzz run passed 1,626,587 executions (two workers).

The first full root race run failed once in additional-path migration with
QUIC-bit greasing (invalid migration selection state after about three seconds).
Ten focused repetitions passed (1.695s), and a full root rerun passed (root
10.897s; transport cached after its 42.180s pass). This intermittent migration
failure is unresolved and retained in REMAINING.md; no fix is claimed.
Root inventory now has 27 missing, 56 differences and 163 matching entries out
of 246; HTTP3 remains 74 missing. The full goal remains active.

## Latest: 2026-09-29 transport token keys

Added TokenGeneratorKey and Transport.TokenGeneratorKey. Initialization copies a
configured secret or generates a random one and derives separate Retry/NEW_TOKEN
keys with domain-separated HMAC-SHA256. Token protectors persist across shared
listener generations. Replacing a transport with the same configured key retains
validation; a changed key or fresh random transport key invalidates old evidence.
Tokens remain implementation-specific and address validation is not TLS replay
protection. MaxTokenAge is still outstanding; existing token format precision and
range need widening before exposing the public duration option.

Own-peer v1/v2 tests cover random/shared/changed keys across listener and transport
replacement, cached-token offers and actual AddrVerified results. Independent
upstream-client tests cover the same replacement matrix. Focused root race passed
(1.482s), independent token/source-policy race passed (1.477s), full root race
passed (root 10.936s; unchanged internal packages cached), and both modules passed
vet. The full independent suite was not repeated this turn; its prior checkpoint
remains historical evidence. Root API inventory now has 28 missing, 56 differences
and 162 matching entries out of 246; HTTP3 remains 74 missing. Goal remains active.

## Latest: 2026-09-29 public source-address policy

Transport.VerifySourceAddress is captured at first use and survives listener
replacement. Nil/false skips Retry while retaining anti-amplification limits;
true requests Retry for an unvalidated source. Valid tokens bypass policy, and
GetConfigForClient receives actual token-evidence status. Legacy ListenAddr
still requires Retry; convenience-listener default alignment remains open.

Own-peer and independent v1/v2 tests cover nil/false/true across two listener
generations, callback source ports and AddrVerified. Multiple Initial packets
can arrive before Retry and legitimately invoke policy more than once; tests
were corrected to avoid a once-per-attempt assumption. Internal token-bypass
and amplification tests from the prior checkpoint retain their coverage.
Validation: full root race passed (root 11.145s, transport cached after the
42.125s run); full default independent race passed (86.357s). Both modules pass
vet. Focused public and independent policy gates passed (1.322s and 1.367s).
The first independent full run used the old assertion and failed only its Retry
policy-count cases; the corrected full run is the result reported above.
Documented opt-in interoperability exclusions remain unchanged.

Root inventory is now 30 missing, 56 declaration differences and 160 matching
entries out of 246; HTTP3 remains 74 missing. The full goal remains active.

## Latest: 2026-09-29 admission without mandatory Retry

Added an internal source-address policy callback after Initial integrity checking.
A false result admits an unverified source with exactly three times the received
byte count as send credit; valid Retry/NEW_TOKEN evidence still validates the
budget. Configuration selection now receives actual token-validation metadata,
and cannot manufacture validation or retain admission callbacks. Nil internal
policy preserves existing Retry behavior. The public VerifySourceAddress field
and its default/restart/reference behavior remain to be integrated; API gap
counts are unchanged.

New v1/v2 tests cover damaged Initial rejection before policy, callback lifetime,
validation-metadata tampering, exact credit, a blocked queued TLS flight without
packet-number consumption, bounded first-flight output and Handshake validation.
Existing v1/v2 and compatible-version Retry tests now verify policy invocation
and valid-token bypass. Full root race passed (root 11.203s, transport 42.078s),
as did root vet. Independent configuration-selection/isolation (race 1.347s) and
early client/server data (race 1.659s) passed. These independent gates exercise
existing token-validated public listeners, not the new internal no-Retry path.
Normative basis: [RFC 9000 sections 8 and 8.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-8).
The full goal remains active; public integration and independent no-Retry tests
are next, with broader conformance and performance gaps unchanged.

## Latest: 2026-09-29 Version Negotiation response policy

Added Transport.DisableVersionNegotiationPackets, captured at transport startup
and retained independently of listener configuration. It suppresses stateless
Version Negotiation responses without changing supported admission or handling
of Version Negotiation on an existing outgoing connection.

Internal tests cover configured v1/v2/both and invariant CID lengths 0/8/255,
checking suppressed output and no connection allocation (race 1.345s). Public
v1/v2 tests cover enabled/disabled probes, listener replacement, supported TLS,
continued stream transfer and outgoing fallback/rejection with the option set
(race 1.563s). An isolated eight-case pinned-reference comparison verifies
suppression and response CID/version semantics (race 1.562s). Root and interop vet
passed. The full suites were not repeated for this focused policy change; their
previous results remain historical evidence rather than a fresh full-run claim.

Root inventory now has 31 missing, 56 declaration differences and 159 matching
entries out of 246; HTTP3 remains 74 missing. The full goal remains active, with
other Transport policies, CID configuration, conformance and performance work
unfinished.

## Latest: 2026-09-29 raw datagrams on shared transports

Added Transport.WriteTo and ReadNonQUICPacket. Raw sends use the primary socket
without QUIC packet construction/accounting. Reads lazily enable an owner-managed
32-packet queue, copy payload ownership, discard new packets at capacity and
consume truncated datagrams once. Concurrent readers are serialized, and shutdown
releases the queue and wakes blocked readers. Known CID routes and stateless reset
tokens take precedence over the two-clear-leading-bits classifier so negotiated
QUIC-bit greasing remains usable. This intentionally extends the reference's
simple documented classifier; see API_PARITY.md for the source/contract.

Internal route/reset precedence, bounded ownership and concurrent-reader tests
passed race (1.307s). Public tests cover direct/plain/OOB sockets, exact raw writes,
classification, truncation and closure. A live v1/v2 stream test confirms actual
cleared-bit packets arrive while raw reception is enabled, then verifies only the
raw marker reaches the raw reader (focused race 1.536s). Independent v1/v2 tests
exchange raw datagrams both ways alongside established incoming/outgoing QUIC
connections, including plain/OOB wrappers and closed listeners (race 1.689s).
Full root race passed (root 10.727s, transport 42.285s); root and interop vet passed.
Full default independent interoperability passed (78.127s), with documented
opt-in known-failure exclusions unchanged.

Root inventory: 246 entries, 32 missing, 56 declaration differences and 158
matching declarations. HTTP3 still has 74 missing entries. Remaining transport
options, CID configuration, preferred sockets, wildcard packet info and the full
conformance/security/performance work are unfinished. The full goal stays active.

## Latest: 2026-09-29 OOB-capable PacketConn wrappers

Added the public OOBCapablePacketConn capability interface and runtime support.
Wrappers receive their own ReadMsgUDP/WriteMsgUDP calls and SyscallConn control
operations; the runtime does not unwrap or bypass them. Concrete UDP sockets keep
AddrPort I/O. OOB wrappers retain ECN and platform-dependent nonfragmentation/MTU
configuration, while plain PacketConn wrappers keep conservative capabilities.
Read payload/control lengths are checked before slicing wrapper-provided counts.

IPv4/IPv6 socket tests verify received ECT1 metadata, per-datagram ECT0/ECT1/NotECT
output with no marking leakage, wrapper method invocation, DF setup and borrowed
socket reuse (race 1.360s). Public lifecycle/shared-dial/multipath wrapper tests
passed race (1.881s); pinned upstream v1/v2 simultaneous-role transfers passed
race (1.456s). Full root race passed (root 11.722s, transport 44.448s), both vet
checks passed, and Linux/amd64 plus Windows/amd64 transport test binaries
cross-compiled. Cross-compilation is not runtime validation on those platforms.
Full default independent interoperability passed (78.472s), retaining the
documented opt-in known-failure exclusions.

Regenerated root inventory: 246 entries, 34 missing, 56 declaration differences,
156 matching declarations. Twelve added entries are one interface plus its
inherited/declared methods, not twelve independent features. HTTP3 remains 74
missing entries. Preferred sockets, remaining Transport options, wildcard packet
info, cross-platform runtime gates and broader conformance/performance work remain
unfinished. The full goal is active; no new performance claim is made.

## Latest: 2026-09-29 generic PacketConn transport adapter

Transport now accepts net.PacketConn implementations with UDP IP/port addresses
and deadline cancellation. The runtime keeps the ReadMsgUDPAddrPort/ECN path for
*net.UDPConn and uses ReadFrom/WriteTo for plain wrappers. Generic connections
retain concrete address ownership for migration/multipath but disable ECN and
kernel-dependent MTU probing. Borrowed shutdown still joins readers, clears
deadlines and returns the socket. Ownership compares canonical addresses rather
than PacketConn interface values, supporting non-comparable wrappers. Constructor
validation rejects typed nils and invalid local addresses; malformed read peer
addresses are discarded and invalid read lengths fail the socket safely.

Public wrapped-socket tests cover listener restart, normal/early listener handles,
shared dialing and incoming roles, cancellation isolation, own-peer multipath,
stream delivery and shutdown reuse (focused race 1.967s). Internal address,
capability, existing UDP ECN and borrowed lifecycle tests passed race (1.385s).
Independent wrapped-socket v1/v2 simultaneous-role transfers passed race (1.377s).
Full root race passed (root 10.117s, transport 42.142s); root and interop vet passed.
The default full independent interop suite passed (77.975s), retaining its
documented opt-in known-failure exclusions.

Public export counts remain 46 root / 74 HTTP3 missing entries: this fills part of
an existing field's behavior rather than adding declarations. OOB-capable wrapper
optimization, preferred-address registration, other Transport options, wildcard
packet-info and broader independent migration/extension combinations remain
unfinished. There is no new performance claim. The full goal stays active.

## Latest: 2026-09-29 shared Transport dialing

Added Transport.Dial and DialEarly on the existing borrowed UDP owner. DialAddr
and shared dialing use one connection-construction/version-fallback path. Failed
shared attempts remove only their own connection; successful shared dials never
enable dedicated close-when-idle shutdown. Resolved UDP peer validation and TLS
ServerName inference happen before socket initialization. Config.LocalAddr stays
a DialAddr bind option; Transport uses its already-bound Conn.

Own-peer v1/v2 tests cover reciprocal incoming/outgoing roles on the same socket,
multiple dials to one peer, canceled attempts returning capacity, dialing after
local listener closure, and multipath negotiation/transfer. Version negotiation
tests exercise fallback and terminal rejection followed by a fresh dial with a
one-connection capacity. The cached-ticket early-data matrix now runs on both
adapters: accepted/rejected 0-RTT, recovery, cancellation after early return and
compatible version switching. Focused race passed (2.124s); full root race passed
(root 9.452s, unchanged internal packages cached), and root/interop vet passed.

Independent v1/v2 tests use one socket per implementation for simultaneous
incoming/outgoing connections, then close both listeners and verify streams in
both directions. Final focused race passed (1.342s). The initial fixture wrongly
overrode its localhost TLS name with an IP absent from its certificate; fixing
the fixture required no production change. The first full interop run compiled
before that fix and failed only those new cases (77.949s). The corrected full
default interop suite passed (78.164s), with documented opt-in exclusions
unchanged.

Root export inventory is now 246 entries, 46 missing, 56 declaration differences,
144 matching declarations. Generic PacketConn, preferred socket registration,
CID configuration/generation, other Transport options and broader independent
extension combinations remain unfinished. The full goal stays active.

## Latest: 2026-09-29 public Transport listener adapter

Added Transport.Conn, Listen, ListenEarly and Close, plus ErrServerClosed and
ErrTransportClosed. The adapter uses a caller-owned *net.UDPConn; listener closure
preserves accepted connections, replacement listeners use fresh generations,
and transport closure joins readers and releases the socket. ListenAddr retains
its owning shutdown behavior. Server configuration construction is shared by
both entry points. Replacement capacity changes preserve live connections and
apply the new limit to future admission.

Public v1/v2 tests cover normal/early listener handles, rejected duplicate
listeners, restart, stale accept/close, capacity increase, post-restart streams,
transport closure and returned socket ownership. Invalid initialization and
concurrent Listen/Close tests also pass. Root focused race passed (1.377s), full
root race passed (transport 42.235s), and root/interop vet passed. The concurrent
Listen/Close test was added after the full root run and passed in the final
focused run. A pinned upstream-peer restart test passed race (1.325s), verifying
30 KB streams in both directions over old and new connections for v1/v2. The
full default independent interop suite passed (77.897s), with its documented
opt-in known-failure exclusions unchanged.

Regenerated root export inventory: 246 entries, 48 missing, 56 declaration
differences and 142 matching declarations. HTTP/3 remains 74 missing. This is a
partial Transport implementation: shared Dial/DialEarly, generic PacketConn,
preferred-address socket registration, application CID generation and remaining
options are unfinished. No performance or complete behavioral parity claim is
made. The full goal remains active and incomplete.

## Latest: 2026-09-29 borrowed UDP socket lifetime

Added NewUDPServerBorrowed for exclusive runtime use of a caller-owned UDP socket.
Shutdown interrupts reads/writes with deadlines, joins readers, releases queued
packet buffers, clears deadlines, then publishes Done. The primary socket stays
open; runtime-added sockets still close. Removal of the borrowed primary is
rejected. Concrete borrowed sockets initialize address-aware ownership before
future client/listener registration. Wildcard destination discovery and generic
PacketConn support remain unfinished.

Race tests cover concurrent Close, owner-triggered shutdown, read-error shutdown,
caller read/write reuse without replacing the cleared deadline, added-socket
ownership, and a saturated receive queue with a blocked owner. The live v1/v2
listener-restart payload/FIN test now uses a borrowed socket. Full root race
passed (transport 42.330s); final borrowed/restart/shared-client focused race
passed (1.375s) after adding automatic concrete-address ownership. Root vet and
final transport vet passed. The full default independent interop suite passed
(77.904s); its documented opt-in known-failure exclusions remain unchanged.

The runtime contract requires the caller to leave reads and deadlines to QUIC
until shutdown finishes; previous deadlines cannot be recovered from net.UDPConn.
Public Transport and generic PacketConn integration remain incomplete. No public
API inventory entry changed, and the full goal remains active.

## Latest: 2026-09-29 listener generations and restart

Added owner-serialized StartListener, generation-bound acceptance and closure,
and UDPServer.AcceptListener. Client-only endpoints can begin listening, and a
closed listener can be replaced while accepted connections keep their original
configuration and routing. Old handles cannot consume the replacement queue or
close it, even when close/restart happen within one owner command. Failed starts
preserve the previous admission state. Runtime ECN/MTU capabilities and endpoint
capacity remain with the socket owner; preferred addresses and multipath require
already available address-aware socket ownership. Non-eight-byte listener CIDs
remain unsupported, matching the current admission engine's restriction.

Live own-peer UDP tests for v1/v2 cover startup on a client-only endpoint, restart,
stale handles, old/new TLS handshakes, per-connection configuration retention and
post-restart stream payload/FIN delivery over both accepted connections. Invalid
configuration tests cover first-start and replacement rollback. Full root race
passed (transport 42.310s), root vet passed, and a final focused race run including
the expanded payload test passed (1.449s). The first payload test needed explicit
receive stream credit: wire defaults intentionally grant none.

These are internal foundations; the public Transport adapter, general PacketConn
ownership, independent simultaneous-role tests and full API parity are still
unfinished. No public inventory entry changed. The full goal remains active.

## Latest: 2026-09-29 listener shutdown independent of endpoint

Added owner-serialized internal CloseListener. It stops new admission and Accept,
preserves accepted incoming and outgoing connections, and refuses unaccepted
incoming connections with transport CONNECTION_REFUSED. Acceptance ownership is
recorded only when Accept returns a connection and is released with its route.
Closing/draining routes survive listener shutdown until their normal disposal;
repeated closure is harmless. Endpoint/socket Close remains separate.

Own-engine protected packet tests cover v1/v2 handshaking, queued and accepted
connections, refusal at the peer, accepted traffic, unknown Initial suppression,
and preservation of outgoing connections. A UDP runtime test verifies Accept's
notification subscription is awakened and the owner remains usable. A draining
route regression checks retention followed by deadline disposal. Full root race
passed (transport 42.183s); the final draining-route guard and regression were
validated by a subsequent focused race run (1.425s). Root vet and final transport
vet passed. Early test failures were fixture errors: missing received-byte
amplification credit and a simulated clock advanced beyond handshake timeout.

The ownership contract follows the documented distinction between shared
Transport listeners and convenience listeners:
https://quic-go.net/docs/quic/server/ (checked 2026-09-29).
Public Transport integration, restart/generation ownership, general PacketConn
support and independent simultaneous-role testing remain unfinished. Existing
public ListenAddr shutdown behavior is unchanged. API inventory remains 55 root
missing entries; the full goal is active and incomplete.

## Latest: 2026-09-29 shared outgoing endpoint registration

Added internal NewClientOnlyEndpoint and AddClient for serialized registration
of multiple already-started outgoing connections, including after UDP runtime
startup. Registration reserves routes before publishing ownership, creates an
address-aware owner when available, inherits runtime ECN/MTU capability, and
rolls back routes if network handoff fails. Failed registrations leave disposal
to the caller. Dedicated close-when-idle endpoints reject further registrations.
Outgoing connections now stay out of the listener acceptance queue.

Live own-peer UDP tests pass for v1/v2 with two clients on one socket, server
acceptance, independent removal and an endpoint that remains alive when empty.
Focused race tests cover collision, capacity, closed/dedicated endpoint rejection
and network-handoff rollback followed by successful retry (1.403s). Full root
race passed (transport 42.159s) and root vet passed. The rollback test was added
after the full run and passed in the final focused run. This is internal
infrastructure, not a public Transport API or independent interoperability proof.

Remaining shared Transport work includes public dial/listen integration, listener
shutdown separate from connection/socket shutdown, caller-owned PacketConn
support, wildcard address handling and independent simultaneous-role tests.
Root API inventory remains 55 missing entries. The full goal remains active.

## Latest: 2026-09-29 public connection ID values

Added ConnectionID and ConnectionIDFromBytes, with comparable immutable values,
Len, independent Bytes copies and reference-compatible String formatting. Inputs
longer than 20 bytes panic; the zero value equals an empty parsed ID. Storage uses
an immutable string rather than importing upstream internals. Application CID
generation and shared Transport integration remain separate missing work.

Root ownership/equality/length/panic tests passed race (1.313s). An isolated
upstream v0.63.0 comparison passed race (1.316s), checking 5,376 values across all
supported lengths plus mutation isolation. Both vet checks passed. Regenerated
root inventory: 55 missing, 56 declaration differences and 135 matching entries
out of 246. No performance claim accompanies this representation choice.

The initially considered stream ReadFrom/WriteTo methods are not present in the
pinned reference, so they were not treated as API gaps. The full independent QUIC
implementation, validation and performance goal remains active and incomplete.

## Latest: 2026-09-29 protected cross-path statistics validation

Added TestStatisticsMultipathCrossPathACK across v1/v2 and both sender roles.
Four actual path-one emissions establish exact sent totals. A protected PATH_ACK
arriving on path zero declares one path-one packet lost without changing path
zero's flight/RTT or the selected public RTT. A late ACK corrects exactly its
bytes and packet contribution. Repeated evidence on path one adds an arrival
without a second correction, including the same packet number across paths.
Path-one abandonment and engine closure preserve aggregate counters.

The initial four cases passed race (1.380s); the final statistics/path-recovery
selection passed (1.378s), and transport vet passed. This uses
synthetically prepared paths and our own packet protection; it is not independent
multipath negotiation evidence. No production change was needed. The full goal
remains active and incomplete.

## Latest: 2026-09-29 both-role independent late-ACK statistics matrix

Expanded TestStatisticsIndependentLateACK to our server as well as client role,
across v1/v2. The relay now withholds a full-sized datagram from our endpoint in
either direction and retains its exact destination for release. Both paths use
the same payload, retransmission acknowledgment, exact loss-correction and FIN
checks, retaining the EOF ordering needed to preserve peer ACK history.

All four cases passed race (1.541s); ten repetitions passed (2.744s, forty leaf
cases), and interop vet passed. No production change was needed. The statistics
evidence map now covers independent loss corrections in both sender roles;
multipath loss and full differential/WAN accounting remain open. The full goal
stays active and incomplete.

## Latest: 2026-09-29 independent-peer late-ACK loss corrections

Added a UDP relay test withholding one full-sized short-header datagram from our
client to pinned upstream v0.63.0. In v1/v2, retransmission completes while the
original remains withheld and the public loss estimate includes it. Releasing
the exact original bytes causes a real peer ACK to reduce the estimate by exactly
one packet and that datagram's length. The complete 30 KB payload and FIN are
verified afterward.

An initial EOF-first fixture failed to elicit the required old-packet ACK. Peer
qlog showed the delayed packet received but absent from the next ACK range.
Delaying application EOF consumption keeps subsequent stream-credit/ACK-history
retirement out of the test. No production workaround or weakened assertion was
introduced. The test retains bounded failure-only header/ACK-range diagnostics.

Final focused race passed (1.401s), ten repeated race runs passed (2.045s, twenty
leaf cases), and interop vet passed. Scope is independent-server/client-sender
loss correction, not multipath, server-sender or exact upstream statistics parity.
The full implementation and performance goal remains active and incomplete.

## Latest: 2026-09-29 protected-packet statistics loss corrections

Added TestStatisticsProtectedLossCorrection across v1/v2 and both sender roles.
Four real PING emissions are decrypted to verify packet numbers and content;
their observed lengths drive exact sent-byte and lost-byte expectations. An
authenticated ACK of the fourth packet declares the first lost, and a late ACK
removes its contribution from the published loss estimate exactly once.

Both packet replay and repeated ACK evidence in a fresh protected packet preserve
the correction and sent totals. Engine closure retains the corrected snapshot.
The focused race test passed (1.462s), the broader statistics/accounting race
selection passed (1.406s), and transport vet passed. No production
change was needed. This is own-engine wire evidence, not an independent loss
oracle; the full implementation and performance goal remains active/incomplete.

## Latest: 2026-09-29 independent-peer public statistics traffic

Added TestConnectionStatisticsIndependentTraffic against pinned upstream v0.63.0.
Four cases cover v1/v2 and both TLS roles. Each records handshake counters,
transfers and verifies 30 KB plus FIN in each direction, then checks packet-count
growth, byte deltas covering both payloads and positive/ordered RTT observations.
A peer application close cancels Context while retaining sent/received totals.

The new independent race selection passed (1.400s), and interop vet passed.
No production code changed. The fixture does not claim exact upstream accounting
parity, impairment/loss correction coverage or final socket-owner disposal;
those limits are recorded in CONNECTION_STATS_PLAN.md. The full implementation
and performance goal remains active and incomplete.

## Latest: 2026-09-29 public connection statistics snapshots

Added ConnectionStats and Conn.ConnectionStats with all ten reference fields.
The serialized owner publishes value snapshots after packet emission, ingress and
timeout events using an RWMutex; getters do not enqueue owner commands or read
mutable path state. Close publishes before disposal and preserves the final value.
RTT selects the lowest-numbered surviving prepared path, normally zero, with zero
durations before sampling. Loss estimates subtract bounded late corrections.

Public v1/v2 tests cover concurrent readers during transfer, an owner-side getter
callback, and retained values after both socket owners stop. Engine tests check
exact fields, path-zero abandonment/fallback RTT, unsampled RTT and final path
disposal. Focused public/internal race passed (1.404s/1.291s); full root race passed
(root 8.565s, HTTP/3 1.411s, transport 42.234s), full default independent race passed
(85.519s), and both vet checks passed. Root API inventory is now 60 missing,
56 declaration differences and 130 matching declarations out of 246.

Sequential matched echo runs compare publication against a no-op publication
control, retaining counters in both. Median independent time changes 10.008 to
10.076 ms (+0.68%), within wider sample variation. No negligible-overhead or
performance-improvement claim follows. Final gap remains about 1.87x time,
2.60x allocated bytes and 1.81x allocations versus upstream. Raw results/control
are linked in benchmarks/statistics-publication.md.

Statistics accounting policy, bounded loss correction and remaining independent
semantic/lifecycle checks are explicit in CONNECTION_STATS_PLAN.md. The complete
QUIC implementation, validation and performance goal remains active/incomplete.

## Latest: 2026-09-29 routed receive packet/byte totals

Added Conn.receivedTotals at ordinary and bound-multipath datagram ingress.
Counters include duplicate and unauthenticated arrivals, count identifiable
coalesced packets separately, and treat an unparseable remainder as one packet
attempt. Byte totals include the full routed datagram; empty input counts nothing.
Deferred replay bypasses the accounting boundary. Short-header input needs no
extra parse; long-header coalescing uses an explicit accounting parse pass.

Tests cover v1/v2 ordinary/multipath valid, duplicate, corrupt, malformed and
empty input. Reversed real TLS server flights prove actual deferral/replay without
double counting. Coalesced handshake tests assert exact totals, including a corrupt
packet preceding valid input. Focused race passed (1.474s), full root race passed
(root 8.470s, HTTP/3 1.366s, transport 41.058s), and root vet passed. The additional
corrupt-coalescing assertion passed its own race run (1.274s).

The statistics plan now documents the receive-attempt policy and remaining
independent/lifecycle checks. Safe public snapshot publication and RTT selection
are next; the public API remains absent. The full goal stays active/incomplete.

## Latest: 2026-09-29 committed send packet/byte totals

Added owner-managed Conn.sentTotals at ordinary packet, multipath packet and
cached-close emission boundaries. Each committed output counts its full QUIC
length, including padding/protection and excluding UDP framing. Repeated closes
count per emission; sealing a close or returning no output does not count. The
boundary precedes socket writes, so locally rejected writes remain attempted sends.
Stateless endpoint responses outside Conn are excluded.

Four v1/v2 ordinary/multipath fixtures check exact byte/packet deltas and close
preparation/emission/suppression. The cached-close response test checks repeated
ciphertext counts twice. Shared drain assertions verify exact deltas across the
existing handshake, stream and retransmission fixtures. Focused race passed
(1.346s), full root race passed (root 8.520s, HTTP/3 1.412s, transport 41.151s),
and root vet passed.

Updated the statistics plan with accounting boundaries and remaining checks.
Receive totals and public snapshots remain unfinished; no API parity or performance
completion is claimed. The full goal remains active and incomplete.

## Latest: 2026-09-29 connection-level loss accounting

Added owner-managed declaration/correction packet and byte totals to Conn,
independent of path lifetime. Ordinary ACK recovery, explicit loss detection and
loss timers record declarations; multipath ACK dispatch and path timers use the
same accumulator. Validated late ACK events record corrections. Local Retry
requeue, key discard and path abandonment do not declare network loss. Multipath
recovery commits are counted even if later frame completion fails.

Eight controlled-owner cases cover v1/v2, ordinary/multipath recovery,
packet/time-threshold loss, exact bytes, duplicate corrections and retention across
key discard/path abandonment. These are recovery-dispatch tests, not public or
independent-peer statistics validation. Focused race passed (1.400s), full root
race passed (root 8.420s, HTTP/3 1.405s, transport 41.333s), and root vet passed.

Updated CONNECTION_STATS_PLAN.md. Sent/received accounting and public snapshots
remain unfinished; API inventory counts remain unchanged. The full implementation
and performance goal remains active and incomplete.

## Latest: 2026-09-29 recovery late-loss correction events

Added recovery Events.RecoveredPackets/RecoveredBytes as a statistics foundation.
Validated ACKs report retained previously lost packets exactly once, separately
from ordinary newly acknowledged packets and bytes-in-flight accounting. Existing
bounded congestion history now retains validated packet sizes; no separate ledger
or unbounded retention was introduced. Discard, eviction and persistent-congestion
interval retirement limit the correction horizon, as documented in the stats plan.

Tests cover three packet spaces, invalid ACK atomicity, duplicates, exact sizes,
discard/eviction, packet-number collisions across spaces and cumulative ACKs beyond
the history horizon. Full recovery race passed (1.421s), full root race passed
(root 8.513s, HTTP/3 1.406s, transport 41.041s), and root vet passed. The additional
cumulative-ACK regression and related correction cases passed race (1.364s).

Public ConnectionStats and connection aggregation remain unimplemented; no API
inventory improvement or performance gain is claimed yet. The full goal remains
active and incomplete.

## Latest: 2026-09-29 connection statistics accounting audit

Inspected the missing public ConnectionStats API against the pinned v0.63.0
reference and our receive/recovery/public-connection state. Existing authenticated,
amplification and in-flight counters cannot substitute for lifetime public totals.
The reference's public loss comments permit decreases, but inspected counter
writes only increment; this needs behavioral validation rather than assuming
comment parity. Multipath disposal and snapshots after owner shutdown also need
explicit handling.

Recorded concrete accounting stages, commit-boundary questions and exact test
requirements in CONNECTION_STATS_PLAN.md. No declarations or counters were added;
the root API inventory remains unchanged. This is implementation planning based
on inspected code, not completion evidence for the missing API. The full goal
remains active and incomplete.

## Latest: 2026-09-29 partial delivery callback-failure fuzzing

Expanded FuzzStreamDeliveryPartial to inject completion callback failures during
ACK processing and reliable-prefix reductions. Failed callbacks must leave the
complete ledger unchanged, including bitmap coverage and completion flags. The
independent boolean-byte model advances only for successful operations. Added
zero-length FIN-only coverage and seeds that retry rejected completions.

Selected race tests passed (1.359s), transport vet passed, and a fixed-count fuzz
campaign completed 1,000,000 executions successfully (19.639s). The preceding
30-second campaign reached 2,156,940 executions but exited with a harness
context-deadline error, without a saved failing input; that run is not counted as
a pass. The fixed-count rerun passed, but does not diagnose the timed shutdown.
No production change was needed. The full goal remains active and incomplete.

## Latest: 2026-09-29 protected partial-reset sender ACK ordering

Inspected sender completion and its remaining late-FIN optimization gap. Added
TestPartialResetProtectedACKOrders so ACK-order evidence traverses packet
authentication and recovery rather than directly acknowledging internal items.
Eight cases cover v1/v2, both sender roles, and reset ACK before/after data ACKs.
Two STREAM packets and one reset packet are emitted and decrypted to verify
their contents before selective protected ACKs are injected.

Tail/FIN ACKs, including repeated evidence, cannot release a stream with a hole
in its reliable prefix. Completion releases the stream exactly once and clears
queued charges; a repeated reset ACK remains harmless. The fixture does not
implement late full-data/FIN completion without a reset ACK, and uses our engine
instead of an independent peer. No production change was needed.

New focused race passed (1.447s), broader partial-reset/delivery race passed
(1.526s), and transport vet passed. Updated the extension and conformance maps.
The full implementation and performance goal remains active and incomplete.

## Latest: 2026-09-29 released partial-reset peer-visible close coverage

Rechecked draft-ietf-quic-reliable-stream-reset-11 section 5.2 and strengthened
TestPartialResetReleasedValidation. The old test covered server receivers only;
earlier descriptions overstated its role coverage. It now covers both receiver
roles and v1/v2 across six contradictions: changed error/final size in ordinary
or partial resets, data beyond final size, and a contradictory STREAM FIN.

All 24 cases complete TLS, release a partially reset stream, deliver protected
contradictory input, verify local closing, then deliver the protected close to
the sender and verify remote code, offending frame type and draining. These are
own-engine packet tests, not independent-peer validation. No production change
was needed. The conformance map records the expanded evidence and its limits.

Expanded test race passed (1.516s); after naming cleanup, the broader partial-reset
transport race selection passed (1.527s), and transport vet passed. The full
implementation and performance goal remains active and incomplete.

## Latest: 2026-09-29 reuse reply storage across readiness retries

UDPServer.Wait now constructs its command callback and private reply channel once
per operation, reusing them only after receiving the preceding reply. Shutdown
terminates the operation; there is no pool or storage sharing between callers.
Cancellation still joins a submitted callback's result. A new real-owner test
forces 32 wakeups then cancels during successful commitment; it passes on control
and final implementations under the race detector.

Isolated submission benchmarks reduce eight-check operations from 24 allocations
to 3, and 32-check operations from 96 to 3. Immediately ready operations remain
at three allocations. Matched independent-peer echo shows effectively unchanged
allocation counts (35,634 to 35,639 median), without a demonstrated end-to-end
speedup. Final independent/upstream ratios remain about 1.87x time, 2.53x bytes,
and 1.81x allocations. Evidence and control patch: benchmarks/wait-retry.md.

Full root race passed (root 8.621s, HTTP/3 1.410s, transport 41.099s), default
independent race passed (85.327s), and both vet checks passed. Benchmarks ran
sequentially without concurrent tests. The full goal remains active/incomplete.

## Latest: 2026-09-29 echo profile endpoint attribution

Traced the large x/net recvMsg allocation subtree to the pinned upstream client,
not our UDP receiver, which already uses ReadMsgUDPAddrPort. This subtree accounts
for 39.76% of whole-profile sampled allocation objects. Saved the call tree and
a filter selecting our root/internal packages while excluding the interop fixture
package, whose stacks can include upstream work.

The filtered summary identifies packet protection, queues, parsing, recovery
records, deadline contexts, and owner-command replies as investigation targets.
It uses the pre-header-change profile and does not provide isolated server CPU
or complete server memory accounting. Documented concurrency and mutable-deadline
constraints before attempting changes in those paths. See
benchmarks/echo-allocation-attribution.md for evidence and reproduction commands.
No production code changed and no performance gain is claimed in this pass.
The implementation and performance goal remains active and incomplete.

## Latest: 2026-09-29 exact-sized short-header allocation

Profiled the current independent-server echo fixture. Syscall/runtime work
dominates sampled CPU; short-header building and packet-number append account
for about 5.55% of sampled allocation objects cumulatively. This is a combined
client/server process profile, not isolated server CPU evidence.

The short-header builder now allocates the complete buffer once, retaining
independent ownership. Tests cover CID sizes 0/4/8/16/20, both key phases, packet
number truncation, parser agreement, and isolation from later header/CID mutation.
Long-header and early-packet construction remain unchanged. Microbenchmarks reduce
two or three allocations to one across those sizes.

Matched 100-iteration × three-sample independent-peer echo runs reduce median
allocations 37,342 to 35,568 (-4.75%) and bytes 1,691,431 to 1,662,377 (-1.72%).
Median runtime changes 9,991,449 to 10,004,099 ns/op (+0.13%): no throughput gain
is established. Final independent/upstream ratios are about 1.87x time, 2.57x
allocated bytes and 1.81x allocations. Profile summaries, raw measurements and a
reverse control patch are linked from benchmarks/short-header.md.

Full root race passed (root 8.639s, HTTP/3 1.425s, transport 41.271s), full default
independent race passed (85.599s), and both vet checks passed. Benchmarks ran
sequentially after testing finished. The full implementation/performance goal
remains active and incomplete.

## Latest: 2026-09-29 single-packet ACK experiment, reverted

Tested reusing OnACK's existing largest-packet lookup for single-packet ACK
counting and retirement. The candidate passed full recovery race (1.300s), but
matched 100,000-iteration microbenchmarks showed only 175.8 to 174.1 ns/op median
change, with much wider sample variation and unchanged 128 B/op / two allocations.
Mixed/larger ACK costs were not measured. This is insufficient evidence to retain
additional general-path branching as a performance improvement.

Restored sent.go byte-for-byte and verified cmp plus the restored recovery race
suite (cached). No production change remains. Raw samples, the candidate patch,
scope and interpretation are retained in benchmarks/single-ack-experiment.md.
Broader profiles/workloads remain the next basis for performance work. The full
implementation and performance goal remains active and incomplete.

## Latest: 2026-09-29 ACK-only queue end-to-end measurement

Ran the unchanged independent-peer v1 echo benchmark against the saved list
control and final typed queue, sequentially at 100 iterations × three samples.
The overlay diff contains only the ACK-only representation change. Each operation
checks a complete 1 MiB request/response echo and FIN; setup is outside the timer.

Independent-server medians changed from 38,085 to 37,225 allocations (-2.26%) and
1,720,588 to 1,679,040 allocated bytes (-2.41%). Time changed from 11,109,820 to
11,153,798 ns/op (+0.40%), so there is no demonstrated throughput gain. Upstream's
unchanged median time moved about 5.6% between runs. Final independent/upstream
ratios are approximately 1.76x elapsed time, 2.55x bytes and 1.88x allocations.
These short whole-process measurements include client and server, not isolated
server CPU or peak memory.

Both benchmark runs completed with payload checks passing. Raw results and
reproduction details are linked from benchmarks/ack-only.md; the current gap map
now reflects this latest measurement. No production code changed this turn.
The full implementation and performance objective remains active/incomplete.

## Latest: 2026-09-29 typed ACK-only retention queue

Replaced container/list and boxed packet numbers in recovery's ACK-only history
with typed linked nodes. FIFO eviction, indexed arbitrary removal and discard
retain their prior semantics; links are cleared on removal. A slice-ledger model
checks 30,000 operations across capacities 1, 2 and 17 and passes on both the
control and final implementations. No node pool or preallocation was added.

Matched steady-retention microbenchmarks (100,000 iterations, three samples,
Go 1.26.2/M4) reduce allocation from three to two per send and save 32 B/op:
120 to 88 at capacity 32 and 124 to 92 at capacity 1024. Median timing is mixed:
178.4 to 182.2 ns/op and 207.8 to 200.0 ns/op respectively. This is an allocation
improvement, not evidence of end-to-end throughput superiority. Raw results,
fixture scope and a reverse control patch are in benchmarks/ack-only.md.

Validation: final recovery race 1.257s; full root race passed (root 8.950s,
HTTP/3 1.456s, transport 41.741s); full default independent race 86.156s; both
vet checks passed. Measurements ran sequentially after any concurrent testing
had ended. The full implementation and performance goal remains incomplete.

## Latest: 2026-09-29 semantic receive failures cannot escape as ACKs

Added 12 protected-packet cases for stream-credit, stream-count and final-size
violations across v1/v2 and both roles. Each packet first contains a valid PING;
the final-size case also contains a valid FIN before contradictory data. After
the semantic error, the test decrypts and scans the complete output payload:
only the correct transport close and optional padding may appear, and the ACK
pending flag must be cleared. No ACK of the failed packet may escape.

This closes a specific evidence gap left by the earlier receive audit: internal
receipt bookkeeping precedes semantic frame handlers, but closing takes priority
over ACK output for these violations. No production change was required. Earlier
frame-effect rollback, other failure types and multipath closing remain separate
questions; CONFORMANCE.md retains those limits.

Validation: focused matrix race 1.434s; combined receive-failure, crypto-close,
ACK-loop and protected-unsent-ACK race selection 1.523s; scoped vet and whitespace
checks passed. The full goal remains active and incomplete.

## Latest: 2026-09-29 non-eliciting packet gaps and authentication

Added 12 protected-packet cases covering v1/v2, both roles, and ACK/ACK_ECN/PADDING.
After completed TLS, duplicate non-eliciting packets with large gaps must leave
the quiescent connection silent. A PING with a corrupted authentication tag must
not advance ACK history or produce output. A later valid PING must elicit exactly
one ACK containing the earlier valid packet numbers and excluding the corrupted
packet; the test decrypts the emitted ACK with the peer's keys.

CONFORMANCE.md now maps the relevant receive/scheduling paths to RFC 9000
sections 13.1 and 13.2.1 and identifies remaining semantic-failure, multipath,
timing and eviction gaps. These are own-engine tests, not independent-peer
evidence. No production change was needed.

Validation: focused new race matrix 1.403s; combined ACK-loop, receive-ECN,
protected unsent-ACK close and ACK-view race selection 1.432s; scoped vet and
edited-file whitespace checks passed. The full goal remains active/incomplete.

## Latest: 2026-09-29 protected close after an ACK of an unsent packet

Added TestUnsentACKProtectedClose: v1/v2, both endpoint roles, all three packet
number spaces, and ACK/ACK_ECN (24 cases). Each authenticated frame acknowledges
exactly the target's next, unsent packet number. The target must enter closing
with PROTOCOL_VIOLATION and the corresponding ACK trigger; the other engine must
decrypt the emitted close, observe that same code/trigger and enter draining.
Handshake cases exchange real TLS flights while withholding client Finished.
No production change was needed.

Expanded CONFORMANCE.md with the inspected send-number/range checks, bounded
metadata behavior, locally rejected send ledger, close mapping, and specific
limits of the evidence. These are own-engine packet tests; they do not establish
independent interoperability or exhaustive optimistic-ACK/multipath behavior.

Validation: focused new matrix race 1.473s; selected recovery/transport ACK,
rejection, migration and close regressions passed under race (1.294s/1.586s);
scoped vet and edited-file whitespace checks passed. The full goal remains
active and incomplete.

## Latest: 2026-09-29 response-limit resumption policy

Added an independent v1/v2 matrix covering seven receive-limit transitions:
unchanged, increased, decreased, implicit/explicit pooled default, legacy/new
configuration with equal effective limits, and direct-to-pooled construction
with equal or different effective limits (14 cases). The peer always enables
early data. Both sides must report resumed TLS on the second connection; a packet
observer and the peer's Used0RTT state verify early traffic only when the effective
receive limit matches. Changing only the request encoder budget does not disable
early use when the receive limit remains equal.

After increase/decrease, a response between the old and new limits confirms the
resumed connection enforces the current configuration: it succeeds after increase
and produces local H3_EXCESSIVE_LOAD after decrease. This covers live use of the
receive-limit cache scope, without weakening the conservative policy that even
an increased limit disables early data. Broader ticket eviction, cancellation,
rejection and extension combinations remain open.

Only tests/docs changed. The new matrix, existing additional-settings resumption
matrix and transport protocol-options tests passed under the race detector
(1.845s); interop vet and edited-file whitespace checks passed. The default-v2
fallback exclusion in the protocol-options matrix remains a known failure, not a
passing case. The full goal remains active and incomplete.

## Latest: 2026-09-29 transport response limits and DATAGRAM configuration

Added Transport.EnableDatagrams and MaxResponseHeaderBytes. Explicit HTTP DATAGRAM
enablement requires QUIC support in an explicit QUICConfig; nil QUICConfig is
configured automatically. The legacy ClientConfig opt-in remains supported.
Positive response limits override the receive side of ClientConfig.MaxHeaderBytes;
otherwise its explicit limit applies, or pooled clients default to 10 MiB.
Direct clients retain the 64 KiB default. Decoder/frame-reader/SETTINGS limits and
TLS cache scoping use the effective receive budget, independently of request
encoding. Existing field-count and aggregate buffering budgets remain in force.

Independent tests observe negotiated DATAGRAM settings and verify a 1 KiB request
still reaches the server with a 512-byte response allowance. Encoded and decoded
header excesses are tested separately, and default pooled clients accept 70 KiB
headers in v1/v2. The stricter typed-error assertions uncovered local HTTP errors
escaping as internal types; conversion now publishes *http3.Error with local
code/reason and preserves the underlying cause. Initial focused/full tests failed
on this gap before the fix. Unit tests cover option precedence, caller immutability,
invalid DATAGRAM configuration before pool admission, and error identity.

Nil-config fallback to a v2-only pinned peer reproduced its known missing
version_information failure. That case is retained behind QUIC_INTEROP_VN_FAILURES,
not counted as passing; explicit-v2 and automatic-v1 negotiation remain enabled.
The inventory now has 74 missing HTTP/3 entries, 7 declaration differences and
80 matches. Full resource-policy parity and combined resumption tests remain open.

Validation after the fix: HTTP/3 race 1.407s; final root race passed with cached
packages; final focused independent options race 1.404s; full default independent
race 86.004s; both vet checks and edited-file whitespace checks passed. The
known v2 fallback exclusion remains explicit. The full goal remains incomplete.

## Latest: 2026-09-29 resumed early requests through ServeListener

Expanded the independent HTTP/3 early-request matrix to use both ServeQUICConn
and ServeListener, with resource policy allowing or deferring early dispatch in
v1/v2 (eight combinations, four new). A seeded ticket and address token permit
actual accepted 0-RTT without Retry. Holding the server flight proves ConnContext
runs before handshake completion and handlers run early only with approval. The
deferred case observes the policy decision before releasing TLS and dispatching.

Both peers must report TLS resumption and the server must report accepted 0-RTT.
Early and established response payloads are checked; the later request observes
completed TLS while the first request's snapshot remains unchanged. Cleanup now
joins HTTP workers explicitly, including when ServeListener returns separately.
No production change was needed. Rejection, cancellation and changed-policy
interleavings through this entry point remain open.

Validation: expanded early matrix plus listener ownership/accept-failure tests
passed under the race detector (1.799s); interop vet and edited-file whitespace
checks passed. The full goal remains active and incomplete.

## Latest: 2026-09-29 application-owned HTTP/3 listeners

Added QUICListener and Server.ServeListener, including regular, early and custom
listeners. Close/Shutdown stop acceptance without closing the supplied listener;
the application retains its lifetime. ServeListener returns when acceptance ends,
independently of active handlers. Those workers remain registered for Shutdown to
join, and an accept error does not cancel requests already in progress. Existing
Serve(ctx, *Listener) keeps its owning/joining behavior. Listener registration now
uses private entry identities so non-comparable custom listener values work.

Independent v1/v2 tests cover both concrete listener types and both shutdown
modes. Each listener serves a second HTTP server after the first stops. An active
response survives graceful shutdown timeout and finishes normally; Close cancels
its handler. ServeListener returns http.ErrServerClosed before the retained
handler finishes. A custom listener's injected accept error returns unchanged
while its response remains usable and Shutdown still waits for that request.
Unit cases cover non-comparable values, Close, Shutdown and accept failures.

The HTTP/3 export inventory now has 76 missing entries, 7 declaration differences
and 78 matches; the extra declaration difference is the ServeListener parameter
name. Early-listener tests here use ordinary handshakes, not resumed 0-RTT.
The overall implementation, parity and performance objective remains incomplete.

Validation: final root race suite passed (HTTP/3 1.428s, other packages cached);
full default independent race passed (85.963s). The final strengthened listener
lifecycle assertions also passed in a focused race run (1.631s). Both vet checks
and edited-file whitespace checks passed. No performance claim is made.

## Latest: 2026-09-29 connection context cancellation during HTTP/3 startup

Added an independent-peer matrix for cancellation while HTTP/3 critical-stream
setup is blocked. Both QUIC versions cover zero unidirectional stream credit,
one byte of stream credit, and one byte of connection credit. The upstream peer's
qlog must observe the corresponding STREAMS_BLOCKED, STREAM_DATA_BLOCKED (stream
3), or DATA_BLOCKED frame before cancellation. The peer neither reads the control
stream nor grants more credit.

Each case runs with a detached callback context and tests callback cancellation,
owner cancellation, or selected-context deadline expiry (18 combinations).
Assertions cover one callback, no request dispatch, startup termination, released
server admission, and closed QUIC ownership. Callback cancellation and deadline
expiry retain the explicit cause. Owner cancellation may close QUIC before the
detached-context bridge runs, so its exact error ordering is intentionally not
asserted. Existing startup helpers already handle these cases; no production
change was required.

Validation: new startup matrix plus existing live connection-context and nil-result
cleanup tests passed under the race detector (7.572s); interop vet passed. Only
tests/docs changed. This does not establish all deadline, selected-config,
migration or early-data interleavings. The overall goal remains active/incomplete.

## Latest: 2026-09-29 HTTP/3 server connection contexts

Added Server.ConnContext, ServerContextKey, RemoteAddrContextKey and standard
http.LocalAddrContextKey publication. Each admitted connection invokes the
callback once before HTTP/3 setup and outside server locks. Request contexts use
its values/deadlines/cancellation. The local lifetime bridge preserves owner
shutdown even if a callback returns a context detached from its input. Returning
nil panics with the reference message; cleanup still closes the connection and
releases admission. Setup address values are snapshots, not migration observers.

Independent v1/v2 tests cover metadata and custom values across two requests,
callback count, parent/callback cancellation causes, detached-context server
closure and nil-callback cleanup. Unit tests cover owner and selected-context
cancellation, already-canceled owners and callback deadlines. A Server.Close
race can yield an endpoint/transport cause before ErrServerClosed; the test
checks termination without imposing an unsupported cause ordering. Broader
selected-config, migration and deadline interleavings remain open.

Validation: focused independent context race 1.370s; final HTTP/3 race 1.518s;
full root race passed (cached unaffected packages); full default independent race
79.511s; both vet checks passed. HTTP/3 inventory now has 81 missing entries,
6 declaration differences and 74 matches. The overall goal remains incomplete.

## Latest: 2026-09-29 cached-only admission during 0-RTT recovery

Expanded the independent pooled-early matrix with cached-only requests sharing an
early slot initiated by an unrestricted HEAD_0RTT pool request. The fixture observes an actual
0-RTT packet and holds server TLS until the cached request's upload Read proves
stream/header admission. Acceptance/rejection and retries enabled/disabled are
covered in v1/v2 (eight new combinations).

With retries enabled, rejection recovers and reuses the same cached slot. With
retries disabled, the cached request returns Err0RTTRejected and a fresh explicit
cached-only request succeeds after recovery. Checks cover resumption, payload,
exact handler counts, one original-body close, and one recreated-body close only
when replay is required. The existing later concurrent requests and callback-error
checks still run, and exactly two TLS hellos (seed and resume) exclude redial.
The fixture proves pre-handshake admission and observed connection-level early
traffic, not per-stream decryption or exhaustive loss/cancellation interleavings.

Only tests/docs changed. The complete expanded pooled-early race matrix passed
(1.751s), as did interop vet; the broader implementation goal remains active/incomplete.

## Latest: 2026-09-29 cached-only 425 retry admission

Added independent v1/v2 tests for a replayable cached-only request receiving 425.
With its connection retained, the retry succeeds on that same connection. With
CloseIdleConnections invoked during GetBody (after the old response/upload have
closed), the retry returns ErrNoCachedConn, closes the recreated upload and makes
no new dial or application attempt. Both branches verify exactly one GetBody,
one close per upload body, and unchanged caller method; successful response
identity also remains intact.

The fixture deliberately warms the connection first. It proves cached-only 425
admission and ownership, not pre-handshake HTTP delivery; the cached-only actual
0-RTT rejection/recovery combination remains open. No production code changed.
The new cases plus the complete existing 425 matrix passed under race (1.772s),
and interop vet passed. The full implementation goal remains active/incomplete.

## Latest: 2026-09-29 cached-only HTTP/3 requests

Added RoundTripOpt, OnlyCachedConn and ErrNoCachedConn. Options flow through both
initial admission and the automatic retry. A miss cannot allocate a pool slot,
eject another origin or start a dial; an existing dialing/early-recovery slot can
be shared. Canceled waiters release only their own reservation. RoundTrip delegates
to the same path with default options.

Unit tests check empty/full-pool misses, body cleanup, shared pending dials and
waiter deadline expiry. Independent v1/v2 tests check warm reuse and idle eviction.
A dedicated peer fixture warms stream zero, then rejects a cached stream-four
request with GOAWAY; its retry must return ErrNoCachedConn rather than dial.
Ordinary GOAWAY retry cases remain passing. The cached-only 425 and rejected-early
combination matrix is still open.

Validation: HTTP/3 race 1.463s; focused independent pool/GOAWAY race 1.470s; full
root race passed (cached unchanged results); final cached-waiter race 1.268s;
both vet checks passed. Full default independent race passed (78.713s). Inventory now has
84 missing HTTP/3 entries, 6 declaration differences and 71 matches. The overall
goal remains active and incomplete.

## Latest: 2026-09-29 HTTP/3 connection context and explicit close

Added ClientConn.Context and CloseWithError. Context exposes the transport's
lifetime and typed QUIC cause; retired and replacement early HTTP generations
share it. Explicit closure routes through existing ownership serialization, so a
retired generation cannot terminate its replacement. Invalid varint codes fail
before ownership changes. Close remains available to join HTTP workers.

Independent v1/v2 tests check local/remote closure code, message and origin on
both peers, continued HTTP delivery after an invalid close, and retained context
identity/cause after cleanup. Existing live early-recovery tests now check shared
transport context and stale explicit closure before successfully using the fresh
session. The ownership handoff race alternates ordinary and explicit-code closes.

Validation: focused independent closure/early-recovery race passed (2.607s); full
root race passed (HTTP/3 1.364s, unaffected packages cached); both vet checks passed.
HTTP/3 inventory now has 88 missing entries, 5 declaration differences and 68
matches. This is an incremental API/lifecycle checkpoint, not complete closure
ordering or implementation parity. The overall goal remains active and incomplete.

## Latest: 2026-09-29 live additional-settings resumption matrix

Expanded the independent server ticket matrix with unchanged, changed, added,
removed and newly configured extras in v1/v2. It observes an actual early QUIC
probe under a held server flight, excludes Retry, checks both peers resume TLS,
and verifies acceptance only for unchanged extra policy. Rejected early probes
cannot leak into fresh datagrams; subsequent HTTP and its settings snapshot use
the current configuration. The probe isolates TLS ticket behavior from peer HTTP
early-stream recovery; it is not a claim of unknown-extension application safety.

Added a 20-case direct/pooled client matrix across both versions and five map
policies. The independent server always permits early data. Same settings produce
observed 0-RTT packets and accepted early data; changes, additions, removal and an
empty map suppress early packets while preserving resumption. All cases complete
HTTP and check the client's current advertisement at the peer.

Validation: expanded server ticket race passed (1.618s); final direct/pooled client
race passed (1.565s); interop vet passed. Only tests/docs changed; the preceding
full root/default interop results apply to unchanged production. The initial
mistargeted command matched no tests and is excluded from this evidence.
The implementation/conformance/performance goal remains active and incomplete.

## Latest: 2026-09-29 configurable additional HTTP/3 settings

Added Server.AdditionalSettings and Transport.AdditionalSettings, plus the direct
ClientConfig equivalent. Configuration validates varint bounds, disallows managed /
reserved IDs, copies client maps, rejects duplicate merged entries and caps extras
at 59 so five managed settings fit the existing 64-entry budget. Sorting gives
stable wire order. Invalid settings fail before dialing or server ownership.

Canonical additional values now enter both client-local and authenticated server
HTTP ticket scopes. Identical maps keep compatibility regardless of insertion
order. Added/removed/changed values disable early use of the prior ticket without
losing ordinary resumption. Empty maps preserve the prior scope. Unit tests cover
this binding, private ticket copies, map ownership and exact-fit/overflow budgets;
a live changed-extra resumption matrix is still outstanding.

Independent v1/v2 tests observe additional settings from direct clients, pooled
clients with merged maps, and servers, including zero and maximum-varint values.
The export inventory now has 90 missing HTTP/3 entries, 5 declaration differences
and 66 matching entries. API/README notes describe stricter validation/resource
policy and the need for applications to implement advertised extension semantics.

Validation: HTTP/3 race 1.363s; focused independent race 1.455s; full root
race passed (HTTP/3 1.381s, unaffected packages cached); final boundary/ticket
unit race 1.418s; full default independent race 77.981s; both vet checks passed.
The full goal remains active and incomplete.

## Latest: 2026-09-29 settings on server response writers and client bodies

Public server response writers now implement Settingser, matching the pinned
reference's handler-facing interface. They retain the concrete writer's Flush,
FlushError and AcceptTunnel behavior and expose Unwrap. The settings conversion
is shared with ClientConn and keeps snapshots private and session-bound.

Client response bodies also expose Settingser through pool reservations, gzip
wrappers and successful CONNECT tunnels. This is a local convenience extension:
the pinned reference exposes this interface on connections and server response
writers, not response bodies. The parity notes correct the previous assumption
that request/response-body exposure was a missing reference requirement. The
symbol inventory remains 92 missing HTTP/3 entries; this change closes a behavior
gap on an unexported response-writer implementation, not an exported symbol gap.

Independent v1/v2 tests inspect server-side custom peer settings with DATAGRAM
on/off, snapshot isolation, flushing and response payload. Client tests cover
receipt-channel identity, unknown settings after body/connection closure,
automatic/explicit/disabled gzip, HEAD/range responses, and IPv4/IPv6 datagram
tunnels. HTTP/3 package race passed (1.412s); focused independent race passed
(1.463s); full root race passed (cached unaffected packages); final full default
interop race passed (78.044s); both vet checks passed. Documentation and inventory
were refreshed. The broader API, conformance and performance goal remains open.

## Latest: 2026-09-29 public HTTP/3 peer settings

Added ClientConn.ReceivedSettings, ClientConn.Settings, Settings and Settingser.
The session retains a bounded copy of live wire settings only after validation
and successful encoder initialization. Receipt is distinct from remembered early
settings and session closure. Public snapshots preserve advertised QPACK/unknown
values and return independent maps. Rejected early sessions keep their own state;
replacement clients observe their replacement sessions.

Independent tests compare this API against the pinned v0.63.0 client using the
same upstream server in v1/v2, with DATAGRAM disabled/enabled. They check extension
flags, explicit QPACK and unknown values, snapshot ownership and reads after close.
Internal tests cover live promotion, invalid early settings, empty SETTINGS,
duplicate identifiers, entry budgets, closure without receipt and storage copies.
Differences in publication timing and snapshot identity, plus remaining raw/body
interfaces, are documented in API_PARITY.md. The regenerated HTTP/3 inventory has
92 missing entries (previously 101), 5 declaration differences and 64 matches.

Validation: focused independent race 1.344s; HTTP/3 session and public package
race 1.400s / 1.578s; full root race passed (including cached unaffected packages,
HTTP/3 session 1.447s); full default interop race 76.992s; both vet checks passed.
Final lifecycle assertions verify the expected duplicate/over-budget error codes.
The full implementation and validation objective remains active and incomplete.

## Latest: 2026-09-29 persistent-congestion oracle and capacity-pressure evidence

Added an endpoint-pair slice oracle for persistent-congestion detection and
history consumption. Fuzz inputs combine loss, late ACK, discard, eviction,
local removal, missing RTT samples, non-eliciting packets, MTU probes and clock
regressions. A bounded four-worker campaign passed 5,821,234 executions in
31.265s. Final recovery race passed (1.241s); seeds and recovery tests also passed
against the previous list control (1.244s); recovery vet passed.

A new benchmark retains 32 or 1024 records near capacity, with delayed loss,
alternate late ACKs and periodic congestion scans. Sequential control/final runs
retain the 32-byte / one-allocation reduction per operation at both capacities.
Timing is mixed (32-record median regresses, 1024 improves), so there is no claim
of universal speedup. The occupancy assertion prevents silently measuring a
drained history. Raw outputs and exact fixture details are appended to
[the report](benchmarks/history-intrusive.md). Production code is unchanged;
overall performance, API, conformance and independent-validation work remains.

## Latest: 2026-09-29 embedded recovery-history links

Removed the separate container/list node by embedding ordering links in congestion
records. History send/ACK now costs 64 B and one allocation instead of 96 B and
two allocations. A 30,000-operation slice-ledger model covers capacity eviction,
ACK retirement, space discard and removal, and passes against both versions.
Existing persistent-congestion and local rejection coverage remains passing.

Sequential matched echo medians reduce allocation count 4.24% and bytes 1.47%.
Timing changes -1.01%, with an 11.148 ms final sample, so no throughput improvement
is established. Final echo remains 1.87x slower and 2.66x allocated bytes versus
upstream v0.63.0. Reproduction, reverse patch, raw samples and limitations are in
[the measurement report](benchmarks/history-intrusive.md).

Validation: recovery race 1.359s; full root race (root 8.849s, HTTP/3 1.372s,
transport 40.912s); full default independent race 77.958s; both vet checks passed.
The control recovery race also passed (1.259s). The overall goal remains active
and incomplete.

## Latest: 2026-09-29 recovery-history allocation reduction

Refreshed the matched v0.63.0 echo baseline after the stream API additions, then
profiled a separate 500-operation independent-server run. ACK-time assignment of
congestion records into container/list interfaces accounted for 42 MiB (4.87%)
of sampled allocated bytes. Recovery history now stores record pointers and
updates state in place, preserving ordering, bounds and persistent-congestion
semantics. Existing internal representation assertions were adjusted; a focused
history benchmark and reverse control patch make the allocation change repeatable.

The matched history microbenchmark reduces 144 to 96 B/op and 3 to 2 allocations.
Sequential three-sample echo medians reduce allocation count by 4.11% and bytes
by 1.09%, with considerable byte variability. Time changes only -0.12%, so there
is no demonstrated throughput gain. Final echo remains about 1.90x slower and
2.81x allocated bytes versus the pinned peer. Raw results, textual profiles,
methodology and limitations are in benchmarks/history-boxing.md. Profiling and
benchmarking ran separately, after correctness checks completed.

Validation: full recovery race passed (1.318s), full root race passed (root
8.585s, HTTP/3 1.336s, transport 40.917s), full default independent race passed
(76.902s), and both vet checks passed. Formatting/whitespace checks passed.
The broader performance, conformance, API and independent validation goal remains
active and incomplete.

## Latest: 2026-09-29 receive final-size callback

Implemented ReceiveStream.SetReceiveFinalSizeCallback, promoted on Stream.
The engine observes final offsets only after validated FIN/reset processing,
including incomplete receive prefixes, and clears its one-shot observer before
notification. Public handles retain the size across read completion/release and
runtime shutdown. Pending callbacks can be replaced or removed; known sizes are
reported synchronously outside the owner. Deferred callbacks execute on the
owner and must not block or call QUIC methods. Unknown-size connection closure
does not notify, and early rejection discards a pending public callback.

Engine tests cover FIN gaps, ordinary/partial reset final sizes, duplicate
suppression, late registration and invalid final-size changes. Public v1/v2 tests
in both roles cover replacement/removal, reentrant known-size registration,
unknown closure and retained metadata after actual runtime shutdown. The existing
16-case independent Peek matrix now also checks exactly one final-size callback
on FIN/partial-reset streams. Root API inventory refreshed to 72 missing entries.

Validation: focused public/engine race tests passed (1.387s/1.349s), strengthened
shutdown selection passed (1.302s), full root race passed (root 8.897s, HTTP/3
1.320s, transport 41.156s), and full default independent race passed (77.859s).
Both vet and formatting/whitespace checks passed. Concurrent callback ordering,
early/multipath combinations, broader API parity and the overall objective remain
open.

## Latest: 2026-09-29 atomic early-write rejection and stale new APIs

Added TestTryWriteAllEarlyRejection for v1/v2, uni/bidi streams and delivered or
dropped early packets. The test requires every multi-frame atomic payload/FIN
item to leave in 0-RTT before completing the rejecting resumed handshake. It
checks real TLS resumption, early rejection, retirement of original sent items,
empty stream generations, cleared used credit and reset reservations, then
explicitly opens the same ID and delivers a fresh atomic payload with loss.
Old application bytes are never replayed into the fresh generation.

Extended the public adapter's reused-ID isolation test to TryWriteAll,
WriteWithLimit and Peek for both open and FIN-closed stale handles. They return
Err0RTTRejected, and the stale limiter is never invoked. This adapter test uses
the release callback deliberately; the engine matrix separately supplies real
TLS rejection evidence. It is not independent-peer early atomic-write coverage.

Validation: the eight-case engine race matrix passed (1.383s), public stale-handle
race selection passed (1.330s), and root/transport vet passed. Formatting and
whitespace checks passed. Test-only changes did not require broad runtime-suite
reruns. Independent extension combinations, performance and the broader goal
remain unfinished.

## Latest: 2026-09-29 atomic-write reset reservations across extensions

Added TestTryWriteAllResetReservation for v1/v2 and uni/bidi streams, covering
established connections, accepted TLS resumption and synthetic multipath after
primary-path abandonment. A multi-frame slice is rejected with no changes when
the available budget is one byte short after including the reliable-reset
reservation. Exact-fit admission consumes the planned queue charges, and another
atomic write reuses rather than duplicates that reservation. After removing the
synthetic unrelated queue charge, exact bytes and FIN arrive; ACK cleanup returns
the reservation. Single-path cases include loss/reordering, and resumed cases
assert actual TLS resumption and early acceptance. Multipath sends use path one
after path zero is abandoned; path validation itself is synthetic in this fixture.

Validation: the initial 12-case race matrix passed (1.474s); after strengthening
the surviving-path and accepted-resumption assertions, the full TryWriteAll engine
selection passed (1.419s). Transport vet and formatting/whitespace checks passed.
No production code or shared fixture changed, so broad runtime suites were not
rerun. Early rejection, independent extension combinations, large-admission
fairness and the overall goal remain open.

## Latest: 2026-09-29 atomic TryWriteAll admission

Implemented SendStream.TryWriteAll and public ErrWouldBlock, also promoted on
Stream. The method ignores write deadlines and refuses a busy writer lock.
On the connection owner it preflights the whole slice against stream/connection
credit, queue charges and any reliable-reset reservation. Portable chunks are
then committed in the same serialized operation. An insufficient budget appends
no prefix and consumes no credit. Accepted data is copied into owned frame
storage and uses normal reliable recovery. The local bounded queue and portable
chunk policy remain differences from the reference's buffering strategy.

Engine tests cover both flow-limit refusals, one-byte queue-budget shortfall,
retry after grants, multi-frame loss recovery and caller-buffer mutation. The
initial retry fixture raised only sender credit and correctly hit peer flow
control; it now models the grant on both sides. Public v1/v2 tests in both roles
check expired deadlines, exact rejection/success boundaries and closed streams.
Independent peers receive a complete owned multi-frame slice in both versions,
roles and directions. The regenerated root inventory has 74 missing entries.

Validation: final focused engine race passed (1.341s), public selection passed
(1.289s), independent selection passed (1.369s), full root race passed (root
8.682s, HTTP/3 1.319s, transport 40.767s), and full default independent race passed
(77.968s). Both vet and formatting/whitespace checks passed. Large-admission
fairness, early-data/multipath atomicity and the overall conformance/performance
objective remain open.

## Latest: 2026-09-29 blocked limiter deadline and cancellation lifecycle

Added TestWriteLimiterBlockedLifecycle: v1/v2, both sending roles and uni/bidi
streams with three-byte connection and stream windows. An asynchronous six-byte
write admits exactly three bytes through one callback and then blocks. The
fixture separately expires its deadline, changes the live deadline to the past,
or cancels the stream after observing callback admission. Each returns the exact
committed prefix and preserves one external credit charge. Cancellation keeps
the local error code, and a later write does not invoke the limiter. Deadline
cases consume the peer prefix, resume on new QUIC credit, restrict the next
write externally and finish with a nil limiter; the peer receives the exact
original six bytes across reads.

Validation: the initial 16-case race matrix passed (2.308s); the final 24-case
matrix including live deadline changes passed (2.297s). Root vet and edited-file
formatting/whitespace checks passed. No production code changed, so broader
root/interop suites were not rerun. Independent timing/cancellation, early-data
and multipath limiter matrices remain open. The overall goal remains incomplete.

## Latest: 2026-09-29 external write-credit limiter

Implemented SendStream.WriteWithLimit and ErrWriteLimitReached, with the method
promoted on Stream. The owner invokes a nonblocking callback after QUIC credit
and queue admission checks, clamps its result and commits only that prefix. A
restricted result returns the committed byte count and the limiter sentinel.
Loss recovery reuses the frame without spending external credit again. Ordinary
Write uses the same path with a nil limiter; bounded owner batches, deadlines and
stream serialization remain in place.

Engine tests verify stream/connection-limited offers, queue pressure, zero and
negative allowance, upper clamping, unchanged accounting on refusal, and actual
first-application-packet loss/recovery without a second callback. Independent
v1/v2 tests in both TLS roles and stream directions verify zero refusal, limited
prefix, explicit suffix retry and exact peer delivery. API inventory refreshed:
77 missing root entries remain. TryWriteAll and limiter-specific early/multipath,
concurrent cancellation and deadline matrices remain open.

Validation: initial engine race selection passed (1.400s), independent limiter
selection passed (1.363s), final strengthened engine selection passed (1.295s),
full root race passed (root 7.639s, HTTP/3 1.332s, transport 40.891s), and full
default independent race passed (78.057s). Both vet and formatting/whitespace
checks passed. A failed test-file generation was corrected; its root selection
matched no tests and is not counted as coverage. The overall goal remains active
and incomplete, including known strict peer failures and performance gaps.

## Latest: 2026-09-29 Peek oracle and bounded fuzz validation

Extended the independent absolute-offset receive-storage model to issue Peek
requests after random pushes, reads and growth. Expected prefixes come only from
the byte map. Each call snapshots payload storage, presence bitmap, all receive
fields and connection flow accounting, and verifies no mutation. Incomplete
requests must return would-block without changing caller bytes. Terminal checks
repeat exact and oversized peeks before the consuming FIN/partial-reset read.
The near-MaxVarint case now peeks its complete prefix before consumption.

Validation: full stream race suite passed (1.343s), bounded four-worker fuzzing
passed (28,292 executions, 21.236s), and stream vet passed. The fuzz run reported
no further execution-count increases after its first three seconds, then exited
successfully; this is a bounded campaign, not sustained-throughput evidence.
Added an explicit 127-byte-stride seed that peeks across bitmap and physical-ring
boundaries; the final seed/high-offset race selection passed. No production code
changed, so public/transport/interop suites were not rerun. The overall goal and
broader lifecycle/concurrency validation remain open.

## Latest: 2026-09-29 nonconsuming stream Peek

Implemented ReceiveStream.Peek, promoted on bidirectional Stream. A bitmap scan
checks the requested contiguous prefix before copying from the receive ring;
incomplete requests leave the caller buffer untouched. Peek changes no offsets,
consumption credit, terminal-read flags or release state. It waits for a full
request or a terminal short prefix, respects read deadlines and shares Read's
serialization. Exact-sized terminal prefixes succeed; larger requests return
the prefix and EOF/partial-reset error. Completed buffers remain peekable after
runtime shutdown through the existing serialized retained-read path.

Primitive tests cover gaps, ring wrap, repeated peeks, FIN and partial reset.
Public v1/v2 tests prove deadline retry, read-after-peek and no extra stream or
connection credit. Independent peer tests cover both TLS roles, both stream
directions and FIN/partial-reset endings (16 cases), including deadline retry and
exact byte/error preservation. The retained-read runtime test now peeks before
consuming its payload. Root API inventory was regenerated: missing entries fall
from 82 to 80; other declaration and behavior gaps remain.

Validation: focused public/primitive race tests passed (1.845s/1.361s), independent
Peek passed (1.944s), retained-read shutdown passed (1.412s), full root race passed
(root 7.838s, HTTP/3 1.362s, transport 40.687s), and full default independent race
passed (77.786s). Both vet and formatting/whitespace checks passed. An initial
interop selection was accidentally run from the root and matched no tests; it
is not counted as coverage. Broader cancellation, early-data, concurrent deadline
and fuzz matrices remain open, as does the overall goal.

## Latest: 2026-09-29 reproducible exported API gap inventory

Added scripts/api-inventory, a standalone Go export-data comparison command.
It loads the pinned peer through the independent interop module and enumerates
exported types, functions, constants, variables, fields and method sets. Aliases,
embedded fields/methods, shadowing and pointer-only availability are resolved by
go/types rather than inferred from source spelling. Sorted root and HTTP/3
artifacts record 82 missing entries out of 246 and 101 out of 161 respectively.
Counts are symbol entries, not independent features or conformance percentages.

The audit makes previously incomplete scope concrete: Transport/shared sockets,
qlog/statistics, path methods, Peek/TryWriteAll/WriteWithLimit/final-size callbacks/
priority, and HTTP/3 raw connections/request-stream APIs remain absent. Source
checks confirmed selected stream/statistics methods in the pinned reference;
alias fields and promoted local Stream.Read appear in the inventory. Declaration
differences require interpretation, especially names and internal alias paths.
Other public packages and full behavioral contracts still need separate audits.

Validation: both generators completed against the current worktree and v0.63.0,
and tool vet passed. This checkpoint changes audit tooling and documentation,
not transport production code; root/interop runtime suites were not rerun. The
broader objective remains active and far from complete.

## Latest: 2026-09-29 version-negotiation error metadata

The exported VersionNegotiationError now provides Ours/Theirs version slices
and unwraps to net.ErrClosed, matching those reference API contracts. Existing
ChosenVersion/PeerVersions fields and local message remain. The packet engine
captures the attempted version and separately owned peer-offer slices; the
public no-common-version dial return copies the full configured preference list
without modifying the closed engine's terminal error. Peer offers remain
unauthenticated, and fallback authentication behavior is unchanged.

A new UDP rejection fixture checks both preference orders, unknown/reserved peer
offers, error identity and independence from caller/legacy slices. Existing
public and independent rejection tests now inspect the metadata. Initial interop
compilation caught a missing net import in the extended test; it was corrected.

Validation: focused local negotiation race tests passed (root 1.631s, transport
1.296s); independent rejection passed (1.306s), followed by the public dial
selection (1.308s, retaining its explicit known-failure skip). Full root race
passed (root 7.140s, HTTP/3 1.337s, transport 40.527s), both vet checks passed, and
formatting/whitespace checks passed. The full interop suite was not rerun for this
error-metadata change. Complete early fallback, API and conformance work remains;
the overall goal is still active and incomplete.

## Latest: 2026-09-29 typed DATAGRAM payload-limit errors

Oversized DATAGRAM sends now return the exported `*DatagramTooLargeError` with
`MaxDatagramPayloadSize int64`. The engine calculates the admission limit once
from peer frame credit and the current selected path's packet budget. Early
sends still use remembered credit and reserve long-header overhead. The typed
error supports errors.As and limit-sensitive errors.Is while unwrapping to the
previous engine sentinel. Unsupported DATAGRAMs and queue pressure retain their
existing distinct behavior.

Strengthened tests verify reported limits for peer frame credit, configured path
size, multipath shrink, early data and compatible-version cases. The independent
Initial-packet-size fixture now rejects an oversized public send, verifies the
reported boundary, then delivers a payload exactly at that boundary to the pinned
peer in v1/v2, both roles and all three configured sizes. Conservative header/CID
reservation can differ from upstream's estimate; this establishes usable limits,
not exact maximum-size parity or delivery after subsequent path changes.

Validation: focused DATAGRAM engine race tests passed (1.471s), independent size
tests passed (1.645s), full root race suite passed (root 7.129s, HTTP/3 1.429s,
transport 40.791s), and full default independent race suite passed (77.169s).
The full root run includes the strengthened early DATAGRAM matrix. Both vet
checks and formatting/whitespace checks passed. Strict known v2 peer failures
remain unresolved, and the overall goal remains incomplete.

## Latest: 2026-09-29 public stream-limit error parity

Nonblocking OpenStream/OpenUniStream now report the exported
`*StreamLimitReachedError` when peer stream-count credit is exhausted. Its message
matches the pinned v0.63.0 reference, and errors.As/errors.Is identify the public
type. Unwrapping retains the previous local would-block cause. Synchronous opens
continue waiting for credit, preserving context deadline/cancellation errors;
this change does not turn stream-count exhaustion into a connection failure.

Extended public v1/v2 tests cover both roles, disabled/positive limits, typed and
legacy error identity, and renewed credit. The independent STREAMS_BLOCKED gate
now also exercises nonblocking opens on both implementations with zero incoming
credit and compares their public error types/messages after the existing
synchronous cancellation checks.

Validation: focused public race tests passed (1.831s), focused independent tests
passed (1.329s), full root race suite passed (root 7.149s, HTTP/3 1.511s, transport
40.746s), and full default independent race suite passed (77.253s). Both vet checks
and edited-file formatting/whitespace checks passed. Known strict v2 peer failures
remain outside the default suite. The full API, conformance and performance goal
remains incomplete.

## Latest: 2026-09-29 independent shared connection-credit recovery

Added `TestDataBlockedSharedRecoveryUpstream` against unmodified quic-go v0.63.0
for both versions, TLS roles and uni/bidi streams. Two streams consume one byte
each from a shared two-byte connection window while retaining ample stream
credit. Both public writes then hit their deadlines, and peer traces observe
DATA_BLOCKED at limit two. Peer reads grant more connection credit; both streams
write another byte and block again with DATA_BLOCKED at limit four. FIN succeeds
with connection credit exhausted, and the peer verifies exact payload and EOF.
The trace accepts retransmissions rather than equating wire count with logical
deduplication.

The first run reached final close in all eight cases but failed because the test
retained its deliberately expired write deadlines. Resetting those deadlines
before Close fixed the fixture; no production change was needed. The complete
eight-case race run passed (4.658s), and interop vet passed. This test-only change
did not require rerunning the full root or interoperability suite. Independent
blocked-signal early-data/multipath coverage and the overall goal remain open.

## Latest: 2026-09-29 byte-blocked early and multipath lifecycle

Added combined DATA_BLOCKED/STREAM_DATA_BLOCKED regressions for v1/v2 and uni/bidi
streams. Real TLS resumption covers acceptance, dropped early packets, rejection
and larger handshake limits. Two stream notifications and one shared notification
must actually leave in the early flight. Checks cover retirement, exact queue
accounting, shared waiter state, suppression after ACK and fresh notifications
after rejection or promotion. Promotion drops the early flight; application reads
wait until after state checks so unrelated ACKs or new grants cannot mask failures.

A synthetic multipath test sends the three notifications on path zero, abandons
that path and verifies transfer of the same items to path one. Protected grants
on the survivor retire queued and sent references across paths, refund exactly
96 charged bytes, and allow new writes followed by fresh blocked notifications.
The shared path fixture now accepts optional initial transport parameters while
preserving its default behavior.

Validation: focused byte-blocked race tests passed (1.560s), the full transport
race suite passed (40.540s), and transport vet passed. Formatting and edited-file
whitespace checks passed. This checkpoint changes tests and documentation only;
root/public and independent interoperability suites were not rerun. Independent
multi-stream, early-data and multipath blocked-signal validation remains open,
along with the broader conformance and performance goal.

## Latest: 2026-09-29 shared DATA_BLOCKED signaling

Connection-credit exhaustion now emits one reliable DATA_BLOCKED for the current
limit. Writes detect stream and connection constraints independently before
payload allocation. A per-stream flag plus connection counter tracks which
senders encountered shared blocking; FIN and ordinary/partial reset remove their
interest. The notification survives until the last recorded blocked sender ends,
or an increased MAX_DATA/current handshake grant clears the blocked generation.
Queue pressure preserves retryability; none of these operations refunds used
credit. Early rejection rebuilds all shared tracking with the stream set.

New engine regressions cover both versions/roles/directions, multiple waiters,
deduplication, queue pressure, simulated notification loss, protected grants,
three terminal paths, a fresh waiter at an unchanged limit, and independent
stream/connection grants. The pre-existing stream-scope negative test now allows
DATA_BLOCKED while still rejecting a false STREAM_DATA_BLOCKED; it preserves
queue accounting around synthetic pressure. Independent peer traces observe the
connection limit while public writes stall after a one-byte connection grant.

Validation: full root race suite passed (root 7.294s, HTTP/3 1.297s, transport
40.625s); full default independent race suite passed (74.194s); both vet checks
passed. Final strengthened byte-blocking race selection passed (1.527s).
Independent multi-stream shared blocking, DATA_BLOCKED-specific early rejection
and multipath retirement remain coverage gaps. Existing strict v2 peer key-update
failures remain unresolved; default-suite success does not override them.
The overall goal remains incomplete.

## Latest: 2026-09-29 STREAM_DATA_BLOCKED signaling

Nonempty writes that exhaust stream receive credit now emit one reliable
STREAM_DATA_BLOCKED per stream/limit before payload allocation. Queue pressure
keeps the signal retryable without failing the connection. MAX_STREAM_DATA or
accepted-handshake credit increases retire stale notifications; FIN and both
ordinary/partial reset also remove them. Retirement scans are skipped for streams
that never signaled blocking. Connection-only exhaustion and empty non-FIN writes
are distinguished by regression tests. DATA_BLOCKED remains unimplemented here.

Engine tests cover v1/v2, both roles, uni/bidi streams, bounded signaling,
nonincreasing/increased protected grants, reblocking and terminal cleanup.
Independent peer traces verify stream ID/limit during public writes blocked at
a one-byte peer window, with partial-write deadline behavior.

Validation: final focused engine race selection passed (1.636s); full root race
passed (root 18.827s, HTTP/3 2.274s, stream 2.561s, transport 43.209s); both vet
checks passed. Initial full independent run failed (83.912s) in the v2 one-byte
HTTP/3 setup test. Three isolated attempts reproduced the timeout, and trace
instrumentation identified round zero at peer key phase one. A temporary peer
copy with only the v2 key-update label corrected passed the same strict case
(1.434s). This is diagnostic evidence, not success against the unmodified peer.

The default v2 fixture now uses two bytes, still blocking SETTINGS and testing
setup rejection/recovery. V1 retains one byte. Three focused default runs passed
(1.422s), followed by the full default independent race suite (73.817s). The strict
one-byte case remains under QUIC_INTEROP_KNOWN_FAILURES and was rerun: it still
fails at peer phase one (10.308s). No production key derivation or pinned dependency
was changed. KNOWN_FAILURES.md records the narrower default and strict command;
v2 key-update interoperability remains an unresolved goal requirement.

## Latest: 2026-09-29 blocked signaling across early and multipath transitions

Added real TLS resumption tests for STREAMS_BLOCKED in v1/v2 and both stream
directions. They exercise accepted/rejected early data, a dropped isolated 0-RTT
notification recovered after handshake, and increased stream limits on accepted
resumption. Checks cover DidResume/acceptance state, exact retirement, no implicit
peer stream opening, fresh notification after rejection and signaling a new limit.
The promotion case drops the old early packet, eliminating its ACK as an
explanation for retirement when current transport parameters become active.

A synthetic multipath test sends the notification on path zero, fails that path,
and verifies transfer of the same logical item to path one. Protected survivor
MAX_STREAMS retires old sent references and the transferred queue entry once,
returns the charge, permits another open and directs the next blocked signal to
the survivor. This is own-engine extension coverage, not independent early or
multipath interoperability.

Validation: the final focused STREAMS_BLOCKED race selection passed (1.545s),
and transport vet passed. No production code changed; full root/default interop
suites were not rerun. The overall conformance and performance goal remains open.

## Latest: 2026-09-29 STREAMS_BLOCKED emission and retirement

Opening a stream at the peer's count limit now queues STREAMS_BLOCKED rather
than only returning ErrWouldBlock. Notifications are deduplicated per direction
and limit, consume fixed bounded queue credit, and use reliable retransmission.
Larger MAX_STREAMS grants retire stale notifications across queued/sent state;
nonincreasing grants do not reduce credit or retire the current signal. Local
queue pressure preserves the blocked return and permits a later signaling retry.
No stream ID/object is consumed for a failed open. Initial/accepted-early limit
promotion now shares the stale-signal retirement path.

Protected engine regressions exercise both versions, roles and stream directions,
zero-limit blocking, queue pressure, duplicate attempts, simulated recovery loss,
MAX_STREAMS updates and reblocking at a new limit. Independent peer trace tests
verify wire emission while synchronous opens are blocked, in both TLS roles and
versions, and confirm cancellation. Explicit early/multipath blocked-signal
interoperability and other blocked frame types remain open audit items.

Validation: focused engine race test passed (1.450s); full root race suite passed
(root 7.186s, transport 40.370s). Full default independent race suite passed
(72.544s); the subsequently added independent blocked-signal trace test passed
(1.326s). Root and interop vet passed. An initial focused invocation in the root
matched no tests; only the corrected interop invocation is counted as coverage.
The overall implementation goal remains incomplete.

## Latest: 2026-09-29 independent receive-storage oracle

Added FuzzReceiveStorageModel with an absolute-offset byte map as its oracle,
separate from the ring implementation and its bitmap/remapping logic. Actions
cover contiguous and out-of-order input, conflicting overlaps, flow violations,
reads and credit growth across 4 KiB through odd 32 KiB windows. Each action
checks exact read bytes, errors, accounting, occupied-byte count and storage
bounds. Final gap filling verifies all retained bytes before EOF or partial-reset
completion. Seeds include repeated wraparound, sparse growth, empty input and
partial reset. This is a local model, not independent network-peer evidence.

A deterministic maximum-offset regression now forces physical 4 KiB to 8 KiB
to 8,193-byte expansion near the QUIC varint limit, verifies overlap rejection,
then checks exact bytes and terminal credit. The older high-offset growth test
only raised logical capacity after the storage redesign and no longer proved
physical remapping at that boundary.

Full stream race tests passed (1.236s), and the bounded model fuzz campaign passed
14,878 executions (31.221s). Added explicit empty/partial-reset seeds afterward;
the final focused race selection and scoped vet passed. No production code changed
in this checkpoint, so root and independent interop suites were not rerun.
Overall conformance, security and performance completion remains unproven.

## Latest: 2026-09-29 receive storage grows with buffered span

Receive now separates advertised capacity from allocated ring size. Small
in-order input starts with at most 4 KiB data plus a 512-byte bitmap; larger spans
expand geometrically, bounded by receive capacity. Expansion remaps occupied bits
relative to the consumed offset. Credit-only growth avoids payload copying;
partial-reset suffix trimming precedes allocation. Far-ahead data still needs
span-sized storage, so fully sparse receive storage remains open work.

Deterministic regressions cover wraparound, overlap conflicts after expansion,
credit growth with unread data, partial-reset tail removal and odd window sizes.
The existing high-offset growth test now checks the actual physical ring size
rather than assuming every credit increase reallocates it.

Validation: full root race suite passed (root 7.454s, HTTP/3 1.378s, stream 1.213s,
transport 40.119s); default independent interop race passed (72.544s). Both vet
checks passed. FuzzReceiveGrowth passed 1,720,928 executions (15.315s), and
FuzzReceive passed 1,692,827 (16.271s).

Isolated allocation medians: default-window first-byte allocation falls from
589,995 B to 4,776 B (99.19% reduction), still four allocations. Matched fixed-window
bulk echo shows a tradeoff: 10.174 to 10.234 ms (+0.58%), 1,843,467 to 1,865,247 B
(+1.18%). No throughput improvement is claimed. Final echo remains 1.93x slower
and allocates 2.90x the pinned reference's bytes in this run. Raw results, the
control patch and limitations are in docs/benchmarks/receive-ring.md.
Overall conformance, compatibility and performance completion remains unproven.

## Latest: 2026-09-29 receive-window default alignment

Public zero-value configuration now advertises 512 KiB per-stream and 768 KiB
connection receive credit, matching pinned quic-go v0.63.0's code defaults. Zero
maxima enable growth up to 6 MiB and 15 MiB (or the initial window when larger).
Explicit maxima equal to initial windows retain fixed-window behavior. The
16 MiB stream capacity bound and rejection rather than clipping remain gaps.

Configuration tests verify both initial transport parameters and default growth
ceilings, including initial values larger than the ordinary ceilings. New
independent peer traces verify the default wire credits in both TLS roles and
v1/v2. The existing small-window independent fixture now explicitly fixes maxima;
the historical echo benchmark also pins both server implementations to its
original 64 KiB / 1 MiB fixed windows. Historical performance results are not
measurements of the new defaults.

The change exposes a memory cost: the current circular receive buffer allocates
512 KiB data plus 64 KiB bitmap on the first retained byte, compared with 72 KiB
combined at the old default. Objects and allocator overhead are additional.
README and remaining-work documentation record sparse storage and memory tuning
as open work; aligning advertised defaults is not a performance improvement claim.

Validation: full root race suite passed (root 7.260s, HTTP/3 1.409s, internal
packages cached); full default independent race suite passed (72.790s). After
pinning the fixed fixture and adding default peer-trace assertions, the focused
receive-window interop selection passed (1.735s). Both module vet checks passed.
The overall implementation and validation goal remains incomplete.

## Latest: 2026-09-29 multipath keepalive PTO selection

Connection keepalive now uses one shared path selector for its deadline and
PING enqueue. Among eligible idle paths, the smallest base PTO wins, with path
ID as tie breaker; the selected PTO supplies the saturating 1.5-PTO floor. A slow
retained path cannot extend a faster path's probe interval. Abandoned paths,
missing CIDs, migration/MTU validation, unvalidated nonzero paths and queued work
are excluded. No eligible path still produces spaced attempts without extending
idle lifetime. Congestion and pacing remain enforced by packet output.

Datatracker recheck still identifies multipath draft 21. Section 5.9 leaves this
scheduling policy unspecified; this is connection liveness, not per-path NAT
mapping retention or a claim of independent multipath interoperability.

Focused race tests passed (1.492s). The strengthened selection test also checks
actual scheduler output for ordinary/tie cases; the full root race suite then
passed (root 7.157s, HTTP/3 1.338s, transport 39.993s, others cached). Public tests
include negotiated multipath and primary-path retirement. Root vet passed.
Independent interop was not rerun for this multipath selection change. Broader
conformance, independent multipath and performance work remains incomplete.

## Latest: 2026-09-29 single-path keepalive PTO floor

Single-path keepalive now applies the pinned reference's minimum interval of
1.5 base PTO after capping the requested interval at half the negotiated idle
timeout. Arithmetic saturates at the maximum duration, includes peer ACK delay,
and excludes exponential probe backoff. This is a compatibility policy, not a
new RFC requirement. Multipath retains its existing policy pending a suitable
viable-path PTO selection audit; the API matrix keeps that gap explicit.

The deterministic v1/v2 regression failed before the fix (1 ms scheduled instead
of 637.5 ms). It now checks no early PING, protected exchange and authenticated ACK
rearming, RTT changes and overflow. Independent keepalive cases now include 1 ms
alongside disabled and 10 s configuration, in both TLS roles and QUIC versions.

Validation: focused engine keepalive suite passed (1.346s); full root race suite
passed (root 7.220s, HTTP/3 1.329s, transport 40.047s, other packages cached).
The subsequently strengthened authenticated-ACK test passed (1.263s). Independent
keepalive/migration selection passed (26.348s); root and interop vet passed.
The complete independent suite was not rerun. Overall goal remains incomplete.

## Latest: 2026-09-29 protected partial-reset history audit

Released-stream contradiction tests now negotiate reset support through TLS and
send authenticated v1/v2 packets instead of calling the frame handler directly.
They check changed final sizes/error codes in both reset forms, data beyond final
size and contradictory FIN, including the reported offending frame type.

A new eviction regression completes and releases 1,025 actual unidirectional
streams in each TLS receiving role and QUIC version. It checks the 1,024-entry
history bound and exact consumed/stream-count credit. Late input for the evicted
ID, including contradictory historical values, cannot recreate stream objects or
credit; the next retained ID still rejects a changed reset code. This is
own-engine protected-packet coverage, not independent-peer evidence.

The clause-level map now explains the bounded-retention interpretation using
RFC 9000 section 4.5 and reliable-reset draft 11 section 5.2. It distinguishes
mandatory closure on a detected contradiction from retention of all historical
values. It also records the still-unimplemented late full-data/FIN ACK alternative,
bidi-history and adversarial longevity gaps, and corrects the remembered-state
map to use EarlyTransport.Supports with partial-reset capability memory.

Validation: focused protected-history race tests passed (1.535s); full stream and
transport race suites passed (1.332s and 40.059s); scoped vet passed. No production
behavior changed in this checkpoint, and independent interop was not rerun.
The overall conformance, compatibility and performance goal remains incomplete.

## Latest: 2026-09-29 bounded public write batching and measured improvement

Public SendStream.Write now submits up to 32 KiB per serialized-owner operation
instead of one packet-sized fragment per command. It yields between bounded turns
and reports partial progress before waiting on blocked flow control. Existing
partial-commit deadline/retry tests verify offsets and exact payloads, and focused
cancellation/early/partial-reset interop checks passed.

Initial batching reduced command allocations but increased payload-buffer churn.
The cache now allows 32 entries while maintaining the prior 32 KiB retained
payload-capacity ceiling through explicit byte accounting. Taking buffers returns
capacity, teardown clears accounting, and oversized buffers remain excluded.
Unit tests cover byte/entry limits and reclaimed budget; the ownership fuzzer
checks exact capacity sums and aliasing in addition to delivery ownership. The
pointer table can retain more entries, so this is not a total-memory parity claim.

Final matched 100x/count=3 echo medians: the single-fragment/16-entry control took
11.960 ms, 1,860,022 B and 43,126 allocations per exchange; final batching/cache
took 11.083 ms, 1,861,372 B and 41,640 allocations. That is about 7.3% lower latency,
3.4% fewer allocations and effectively unchanged bytes (+0.07%). Upstream v0.63.0
in the final run took 6.034 ms, so ours remains about 1.84x slower and allocates
about 2.84x as many bytes. No general performance superiority is claimed.

The initial fresh baseline and reference were both markedly slower; repeated
controls prevented attributing that shared timing change to our code. An 8 KiB
candidate did not improve the tradeoff and was not selected. Raw runs, diagnostic
profiles (before the final cache fix), final control overlay patch and reproduction
instructions are retained in docs/benchmarks/stream-echo.md. Benchmarks did not
run concurrently with tests or fuzzing; only separate profiling runs were instrumented.

Validation: focused root race passed 7.214s and targeted interop 2.070s before the
cache refinement; focused final cache/reset transport race passed 1.754s.
FuzzSendBufferOwnership passed 736,083 executions in 16.312s. Final full root race
passed (root 7.499s, HTTP/3 1.330s, transport 40.440s; unchanged packages cached),
full default interop race passed 71.857s, and both vet checks passed.

Next: continue normative/combined-extension audit and profile remaining runtime,
packet and allocation overhead. WAN/tail/multiconnection/fairness and cross-platform
performance still need evidence. The overall goal remains active and incomplete.

## Latest: 2026-09-29 negotiated partial resets over surviving multipath

Added a public UDP integration test negotiating multipath and partial-reset support
through TLS for v1/v2. A distinct relay carries path one, validated through actual
network traffic, before the client retires path zero through AbandonPath. Streams
then send a 32 KiB reliable prefix followed by partial reset. Cases cover both
stream directions and optional relay packet loss; bidi streams also send a reliable
partial reset in the reverse direction. Assertions check both negotiated flags,
exact prefix bytes, reset identities, stream retirement, retained WaitAcknowledged
errors and relay traffic/drop counters.

The first fixture called server.AbandonPath immediately after client.OpenPath;
some runs correctly rejected this because server-side validation had not yet
completed. The final fixture uses the client's public abandonment and lets the
server process PATH_ABANDON normally. No production change or delay was needed.
The old synthetic primary-disposal test remains useful for post-collection state;
the new public test proves negotiated behavior after retirement, not that exact
garbage-collection timing. This is own-peer integration, not independent multipath
extension interoperability or a full combined-extension audit.

Validation: focused race passed three repetitions (1.655s); full root race passed
(root 7.315s, unchanged packages cached), and root vet passed. Interop source and
production code were unchanged, so prior independent suite evidence remains
applicable without an identical rerun. No performance claim is made.

Next: remaining normative/state-machine audit, independent combined-extension
coverage and refreshed performance measurements. The overall goal remains active
and incomplete; docs/REMAINING.md now reflects the new partial-reset evidence.

## Latest: 2026-09-29 independent early-reset receiver and selected policy

Added live public listener tests receiving partial resets from unmodified quic-go
v0.63.0 in v1/v2, with both uni/bidirectional streams. A selected server policy
issues an early-enabled ticket with receive support, then accepts, rejects early
data, or withdraws receive support on resumption. Both endpoints' TLS resumption,
early acceptance and asymmetric selected capability flags are checked.

In accepted no-loss cases, the reference server-flight reader remains gated until
our listener consumes the complete prefix and remote reset. Both observations
occur before client Finished. Rejected cases explicitly recover the reference
client and deliver only new bytes on the reused stream ID. Bidirectional cases
terminate the reverse direction and all cases verify public stream release.

The loss fixture strips one 0-RTT packet addressed to the first Retry response's
CID while preserving any coalesced Initial. It records the first Retry because
repeated Initials can elicit later replies that the client ignores. The test
asserts that the targeted post-Retry packet was dropped. An initial fixture that
could drop pre-Retry traffic was tightened; a subsequent fixture overwrote the
accepted Retry CID and correctly failed its drop assertion. Those runs are not
loss validation evidence. Holding the entire server TLS flight during actual loss
then caused 5-second read timeouts (61.083s across three repetitions): reference
application-data PTO probes wait for handshake confirmation, as confirmed in its
sent-packet handler. The final loss case allows handshake progress before reading,
then verifies recovery and complete prefix/reset delivery. The strict pre-Finished
assertion remains in the no-loss case. This is not broad impairment coverage.

No production change was needed. Final focused race tests passed three repetitions
(2.409s), plus the final explicit client DidResume assertion passed a focused run
(1.731s). Full default interop race passed 72.299s; that run preceded the last
assertion-only addition, subsequently covered by the focused run. Interop vet
passed. Root code/tests were unchanged and the prior root race evidence remains
applicable without another identical run.

Next: broader loss/reordering, concurrent selected connections and combined
extension/path validation, plus the remaining full-data/FIN completion audit.
The broad independent implementation goal remains active and incomplete.

## Latest: 2026-09-29 public early partial-reset lifecycle and packet loss

Added live public client tests against unmodified quic-go v0.63.0 for v1/v2.
Each obtains an early-enabled ticket, resumes while the reference TLS callback is
held, writes a nonzero reliable prefix, marks its boundary and cancels before the
callback is released. Tests check remembered asymmetric capability flags and
ordinary TLS resumption separately from early acceptance.

Accepted early streams deliver the entire prefix and original remote reset code,
retire at the sender, and retain the local reset in WaitAcknowledged. A loss case
drops the first early packet containing prefix data, requiring recovery of those
bytes after cancellation. The fixture asserts that it actually dropped a packet.
This single-packet case is not broad impaired-path or reordering validation.

Policy rejection and capability withdrawal verify the public rejection gate and
explicit NextConnection recovery. The fresh stream reuses the old numeric ID;
stale SetReliableBoundary and CancelWrite cannot affect it, and stale Write,
Close, Release and WaitAcknowledged report Err0RTTRejected. Post-handshake remote
capability reflects withdrawal, and the fresh stream delivers only its new bytes.

No production changes were required. The final focused race test passed three
repetitions (1.597s), full default interop race passed 71.843s, and interop vet
passed. Root production/tests were unchanged, so the prior full root race result
remains applicable; it was not rerun merely for this new interop fixture.

Next: independent receiver-role early partial resets, bidirectional early streams,
selected server policy and broader loss/reordering/combined-extension coverage.
The overall implementation and validation goal remains active and incomplete.

## Latest: 2026-09-29 public partial-reset reception and capability state

Config.EnableStreamResetPartialDelivery now controls local receive advertisement.
The engine construction gate was removed after sender, receive state, ticket
memory and early sending integration. ConnectionState exposes independent Local
and Remote SupportsStreamResetPartialDelivery flags, populated from engine owner
state (including remembered remote support during early sending). Configuration
selection already forwards normalized parameters, so GetConfigForClient can enable
or disable advertisement independently of listener defaults.

Engine early-reset tests now construct normally configured servers and exchange
real TLS parameters/tickets; the test-only TLS replacement is removed. The ordinary
sender fixture also negotiates support through TLS. The synthetic multipath
capability fixture remains limited evidence for that combined-extension case.
The old disabled-advertisement test now validates negotiated support in v1/v2.

New live receiver interop against unmodified quic-go v0.63.0 covers v1/v2, both TLS
roles, uni/bidirectional streams, and enabled/disabled reception. Server cases
select the opposite receive policy from the listener default, checking both
overrides. Tests verify asymmetric flags on both endpoints, complete reliable
prefix delivery when enabled, reset identity, optional pre-reset suffix bytes,
stream release and capability snapshots after shutdown. Three repetitions passed
2.263s. One initial file-write/test command used an incorrect working-directory
path and matched no tests; it is not counted as validation.

Validation: focused negotiated engine race tests passed 1.848s; full root race
passed (root 7.381s, HTTP/3 1.543s, internal HTTP/3 1.319s, transport 41.672s;
unchanged packages cached). Full default interop race passed 71.885s, and both vet
checks passed. No new performance claim is made.

Next: public early boundary cancellation/rejection and stale handles, independent
impaired paths and broader combined-extension/state-machine validation. Basic
public receive enablement is now present, but full extension validation and the
overall independent implementation goal remain incomplete and active.

## Latest: 2026-09-29 public reliable boundaries and reserved reset admission

Public SendStream.SetReliableBoundary now snapshots committed bytes on the owner.
CancelWrite selects partial reset for a supported nonzero boundary, falling back
to ordinary reset otherwise. Boundary calls advance until cancellation; repeated
cancellation and later boundary calls preserve the original reset. STOP_SENDING
continues to use ordinary cancellation. Server application readiness permits the
operation before server TLS completion, matching early-listener send readiness.

WriteStream reserves one 32-byte reset control allocation for capable streams and
all early send streams. Early reservation covers newly enabled server support
absent from the ticket. Reservations reduce all shared queue admission budgets but
do not represent outstanding packets. They are consumed at reset enqueue and
released on normal FIN acknowledgement, ordinary/peer cancellation, stream release,
early rejection and teardown. Full public boundaries fall between committed STREAM
items, so their cancellation does not require arbitrary-offset trim metadata.
Direct engine reductions still require bounded metadata admission and may fail.

Tests cover full-budget cancellation without discarded suffix funding, advancing
boundaries, repeated calls, ordinary/peer reset and FIN cleanup, support newly
enabled after early writes, and a synthetic application-ready server gate. Live
public sender interop against quic-go v0.63.0 passes v1/v2 and both TLS roles with
supported and unsupported receivers. It checks reliable-prefix delivery, original
reset identity, post-reset writes, retirement and WaitAcknowledged after release.
Optional suffix bytes already delivered before reset are correctly permitted.
Three focused interop repetitions passed 1.473s.

Validation: final focused early/boundary/partial-reset race passed 2.510s; final
full root race passed (root 7.374s, HTTP/3 1.373s, transport 41.394s; unchanged
packages cached). Full default interop race passed 71.909s and both vet checks
passed. No benchmark or performance claim accompanies the admission change.

Local receive advertisement and public receive configuration/capability state
remain disabled. Next: negotiated receive enablement and selected configuration,
public early boundary/rejection lifecycle and independent impaired-path coverage.
The broad independent implementation goal remains active and incomplete.

## Latest: 2026-09-29 early partial-reset sending and rejection cleanup

CancelWriteAt now consults remembered server capability while sending 0-RTT;
completed handshakes use current peer parameters. The early packet writer enables
RESET_STREAM_AT on the same basis. Public boundary/cancel integration and local
receive advertisement remain disabled.

New protected engine tests cover v1/v2, both stream directions, actual early
prefix delivery, reordering/replay, lost early flights recovered in 1-RTT,
decreasing boundaries, missing support, policy rejection and server withdrawal.
Test-only server TLS replacement bypasses the advertisement gate but exchanges
real TLS parameters and tickets. It is not public enablement evidence.

Rejection testing exposed a real accounting leak: trimmed leaves have no separate
payload/metadata charge, so subtracting leaf bytes retained 6,700 bytes of a shared
logical item's charge in the first reproducer. rejectEarlyStreams now invokes the
existing retireSendItem operation, releasing each logical group once. Tests include
both transmitted and still-queued trimmed items at rejection, stream-ID reuse,
and replay of rejected ciphertext against the fresh stream generation.

Independent live v1/v2 interop against unmodified quic-go v0.63.0 proves zero-prefix
RESET_STREAM_AT sending before TLS completion. The reference TLS callback is held
until the early packet arrives; no STREAM data remains queued. Accepted resets
produce the correct remote error and eventual sender retirement. Policy rejection
and withdrawn capability preserve ordinary resumption and permit clean new streams.
This does not prove independent nonzero-prefix early delivery or impaired paths.

Validation: focused early stream/datagram/reset race checks passed (1.941s), then
expanded partial-reset tests passed three repetitions (2.098s). The new independent
interop test passed (1.403s). One earlier command ran in the root module and matched
no tests; it is not validation evidence. Full root race passed (root 7.006s, HTTP/3
1.326s, transport 39.837s; unchanged packages cached), full default interop race
passed 71.641s, and both vet checks passed. The final queued/replay test additions
were verified by the subsequent focused repetitions. No performance claim is made.

Next: public SetReliableBoundary and cancellation policy, including capacity for
reset control output at a full send queue, followed by negotiated receive
advertisement and public capability state/configuration. The goal remains active.

## Latest: 2026-09-28 remembered partial-reset capability for 0-RTT

EarlyTransport now snapshots ResetStreamAt and encodes private transport schema 3.
Schema 2 remains readable with support false; malformed flags, truncation and
trailing data are rejected. The unchanged embedded credit record remains schema
1. EarlyTransport.Supports combines validated credit compatibility with the rule
that previously advertised reset support cannot be withdrawn when accepting
0-RTT. Both the server acceptance gate and client handshake-completion check use
this method. Authenticated server tickets and client-local cached state both
retain the capability; ordinary TLS resumption remains possible after rejection.

TLS-only v1/v2 tests cover preserved support, newly enabled support, withdrawn
support and a misbehaving server accepting early data while withdrawing support.
They assert remembered values on both endpoints. Legacy schema-2 server tickets
still resume and accept early data with reset support absent. Record round-trip,
malformed-input, snapshot and compatibility coverage includes the new flag.
These tests do not exercise RESET_STREAM_AT delivery in early packets: engine
CancelWriteAt still requires completed TLS, and local receive advertisement and
public APIs remain gated. No extension parity or performance claim is made.

Validation: focused wire/handshake race passed (final handshake 1.959s); full root
race passed (root 7.244s, HTTP/3 1.322s, transport 41.559s, unchanged packages
cached). Additional test-only assertions and the legacy-ticket case passed the
subsequent focused checks. Full default interop race passed 73.236s. Both vet
checks passed. FuzzEarlyTransport passed 7,108,211 executions in 15.283s with
current enabled/disabled and legacy-schema seeds. No benchmark ran concurrently.

Next: actual early reset sending and rejection/handle lifecycle, followed by
negotiated receive enablement and public boundary/cancellation integration.
The overall independent implementation goal remains active and incomplete.

## Latest: 2026-09-28 engine partial-reset sending and independent interop

Connected the prepared transaction to engine CancelWriteAt after completed TLS.
It validates the peer's advertised reception capability, stream bounds, path
viability and budget before mutating state. Superseded RESET_STREAM_AT controls
are deduplicated and retired in the same budget calculation, allowing their
charge to fund a replacement even at capacity. Repeated identical requests do
not queue another control. Reductions preserve the first application error and
final size. A read-only Send.PrepareResetAt supports the preflight.

Logical STREAM completion now reports partial-prefix coverage separately from
full data/FIN delivery. The required prefix is proven covered only when all
retained logical items are covered, so higher-offset ACKs cannot skip holes.
The current reset and prefix may be acknowledged in either order. Ordinary
CancelWrite/STOP_SENDING removes the prefix obligation, preserves the reset code,
retires partial controls and requires the ordinary reset ACK. Path send validators
use the peer's receive capability.

Protected engine tests pass for v1/v2 with loss/reordering, exact prefix reads,
flow-credit invariants, and sending on a surviving multipath path after primary
path disposal. Those engine fixtures inject the receive capability after TLS;
they are not negotiation evidence. Separate live interop tests use actual TLS
advertisement from unmodified quic-go v0.63.0 clients: independent engine servers
send zero, 256-byte and 1,500-byte prefixes from 5,000-byte streams. The peer's
capability flags confirm asymmetric support, reads verify exact bytes plus remote
reset code, and the sender retires after ACK. Three focused repetitions passed
(1.417s). No independent impaired-path or reverse-role extension claim is made.

A FIN-then-reset delivery-reporting bug was also fixed. Reset errors now survive
both the earlier FIN context notification and subsequent engine storage release.
Automatic release can report receive and send-delivery errors separately; public
WaitAcknowledged retains the reset instead of reporting discarded bytes as
successfully delivered. Existing one-argument release observers keep their read
result contract. Early rejection remains dominant, and retained completed reads
carry only the small reset-error snapshot. Tests cover ordinary local/remote reset
after FIN and public WaitAcknowledged after automatic release.

Final focused regression race checks passed (root 1.535s, internal transport
1.678s). Final full root race passed (root 7.420s, HTTP/3 1.414s, internal HTTP/3
1.550s, transport 40.108s; unchanged packages include cached results). Final full
default interop race passed (71.449s), and both vet checks passed.

Local advertisement and the public SetReliableBoundary/partial-cancel API remain
unavailable; 0-RTT capability memory, admission reservation policy and broader
combined-extension/independent peer coverage remain. The transport conservatively
waits for current-reset ACK rather than inferring full FIN delivery from reduced
obligations. No performance claim is made. The full project goal remains active.


## Latest: 2026-09-28 stream-wide partial-reset budget transaction

Separated send-item retention into read-only preparation and commit. Added a
stream-wide transaction that deduplicates logical roots across queued and sent
mappings on base/multipath spaces, prepares every reduction, and checks net
metadata charges plus the pending reset frame's budget before changing live
state. Retiring abandoned tails can fund the retained prefix. Whole data-only
items before the boundary retain their existing delivery/ACK path without an
extra ledger. Failed admission or invalid reductions leave every item unchanged.

Commit releases satisfied obligations first, applies retained-prefix changes,
then rewrites queues once rather than scanning all queues for every item. Retired
packet mappings are pruned so released payload does not accumulate; live old
wire images remain available for late ACKs. Returned completion records describe
reduced obligations, not full STREAM/FIN delivery. Repeated commit is a no-op.
Preparation/commit requires one serialized owner operation with no intervening
write, ACK or path event.

Tests cover admission failure without partial retirement, reset-frame budget,
shared roots across paths, funding prefix metadata from discarded tails at full
capacity, preserving unrelated/full-prefix items, invalid boundary increases,
exact charges, live versus retired packet mappings and repeated commit.
Focused race passed (1.287s). The existing item mutation/ACK/loss/split fuzzer
passed 1,516,803 executions in 15.405s after the final pruning change. Initial
full root race passed (root 8.141s, HTTP/3 1.396s, internal HTTP/3 1.313s,
transport 44.430s); the final root run passed again after pruning (transport
43.753s, other packages cached). Full default interop race passed (72.498s).
Vet passed in both modules, with root vet repeated after pruning.

The transaction is not yet invoked by a connection-level partial reset. Stream
state, reset-frame/path selection, prefix completion bookkeeping and control-frame
supersession still need integration before public/early-data negotiation. The
reference SetReliableBoundary API marks/increases a boundary before CancelWrite;
this differs from decreasing wire obligations after reset and must be preserved
in API integration. No performance or complete extension-support claim is made.
Advertisement remains disabled and the full project goal stays active.


## Latest: 2026-09-28 partial-reset send-item retention

Added retainStreamItemPrefix to connect reduced delivery obligations with shared
STREAM item ownership. Original sent-frame images remain unchanged for late
ACKs. Replacement leaves borrow only required bytes, remove FIN, and are used
by loss traversal and queue rewriting across base/multipath packet-number spaces.
Further MTU splits and reliable-prefix reductions preserve the same logical
ownership. ACK of an old full packet or a trimmed retransmission can release the
shared charge exactly once. Unary replacement branches now have safe ACK traversal.

Metadata admission uses existing split reservations first, then available send
capacity, and limits historical nodes to 128 per logical item. Invalid increases,
capacity exhaustion and failed completion callbacks leave that item's state
unchanged. Zero-prefix retirement needs no additional retained metadata, even at
full capacity. Tests cover cross-path loss, stale/repeated ACKs, reductions after
splitting, satisfied-prefix retirement, callback failure, full-budget reservation
use and history exhaustion. The operation is not yet called from a stream-wide
partial-reset API: complete preflight/commit, prefix progress, reset-frame queuing,
normal-write reservation policy and public/early-data negotiation remain open.

The new mutation/ACK/loss/split model fuzzer passed 1,436,949 executions in
15.705s. Final focused race checks, including full-budget reservation use and
model seeds, passed (1.334s). Full root race passed (root 7.579s, HTTP/3 1.397s,
internal HTTP/3 1.352s, transport 39.896s; unchanged packages include cached
results). The reservation-specific test was added after the full run and is
covered by the final focused run. Full default interop race passed (71.734s),
and vet passed in both modules. No performance improvement or negotiated partial
reset support is claimed. Advertisement stays disabled and the full goal active.


## Latest: 2026-09-28 Retry-stall diagnosis and partial delivery ledger

Added bounded failure-only handshake packet traces, reference PacketDropped
capture, endpoint state reporting, and guaranteed reference-client cleanup in
the shared-endpoint fixture. Repeated runs reproduced Initial/Retry loops in
both versions, before any TLS server flight. A reference debug run isolated a
shared-endpoint client at `[::]:60381` receiving no Retry while its sibling
completed. Read-only socket inspection found an unrelated IPv4 cloudflared
socket at `*:60381`, plus `*:53850`, matching an earlier failed v2 client port.

A standalone socket probe demonstrated this host's behavior: a second IPv6
wildcard socket can bind the occupied IPv4 port, and IPv4 replies go only to the
IPv4 socket. Explicit udp4 binding rejects the conflicting port. The affected
IPv4 handshake-loss/shared-endpoint fixtures now create udp4 sockets and use
upstream Dial with unchanged deadlines and loss assertions. No unrelated process
was modified. Independent production Dial already selects the destination's
family. Fifty repetitions of both complete matrices passed after the fixture
change (30.866s). Diagnostic sources and the relevant failing reference log are
in docs/diagnostics. Other historical stalls remain unclassified. A later full
run timed out at
InitialPacketSizeUpstream's still-wildcard Dial (75.439s, untraced). All direct
reference DialAddr fixture calls now use a helper that resolves the destination,
binds that UDP family, preserves ServerName inference and invokes upstream Dial.
This also changes stream-echo benchmark setup for both variants; historical
measurements remain labeled, and no performance comparison was rerun.

Extended the existing STREAM delivery ledger with a reducible required prefix
and removable FIN obligation, preserving original ACK-validation bounds. Late
full-packet ACKs and trimmed retransmission ACKs both work; suffix ACKs cannot
fill prefix gaps. Completion callback errors leave reductions unapplied.
Regression tests cover byte/bitmap boundaries, overlapping/late ACKs, zero/FIN-only
prefixes and reductions. A packet-mapping test verifies shared ownership releases
once whether the original or trimmed retransmission is acknowledged. The ledger
operation is not yet wired into production partial-reset queue rewriting or the
connection API; sender ACK-prefix tracking and retirement remain next.

New model fuzzing passed 1,889,369 cases in 16.088s. Full root race passed
(root 6.994s, HTTP/3 1.476s, transport 41.118s; unchanged packages include cached
results). Final focused ledger/packet-ownership/seed race checks passed (1.280s).
Both modules' vet checks passed, with transport rechecked after the additional
packet-ownership test. Final full default interop race passed with the shared
address-family helper (71.261s), and interop vet passed again. This does not
classify all historical stalls or close the separately documented strict interop
failures. No performance claim is made. The full goal remains active.


## Latest: 2026-09-28 partial reset receive transport integration

Connected RESET_STREAM_AT to transport receive dispatch and supplied the local
capability to base/multipath/closing packet validators. Negotiation remains
blocked by NewConn while sender recovery and early-data integration are missing.
A nonconsuming terminal query now reports a completed partial reset correctly
while its reliable prefix remains unread. Cancellation preserves its local
cause, retires STOP_SENDING on completion, discards completed unread bytes and
restores connection/stream-count credit. Completed accepted prefixes survive
connection shutdown through the existing retained-read path.

Released partial-reset streams retain final-size/error-code validation in a
FIFO history capped at 1,024 streams. Recent inconsistent late STREAM/reset
frames produce final-size or stream-state errors. After eviction, merged closed
ID ranges still prevent recreation and duplicate credit, but those consistency
checks are no longer available. This bounded policy needs final normative review.
No payload is retained by the history.

New direct-handler tests cover both versions: prefix gaps, nonconsuming terminal
notification, unread data retention, exact-once release/credit, canceled prefix
completion and STOP retirement, late error/final violations, bounded history,
and completed reads across shutdown. Focused race passed (1.479s). Full root
race passed (root 7.368s, HTTP/3 1.326s, internal HTTP/3 1.334s, transport 39.991s;
unchanged packages include cached results). The shutdown test was added after
the full run and passed in the focused run. Vet passed in both modules, followed
by a transport vet check after that additional test.

Two full default interop race attempts failed (75.328s and 75.349s). The first
hit a five-second upstream idle timeout in the v2 independent-server handshake
case dropping server datagram 2; ten exact race reruns passed (1.441s). The second
hit the five-second UDP read deadline in TestSharedServerEndpoint/1; ten exact
reruns passed (1.454s). Ten repetitions of both complete affected test groups
then passed (7.160s). Causes remain unconfirmed, and the full suite is not recorded
as passing for this checkpoint. Both observations are retained in
interop/KNOWN_FAILURES.md with original deadlines and coverage intact.

Next: sender packet ACK ranges, prefix-aware loss/MTU retransmission and shared
payload retirement; then negotiated packet/public API/0-RTT coverage. The
intermittent handshake failures also require packet-level diagnosis. Partial
reset is not publicly enabled and no performance improvement is claimed. The
full project goal remains active.


## Latest: 2026-09-28 partial reset send-state foundation

Added internal Send.ResetAt, contiguous-prefix ACK reporting, exact reset-frame
ACK handling, and retained STREAM-prefix filtering. Reset boundaries cannot
exceed committed bytes or increase after a previous partial reset. The first
application error and final size stay stable. Completion requires both current
reset information and the reliable prefix acknowledged; stale larger-reset ACKs
cannot retire newer smaller resets. Ordinary reset fallback removes the prefix
obligation but requires its own ACK. Full data-plus-FIN acknowledgement remains
an alternative terminal path. Retransmission filtering borrows existing owned
payload, removes abandoned suffix/FIN, and does not change flow credit.

Stream race tests passed (1.313s), covering both ACK orders, stale reset ACKs,
reductions, preserved frame snapshots, prefix trimming, ordinary-reset fallback,
zero-prefix obligations, FIN completion and invalid updates. Packet-level ACK range
tracking, reliable queue/refcount changes, STOP_SENDING and loss integration are
still required; direct state tests do not establish those properties. Local
advertisement remains disabled. No performance claim is made for this change.

Full root race passed (root 7.163s, HTTP/3 1.501s, internal HTTP/3 1.310s,
transport 39.584s; unchanged packages include cached results). Both modules' vet
checks pass. Full default interop race passed (71.414s). Remaining integration
is recorded in PARTIAL_STREAM_RESET.md. The full goal stays active.

## Latest: 2026-09-28 partial reset receive-state foundation

Implemented internal Receive.ResetAt with a pending reliable-prefix obligation.
Reads wait for missing prefix bytes, deliver retained bytes through the current
reliable size, then surface reset and consume the discarded final-size suffix.
Lower reliable sizes win over reordered larger values; ordinary RESET_STREAM
removes the obligation. Partial-reset error-code changes and final-size/flow
violations fail before mutation. Already delivered EOF is preserved. Tail
trimming iterates bounded receive bitmap storage, including circular wrap.

New tests cover gaps, partial reads, receive-window growth, reliable-size
reductions, ordinary-reset interactions, atomic validation/accounting, FIN before
and after delivery, wraparound, late data and terminal resource release. Stream
race tests passed (1.323s); the expanded receive-operation fuzzer passed a
15-second run with 4,791,354 executions (16.278s including setup/termination).
This bounded fuzz run is not a security audit.

Full root race passed (root 8.289s, HTTP/3 1.393s, stream 1.245s, transport
44.996s; unchanged packages include cached results). Both modules' vet checks
pass. Full default interop race passed (71.806s). Wire advertisement remains
gated off: sender recovery, transport dispatch/tombstones, public API and ticket
integration remain. PARTIAL_STREAM_RESET.md tracks those requirements. The full
goal stays active.

## Latest: 2026-09-28 partial stream reset wire foundation

Checked draft-ietf-quic-reliable-stream-reset-11 (2026-09-06) and added its
RESET_STREAM_AT 0x24 frame plus empty reset_stream_at 0x1d transport parameter.
Frame encoding validates sizes/varint bounds before touching output; parsing
rejects reliable sizes above final size with typed frame errors. Packet-context
validation requires local receive capability and application data packet space.
The frame is ack-eliciting/in-flight and non-probing. NewConn explicitly rejects
local advertisement until stream-state integration is implemented.

Tests cover literal encoding and truncation, varint boundaries, output ownership,
malformed reliable size, packet/role/capability matrix, empty/nonempty/duplicate
parameters and nonminimal TP encodings. Wire race passed (1.382s); transport race
passed (39.224s). A 15-second parser fuzz run passed with 5,619,943 executions
(16.273s including setup/termination). This is a bounded parser check, not a
security audit. Both modules' vet checks pass. Full root race passed (root
7.108s, HTTP/3 1.536s, handshake 2.064s, packet 1.624s, stream 1.408s; other
packages include cached results). Full default interop race passed (71.663s).

PARTIAL_STREAM_RESET.md records the remaining receive/send state machines,
negotiation, ticket memory, public API and independent-peer gates. No connection
advertises partial-reset support yet. The full goal stays active.

## Latest: 2026-09-28 reject 0-RTT under selected reduced limits

Added independent v1/v2 resumption gates for selected reductions in connection
receive credit, stream receive credit, and each stream-count limit, with unchanged
acceptance controls. Allow0RTT stays enabled, so rejection must follow ticket
limit compatibility rather than a policy opt-out. Actual early wire packets,
valid-token/no-Retry admission, TLS resumption on both peers, Used0RTT state and
fresh DATAGRAM delivery are checked. The upstream client transitions rejected
streams through NextConnection before a new 4 KiB STREAM transfer. The transfer
exceeds the selected 1 KiB windows and verifies credit-update progress; reduced
stream-count gates also reject an extra stream.

The initial three-repetition race matrix passed (1.898s); after adding fresh
STREAM and count enforcement checks, three repetitions passed (1.819s). No
production code change was required. Interop vet and the full default interop
race suite pass (71.410s). API_PARITY.md records the public-control
coverage and remaining per-direction/CID/DATAGRAM-size/combined-extension gaps.
The full goal stays active.

## Latest: 2026-09-28 selected 0-RTT policy and TLS resumption

Extended the independent early-DATAGRAM gate with GetConfigForClient selection.
A single EarlyListener issues the seed ticket with early data enabled, then
accepts/rejects the resumed connection's early data through selected QUIC policy
while listener defaults oppose it. Selected DATAGRAM reception also overrides a
disabled listener default. Valid NEW_TOKEN evidence avoids Retry; the fixture
observes an actual 0-RTT packet and holds the server flight before client Finished.

V1/v2 acceptance/rejection gates verify early exposure/delivery, pre-Finished
server response, both sides' Used0RTT state, successful TLS resumption even when
early data is rejected, and fresh DATAGRAM delivery without rejected data leakage.
Three race repetitions passed (1.516s). Existing TLS-ticket rejection variants
remain covered separately. Production code is unchanged in this checkpoint.

Interop vet and the full default interop race suite pass (71.160s).
Selected reduced-limit combinations and early data plus multipath remain open.
The full goal stays active.

## Latest: 2026-09-28 isolate selected policies and integrate HTTP DATAGRAMs

Added a same-listener v1/v2 independent-peer test: three live connections retain
different stream limits and DATAGRAM policies, a fourth source is refused, then
the earlier connections continue STREAM/DATAGRAM transfers under their original
settings. Five race repetitions passed (1.601s).

Listener.DatagramsEnabled now reports default or callback-selected capability,
allowing HTTP/3 startup when GetConfigForClient alone enables DATAGRAM reception.
Conn.DatagramsEnabled continues to report the actual selected policy and
ServeQUICConn continues to reject connections with reception disabled. Added
selector-enabled variants of the v1/v2 IPv4/IPv6 HTTP DATAGRAM echo/FIN-lifecycle
test. Static and dynamic positive gates plus static-disabled preflight passed
three race repetitions (1.592s); a new selected-disabled connection test passed
three race repetitions (1.351s). API_PARITY.md documents the listener capability
semantics and remaining callback/combined-extension audits.

Full root race passed (root 7.289s, HTTP/3 1.310s; unchanged packages include
cached results). Both modules' vet checks pass. Full default interop race passed
(71.451s). The full goal remains active.

## Latest: 2026-09-28 public GetConfigForClient and selected handle policy

Added GetConfigForClient and ClientInfo (RemoteAddr/AddrVerified). Selection
normalizes per-connection settings after token validation; nil selects defaults,
errors/invalid settings refuse admission, and nested selectors are ignored.
Immutable adapter metadata carries selected DATAGRAM reception and window-growth
callbacks into accepted Conn handles. Listener-wide version negotiation,
capacity and registered preferred sockets stay fixed; enabling multipath requires
listener opt-in. Callback execution remains synchronous on the listener owner.

Independent v0.63.0 client tests cover v1/v2 selection, nil defaults, stream
limits, advertised DATAGRAM capability, actual DATAGRAM delivery, disabled
reception, verified source information and error/invalid-config refusal. Initial
fixture failures exposed wildcard local-bind reporting and repeated stateless
rejection; assertions now compare actual source IP/port appropriately and require
one selection only for admitted connections. Three race repetitions pass
(1.398s). The public receive-window policy test exercises a selected server policy
and the exact accepted Conn handle (1.305s).

Full root race passed (root 7.123s, HTTP/3 1.532s, internal HTTP/3 1.320s,
transport 39.574s; unchanged packages include cached results). Both modules' vet
checks pass. Full default interop race passed (71.246s).

API_PARITY.md records remaining mixed-policy, combined-extension, version and
callback scheduling audits. HTTP/3's listener-level DATAGRAM preflight still
requires enabling the listener default even if the selector would enable it.
This feature does not establish complete configuration or protocol parity. The
full goal stays active.

## Latest: 2026-09-28 admission configuration selection groundwork

Added an internal ConfigureClient selection point after authenticated Initial
parsing and address-token validation, before connection/TLS allocation. It
receives a shallow configuration template and verified source tuple. Admission
retains routing/version identity and token policy, and clears the selector from
connection configuration. Selection errors retain their local diagnostic while
sending a bounded, fixed CONNECTION_REFUSED Initial without that diagnostic.
The existing INVALID_TOKEN response shares the close encoder.

V1/v2 Retry and compatible-switching tests now exercise selection and its
advertised receive credit; NEW_TOKEN tests cover valid and changed-port evidence
and invalid/expired/wrong-IP/version/retired/tampered token fallback. Refusal
coverage verifies no connection allocation, deterministic repeated response,
response size, local error identity and remote transport code. Admission race
tests passed three repetitions (1.981s); the final advertised-credit assertion
passed three focused repetitions (1.414s).

Full root race passed (root 7.084s, HTTP/3 1.316s, internal HTTP/3 1.491s,
transport 39.573s; unchanged packages include cached results). Both modules' vet
checks pass. Full default interop race passed (70.947s).

Public GetConfigForClient remains unfinished. Next work must normalize selected
public settings, propagate per-connection datagram/window callback policy into
accepted handles, resolve listener-wide socket/version interactions and validate
selection/refusal with an independent peer. API_PARITY.md keeps that distinction
explicit. The full goal stays active.

## Latest: 2026-09-28 add public Config.Clone

Added Config.Clone with the v0.63.0 shallow-copy contract. Scalars and slice
headers are independent; referenced storage, pointers, callbacks and token
stores remain shared. Cloning applies no defaults or validation, requires a
non-nil receiver, and requires the caller to avoid concurrent mutation. Local
configuration extensions follow the same copy rule. HTTP/3 now uses the method
for its existing per-dial datagram override. API_PARITY.md records the contract.

The independent/reference comparison checks scalar mutation, Versions backing
storage sharing, and slice reassignment; its race run passed (1.308s). Root and
HTTP/3 race suites passed (7.069s / 1.486s). Both modules' vet checks pass. This
checkpoint adds no transport algorithm changes and does not rerun the full
transport/interop matrix from the preceding checkpoint. GetConfigForClient,
partial stream reset, observability and broader parity/validation work remain;
the full goal stays active.

## Latest: 2026-09-28 end handshake timeouts at TLS completion

Aligned the absolute handshake timeout's lifecycle with the v0.63.0 reference:
both local handshake limits stop at TLS completion. A delayed HANDSHAKE_DONE no
longer expires an already-complete TLS handshake. Confirmation remains separate;
handshake keys, recovery PTO and ordinary negotiated idle expiry retain their
existing behavior. Public and engine configuration comments and API_PARITY.md
now describe this boundary without claiming complete timer-policy parity.

New v1/v2 engine tests complete real TLS and withhold the server confirmation
flight. They failed before the fix, then passed with assertions for surviving
the old deadline, PTO output, late confirmation/key discard, and idle expiry.
Focused handshake tests passed three race repetitions (1.482s). A live v0.63.0
peer test holds short-header packets for 750 ms after server TLS completion,
beyond the independent client's 500 ms absolute limit. Both versions dial
successfully after release; three race repetitions passed (6.066s).

Full root race passed (root 7.074s, HTTP/3 1.545s, transport 40.047s; unchanged
packages include cached results). Full default interop race passed (71.063s).
Both modules' vet checks pass. Known opt-in interop failures, remaining API
gaps, and broader protocol/performance validation remain. The full goal stays
active.

## Latest: 2026-09-28 emit ordered ACK events directly

Bounded ACK lookup traverses wire ranges in reverse, emitting acknowledged
packets in ascending order without a separate sort. Wide-range ledger scans
retain sorting; lookup bounds, RTT/ECN processing and loss detection are
unchanged. Added mixed new/duplicate disjoint-range regression coverage with
ACK/loss ordering, flight accounting, RTT and scan fallback assertions.
Recovery race tests pass (1.377s).

Short send/ACK-cycle medians improve about 5.3%/4.1% for batches 64/256 versus
the preceding typed-sort checkpoint; smaller batches are slower and variable.
Allocations are unchanged. Isolated echo medians are 11.029 ms, 1,977,878 B and
43,151 allocations versus upstream v0.63.0 at 5.364 ms, 703,143 B and 19,577
allocations. This is effectively unchanged echo behavior, still about 2.06x
slower than the reference. Raw measurements and limitations are recorded in
docs/benchmarks/stream-echo.md.

Full root race passed (root 7.208s, HTTP/3 1.489s, transport 39.803s; unchanged
packages include cached results); full default interop race passed (69.367s).
Both modules' vet checks pass. Known opt-in interop failures and broader
protocol/API/performance gaps remain; the full goal stays active.

## Latest: 2026-09-28 remove reflection allocations from recovery sorting

Replaced recovery event sort.Slice with slices.SortFunc/cmp.Compare, preserving
ascending unsigned packet-number order. Recovery race tests passed (1.254s).
Send/ACK-cycle measurements remove one allocation/24 B for single-packet ACKs
and three allocations/136 B for larger tested batches. Short sequential 64/256
batch timings were about 2.3%/1.7% higher, so this is an allocation improvement,
not a claim of universal CPU improvement.

The isolated 100-operation, three-sample echo comparison has independent medians
11.000 ms, 1,972,595 B and 43,150 allocations, versus upstream v0.63.0 at
5.352 ms, 704,753 B and 19,593 allocations. Independent bytes/allocations decrease
about 1.5%/2.3% from the preceding checkpoint. Raw before/after and echo records
are saved under docs/benchmarks. Full root race passed (root 7.183s, HTTP/3
1.303s, transport 39.584s; unchanged packages include cached results); full
interop race passed (69.243s). Both modules' vet checks pass. Broader performance
and complete protocol/API coverage remain unfinished. Full goal stays active.

## Latest: 2026-09-28 borrow ACK ranges for immediate serialization

Added a private receive-state ACK view for serialized base/multipath packet
builders. Ranges and ECN counters are borrowed only until immediate encoding;
Received.Ack retains owned snapshot semantics. Existing ACK/recovery race tests
passed (4.339s); new mutation/serialization/snapshot regression tests passed three
race repetitions (1.282s), including ECN-present and absent cases.

Isolated encoding microbenchmarks measure zero allocations for borrowed ACKs
versus 88 B/two allocations for a four-range ECN snapshot. Isolated 100-operation,
three-sample echo medians are 11.307/5.432 ms, 2,002,468/705,890 B and
44,145/19,603 allocations against v0.63.0. Independent bytes drop about 6.0% and
allocations 3.4% relative to the preceding checkpoint. No timing improvement is
established; the same-run reference remains about 2.08x faster. Raw measurements
are saved under docs/benchmarks. Full root race passed (root 7.065s, HTTP/3
1.326s, transport 39.664s; unchanged packages include cached results); full
interop race passed (69.440s). Both modules' vet checks pass. Full goal stays
active.

## Latest: 2026-09-28 reuse base-path MTU write metadata

Inspection found that the UDP owner already reuses its outbound descriptor after
synchronous writes. Extended that lifetime contract to one reusable base-path MTU
metadata slot, reset after write/error handling. Public callers keep owned
metadata and Outbound size is unchanged. Focused MTU/write-error tests passed
(10.790s), then the expanded owned/borrowed v1/v2 STREAM/DATAGRAM fallback matrix
passed three race repetitions (1.399s).

Isolated microbenchmarks measure owned metadata at 64 B/one allocation versus
reused at zero; ordinary envelopes remain 160 B/one allocation. Isolated echo
medians against v0.63.0 are 11.172/5.514 ms, 2,129,722/706,199 B and
45,707/19,610 allocations. Relative to the prior baseline, independent allocations
fall about 1.7% and bytes about 1.4%; no timing win is established. The same-run
reference remains about 2.03x faster. Raw evidence and ownership details are in
docs/benchmarks/stream-echo.md. Full root race passed (root 7.218s, HTTP/3
1.327s, transport 39.628s; unchanged packages include cached results); full
interop race passed (69.494s). Both modules' vet checks pass. Full goal remains
active.

## Latest: 2026-09-28 reject universal inline MTU metadata

Tested embedding base-path MTU error metadata in Outbound. Race-enabled MTU and
write-fallback gates passed (10.787s). An isolated three-sample, 100-operation
stream echo comparison measured about 1.9% fewer allocations and 1.6% fewer bytes;
no timing improvement is established. An earlier overlapped run is preserved
and excluded from timing claims.

A new reusable BenchmarkOutboundEnvelope exposed the tradeoff: packets without
MTU metadata allocate 208 B rather than 160 B, both one allocation (three samples,
100,000 iterations). The 30% envelope-byte increase argues against applying the
layout to every packet. Reverted all three experimental edits byte-for-byte;
production behavior remains unchanged. Raw comparison/envelope records and the
reason for rejection are saved in docs/benchmarks/stream-echo.md. Transport vet
and diff checks pass. No delivered performance improvement is claimed; a
conditional combined allocation is a next candidate. Full goal remains active.

## Latest: 2026-09-28 refresh performance evidence to v0.63.0

Ran the unchanged exact-byte/FIN v1 stream-echo benchmark for 100 operations per
sample, three samples per implementation, without race/profiling or simultaneous
test processes. Median independent/reference exchange times were 11.099/5.360 ms,
allocated bytes 2,160,645/702,596 and allocations 46,503/19,565. The independent
server remains 2.07x slower with 3.08x bytes and 2.38x allocations. This is one
loopback workload, not universal performance or an isolated peer-upgrade effect.

A subsequent 300-operation independent-server CPU/allocation profile passed its
payload checks. Raw outputs, binary profiles, summaries and limitations are saved
under docs/benchmarks/stream-echo-v063*. Profiles include the upstream client and
setup; Darwin syscall/runtime wait samples dominate CPU. Allocation evidence
identifies socket/address handling, ACK/recovery history and per-packet MTU write
metadata among remaining costs. Embedding the latter may remove an allocation
but enlarge all Outbound objects; it needs a measured tradeoff before adoption.
No implementation changed. Performance superiority and broader workload/platform
coverage remain unachieved. Full goal remains active.

## Latest: 2026-09-28 independent peer-generated stateless reset

Added a live quic-go v0.63.0 restart fixture for both versions. It first exchanges
stream bytes, suppresses orderly close writes while stopping the peer transport,
then replaces that transport with the same caller-owned UDP socket and reset key.
A later client STREAM triggers an actual reset from the unmodified peer, which
has lost the old connection state. The test checks emitted peer traffic, typed
reset context cause, net.ErrClosed/net.Error semantics and subsequent stream
write/open failures. No reset token or packet is fabricated by the test.

Three focused race repetitions passed (1.523s). Full interop race passed
(69.545s), followed by interop vet.
No production code changed. This closes the independent client-receive evidence
gap for ordinary v1/v2 restart; reverse-role reset generation, migration/multipath
restart and exhaustive public-operation propagation remain open. Full goal remains
active.

## Latest: 2026-09-28 public stateless-reset error

Replaced the untyped engine stateless-reset sentinel with a typed sentinel and
exported its type at the root API. StatelessResetError implements net.Error with
Timeout=false and Temporary=true, and unwraps to net.ErrClosed. The error text
and existing sentinel matching remain stable. Packet/token eligibility and
silent-draining logic are unchanged. Temporary matches the reference contract;
it does not make the closed connection reusable.

Extended real engine v1/v2 used-token detection to verify the type/network-error
contract. Added public wrapped-error inspection and an HTTP/3 nonconversion case.
Existing tests cover unused/retired token exclusion, authenticated packet suffixes,
secondary path reset handling and no output during draining. Full root race
passed (root 7.245s, HTTP/3 1.452s, transport 39.786s; unchanged packages include
cached results), and full interop race passed (69.401s). Both modules' vet
checks pass. Independent live peer reset generation and exhaustive public
operation propagation remain unproven. Full goal remains active.

## Latest: 2026-09-28 unify public and wire transport-error selection

Receive-failure closing now consumes the normalized transport error instead of
maintaining a second code/frame mapping. It records that cause before sealing
close packets and returns it through receive/network-owner activation and bound
packet paths. TLS crypto-error reasons remain empty on wire. Remote closures,
resets and unclassified socket failures do not elicit a new protocol close.

Focused close/network-owner/handshake tests passed (1.945s). A new v1/v2 matrix
compares returned error identity, retained original cause and net.ErrClosed with
protected Initial close codes/frames for protocol, early-limit, parameter, CRYPTO
buffer/conflict and TLS failures. Negative cases guard against replying to remote
or unclassified errors. The expanded focused group passed three race repetitions
(1.468s). Full root race passed (root 7.108s, HTTP/3 1.326s, transport
39.766s; unchanged packages include cached results); full interop race passed
(69.656s). Both modules' vet checks pass. A final review corrected first-error
selection: if a terminal error was already recorded, wire closure must use that
code/frame rather than a later error. The expanded receive-failure group passed
three race repetitions after this guard (1.341s); root vet passed again. The full
suite runs preceded this final guard.

Exhaustive error-source and public-operation mapping, observability and broader
conformance remain incomplete. Full goal remains active.

## Latest: 2026-09-28 public transport error contract

Added public TransportError/TransportErrorCode, standard code constants and crypto
code helpers. Recorded local protocol, TLS-alert, parameter, accepted-early-limit
and CRYPTO reassembly failures now carry code/frame/local identity and preserve
underlying errors. Remote transport closes carry remote identity and preserve
legacy PeerCloseError. Errors match net.ErrClosed and compare code/frame/endpoint.
Explicit local transport closure records its requested details. Application and
unrelated errors are not reclassified. Existing TLS wire-reason privacy remains.

Focused close/handshake tests passed (1.907s). Added mapping, crypto-range and
identity tests, plus strengthened peer-close distinctions. Public certificate
failure retains tls.CertificateVerificationError beneath the crypto transport
error, and authenticated downgrade rejection retains both the legacy engine
failure and new public type (three focused race repetitions, 1.416s). Full root
race passed (root 7.234s, HTTP/3 1.337s, transport 39.759s; unchanged packages
include cached results); full interop race passed (69.650s). Both modules' vet
checks pass.

Exhaustive failure-source/public-operation coverage, network-owner error mapping,
text formatting and broader lifecycle parity remain unfinished. Close-code
selection and normalization remain separate mappings requiring consistency audit.
Full goal remains active.

## Latest: 2026-09-28 stream cancellation error identity

Error-contract audit found StreamError.Is matched every StreamError regardless
of stream ID, code or endpoint. It now matches all three fields and safely rejects
a typed nil target, aligning with the pinned v0.63.0 reference. errors.As retains
type-based discovery. Public tests cover wrapped identity, mismatched fields and
actual v1/v2 local send cancellation plus peer receive reset. Three focused race
repetitions passed (1.441s); the final fixture also bounds receive reads with the
existing test deadline. Full root race passed (root 7.271s, HTTP/3 1.318s,
transport 39.734s; unchanged packages include cached results), and full interop
race passed (72.490s). Both modules' vet checks pass.

The audit records the next transport-error gap: local engine failures, remote
transport closes and TLS/parameter/CRYPTO failures currently expose different
types, while wire close codes are selected separately. A root alias alone would
not deliver the requested public contract or preserve all causes. The complete
TransportError/code API and propagation remain unfinished. Full goal stays active.

## Latest: 2026-09-28 typed application-close errors

Added public ApplicationError and ApplicationErrorCode aliases to independent
engine types. Local application closure now records code, reason and Remote=false;
peer application closure records Remote=true and wraps the legacy PeerCloseError.
Legacy ErrLocalClosed/ErrPeerClosed matching survives. Application errors match
net.ErrClosed and other application errors by code and endpoint. Local reasons
retain the caller's full text; wire reasons keep the existing 900-byte bound.
Repeated close preserves the first cause. Transport closes remain distinct.
HTTP/3 conversion recognizes both local and remote ApplicationError and preserves
its chain, with fallback for legacy peer errors constructed directly by callers.

Closing/draining tests passed, with expanded identity, reason-bound and repeated
close checks passing three race repetitions (1.486s). Public loopback v1/v2 tests
verify both connection contexts, DATAGRAM send and stream-open errors after local
and peer closure (three race repetitions, 1.318s). Independent HTTP setup gates
now additionally require the typed root application error. Full root race passed
(root 7.148s, HTTP/3 1.395s, transport 39.814s; unchanged packages include cached
results); full interop race passed (69.239s). Both modules' vet checks pass.

This retains a compatibility chain rather than the reference's direct
Unwrap-to-net.ErrClosed behavior. Full transport-error API parity and exhaustive
close/recovery orderings remain open. Full goal remains active.

## Latest: 2026-09-28 HTTP/3 client setup error conversion

Constructor, Dial/DialEarly, NextConnection and pooled RoundTrip now convert
eligible errors at their return boundaries, after existing cleanup and retry
logic. Stream/application failures retain the original chain; timeout, transport,
context and early-rejection behavior continues through the existing converter.

A live v1/v2 fixture advertises no critical-stream credit and sends a peer
application close carrying a settings error code/message. Constructor, ordinary
dial, fresh early dial and pooled request paths must expose both HTTP Error and
PeerCloseError. The first constructor fixture closed the peer before raw QUIC
Dial finished and therefore did not reach the HTTP constructor. Synchronizing raw
connection readiness corrected the fixture; the full eight-case matrix then
passed ten race repetitions (1.744s). The already-running full suite used the
old fixture and failed those two constructor cases (71.796s); its other output
is not counted as a successful full gate. The corrected full interop race gate
passed (69.563s), followed by interop vet.
Public HTTP/3 race passed (1.781s); full root race and vet passed with cached
already-tested packages.

Root ApplicationError parity, all early reconstruction failure orderings and
exhaustive HTTP/API conformance remain open. Full goal remains active.

## Latest: 2026-09-28 server handler HTTP/3 error conversion

Added a request-owner error mapping hook, installed by the public HTTP server.
Handler body reads/closes, response writes/flushes/error inspection and accepted
tunnel operations now expose the same HTTP Error wrappers as client operations.
The concrete response writer remains intact for ResponseController and tunnel
acceptance. Wire/task errors are retained before conversion; a focused writer
test verifies that Finish still observes the original stored failure.

A live upstream-client cancellation fixture covers v1/v2 ordinary handlers and
accepted CONNECT tunnels. Reads, writes, Flush or CloseWrite must expose both
HTTP and original QUIC stream errors with matching code/endpoint. The expanded
fixture passed ten race repetitions (1.694s). Public HTTP/3 and internal adapter
race suites passed (1.627s/1.416s), followed by the expanded adapter tests (1.310s).
Full root race and vet passed, using cached results for already-tested packages.
Full interop race passed (69.690s), and interop vet passed.

Setup/dial conversion, root ApplicationError type parity, server datagram peer
failure evidence and broader close/cancellation ordering coverage remain open.
This is not a complete public error/API parity claim. Full goal remains active.

## Latest: 2026-09-28 preserve tunnel upload failure causes

A new live upstream-peer test reproduced generic io.ErrClosedPipe from tunnel
writes after an authenticated peer reset, in both v1 and v2. Reads already
reported the reset. Upload cleanup now supplies failures to bodies implementing
CloseWithError(error), including io.PipeReader. Ordinary bodies retain Close;
successful uploads use Close. Existing sync.Once ownership still arbitrates
cancellation and deferred cleanup, with no duplicate body close.

The live regression passes ten race repetitions after the fix (1.472s). New
upload tests cover read, wire and canceled failures plus successful ordinary
closure, verifying the exact cause and a single cleanup. Public HTTP/3 and
internal HTTP adapter race tests passed (1.483s/1.307s); full root race passed
with the expanded adapter tests (1.296s) and cached unchanged packages. Root vet
passes. Full interop race passed (69.445s), and interop vet passed.

Server handler I/O conversion, setup paths and root application-close type parity
remain incomplete. This fixes the reproduced peer-reset tunnel path; it does not
establish all connection-close/cancellation orderings or full HTTP/3 parity.
Full goal remains active.

## Latest: 2026-09-28 tunnel error return boundaries

Tunnel read/write/close and datagram methods now apply the public HTTP/3 error
conversion, including context causes. Added tests for a real io.Pipe partial
write followed by a stream reset, canceled-write precedence, peer-close reads,
exactly-once cleanup, upload EOF and datagram cancellation/negotiation errors.
Public HTTP/3 race tests passed (1.423s); existing live tunnel, capsule and HTTP
datagram fixtures passed three race repetitions (2.482s), and interop vet passed.
Full root race and vet passed, with cached results for already-tested packages.

These gates do not establish end-to-end peer-reset cause preservation in tunnel
writes: inspection found WriteHTTPRequest closes request.Body without conveying
its cancellation/write cause to the pipe reader. A live reset regression and
upload cleanup correction remain necessary. Server handler I/O and setup error
conversion also remain unfinished. Full goal remains active.

## Latest: 2026-09-28 HTTP/3 client error identity

Added public ErrCode constants and Error with Remote, ErrorCode, ErrorMessage,
Unwrap and code/endpoint-based Is. Client exchange returns and response-body
read/close boundaries now wrap QUIC stream cancellations and authenticated peer
application closes. The complete underlying chain survives wrapping; transport
closes, timeout errors, EOF and ordinary cancellation retain their types. Unknown
code text preserves the full value rather than truncating to 16 bits.

Unit tests verify local/remote stream identity, wrapped causes, peer application
versus transport closure, idempotent conversion, partial-read byte counts,
response ownership and cancellation precedence. Public HTTP/3 race tests passed
(1.391s). Live v1/v2 reset and GOAWAY retry plus client close/runtime-stop cases
passed three race repetitions (2.785s), now asserting both HTTP and QUIC error
inspection for unretried resets. Full root race suite and vet passed, with cached
results for packages already tested. Full interop race passed (69.327s), and
interop vet passed.

This is client boundary coverage, not complete error parity. Server handler I/O,
tunnel writes/datagrams and setup/dial paths remain to integrate and audit. Root
connection closure still exposes PeerCloseError, so reference ApplicationError
and local application-close contracts remain unfinished. No production upstream
dependency was introduced. Full goal remains active.

## Latest: 2026-09-28 refresh peer to v0.63.0 and HTTP request contracts

Updated only the isolated interop module's quic-go dependency from v0.62.0 to
v0.63.0, the [September 22 release](https://github.com/quic-go/quic-go/releases/tag/v0.63.0).
Production remains independent. The upstream interface.go and key-update AEAD
source are unchanged between those versions. Refreshed the
[IETF multipath record](https://datatracker.ietf.org/doc/draft-ietf-quic-multipath/):
draft-21 remains in the RFC Editor queue, In Progress (First Edit), rather than a
published RFC. This status refresh is not a new full conformance audit.

Server non-CONNECT and extended CONNECT requests now leave URL.Scheme/URL.Host
empty, store authority in Request.Host, and preserve origin-form RequestURI.
Ordinary CONNECT retains authority-form RequestURI and URL.Host. Adapter tests
cover escaped paths, query, OPTIONS asterisk and both CONNECT forms; the extended
handler test verifies the same contract. Added ClientConn.LocalAddr/RemoteAddr,
with live address assertions against the underlying connection and server socket.
Live ordinary request metadata is checked in both peer roles for v1 and v2.

The pre-change v0.63.0 full interop race suite passed (69.378s). Focused HTTP live
exchanges passed three repetitions (1.608s); HTTP adapter/public package race
checks passed (1.381s/1.578s), followed by the strengthened extended-handler check
(1.339s). Full root race passed using cached results for already-tested packages.
The final full interop race suite passed (69.516s); both modules' vet checks pass.

The strict opt-in v2 bulk receive-window test still fails against v0.63.0 after
122,220 bytes at peer key phase 1 (5.273s); this is recorded separately from
passing default coverage. Public HTTP/3 error wrapping remains missing. Existing
benchmark records retain their v0.62.0 reference and cannot establish performance
against the new pin. Full implementation, conformance, API parity and performance
goal remains active.

## Latest: 2026-09-28 preserve MTU fallback across path-zero handoff

A new v1/v2 regression reproduced path adoption restoring 1452 after a base
EMSGSIZE fallback had reduced the usable size to 1200, both before and after the
baseline challenge was sent. Adoption now seeds from the current base limit,
transfers reserved challenge ownership and the validation gate, and keeps the
existing validation deadlines, recovery, queue charges and packet numbers.
Already-completed validation does not start a new check. The fallback timestamp
carries into the ten-minute upward-probe cooldown when discovery is enabled;
disabled discovery retains no raise timer.

The expanded test exchanges protected challenge/response and split STREAM packets
across the handoff and checks full queued-data delivery at no more than 1200 bytes.
A completed-validation case checks that re-opening discovery only proposes a fresh
probe rather than restoring the previously failed size. The focused handoff,
base-write-fallback and configured-size group passed three race repetitions
(1.653s). Full root race suite passed (root 7.344s, HTTP/3 1.361s, transport
40.328s; unchanged packages include cached results); root vet and diff checks
pass. This verifies an internal ownership transition, not public runtime
migration or kernel EMSGSIZE behavior. Full goal remains active.


## Latest: 2026-09-28 wildcard/base packet builder MTU write fallback

The base builder now reports metadata for committed oversized STREAM/DATAGRAM
packets. Synchronous EMSGSIZE rejects recovery flight without congestion loss or
packet-number/AEAD rollback, restores idle activity, fixes the original-path limit
at 1200, and validates the baseline before sending application data. Reliable data
is queued and split; unreliable rejected or newly oversized queued messages are
released without retransmission. A dedicated challenge slot avoids depending on
space in a full data queue. Validation expiry reports NO_VIABLE_PATH rather than
leaving data indefinitely blocked. The base builder does not initiate upward MTU
discovery after fallback.

V1/v2 endpoint tests inject the socket failure and exchange real protected
challenge/response and application packets. They verify reliable byte delivery,
smaller DATAGRAM delivery, dropping a second queued large message, recovery and
queue accounting, rejected-packet ACK handling, full-queue challenge progress and
baseline-validation failure. Three focused race repetitions passed (1.502s).
Full root race passed (root 7.525s, HTTP/3 1.365s, transport 41.297s; unchanged
packages include cached results), and independent packet-size gates passed three
race repetitions (1.632s). A final review added a restart of an already-pending
explicit validation into the reserved slot, preventing its challenge from being
stranded behind blocked data. The expanded base/path-validation/write-error group
passed three race repetitions after that fix (1.678s); vet and diff checks pass.
The pending-response test also asserts application output remains blocked.
These are deterministic endpoint tests, not kernel EMSGSIZE or constrained-link
independent-peer proof. Full goal remains active.


## Latest: 2026-09-28 public InitialPacketSize integration

Added public InitialPacketSize, defaulting to 1280 and clipping nonzero inputs to
1200–1452, matching pinned reference configuration code. Dial and Listen forward
it. DisablePathMTUDiscovery now disables upward probing while retaining the
original configured size. The base builder and STREAM/DATAGRAM admission also use
the configured limit for wildcard sockets, which do not adopt address-aware path
state. Retry repacketization and congestion reset now preserve the configured
size; a 1200-byte Retry token regression requires the larger configured budget.

Public v1/v2 tests check normalization/caller ownership, actual first-flight bytes,
and maximum DATAGRAM delivery in both directions with discovery disabled. Three
race repetitions passed (1.512s). Pinned upstream qlog observes Initial lengths
and maximum DATAGRAM reception in both roles at default/1200/1452; three race
repetitions passed (1.592s). After the Retry adjustment, Retry tests passed three
race repetitions (1.536s), public wire/DATAGRAM tests passed again (1.432s), and
root vet passed. Full root race before that final Retry adjustment passed (root
7.267s, HTTP/3 1.326s, transport 40.475s; unchanged packages include cached results).

The default interop suite exposed a stale shutdown observation in
TestHTTP3ClientResponseAfterRuntimeStops: SendDatagram now preserves the connection
cause after owner exit, so it cannot reveal ErrEndpointClosed. The test now probes
CloseWithError only after observing connection closure, keeping its actual owner
shutdown requirement. This failure is from the earlier cause-preservation change,
not established as a packet-size defect. The corrected shutdown gate passed three
race repetitions (1.909s). The full default interop race suite then passed
(69.884s), with interop vet passing too; opt-in external skips are not passes.

A fresh sequential 100x/three-sample matched echo benchmark records median
12.806 ms / 2137220 B / 45566 allocations for this implementation versus
6.235 ms / 707371 B / 19640 allocations upstream. This remains about 2.05x slower;
no timing effect is attributed to the default change. Raw output and limitations
are in docs/benchmarks/stream-echo-initial-1280.txt and stream-echo.md.

Wildcard write-error fallback, reference migration reset parity, constrained-path
independent gates and broader performance comparisons remain open. Full goal
remains active.


## Latest: 2026-09-28 large DATAGRAM local write-failure recovery

Endpoint output now retains payload-free metadata identifying the latest committed
oversized DATAGRAM. A synchronous EMSGSIZE rejects that packet in recovery,
restores the pre-send idle activity, and resets the path to baseline validation.
It does not rewind the packet number or AEAD usage, retransmit the message, count
congestion loss, or release its payload/queue accounting twice. The already freed
DATAGRAM payload remains unretained. Baseline-sized failures and other write
errors retain connection removal. Probe and reliable STREAM handling are unchanged.

V1/v2 tests cover enabled/disabled discovery, queue/flight/congestion/idle and
packet-number accounting, rejected-packet ACK validation, subsequent baseline
challenge classification and protected smaller-DATAGRAM delivery after validation.
The write-failure group passed three race repetitions (1.514s); the expanded
post-fallback delivery group also passed three repetitions (1.372s). Full root
race suite passed (root 7.190s, HTTP/3 1.314s, transport 40.229s; unchanged
packages include cached results). Root vet and diff checks pass. These are
endpoint tests with an injected synchronous socket error, not kernel error or
independent-peer proof.

InitialPacketSize still needs public adapter integration, migration-policy parity
review and independent configured-size validation. Full goal remains active.


## Latest: 2026-09-28 path-aware DATAGRAM sizing and queued shrink handling

DATAGRAM admission now uses the selected confirmed path's packet limit while
reserving the largest CID and enforcing peer max_datagram_frame_size. Early and
pre-path messages retain their portable limits. At output, queued messages that
no longer fit the current path are discarded without allocating a packet number,
sealing or charging flight; payload/queue credits are released. Smaller idle
paths skip oversized messages during borrowing so they stay sendable at source
and rollback cannot restore already-discarded accounting.

V1/v2 regressions cover 1410-byte encrypted DATAGRAM delivery, rejected 1411-byte
admission at a 1452 path, peer frame caps, a full queue with 63 oversized messages
followed by one fitting message, exact packet-number/AEAD/flight/queue accounting,
and ownership after attempted borrowing by a smaller path. Three focused race
repetitions passed (1.386s). Full root race suite passed (root 7.181s, HTTP/3
1.335s, transport 40.270s; unchanged packages include cached results); root vet
and diff checks pass. Independent public Dial/Listener and HTTP DATAGRAM gates
passed three race repetitions (1.577s), covering existing ordinary-size traffic,
not new maximum-sized messages. The [RFC 9221 section 5](https://www.rfc-editor.org/rfc/rfc9221.html#section-5)
MTU/no-fragmentation constraint was checked against the IETF-hosted RFC text. This is local policy for unreliable
queued messages, not a fragmentation scheme or delivery guarantee.

Public InitialPacketSize remains pending: DATAGRAM local EMSGSIZE handling,
adapter plumbing, migration-policy comparison and independent configured-size
observations are still required. Full goal remains active.


## Latest: 2026-09-28 configured original-path packet sizing and fixed fallback

Extended engine InitialPacketSize beyond padding: path-zero adoption retains the
configured original-path size capped by peer UDP limits, ordinary STREAM admission
uses it, and initial NewReno sizing uses the configured datagram size. A fixed MTU
search state supports EMSGSIZE/black-hole fallback without probes when discovery
is disabled or unavailable. It suppresses both next-probe and periodic-raise
scheduling. New paths remain at 1200; migration/failure reset discards the original
size while preserving the discovery policy.

V1/v2 tests verify adoption, peer clamping, enabled/disabled discovery, reset,
periodic-raise suppression and encrypted STREAM delivery in packets above 1200.
A configured-size endpoint EMSGSIZE regression verifies connection survival,
retransmission ownership, baseline validation and splitting, without enabling
probes. The combined new-size/MTU/write-failure race group passed three repetitions
(2.097s); initial congestion/configuration boundaries passed separately (1.285s).
Full root race suite passed (root 7.179s, HTTP/3 1.317s, transport 43.707s;
unchanged packages include cached results). Root vet and diff checks pass.

Public Config remains unexposed: DATAGRAM sizing, adapter plumbing, reference
migration policy comparison and independent configured-size observation are still
required. API_PARITY.md now tracks these remaining gates. Full goal remains active.


## Latest: 2026-09-28 configurable engine Initial padding foundation

Added engine-only InitialPacketSize (zero=1200, explicit 1200–1452) and used it
for Initial padding and size checks, bounded by an already-known peer UDP limit.
The base non-Initial builder stays at 1200. An explicit pre-padding frame-size
check prevents oversized CRYPTO content from being trimmed as though it were
padding. The public Config option remains absent until ordinary packet sizing,
congestion/path baselines and MTU fallback/migration semantics are integrated.

V1/v2 engine handshakes cover 1200/1280/1452 in both directions. The shared test
driver now checks each sender's configured Initial size, while keeping its old
1200 assertion for other packets. Peer-limit and configuration-boundary tests
passed three race repetitions with these handshakes (1.408s). An oversized-frame
regression verifies failure before ciphertext/packet-number commitment and passed
three race repetitions (1.296s). Full root race suite passed (root 7.395s,
HTTP/3 1.348s, transport 42.481s; unchanged packages include cached results);
the oversized-frame test was added during that run and separately validated.
Root vet and diff checks pass.

API_PARITY.md records the remaining integration gates and the reference's actual
1280 default / nonzero 1200–1452 clamping behavior. This is groundwork, not public
InitialPacketSize parity, MTU validation, or an independent interoperability
claim. Full goal remains active.


## Latest: 2026-09-28 typed timeout errors and terminal cause preservation

Added exported IdleTimeoutError and HandshakeTimeoutError aliases to independent
engine types implementing net.Error (timeout=true, temporary=false) and unwrapping
to net.ErrClosed. Existing internal sentinel identities remain usable. Error text
remains local. Handshake inactivity now reports IdleTimeoutError; the absolute
handshake limit reports HandshakeTimeoutError and takes precedence when both have
passed. This corrects the preceding turn's single-type timeout classification.

A public v1/v2 propagation regression initially failed: stream Write after client
owner shutdown returned ErrEndpointClosed instead of the retained timeout cause.
Send Write/Close/WaitAcknowledged, stream open/accept and DATAGRAM send/receive now
map that adapter error to the connection context cause. They leave unrelated
errors unchanged. Receive buffer draining continues through its existing path.

Tests cover wrapping, errors.As/errors.Is/net.Error, real silent-peer Dial under
idle and absolute limits, connection context cause, stream reads/writes/opens,
FIN, acknowledgment waits and DATAGRAM operations. The initial focused race group
passed three repetitions after the cause fix (root 1.865s, transport 1.520s);
additional send and DATAGRAM assertions passed in the full root race suite
(root 7.181s, HTTP/3 1.323s, transport 40.201s; unchanged packages include cached
results). Root vet and diff checks pass. Independent v1/v2 keepalive/idle-expiry
checks passed in both roles (25.350s).
Full protocol, API, independent validation and performance goals remain active.


## Latest: 2026-09-28 handshake inactivity limit and Config inventory

Added public HandshakeIdleTimeout with a five-second default, forwarded through
Dial and Listen. The engine chooses the earlier absolute or inactivity limit;
existing authenticated receive / first ack-eliciting send accounting drives
inactivity. Garbage and repeated sends cannot extend it. Idle expiry ends at TLS
completion; the existing absolute limit continues to confirmation. Public zero
HandshakeTimeout now derives twice the idle duration with overflow saturation;
an explicit absolute override is retained. Negative durations are rejected.

V1/v2 engine tests verify activity, garbage, send restart rules and cleanup.
Additional tests prove sustained traffic cannot move the absolute limit, late
input/output cannot revive an expired handshake, and TLS completion removes the
idle limit. Public configuration tests cover defaults, override, overflow and
caller ownership. A real bound silent UDP peer verifies Dial timer expiration.
Three focused race repetitions passed (root 1.450s, transport 1.508s); root vet
passed. Independent receive-window matrix passed three race repetitions (2.317s).
Full root race suite passed (root 7.162s, HTTP/3 1.352s, transport 40.068s;
unchanged packages include cached results).

Started docs/API_PARITY.md with every pinned reference Config field, distinguishing
presence from behavior parity. Reference code uses 768 KiB initial connection
credit despite its interface comment saying 512 KiB. Remaining gaps include
per-client QUIC configuration, packet sizing, partial stream reset, tracing,
exported timeout error types, receive defaults/tuning, and the rest of the API
inventory. No full conformance or performance claim. Goal active.


## Latest: 2026-09-28 connection receive-window growth veto

Added Config.AllowConnectionWindowIncrease with the pinned upstream signature.
Both Dial and Accept bind the callback to the public connection on the engine
owner before returning it. Proposed additional connection credit is presented
before committing growth; denial preserves the current window and later
consumption can retry. Nil allows bounded growth. Cleanup clears the captured
public handle. Callbacks must not block or reenter connection/stream methods;
this does not cap independent stream-buffer growth or total memory.

V1/v2 engine tests verify denial, later approval, delta and pre-commit state,
maximum-bound suppression and cleanup. Public client/server tests verify handle
identity and installation with controlled read timestamps on real connections.
Both groups passed five race repetitions (root 1.532s, transport 1.925s), and
root vet passed. Full root race suite passed (root 7.151s, HTTP/3 1.325s,
transport 40.857s; unchanged packages include cached results). Accept now checks
closure before installing the callback, matching Dial; the root package race
suite passed again after that guard (6.869s), and root vet passed again.

The independent receive-window three-repetition run failed once during upstream
DialAddr, before application reads. Twenty focused race repetitions and ten
full receive-window matrix repetitions then passed (1.656s / 4.415s); see
interop/KNOWN_FAILURES.md. This unresolved failure remains enabled. Updated
the README's obsolete fixed 64 KiB cap statement. Default/tuning parity, total
memory behavior, broader API conformance and performance remain open. Goal active.


## Latest: 2026-09-28 terminal receive-window tuning accounting

Added v1/v2 tests separating actual terminal reads from discarded receive credit.
Reading through FIN may grow the connection window while leaving terminal stream
capacity unchanged. EOF-only reads and repeated credit updates cannot change the
consumption sample. Canceling completed or open reads and receiving RESET_STREAM
must release connection credit without counting discarded bytes toward growth.
A reset-gap case commits 2047 bytes at the sender but cancels before transmission;
the receiver releases that missing final-size credit without tuning its window.
Terminal streams must not enqueue MAX_STREAM_DATA.

Five initial terminal race repetitions passed (1.537s). The expanded reset-gap,
growth and consumption-policy group also passed five race repetitions; transport
vet and diff checks pass. No production fix was needed. Memory-policy callbacks,
load/performance evaluation and full protocol/API parity remain open. Goal active.


## Latest: 2026-09-28 opt-in bounded receive-window growth integration

Added public MaxStreamReceiveWindow and MaxConnectionReceiveWindow, propagated
through client and listener configuration. Zero selects each initial window,
preserving fixed-window defaults. Validation rejects undersized maxima, stream
maxima above 16 MiB and connection maxima beyond the varint range.

ReadStreamAt supplies deterministic consumption timestamps; public reads use
current time. Separate stream/connection samplers count only bytes actually read,
not reset/discard credit. Stream storage grows before the normal credit-update
path advertises new limits. Final-size-known streams skip storage growth, while
connection consumption still contributes to future credit. Multipath uses the
minimum positive measured RTT among non-abandoned path recovery states; this
conservative local heuristic still needs heterogeneous-path evaluation.

Controlled v1/v2 tests prove fast versus slow consumption, unread-data preservation,
credit advertisement and peer writes beyond the old stream limit. Configuration
boundary tests pass. Full root race suite passed (transport 40.114s; unchanged
packages include cached results). Independent receive-window tests passed three
race repetitions with growth enabled in large-window cases (2.521s); they verify
initial advertisement/delivery, while deterministic engine tests prove growth.
Root and interop vet pass. No WAN throughput improvement is claimed. Memory-load,
policy/callback/default parity and wider extension combinations remain open.
Full goal active.


## Latest: 2026-09-28 bounded consumption policy for receive-window growth

Added receiveWindowSample, an internal owner-side consumption sampler. After
establishing an epoch, consuming at least half the current window within two
measured RTTs proposes one doubling, bounded by the configured maximum. The
first read alone cannot trigger growth; missing RTT, clock reversal and slow
consumption suppress it. Arithmetic clamps consumption and avoids duration
multiplication overflow. This is a local tuning policy, not an RFC requirement.

Tests cover the time boundary, caps, fixed windows, missing RTT, backward clocks,
odd windows and very large values. Targeted race tests passed (1.424s), transport
vet and diff checks passed. A bounded fuzz campaign checks monotonic, capped,
single-doubling proposals across varying reads and clock changes; 1,650,456
executions passed in 20.358s.

The sampler remains disconnected from production reads. Integration still needs
maximum-window configuration, timestamped read handling, separate stream versus
connection samples, sampled RTT selection, and ordering capacity growth before
credit advertisement. RESET_STREAM/discard accounting must not masquerade as
application consumption. Public behavior is still fixed-window. Full goal active.


## Latest: 2026-09-28 receive-buffer growth foundation

Added internal stream.Receive.Grow as the storage prerequisite for receive-window
autotuning. Growth remaps occupied bitmap positions by absolute stream offset,
preserving wrapped unread data, gaps, final size and shared flow accounting.
It does not grant or advertise credit; SetLimit remains separate. Empty receivers
retain lazy allocation, shrinking is rejected, and the 16 MiB bound remains.
The transport does not call this yet: public receive windows are still fixed.

Unit tests cover byte/bitmap boundaries, circular wrap with gaps, retained FIN,
conflicting overlaps, invalid growth, lazy maximum capacity and near-MaxVarint
offsets. A differential fuzzer compares growing storage with a fixed-capacity
receiver under the same advertised credit, pushes, reads and resets. A 20-second
run passed 2,767,942 executions (20.268s). Full stream race tests and stream vet
pass. Growth scheduling, maximum-window configuration, connection-credit tuning
and memory policy remain to be integrated. Full goal active.


## Latest: 2026-09-28 independent configured receive-window matrix

Added TestReceiveWindowsUpstream covering v1/v2, both TLS roles, small/large
configured windows, and remote bidi/local bidi/remote uni receive directions.
Upstream qlog parameters must match all three stream credits and connection
credit, preventing a transfer-only test from passing with ignored configuration.
V1 transfers 275,000 bytes and exercises credit replenishment. Default v2
transfers 64 KiB, covering advertisement and small-window replenishment but not
128 KiB stream-window turnover.

The initial full-payload matrix reproduced v2 failures in all twelve combinations
across three repetitions (181.593s); v1 cases passed. A focused trace showed key
phase 1 and a stall at 121,989 bytes, consistent with the pinned upstream mismatch.
A 96 KiB shorter attempt still hit phase 1 once at 97,538 bytes. The final 64 KiB
v2 matrix passed five race repetitions (2.821s). Interop vet and diff checks pass.
Full v2 payload remains available under QUIC_INTEROP_KNOWN_FAILURES=1, documented
in interop/KNOWN_FAILURES.md. This unresolved independent bulk gap is not counted
as a passing result. No production changes were made. Full goal remains active.


## Latest: 2026-09-28 configurable initial receive windows

Added public Config.InitialStreamReceiveWindow and
InitialConnectionReceiveWindow, preserving zero-value defaults of 64 KiB per
stream and 1 MiB per connection. prepareConfig snapshots values into all stream
directions and connection transport credit without mutating caller configuration.
Unsupported stream windows above 16 MiB and connection values beyond the QUIC
varint maximum are rejected. Raised the engine's previous 64 KiB policy cap to
the existing receiver's 16 MiB capacity bound; storage remains lazily allocated.

Configuration tests cover defaults, tiny and larger windows, bounds and caller
ownership. Reused the engine transfer fixture to validate a 512 KiB v1/v2
transfer with 128 KiB stream / 192 KiB connection windows, loss, reordering and
credit replenishment. Selected public/transport race tests and root vet pass.
This is fixed-window configuration, not autotuning or full API parity. Maximum
window controls, independent size matrices and memory-load testing remain open.
Full goal active.


## Latest: 2026-09-28 broad regression and external multipath checkpoint

Full root race suite passed (root 6.968s, transport 39.395s; some unchanged
packages cached). Default independent interop race suite passed (68.831s).
Root and interop vet passed. Then explicitly enabled the pinned Picoquic binaries
and ran both multipath roles: all 28 v1/v2, IPv4/IPv6, loss and reverse-role bulk
leaf cases passed (49.737s), with no selected skips. Binary hashes match the
recorded pinned builds.

Commands, raw suite outputs, verbose external results and scope limitations are
in docs/validation/2026-09-28.md. Default-suite external/known-failure skips are
not counted as successes. Greasing, compatible-switching and external early-data
variants were not selected in this external run. No production fix was needed.
The full conformance, API parity, security, cross-platform and performance goal
remains active.


## Latest: 2026-09-28 partial-handshake stream and DATAGRAM credit isolation

Expanded the v1/v2 protected partial-handshake CID fixture to advertise larger
connection credit, per-stream credit and stream count plus newly enabled DATAGRAM
support. Before Finished, it consumes the remembered two streams / 4096 bytes
per stream / 8192 connection bytes and requires excess writes, a third stream
and DATAGRAM sending to remain blocked or unsupported. After accepted handshake
completion, it verifies the new limits, preservation of consumed byte accounting,
and actual DATAGRAM delivery. No server application reads generate credit updates
that could mask missing transport-parameter promotion.

Five initial race repetitions passed (1.502s); the final test adds explicit
DATAGRAM delivery and also passes five race repetitions. Transport vet and diff
checks pass. No further production fix was needed. CONFORMANCE.md records this
stronger evidence. Independent partial-flight coverage and broader frame-driven
credit auditing remain open, along with the full implementation goal.


## Latest: 2026-09-28 wire-level partial-handshake CID regression

Strengthened TestEarlyCIDLimitPromotion by replacing direct peer-parameter
injection with delivery of real protected handshake packets. The fixture splits
the resumed server CRYPTO flight between EncryptedExtensions and Finished.
It asserts that fresh CID credit of four is visible while early write keys
remain active, and that issuance stays at remembered credit of two. It then
observes actual 0-RTT packets and the server's two peer CIDs before delivering
Finished and verifying promotion to four.

Five initial race repetitions passed (1.461s); five more with the explicit
0-RTT wire assertion passed (1.396s). Transport vet and diff checks passed.
No further production fix was needed. CONFORMANCE.md now describes this stronger
evidence. This remains an engine-pair fixture, not an independent-peer test;
full normative mapping, external combinations, API parity and performance work
remain open. Goal active.


## Latest: 2026-09-28 preserve remembered CID credit during early sending

The scheduler audit found a fresh-parameter dependency in issueLocalIDs: newly
received ActiveConnectionIDLimit could control queued CID frames while only
0-RTT application write keys were available. CID issuance now uses the remembered
stream-set credit while sendingEarly is true, then uses current peer credit.

TestEarlyCIDLimitPromotion reproduced the pre-fix problem in v1/v2 (four active
IDs against remembered credit of two). The test injects partial-handshake
parameter state, verifies bounded CID issuance, then completes real accepted TLS
resumption and verifies promotion to four. This is not an independent wire-level
partial-flight reproduction. Five grouped credit-promotion race repetitions
passed (1.614s); early engine/compatible-version/local-ID race tests passed
(1.811s); transport vet passed. The upstream early-client acceptance race check
also passed. CONFORMANCE.md records the finding and its evidence limits.
Full RFC coverage, independent combinations, API parity and performance goals
remain active.


## Latest: 2026-09-28 start normative evidence mapping

Added docs/CONFORMANCE.md with a focused RFC 9000 section 7.4.1 audit after
checking the official RFC and current wire/TLS/transport implementations.
The map distinguishes stored state, acceptance checks, credit promotion,
implementation-specific close behavior and unverified extension combinations.
It is explicitly incremental; no full conformance claim is made.

Corrected stale REMAINING.md summary entries that still described accepted
0-RTT, early APIs and application invalidation as absent. Those foundations have
implementation and tests; complete behavior/combination coverage remains open.
Selected wire/handshake race tests passed (1.209s/1.705s). Exact transport promotion
and accepted-limit-close selections passed (1.318s). The initial broad regex
matched no transport tests; that result is explicitly excluded from evidence.
No production change was necessary. Full RFC maps, API parity, security review,
independent matrix and performance objectives remain active.


## Latest: 2026-09-28 reduce idle send-cache capacity

Compared send-cache limits of 64, 16 and 4 entries using three 100-exchange echo
samples each. Selected 16 entries (32 KiB payload capacity), reducing the prior
128 KiB maximum by 75%. Relative to the 64-entry control, median allocated bytes
rise 3.4% (2,056,774 to 2,126,802); four entries raise them about 30.1%. Timing
remains similar and these samples do not establish a speedup. This balances
idle retention against allocation churn; slice/allocator and connection overhead
are additional. Actual multi-connection heap/RSS load measurements remain open.
Commands and raw samples are in docs/benchmarks/stream-echo.md.

Focused stream/cache, encrypted cross-path ACK and fuzz-seed race tests passed
(1.209s/1.334s), transport vet passed, and the race-enabled independent/upstream
echo comparison passed. Full conformance, API parity, multipath combinations,
independent interoperability and performance work remain active.


## Latest: 2026-09-28 encrypted cross-path ACK after send-buffer reuse

Added TestSendBufferCrossPathLateACK for v1/v2. A STREAM packet is withheld on
path zero, its shared item retransmitted on path one, and an encrypted PATH_ACK
received on path zero completes the retransmission through production dispatch.
A second stream must reuse that exact buffer. Encrypted late-original and duplicate
retransmission ACKs then arrive over different paths without changing the fresh
stream's payload, ownership or send-byte charge. Finally the delayed original
ciphertext and fresh STREAM packet are delivered and both payloads remain correct.

Five race repetitions passed (1.439s); transport vet and diff checks passed.
The fixture uses synthetic multipath setup, marks the secondary path validated,
and injects loss by queuing the shared item. It tests actual sealing, receive
and completion dispatch, but is not independent-peer interoperability or a test
of path validation/loss detection. No production fix was necessary. Full protocol,
API, interoperability and performance goals remain active.


## Latest: 2026-09-28 randomized send-buffer ownership validation

Added FuzzSendBufferOwnership, a bounded operation-sequence model using two
packet histories that share retransmitted send items. It interleaves new writes,
successful/refused and duplicate/late ACKs, cross-history loss/retransmission,
cancellation and pruning. Assertions cover retained payload contents,
exactly-once completion, connection send-byte charges, cache bounds, and cache
aliasing with live/canceled payloads or another cached buffer. Successful
completion must also remove the old frame reference.

An initial 30-second fuzz run passed 719,629 executions (31.282s). After adding
explicit canceled-payload preservation assertions, a 20-second race-enabled fuzz
run passed 15,438 executions (23.413s), including the 63-input discovered corpus.
Seed and focused cache race tests passed (1.391s); transport vet and diff checks
passed. No production fix was necessary. This validates logical ownership,
not packet authentication, ACK-of-unsent rules or full wire-level multipath.
The full conformance/API/interop/performance goal remains active.


## Latest: 2026-09-28 bounded completed STREAM payload reuse

Audited retained send ownership and introduced a per-connection cache for
successfully acknowledged unsplit STREAM payloads. Send.WriteBuffer transfers
storage to reliable output while still copying caller data. ACK completion
recycles storage only after stream completion succeeds, and clears the old item
frame. Shared stale packet references remain guarded by item.acked. Split writes,
cancellation and early rejection keep existing garbage-collected ownership.
The cache is bounded to 64 buffers of at most 2048 bytes; cleanup erases full
buffer capacity and releases the cache.

Three 100-exchange echo samples reduce median allocation bytes from 3,214,724 to
2,066,808 (35.7%) and allocations from 47,251 to 46,289 (2.0%). Timing remains
similar (11.485 to 11.406 ms); upstream remains about 2.11x faster in this workload.
Idle cached capacity can increase by up to 128 KiB per connection. Commands,
samples, profile and limits are in docs/benchmarks/stream-echo.md.

Tests exercise actual reuse followed by late ACK/loss, completion failure,
caller isolation, blocked writes, split exclusion, bounds and v1/v2 cleanup.
Full stream/transport race tests passed (1.174s/39.140s), root vet passed, and
independent public stream/migration/HTTP early-recovery race gates passed (18.996s).
Full conformance, API parity, multipath combinations, memory-load measurements
and remaining performance work are still open. Goal remains active.


## Latest: 2026-09-28 ACK result allocation reduction

Reprofiled the independent/upstream 1 MiB stream echo comparison. Retained
Send.Write payload copies remain the largest allocation source; safe reuse still
requires auditing old packet and split retransmission references. Recovery ACK
results were another measured source of allocation. OnACK now counts matching
retained packets and allocates its result once, retaining bounded sparse/dense
traversal and independently owned results. Duplicate ACKs allocate no result.

New batch benchmarks show 20–23% fewer allocation bytes for 16–256 packet batches,
with similar timing. Three 100-exchange independent echo samples reduce median
bytes from 3,340,695 to 3,214,724 and allocations from 47,999 to 47,251. Timing
changes from 11.558 to 11.485 ms, insufficient to claim a robust speedup. Upstream
is still about 2.11x faster and uses about 4.61x fewer bytes in this workload.
See docs/benchmarks/stream-echo.md for commands, raw samples and profile summaries.

Recovery race tests passed (1.272s), transport race tests passed (39.241s), root
vet passed, and both echo roles passed the race-enabled benchmark. Added explicit
sparse/dense, ordering, late/duplicate ACK, flight accounting and returned-storage
ownership coverage. The full implementation/performance goal remains active.

## Latest: 2026-09-28 extension resource replay policy interoperability

Added upstream-client v1/v2 coverage for ExtendedConnectHandlers and
DatagramHandlers with default, explicit denial and explicit approval policies.
Duplicate invalid forwarded Early-Data values reach the policy as one canonical
marker on established TLS. Denial returns 425 without invoking the resource;
approval permits a working tunnel. A subsequent unmarked tunnel on the same
single-slot connection exchanges stream data and, for datagram handlers, HTTP
datagrams. Policy invocation counts verify that ordinary established requests
do not spuriously invoke the early-data policy.

Five race repetitions passed (1.807s). The full TestHTTP3 interop race suite passed
(6.152s), and interop vet passed. No production fix was needed. These cases cover
forwarded replay exposure; cancellation under concurrent extension load and direct
0-RTT extension dispatch are not yet covered. Request-trailer misuse, broader
conformance/API/security work, multipath combinations and performance remain open.
The full goal stays active.

## Latest: 2026-09-28 independent Early-Data response misuse

Added upstream v1/v2 fixtures sending Early-Data in final response headers,
informational headers, trailer declarations and actual undeclared trailers. Our
client must report MessageError; invalid informational fields never reach the
application callback, and invalid trailer values never enter Response.Trailer.
Each case uses one request slot and then completes a healthy request on the same
connection, proving the error remains request-scoped and releases its reservation.

Five initial race repetitions passed (1.581s), followed by another five after
adding the trailer-publication assertion. Interop vet passed. Production changes
were unnecessary. Extension replay-policy stress, request-trailer misuse gates,
remaining conformance/security/API work, multipath combinations and performance
improvements remain open. The full goal stays active.

## Latest: 2026-09-28 default server resource replay policy

Added Server.AllowEarlyData, a metadata-only application resource policy. Direct
requests before TLS completion wait unless explicitly approved. A request carrying
Early-Data from another hop receives 425 unless approved, even after local TLS
completion. Deferred requests refresh TLS metadata before dispatch. The policy
wraps the selected ordinary or extension handler inside request panic/cancellation
handling; it does not introduce unjoined workers or read request bodies.

Unit tests cover forwarded/direct requests, complete/incomplete TLS, approval and
denial, nil defaults, cancellation and refreshed snapshots. The independent early
GET fixture now explicitly approves or denies resource dispatch: a gated peer
proves approved handlers run before Finished and denied handlers run after it.
The server ticket matrix additionally sends duplicate invalid forwarded markers
on established TLS and requires 425 with no resource invocation.

Five repeated dispatch race runs passed (1.634s). Full HTTP race suites passed
(http3 1.583s, http3conn 1.371s); full TestHTTP3 interop passed (6.158s); root vet
passed. Response/trailer misuse interoperability, extension-policy stress, remaining
conformance/API/security and multipath/performance goals remain unfinished.
Full goal active.

## Latest: 2026-09-28 Early-Data header semantics

Implemented RFC 8470 section 5.1 field handling. Structurally valid incoming
requests with any Early-Data value or multiple instances expose one canonical
Early-Data: 1 to handlers. Outbound request adapters and the lower-level message
writer preserve presence while emitting one canonical instance. Caller fields/maps
are not mutated, and NeverIndex from any combined instance is retained. Ordinary
HTTP field-octet validation still applies before normalization.

Early-Data is prohibited in response/informational fields, request/response
trailers and trailer declarations. Outgoing response checks fail before QPACK or
stream writes; application trailer permission cannot override the prohibition.
Connection-specific field rejection already covers the Connection-field ban.
The header is not inferred from dispatch-time TLS metadata and does not replace
application replay policy.

Tests cover empty/invalid/combined/repeated values, net/http receive/send adapters,
raw MessageWriter wire decoding, NeverIndex and caller ownership, response contexts,
trailer permission and declarations. The initial request-reader fixture lacked
its required cleanup callback; corrected it. Full HTTP race suites pass
(http3conn 1.307s; http3 cached), all TestHTTP3 interop passes (6.242s), root vet
passes. Independent malformed-field interoperability and a default server resource
replay policy still need work, as do broader conformance/API/multipath/performance
requirements. Full goal active.

## Latest: 2026-09-28 HTTP 425 recreation failure ownership

Expanded TestHTTP3TransportTooEarly with GetBody returning both a body and an
error, canceling the request with a custom cause, closing the pool synchronously,
and returning nil without an error. Every case verifies the old uploader closed
exactly once before GetBody, exactly one recreation callback, appropriate cleanup
of a returned replacement body, no second server request and no caller method
mutation. Request cancellation retains its cause; pool closure returns
ErrTransportClosed; nil replacement bodies fail instead of issuing an empty retry.

The pool uses MaxConnections=1. After each non-closure failure a fresh request
must succeed, checking response/connection reservation cleanup. Following the
closure case new admission must remain rejected. These exercise actual upstream
425 responses over v1/v2, rather than invoking the replay helper alone.

Five full expanded matrix race repetitions passed (3.273s); interop vet passed.
No production change was required. Additional response-time cancellation cases,
RFC 8470 header rules, simultaneous handoff/closure stress and the larger
conformance/API/security/multipath/performance objective remain open. Goal active.

## Latest: 2026-09-28 bounded HTTP 425 retries

Transport now handles 425 for explicit early GET/HEAD when Enable0RTT is enabled.
An eligible response is closed (joining upload cleanup and releasing its pool
reservation) before body recreation. The retry uses ordinary GET/HEAD, ensuring
TLS completion even if a new connection is needed. It shares the existing single
retry budget with protocol rejection. DisableRetries, ordinary methods, upload
trailers, nonrecreatable bodies and any Early-Data field suppress the status retry,
leaving the original response body usable. Direct ClientConn policy stays explicit.
RFC 8470 section 5.2 was checked against the RFC Editor source for ordinary-data
retry and intermediary forwarding requirements.

Independent v1/v2 tests cover GET/HEAD, repeated 425 (no retry loop), disabled policy,
case-insensitive Early-Data presence, ordinary requests, GetBody recreation,
nonreplayable bodies, upload trailers, original request ownership and peer handshake
completion on the second attempt. A blocked original uploader must be closed
before GetBody is called. The initial trailer fixture needed its explicit trailer
permission callback; corrected that setup without weakening retry assertions.

Five expanded race repeats passed (2.617s). Full HTTP race suites passed (http3
1.314s, http3conn cached), complete TestHTTP3 interop passed (5.899s), and root vet
passed. Cancellation/GetBody-error combinations and remaining RFC 8470 header
validation, multipath combinations and wider RFC/API/security/performance work
remain open. Full goal active.

## Latest: 2026-09-28 pooled retry evidence and early shutdown

Expanded the independent pooled early matrix with an informational-response
callback that returns a wrapped quic.Err0RTTRejected. An upstream 103 response
proves the application request reached the handler; the callback must run exactly
once and cause no replay, for both versions and retry settings. The original
callback error is retained, and a subsequent request succeeds without redialing.
This verifies retry evidence comes from actual session rejection, not a matching
application-provided error.

Added pool closure while resumed TLS is deliberately gated and an actual early
packet is observed. Close must join the early worker; the blocked RoundTrip must
return an error with no response; the server sees no application request; and a
later request must return ErrTransportClosed. The TLS gate is released only after
these assertions, so handshake completion cannot account for cleanup.

Ten initial callback-matrix race repetitions passed (2.766s). Ten final repetitions
including closure passed (3.216s); interop vet passed. Production behavior required
no additional changes in this turn. Simultaneous rejection/handoff/closure stress,
body-error evidence cases, HTTP 425 policy and the larger conformance, multipath,
API and measured-performance objective remain open. Full goal active.

## Latest: 2026-09-28 pooled early HTTP connections

Added Transport.Enable0RTT. Its worker dials early, then owns TLS confirmation and
rejected-session recovery under DialTimeout. The existing slot stays charged and
available during handoff; client-pointer publication is synchronized with pool
admission. Close/idle cleanup joins the current client generation. Ordinary methods
still wait for TLS; explicit GET/HEAD early methods retain opt-in behavior.

A request returning Err0RTTRejected waits for worker confirmation and checks the
actual old session's rejection before entering the existing single-retry policy.
Body recreation, trailers and DisableRetries retain their restrictions. Disabled
retries return the request error while leaving the recovered connection usable.
Also corrected automatic gzip negotiation for MethodHead0RTT to match HEAD.

Independent v1/v2 tests cover accepted/rejected early HEAD, both retry settings,
MaxConnections=1, no replacement QUIC dial, idle cleanup, caller method ownership,
HTTP/TLS results and eight concurrent subsequent GETs. Twenty initial race repeats
passed (3.326s); full TestHTTP3 interop selection after concurrent-request coverage
passed (5.687s). HTTP race suites pass (http3 1.440s; http3conn cached), root vet
passes. Broader simultaneous rejection/closure and callback-error evidence cases,
HTTP 425 handling, multipath combinations and the full conformance/API/performance
objective remain open. Goal active.

## Latest: 2026-09-28 initial setup rejection and blocked SETTINGS

Extended independent recovery cancellation to a one-byte stream window, allowing
three streams but blocking completion of the first control SETTINGS write. The
peer receives its prefix before cancellation, proving the fresh generation started.
Existing queued-caller cancellation, joined setup, QUIC closure and stable terminal
cause assertions cover this path alongside blocked encoder/decoder stream opens.

Added TestHTTP3DialEarlySetupRejection. A real seed HTTP exchange with one-byte
initial stream credit obtains a ticket remembering that limit. On resumption the
peer gates TLS until actual partial early control data arrives; DialEarly cannot
return because SETTINGS is incomplete. The peer then rejects early data with
fresh ordinary credit. DialEarly discards the partial generation and returns one
usable client, requiring no caller recovery. A fresh HTTP request succeeds with
TLS resumption and exactly one application request. Both QUIC versions are tested.

Also fixed writeSetup's pre-write cancellation branch to preserve context.Cause,
matching its post-write branch. A unit gate cancels precisely after stream opening
and verifies the custom cause, zero written bytes and cleanup of partial setup.
Ten repeated combined independent race runs passed (7.187s); full HTTP race suites
passed (http3 1.315s, http3conn 1.584s); root and interop vet passed. Pooled early
integration, HTTP retry policy, multipath combinations and the wider conformance,
API/security/performance objective remain unfinished. Full goal active.

## Latest: 2026-09-28 independent server HTTP ticket policy

Added TestHTTP3ServerTicketSettingsUpstream for v1/v2 against upstream quic-go.
An actual seed HTTP exchange obtains an early-capable ticket and address token.
A single endpoint selects immutable old/new HTTP TLS configurations per handshake,
retaining original ticket-key ownership and valid address evidence. A held client
TLS flight proves accepted early probes arrive before Finished; zero Retry is
required so Retry cannot account for rejection.

Equal/increased header limits accept early data. Reduced limits, missing HTTP
metadata, removal of Extended CONNECT and removal of HTTP DATAGRAM support reject
it. Both peers must still report TLS resumption, agree on Used0RTT, exchange a
fresh datagram without delivering a rejected probe late, and complete a fresh
HTTP request. The early probe is a QUIC datagram consumed before starting the new
HTTP session, isolating ticket policy from peer HTTP rejection recovery.

The existing independent early-GET/TLS-dispatch test now configures the server via
ConfigureTLSConfig, proving accepted HTTP handler dispatch before Finished with
the ticket wrapper enabled. Ten final repeated race runs of both test families
passed (3.142s); interop vet passed. Broader setup-rejection, blocked critical-write,
pooled early, HTTP retry and multipath combination gates remain open. Full goal active.

## Latest: 2026-09-28 server HTTP ticket policy wrapper

Added Server.ConfigureTLSConfig: clones the application TLS configuration, sets
h3 ALPN and wraps ticket encryption/restoration with authenticated server HTTP
SETTINGS metadata. The server's wire SETTINGS and ticket policy share one builder.
Restoration clones callback-owned TLS state, preserves other Extra records and
only retains EarlyData for compatible understood SETTINGS. Missing, duplicate,
malformed or incompatible metadata disables early data without disabling ordinary
resumption. Existing false EarlyData is never upgraded.

Default callbacks use the original config's EncryptTicket/DecryptTicket methods
so wrappers retain its key ownership. Application callback results/errors and
GetConfigForClient selection are preserved; dynamically selected configs receive
the same HTTP policy. The helper must be applied before listener creation and
Server configuration must remain immutable. ServeQUICConn alone cannot change a
TLS acceptance decision already made. This does not add application replay policy.

Tests cover equal/increased/reduced header limits, removal of CONNECT/datagrams,
metadata errors, private state/Extra ownership, refreshed records, selected configs,
callback errors and authenticated default ticket encryption with tamper rejection.
Full HTTP race suites pass (http3 1.459s; http3conn previously cached), all TestHTTP3
interop tests pass (5.329s), and root vet passes after fixing a test table that
copied Server's mutex. Independent live server ticket-policy validation remains
next; full RFC/API/multipath/performance goal active.

## Latest: 2026-09-28 cancellation during HTTP reconstruction

Added independent v1/v2 recovery-cancellation tests with rejecting upstream peers
that reduce unidirectional stream credit to one or two. Receiving fresh control
and, where permitted, encoder prefixes proves handoff has happened. The next
critical stream cannot open, so cancellation is exercised during partial HTTP
setup without production hooks. A second recovery caller cancels its serialized
wait independently while the first remains blocked. Canceling the first joins
setup, closes QUIC, and must retain the custom cause for repeated recovery calls.

The test exposed a public NextConnection bug: subsequent calls raced transport
closure against the stored recovery error and returned a generic local-close
cause. Recovery now acquires its cancelable serialization slot and checks the
published result before observing connection state. This preserves the terminal
setup failure and avoids duplicate stream setup.

Ten initial race repetitions passed after the fix (1.993s). The complete TestHTTP3
interop selection passed after expansion to both stream-credit positions (5.254s).
Full HTTP race suites and root vet pass. Deterministic rejection during initial
DialEarly setup, flow-control-blocked critical writes, server HTTP ticket policy,
pooled early integration and HTTP 425 handling remain outstanding. Full goal active.

## Latest: 2026-09-28 proof of public HTTP early delivery

Strengthened TestHTTP3PublicEarlyRecovery with a server-output gate. For accepted
resumption it discards all resumed server datagrams until the upstream HTTP
handler receives our GET/HEAD, then checks that the peer's handshake completion
channel is still open. The gate must have discarded output. Since the client has
received no resumed TLS flight, 1-RTT retransmission cannot conceal failure to
process the early request. After release, normal QUIC loss recovery finishes TLS
and delivers the response. Both versions and both early methods are covered.

The same matrix now cancels NextConnection while the server TLS callback is
blocked, checks the custom cancellation cause, and continues through the actual
request and accepted/rejected recovery. This proves cancellation before handoff
does not retire the session. Existing ordinary-method waiting, concurrent recovery,
old-client closure, request-method ownership and response assertions remain.

Three initial strengthened race repetitions passed (4.756s). Ten final repetitions
with cancellation and output-discard assertions passed (14.431s); interop vet
passed. Deterministic setup rejection, cancellation during/after ownership handoff,
server HTTP SETTINGS ticket policy, pooled early integration and HTTP 425 policy
remain outstanding. The broader implementation/validation goal remains active.

## Latest: 2026-09-28 public HTTP early dialing and recovery

Added DialEarly using the per-connection settings-bound TLS cache. It initializes
an early encoder only when remembered SETTINGS are available; ordinary Dial stays
non-early. MethodGet0RTT and MethodHead0RTT encode GET/HEAD without mutating the
caller request. Ordinary methods wait for handshake completion. Request admission
waiters now wake on session termination as well as client closure/GOAWAY.

ClientConn.NextConnection serializes cancellable recovery, retires a rejected
HTTP generation, resets QUIC streams and creates one fresh ordinary client.
Concurrent callers share that replacement; old Close and delayed request aborts
cannot close it. Requests are never replayed automatically. Setup rejection before
DialEarly exposes a client is reset internally. Cancellation callbacks are joined;
post-handoff failure is terminal. Dial cancellation after return remains detached.

Independent public UDP tests cover v1/v2, accepted/rejected GET and HEAD, ordinary
POST waiting, request-method ownership, explicit reconstruction, repeated/concurrent
recovery and closing the old client. Ten initial expanded race runs passed (4.121s).
After adding concurrent recovery and session-admission wakeup, full HTTP package
race tests passed (1.297s; http3conn cached), all TestHTTP3 interop tests passed
(4.171s), and root vet passed. Accepted peer packet observation does not by itself
prove the HTTP request was consumed before TLS; that stronger gate, deterministic
setup-rejection/canceled-handoff tests, pooled early integration, server HTTP ticket
SETTINGS policy and 425 policy remain open. The full goal remains active.

## Latest: 2026-09-28 HTTP client connection ownership

ClientConn shutdown now routes through a serialized per-generation owner. Close,
initialization cleanup, lifetime monitoring and HTTP transport aborts share this
owner. A released owner cannot close QUIC; a closed owner cannot be released.
Abort codes, reasons and return errors are retained. This also prevents delayed
request/body workers from aborting a replacement session after handoff.

Added private retireRejected: it requires joined session teardown with the actual
Err0RTTRejected cause, releases ownership, cancels old request lifetimes with that
cause and joins the lifetime monitor. Subsequent Close is harmless to QUIC; future
request lifetime admission retains rejection. It neither reconstructs sessions nor
replays requests. Public early dialing and recovery still need to invoke this
mechanism with correctly bound remembered SETTINGS and handle setup rejection.

Tests cover rejecting live/non-early-failed handoffs, cancellation of existing and
future request lifetimes, repeated retirement, late request aborts, preserved
ordinary aborts and 100 concurrent Close/handoff races. Full HTTP race suites pass
(http3 1.379s, http3conn cached), root vet passes. The initial ordinary-failure test
incorrectly expected another HTTP abort after AcceptUni transport failure; corrected
the fixture to verify client closure and refusal to transfer ownership. The complete
independent HTTP3 interop test selection was rerun after ownership integration.
The full implementation and validation goal remains active.

## Latest: 2026-09-28 independent HTTP rejection recovery

Added TestHTTP3EarlySessionRejectionRecovery against pinned upstream quic-go
v0.62.0 over real UDP, for QUIC v1 and v2. The fixture seeds an early-capable
TLS ticket, gates the resumed server handshake, initializes our early HTTP
critical streams and QPACK encoder, and queues a GET before releasing rejection.
It observes a real 0-RTT packet at the peer socket before opening the gate.

The test requires joined HTTP session teardown with Err0RTTRejected while QUIC
remains alive, explicit NextConnection recovery on the same connection, retained
TLS resumption with Used0RTT false, and rejection of the obsolete request handle.
A fresh public HTTP ClientConn then exchanges a successful request with the peer;
stale request cancellation does not break the reconstructed session, and the
server handler sees only the fresh request.

Three initial repeated race runs passed (1.377s). Ten repeated runs together with
HTTP settings-bound resumption and early-server TLS metadata gates passed (1.952s);
interop go vet ./... passed. This fixture supplies remembered defaults solely to
exercise rejection, and does not validate production ticket/settings provenance
or accepted early HTTP requests. Public early-client ownership/reconstruction,
server HTTP settings ticket policy and the broader goal remain unfinished.

## Latest: 2026-09-28 HTTP session rejection teardown

HTTP critical-stream lifecycle now preserves Err0RTTRejected as a session-ending
transport outcome without sending a QUIC connection close. This covers incoming
prefix/control/QPACK reads, including partial codec instructions whose parsers can
otherwise obscure the transport cause. The dispatcher cancels and joins workers;
a fresh session can subsequently own the transport. Ordinary peer FIN/reset and
protocol errors retain their existing fatal behavior.

Local critical-stream open/prefix/SETTINGS writes and early encoder initialization
also avoid aborting QUIC on rejection. Other setup/write failures remain fatal.
Canceled setup preserves its context cause. These changes let an endpoint discard
HTTP/QPACK state before NextConnection and reconstruction; public ClientConn
recovery ownership still needs wiring and is not claimed complete.

Tests cover eight prefix/control/QPACK rejection positions, retained error cause,
zero transport aborts, fresh session setup after joined teardown, encoder setup
rejection and rejection at each local stream opening/writing position. Full
http3conn/http3 race suites and root vet pass. Existing failure tests continue to
check fatal handling for ordinary critical-stream failures. Public HTTP early
request/recovery APIs and server ticket policy remain outstanding. Full goal active.

## Latest: 2026-09-28 HTTP ticket/settings binding

Added a per-connection TLS cache adapter and connected it to HTTP Dial. Tickets
received before validated peer SETTINGS are held privately, bounded to one latest
ticket, then paired with that connection's settings. Settings publication occurs
after successful encoder initialization/promotion. Tickets received later are
stored immediately with the same immutable settings. Nil cache deletion clears
pending state. Caller TLS configuration/cache objects are not replaced or mutated.

A versioned client-local record binds canonical HTTPS authority and local advertised
HTTP policy (header limit and DATAGRAM option), while existing QUIC metadata binds
the transport version. Restoration privately decodes session state and disables
early capability on absent, malformed, duplicate or mismatched HTTP metadata,
preserving ordinary resumption. It does not upgrade a ticket whose EarlyData flag
is false. Refreshed metadata replaces inherited HTTP records while retaining other
opaque Extra values. Ordinary Dial still disables early data.

Tests use real TLS-generated session state to cover deferred publication, scope
mismatch, ownership, cache deletion and concurrent access; codec tests cover
truncation, ambiguity, booleans and defaults. An initial deletion assertion assumed
that Go's LRU cannot retain a present nil entry; corrected it to check for a usable
ticket, which matches the adapter's existing handling. Full HTTP race tests passed
(1.284s; http3conn cached), root vet passed. Three repeated independent v1/v2 HTTP
dial resumption and early-server metadata checks passed (1.450s).

Public HTTP early dialing and rejected-session reconstruction remain unfinished.
Server-side authenticated HTTP settings ticket policy also remains required. The
full RFC/API/multipath/performance goal remains active.

## Latest: 2026-09-28 early HTTP session encoder integration

StartSession now accepts an explicit client-only remembered SETTINGS input,
copies it, and initializes/publishes an early encoder before live SETTINGS arrive.
A non-nil empty settings input uses protocol defaults. Peer() still distinguishes
actual received SETTINGS, and early CONNECT/HTTP DATAGRAM permissions remain
closed. Incoming live SETTINGS validate remembered compatibility and promote the
published encoder in place instead of replacing it or closing readiness twice.
Decoder feedback can now reference successfully encoded early sections before
SETTINGS is received.

Early encoder setup is covered by connection-context write interruption. Failure
cancels and joins the interruption callback, closes decoder state and aborts the
transport after an uncertain critical-stream write. Caller cancellation retains
its cause. Invalid role/settings/budgets are rejected before writer mutation.

Session-level tests exercise early readiness, feedback-before-SETTINGS, caller
slice ownership, in-place promotion, incompatible SETTINGS connection error,
blocked initial-write cancellation, default settings and server-role rejection.
The first test run had a fixture struct-literal compile error, corrected before
validation. Full http3conn/http3 race suites passed (1.327s / cached); root vet
passed. Three final focused early-session race repetitions passed.

This remains an internal opt-in: public HTTP early dialing, ticket/origin binding,
rejection session reconstruction and request replay policy are unfinished. No
public early-client capability is claimed. The complete goal remains active.

## Latest: 2026-09-28 early QPACK encoder promotion

Added fresh encoder initialization from remembered SETTINGS and an explicit live
SETTINGS promotion operation. Promotion validates the full remembered-setting
contract, then updates QPACK limits without replacing table contents, insertion
counts, protected references or outstanding decoder acknowledgments. Nonzero
maximum capacity stays fixed; a zero-capacity early encoder can begin using a
nonzero table after live SETTINGS. Increased blocked-stream and field-section
limits become usable while original local resource limits remain enforced.

Encoder operations and promotion share a lock. Invalid SETTINGS and uncertain
critical-stream writes retain terminal errors; no later encoding or promotion
continues after failure. Constructor state is fresh per connection and never
restores dynamic entries from a previous connection. The low-level table update
validates all limit changes before mutation.

Tests decode early and subsequent field sections, acknowledge references after
promotion, require increased blocked-stream allowance, exercise zero-capacity
activation and local policy caps, verify failure retention and run concurrent
encoding during promotion. Complete http3conn/qpack/http3 race suites passed
(1.543s / 1.289s / 1.694s), root vet passed, and focused early encoder tests were
rerun after stronger blocked-stream/concurrency assertions.

Session publication, ticket/origin binding, rejection session reconstruction and
HTTP request policy still need integration. This adds the encoder transition
component; it does not yet enable public HTTP client 0-RTT. Full goal active.

## Latest: 2026-09-28 remembered HTTP SETTINGS validation

Added an immutable remembered-settings value and a client early SettingsState
constructor. Remembered values are exposed separately from received SETTINGS;
ApplyPeer validates compatibility transactionally before publishing live values.
Unknown/reserved settings do not accumulate in remembered storage. Early values
are unavailable after live publication or terminal validation failure. Rejection
requires a fresh HTTP/QPACK session and ordinary SettingsState, not preservation
of early dynamic-table state.

Validation follows RFC 9114 section 7.2.8.2 and RFC 9204 section 3.2.3: general
limits cannot decrease, nondefault understood settings cannot disappear (including
field-section omission that would otherwise increase the limit), and nonzero
QPACK capacity must remain identical. Capacity mismatch uses
QPACK_DECODER_STREAM_ERROR; other incompatibilities use H3_SETTINGS_ERROR.
CONNECT/DATAGRAM enablement cannot be revoked after acceptance. Zero QPACK limits
can increase. Construction validates duplicate identifiers and boolean values.

Tests cover each setting's change/omission, defaults, ignored settings, ownership,
terminal-error retention, no partial publication, fresh-state recovery and
concurrent readers. Full internal/http3conn and http3 race suites passed (1.311s,
1.476s); root vet passed. This is a validation component, not yet an enabled HTTP
client early-data path. Ticket/origin binding, early encoder initialization,
live-limit updates, session reconstruction and server HTTP settings policy remain
required. The complete QUIC/RFC/API/performance goal stays active.

References: https://datatracker.ietf.org/doc/html/rfc9114#section-7.2.8.2 and
https://www.rfc-editor.org/rfc/rfc9204.html#section-3.2.3

## Latest: 2026-09-28 HTTP early dispatch TLS metadata

Found and fixed stale HTTP request TLS metadata for early-accepted connections.
ServeQUICConn previously captured TLS once during session setup, leaving later
requests with HandshakeComplete=false even after successful TLS completion. The
request dispatcher now obtains a fresh immutable value through an owner-supplied
concurrent-safe provider. Static TLS metadata remains supported for internal users.
Previously dispatched requests retain their original snapshot.

A new independent upstream HTTP/3 client test seeds a valid early ticket and address
token, holds server TLS input away from the client, sends a GET_0RTT request, and
asserts our handler executes before Finished. It then completes TLS and sends an
ordinary GET on the same connection, requiring fresh completed TLS metadata and
an unchanged first-request snapshot. Both versions pass. This does not make the
metadata a transport-origin early-data marker or implement client early HTTP;
SETTINGS restoration, rejection/session reconstruction and retry policy remain.

Validation: complete http3 and internal/http3conn race suites passed (1.218s and
1.416s); root vet passed. Three repeated selected HTTP server/early-response
interop race runs passed (1.647s). The full QUIC/RFC/API/performance goal remains
active; the earlier independent compatible-early acceptance gap remains open.

## Latest: 2026-09-28 Picoquic early/version validation

Added optional unmodified-Picoquic gates for early delivery and compatible-version
rejection recovery. A seeded v1 ticket permits an actual early HTTP/0.9 request;
the relay withholds server input until early output and discards client 1-RTT
packets until the complete early response is verified. The same-version control
passes, ruling out reliable 1-RTT retransmission as the explanation for delivery.

The v1-to-v2 strict acceptance attempt fails with early rejection. Pinned peer
source checks ticket version and derives keys using the current version_index;
this is consistent with the observation but was not instrumented at the TLS
callback. Kept a strict opt-in known-failure test. A separate normal gate verifies
that this rejection permits explicit NextConnection recovery, invalidates stale
handles across ID reuse, and completes a fresh request under v2 without a VN
packet. This does not substitute rejection for the outstanding acceptance target.
Peer binaries/source remain unchanged; process files use temporary directories.

Validation: three repeated control/recovery race runs passed (7.348s), interop vet
passed. The opt-in acceptance test reproduced rejection (1.469s) and is recorded
as a failure. Independent combined acceptance, reverse-role early switching and
v2-to-v1 peer support remain open, alongside the broader RFC/API/performance goal.

## Latest: 2026-09-28 early data with compatible version negotiation

Integrated early data with compatible v1/v2 switching in both directions. RFC 9369
section 4.1 requires original-version 0-RTT even after learning the negotiated
version, while Handshake and 1-RTT use the negotiated version. TLS now permits a
client transition with only early write keys installed, derives early secrets
using the original version, and combines server version selection with early
acceptance. Ticket acceptance remains bound to the original first flight; newly
issued tickets retain negotiated-version provenance. Early headers use the
original version and servers accept only that version for 0-RTT.

Initial combined tests found four acceptance/reorder failures because the receive
path selected original Initial keys for every differing-version long header.
Restricted this selection to Initial packets. The combined STREAM matrix now
passes acceptance, rejection, lower limits, reordering and loss in both directions;
DATAGRAM matrices additionally cover ownership, size bounds and unsupported limits.
A targeted test sends early data after receiving the selected-version Initial,
delays it until server Initial-key retirement, verifies delivery, and rejects
negotiated-version early headers. It also checks new-ticket early resumption in the
selected version and prohibits reusing that ticket in the original version.
Public early dialing now covers compatible negotiation with Retry and explicit
rejection recovery, so the configuration exclusion was removed. Multipath's early
exclusion remains explicit.

Validation: full root race suite passed (root 6.950s, transport 39.298s), root vet
passed, and existing independent resumption/early gates passed (1.794s). The final
expanded ticket-provenance/late-early-packet race test passed (1.376s). Independent
interop for the early-plus-compatible combination remains pending; these local
results do not prove it. HTTP early policy, multipath combinations, post-return VN
recovery, exhaustive RFC/API review and performance goals remain open.

Reference: https://www.rfc-editor.org/rfc/rfc9369.html#section-4.1

## Latest: 2026-09-28 independent early DATAGRAM gates

Added independent upstream DATAGRAM interoperability tests in both roles and
versions. Client cases send only DATAGRAM application frames during early traffic,
observe three actual 0-RTT packets before releasing server TLS, and cover empty,
owned-buffer and 1130-byte boundary payloads. Acceptance, server rejection and
intentional packet loss each verify subsequent fresh messages and responses.
Server cases cache NEW_TOKEN evidence on the same listener and assert zero Retry
packets, hold back the server TLS flight, consume accepted early DATAGRAM data
before client Finished, and queue a server response before Finished. The rejection
control disables TLS tickets on the second handshake while retaining valid address
evidence, separating TLS rejection from admission loss.

Initial runs timed out expecting unreliable traffic after the upstream ordinary
listener dropped early packets and after our Retry discarded pre-validation data.
The client fixture now uses upstream ListenEarly (also applied to the STREAM
gate); server acceptance/rejection uses valid-token admission. No transport code
changed or delivery guarantee was added. A partial fixture edit initially left
the server listener replacement in place; inspection corrected it before final
validation. These failures are not counted as interoperability passes.

Validation: three repetitions of the full selected resumption/early race group
passed (2.780s), and interop vet passed. Final DATAGRAM-only repetitions additionally
assert zero Retry. One repetition exposed a test-only in-order assumption when
the boundary payload arrived first; accepted payloads are now checked as an exact
multiset, consistent with unordered DATAGRAM delivery. Ten final DATAGRAM race
repetitions passed (2.300s), and final interop vet passed. Complete loss/reorder/Retry
and changed-limit interop matrices,
HTTP early policy, extension combinations and post-return VN recovery remain.
The overall RFC/API/performance goal remains active.

## Latest: 2026-09-28 explicit rejection recovery

Added Conn.NextConnection(ctx), following the pinned upstream API's same-Conn
transition semantics. Stream opens and accepts report Err0RTTRejected after TLS
rejection until an explicit transition waits for successful TLS completion and
releases that gate. The owner serializes the transition with stream operations;
no wrapper aliases or duplicate mutex/state ownership are introduced. Calls are
idempotent and concurrent-safe. Existing rejected handles remain invalid after
stream-ID reuse. Cancellation preserves its context cause and does not release a
pending gate; handshake failure cannot enable recovery. Application replay remains
explicit. DATAGRAM operations retain their existing independent queue semantics.

Public v1/v2 tests cover all six open/accept entry points, eight concurrent
transitions, accepted-early no-op transitions, pre-completion deadline/cancellation,
custom cancellation causes, stale FIN handles, fresh ID reuse and cancellation
isolation. An early-listener shutdown test also checks a transition fails when TLS
cannot complete. Independent upstream client-role gates now cover both acceptance
and rejection across listener instances with shared ticket keys. They observe an
actual early wire packet, reject it, recover, then verify a distinct fresh payload
on the reused stream ID after a stale handle cancellation.

Validation: full root race suite passed (root 7.041s, transport 38.981s); root vet
passed. Three repetitions of the resumption/early interop group passed (2.392s);
a strengthened distinct-payload client interop check passed (1.366s). Focused public
race tests were rerun after adding cancellation-cause propagation. Earlier ticket
policy timeout causes remain unverified. Remaining early work includes independent
DATAGRAM coverage, post-return VN recovery, extension combinations and HTTP policy.
Full API/RFC auditing, multipath validation and performance targets remain open.

## Latest: 2026-09-28 early listener acceptance

Added ListenAddrEarly and EarlyListener. Endpoint admission queues a connection
once server application keys and validated peer transport limits are installed;
ordinary listeners still require confirmation. Allow0RTT remains an independent
replay-policy opt-in. Servers can open streams and send DATAGRAMs with 1-RTT keys
before client Finished. Public documentation calls out incomplete client
authentication and the HandshakeComplete / Context signals.

Endpoint tests cover both acceptance policies, early server stream/datagram
operations, eventual completion and no duplicate acceptance. Public v1/v2 UDP tests
stop the client after Retry to prove acceptance before Finished, exercise canceled
Accept, stream and datagram writes, and listener shutdown during the unfinished
handshake. Initial tests lacked peer uni-stream credit; corrected fixtures advertise
that credit explicitly. Independent upstream tests now hold the server TLS flight
(after permitting Retry), read the actual early request on the server, and queue a
server stream response before releasing the flight. Both versions complete and
verify the response and Used0RTT state.

Validation: full root race suite passed (root 6.888s, transport 39.535s); root vet
passed. Three repetitions of the resumption/early interop group passed (2.469s).
Previously documented intermittent ticket-policy timeouts remain unexplained.
NextConnection rejection-generation semantics, post-return version fallback,
combined-extension and HTTP early policies, and full API/security coverage remain.
The overall goal remains active.

## Latest: 2026-09-28 public early dial and independent acceptance

Added experimental DialAddrEarly and server Config.Allow0RTT. Early dialing
returns with usable remembered keys, or waits for establishment without a usable
ticket. The TLS handshake detaches from dial-context cancellation after return;
transport handshake timeout and connection lifetime still govern it. Server opt-in
requires the application to tolerate replay; ordinary DialAddr/default servers
retain rejection. Listener.Accept continues to wait for confirmation. Unsupported
multipath/compatible-switching combinations fail configuration explicitly.

Real UDP v1/v2 tests hold the resumed server handshake until the client has opened,
written and FIN-closed an early stream. Tests cover acceptance, rejection reported
after FIN, explicit fresh stream-ID reuse without replay, stale cancellation,
Used0RTT state and canceling the dial context after early return. New independent
upstream gates cover accepted STREAM delivery in both roles and both versions.
Client-role tests observe an actual early packet at the peer socket before releasing
the TLS gate; server-role tests gate peer input until its early packet leaves.

Validation: full root race suite passed (transport 38.984s); root vet passed.
Independent acceptance gates passed (1.516s initially). A broader resumption/early
run hit a timeout in existing TestServerResumptionTicketPolicy/v1/custom=true/
selected=true during upstream DialAddr. That case plus acceptance passed on rerun
(1.498s). A second broader run timed out in the analogous v2 case. Five standalone
policy repetitions then passed (2.190s), and three repetitions of the full selected
resumption/early group passed (2.406s). Stage/deadline diagnostics were added without
changing timeouts. The intermittent cause remains unverified; neither failed run
is counted as a pass.

Public API parity remains incomplete: no early Listener.Accept variant or explicit
NextConnection transition yet. Rejection currently invalidates old streams while
allowing explicit fresh opens after handshake completion on the same Conn. VN
received after early return fails that connection rather than replacing an exposed
handle. Independent DATAGRAM, combined extension/HTTP early policies, fuller
cancellation/handle concurrency and security audits remain. Full goal active.

## Latest: 2026-09-26 handshake notification and state publication

Added public HandshakeComplete and ConnectionState.Used0RTT. Public connection
state now uses immutable snapshots published through an atomic pointer. Both
established constructors register owner-side completion and confirmation observers;
completion publishes the state before closing the notification channel, while
confirmation refreshes capabilities that require HANDSHAKE_DONE. Multipath's
pre-activation receive path also signals confirmation. Failure, draining and
shutdown release pending observers without falsely signaling handshake success.

Tests cover concurrent readers during a real v1/v2 engine handshake, state-before-
notification ordering, snapshot retention after shutdown, early and late observer
registration, duplicate registration, failure cleanup, established public dial/
accept notifications, and confirmation in the multipath handoff path. The new
manual TLS fixture initially omitted ServerName; it was corrected to the fixture
certificate's IP identity and the root race suite then passed.

Validation: full root race suite passed (transport 39.136s); final multipath and
public notification refinements passed focused race tests; root vet passed;
independent resumption/early-rejection gates passed (1.706s). This enables state
reporting for future early connections but does not expose early dial/accept yet.
Next: early APIs, explicit server replay policy and rejection transition behavior,
then public/independent accepted-early-data coverage. Full goal remains active.

## Latest: 2026-09-25 rejection-safe public stream handles

Added Err0RTTRejected and a permanent atomic rejection marker on public stream
handles. The owner release callback overrides the retained send result even when
FIN previously closed the send context with context.Canceled. Rejected Read,
Write, Close, Release and WaitAcknowledged calls return the rejection sentinel;
repeated calls remain rejected after the UDP runtime stops. Cancellation methods
already check owner-side release state and cannot reach a reused engine ID.

The public adapter test simulates the release callback while a live engine stream
has the same numeric ID, for both QUIC versions and both open/FIN-closed handles.
Stale reads/writes/FIN/cancellation/release/delivery waits leave the live stream's
bidirectional exchange intact. This is explicit adapter-boundary coverage, not
public early network negotiation. The real engine rejection test also now requires
AutoReleaseStream to report rejection for its FIN-closed early stream.

Validation: full root race suite passed (transport 39.119s), root vet passed,
and independent resumption/early-rejection gates passed (1.634s). Normal stream
release, delivery barriers, deadlines and completed reads remain covered by the
root suite. Public early dial/accept remains disabled.

Next: expose early client/server APIs with replay policy, handshake notification
and rejection transition semantics. Current public ConnectionState is stored once
at established construction; an early connection needs safely refreshed snapshots.
Then run actual public early rejection/ID-reuse and independent acceptance tests.
Combined compatible-switching, multipath and HTTP cases remain unfinished.
Full goal remains active.

## Latest: 2026-09-25 early DATAGRAMs and endpoint routing evidence

The private client early gate now queues DATAGRAMs using the remembered peer
frame-size limit and a long-header budget reserving both maximum-length CIDs.
Current handshake limits apply after completion. Existing unreliable ownership
and recovery rules remain: sent payload is released immediately, duplicates are
suppressed, and loss or early rejection never replays a message.

V1/v2 tests cover owned payload, empty messages, duplicate packets, total early
DATAGRAM loss, opt-out/reduced-DATAGRAM-limit rejection, sent plus queued message
cleanup, absent remembered support, larger post-handshake limits, and the exact
1,200-byte packet boundary with two 20-byte CIDs. A separate endpoint test uses
an address token to admit a resumed connection, verifies unknown early packets
allocate nothing, delivers early STREAMs through routing, preserves the original
DCID alias for delayed DATAGRAMs after Initial retirement, rejects the wrong peer
address, and keeps endpoint Accept gated on confirmation. No endpoint code change
was required for this fixed-address admitted route.

Validation: full root race suite passed (transport 38.592s); vet passed; focused
early DATAGRAM and endpoint race tests passed. Existing independent resumption/
early-rejection gates also passed. Public early APIs remain disabled and there is
still no independent accepted-early-data evidence.

Next: public rejection-safe handles and early dial/accept policy. Inspection found
that the current public release callback only stores the read error; a FIN-closed
send handle can retain context.Canceled and report WaitAcknowledged success after
an early rejection. Fix and test that before exposing early streams. Also audit
connection state snapshots, handshake notification, cancellation, endpoint Retry/
NEW_TOKEN combinations, and combined compatible-switching/multipath/HTTP behavior.
Full goal remains active.

## Latest: 2026-09-25 internal client early STREAM sending and rejection

Connected the private early-data gate to remembered stream-credit installation,
early stream opening, long-header packet construction and TLS early write keys.
Early packets share application packet numbers/recovery while keeping AEAD seal
counts separate. Packet sizing reserves the worst-case long-header overhead;
forbidden early frames wait for application keys, and ACKs are never emitted at
early encryption. Application write keys stop early sending and retire early keys.

TLS rejection drops both transmitted and queued early application frames without
automatic replay, removes their recovery flight accounting without congestion loss,
resets streams/credit/IDs, and notifies stream callbacks. Packet numbers continue.
A remembered rejected packet-number prefix detects prohibited ACKs even after new
streams begin. Accepted incompatible remembered limits now produce a protected
PROTOCOL_VIOLATION close. Retry requeues early reliable frames for the new CID and
fresh packet numbers while preserving stream state and early-key use accounting.

New v1/v2 engine tests use real client-generated 0-RTT packets, verifying exact
4 KiB stream delivery before completion, reordered first flights, total early
packet loss recovered in 1-RTT, server opt-out/reduced-credit rejection, cleanup
of queued plus transmitted data, callbacks, fresh stream-ID reuse without replay,
rejected-packet ACK failure, and Retry with a 20-byte CID and 700-byte token.
The credit-promotion handshake test now traverses actual early packet sending.

Validation: full root race suite passed (transport 38.690s); vet passed; independent
resumption/early-rejection gates passed (1.620s). Subsequent rejected-ACK/close and
packet-sizing refinements passed focused race tests. Public connections still do
not enable early data. Remaining: early DATAGRAM sending, public generation-safe
handles/rejection API and replay policy, endpoint routing/admission and independent
accepted-early-data gates, plus combined compatible-switching/multipath/HTTP cases.
Full goal remains active.

## Latest: 2026-09-25 stream credit transition for client early-data work

Separated stream send-credit snapshots from live peer transport parameters.
The transport now installs validated initial credit through one path that updates
connection credit, stream-count credit, and every existing sending stream. New
streams use the current snapshot with endpoint-role/direction-correct limits.
Client current-handshake limits install at completion; server limits remain
available after ClientHello for early receive. Promotion preserves consumed
credit and larger MAX_STREAM_DATA grants. Invalid snapshots cannot partially
change state.

Tests exercise all stream directions in both endpoint roles, existing/new streams,
count limits, consumed-credit preservation and larger explicit stream grants.
A real fresh-ticket/resumed v1/v2 handshake reserves 4 KiB using remembered
credit, then advertises 8 KiB and proves the same stream can use the larger limit
after completion without application reads or MAX_STREAM_DATA masking the change.
The test installs remembered credit explicitly and sends data only at 1-RTT:
this is transition infrastructure, not a completed client 0-RTT send path.

Validation: full root race suite passed (transport 38.683s), root vet passed,
and the subsequently added resumed-credit tests passed under race (1.397s).
Client early opening/packetization, rejection state reset and public APIs remain
unfinished. Follow RFC 9000 section 7.4.1 when connecting the sender: new limits
must never be used for early packets. Full goal remains active.

## Latest: 2026-09-25 internal server early-packet receive

Added a private transport early-data gate. Server receive selects TLS early keys
while sharing the application packet-number space, defers bounded pre-ClientHello
packets, discards known rejection/retirement, validates early frame restrictions,
and delivers STREAM data before handshake completion. Original-destination CID
acceptance survives Initial retirement for reordered early packets. Closing-state
receive also selects the correct early keys. Public constructors remain disabled;
compatible switching and multipath combinations are explicitly gated out.

The first authenticated 1-RTT packet starts a non-extending three-PTO early-read
retirement deadline, included in runtime deadlines and checked on receive/timeout.
Cleanup releases early keys; clients release early write keys once application
write keys are installed. This follows RFC 9001 section 4.9.3:
https://www.rfc-editor.org/rfc/rfc9001.html#section-4.9.3

Engine tests exercise fresh-ticket/resumed v1 and v2, early delivery, duplicates,
pre-ClientHello deferral, server opt-out and reduced-credit rejection, forbidden
CRYPTO, shared packet numbers/1-RTT ACKs, late early delivery after Initial
retirement, deadline non-extension, timeout retirement and post-retirement drops.
They inject client early packets and explicitly account for their recovery state;
they do not establish a functioning client early-send API or independent acceptance.

Validation: full root race suite passed (transport 38.427s); root vet passed;
independent resumption/early-rejection race gates passed (1.607s). Next: client
remembered-credit/send lifecycle, rejection reset, endpoint routing/public policy,
and independent accepted-early-data gates. Full goal remains active.

## Latest: 2026-09-25 internal TLS early-data foundation

Added internal NewWithEarlyData while retaining rejection in the default TLS
constructors and every public connection. The opted-in adapter preserves valid
version-bound ticket capability in private cache copies, derives early traffic
keys, reports TLS acceptance/rejection and discards rejected client write keys.
Server acceptance requires remembered credit compatible with current limits;
client handshake completion rejects accepted early data with smaller peer limits.
A ticket that did not authorize early data is never upgraded during resumption.

Tests cover both versions, fresh versus resumed handshakes, actual protected
0-RTT payload before client completion, server/client opt-out, reduced credit,
malformed opaque ticket transport state, a peer inconsistently accepting reduced
limits, shared-cache snapshot isolation and explicit early-key discard. These are
TLS/packet adapter tests, not completed engine/public 0-RTT or independent early
application delivery. Existing independent resumption/early-rejection race tests
passed (1.604s); full root race tests passed (transport 38.403s), as did root
vet. The public default remains unchanged.

Next: integrate early packet/key/stream lifecycle, remembered send credit and
rejection reset into the engine, then expose early APIs and run independent
acceptance/rejection gates. Compatible-switching, HTTP early-request semantics
and multipath handoff still need combined early-data coverage. Full goal active.

## Latest: 2026-09-25 standards and remaining-work reconciliation

Rechecked the IETF multipath record: draft-21 remains latest and in RFC Editor
first edit. Corrected stale PLAN.md status rows that still marked recovery and
PMTU as absent, resumption/key updates as unimplemented, and public HTTP/3 and
capsule work as unavailable. Kept each capability distinct from full conformance.

Added REMAINING.md with current evidence, unresolved requirements and the next
major feature sequence: accepted 0-RTT. Inspection confirms TLS still disables
early data at ticket issuance, resume/store events and private cache restoration.
Existing remembered-parameter codecs and independent rejection gates do not
prove accepted early data. The new sequence ties TLS state, remembered credit,
packet/recovery lifetime, public rejection semantics and independent combined
gates together rather than treating a TLS flag as implementation completion.

This checkpoint changes documentation and work prioritization only. No production
behavior or new runtime validation is claimed. The latest full root/default
interop/Picoquic race evidence remains in the preceding checkpoint. Goal active.

## Latest: 2026-09-25 synchronous socket output storage

Added SealInto/SealPathInto with explicit nonoverlapping caller-owned storage.
Owned Seal/SealPath and engine/endpoint entry points retain their contracts.
The UDP loop lends a 2048-byte buffer only during one packet construction and
finishes write/error handling before reuse. The connection reference is cleared
on every exit; larger packets allocate. Ordinary, path and migration output use
this scope, while cached close packets remain owned for retransmission.

All-cipher/version primitive tests verify encoding equality, input immutability,
base/path nonces, and buffer growth/shrinkage. Endpoint tests verify owned prior
results survive borrowed output, borrowed references are scoped, and cached close
packets remain independent. Sealing differential fuzzing passed 1,124,364 inputs
in ten seconds with two workers. Full root race tests passed (transport 38.683s)
and root vet passed. The full default interop race suite passed (65.950s),
with its documented known-failure exclusions. External Picoquic
compatible-multipath race fixtures passed
in both TLS roles (25.089s), retaining the known v2-origin peer exclusions.
The demo/driver SHA-256 digests still match the documented pinned binaries.

Five 100-exchange samples show median allocation declining from 4.65 MB to
3.36 MB (about 28%), with 48,350 allocations/exchange. Median time is 11.405 ms
versus upstream 5.437 ms; the small change from the preceding run does not
establish a throughput improvement. Focused sealing changes from 1568 bytes /
2 allocations to 32 bytes / 1 allocation. Scope and raw samples are in
[echo benchmarks](benchmarks/stream-echo.md). Retained send data and the full
standards/API/performance objective remain unfinished. Goal active.

## Latest: 2026-09-25 reusable ordinary receive decode storage

Added packet OpenInto with explicit caller-owned storage and immutable ciphertext;
existing Open/OpenPath continue returning owned output. Ordinary synchronous
connection receive processing reuses a bounded, lazily grown decode slice across
packets and candidate keys. Cleanup clears/releases it. Multipath pre-activation
retention and authenticated address-admission packets still use owned output.
The storage lifetime audit confirms stream receive copies, DATAGRAM queues copy,
TLS CRYPTO reassembly copies, and token/CID retention copies their input.

New gates cover all three ciphers and both versions, undersized/reused decode
storage, authentication failure followed by success, ciphertext immutability,
header append isolation, retained stream/datagram contents after overwriting
scratch, and cleanup erasure. Full root race tests passed (transport 38.697s),
root vet passed, and owned/borrowed differential packet-opening fuzzing passed
1,140,589 executions in ten seconds with two workers.

A 1400-byte packet-open microbenchmark drops from 1440 bytes / 2 allocations to
32 bytes / 1 allocation with warm reuse. Five 100-exchange end-to-end samples
show median independent allocation dropping from 5.92 MB to 4.65 MB (about 21%),
while time moves only from 11.71 to 11.54 ms. Reference median time is 5.43 ms,
so no robust throughput improvement or upstream superiority is claimed. Raw
samples and scope are in [echo benchmarks](benchmarks/stream-echo.md).
The first full default interop run exposed a stale test that still expected the
removed 128-local-stream capacity gate. It now opens all 100 bidi plus 100 uni
streams permitted by the independent peer, verifies a canceled blocking open,
then completes a bidi stream in both directions and checks replenished stream
ID 400. The focused race gate passes (1.478s); this test correction follows the
negotiated-concurrency change, not a decode-storage production fix. The full
default interop race rerun passed (65.819s), and both modules passed vet.
External Picoquic opt-in fixtures and documented known-failure cases were not
part of that default run.

Packet sealing, retained send data, and the full standards/API objective remain
unfinished. Goal active.

## Latest: 2026-09-25 end-to-end profiling and recovery timer allocations

Reran the v1 public-server 1 MiB echo comparison against quic-go v0.62.0 with
five 100-operation samples. The benchmark now explicitly configures 16 incoming
streams on both servers, preserving its original matched policy despite newer
public defaults. After receive bitmap work, median independent time is 12.143 ms
versus upstream 5.502 ms, with 6.01 MB versus 709 KB allocated per exchange.
These fresh samples are not an isolated attribution of the receive changes.

A separate 200-exchange heap profile identifies packet seal/open and retained
send payloads as about 58% of sampled allocation bytes. It also identified
avoidable probe-header/payload construction on every recovery timer update.
Timer arming now skips that sizing on clients and validated server paths;
unvalidated server amplification checks retain their original sizing and gate.
The adjacent five-sample run reduced median allocations from 59,466 to 51,216
(about 14%), bytes from 6.01 to 5.92 MB, and time from 12.143 to 11.712 ms. The
small timing improvement is preliminary; upstream remains about 2.16x faster
and uses about 8.35x fewer allocated bytes in this loopback workload.

Full root race tests passed (transport 38.387s), root vet passed, and both
benchmark variants passed under race with full payload/FIN verification. Raw
samples, profile, summaries, reproduction commands and limitations are in
[stream echo benchmarks](benchmarks/stream-echo.md). Packet-buffer ownership
and send retention are the next evidence-backed allocation targets. The full
conformance, feature-parity, and performance goal remains active.

## Latest: 2026-09-25 receive occupancy bitmap

Receive occupancy now uses one bit per payload byte. Validation checks occupied
overlaps before any state commit; new payload copies and contiguous reads process
spans within bitmap words. This preserves arbitrary ring capacities, gap stopping,
duplicate handling, and atomic conflict rejection. New tests cover capacities
1/7/63/64/65/127/128/129, repeated wraps, reversed byte delivery, partial reads,
and a rejected conflicting suffix after an otherwise new multiword prefix.
Receive fuzzing now varies ring capacity rather than always using 64 bytes.

A bytewise bitmap first attempt reduced memory but nearly doubled cycle time;
it was replaced with word-span processing before acceptance. Final sequential
push/read microbenchmarks improve from about 138/2673/36604 ns to
12.85/214.6/2729 ns for 64/1200/16384-byte chunks. First-byte receive allocation
falls from 131225 to 73880 bytes for a 64 KiB window. Raw samples, including the
slower intermediate version, and narrow benchmark scope are documented in
[bitmap benchmarks](benchmarks/receive-bitmap.md). No network throughput or
upstream superiority claim follows from these measurements.

Receive fuzzing passed 1,534,498 executions in ten seconds with two workers;
independent incoming-stream race tests passed (2.162s), retaining documented
v2 high-count key-update exclusions. Full root race tests passed (transport
38.703s), and root vet passed. Sparse active-payload
allocation and the broader conformance/API/performance objective remain open.

## Latest: 2026-09-25 lazy receive buffers and terminal release

Receive now separates configured capacity from allocated payload storage. Idle
streams, empty FINs, reset-only streams, and rejected first frames allocate no
payload arrays. The first retained byte allocates the circular window after
validation. Reset, consumed EOF, and complete canceled-data discard release the
arrays immediately; subsequent terminal duplicates do not recreate storage.
Flow credit, final-size checks, and window advancement remain covered by tests.

The local 64 KiB receive-allocation benchmark drops idle setup from about
131 KB to 152 bytes (including flow and receive objects), four allocations to
two. Empty FIN/reset-only cases have similar savings. First-byte allocation
remains about 131 KB and grows 16 bytes for the added capacity field. This does
not yet solve sparse-payload memory cost or establish network throughput gains.
Raw before/after samples and scope are in
[receive allocation benchmarks](benchmarks/receive-lazy.md).

Full root race suite passed (transport 38.749s), root vet passed, and independent
stream-limit race tests passed (2.212s), retaining the documented v2 high-count
key-update exclusions. A 10-second two-worker receive fuzz campaign passed
1,393,950 executions; this is smoke coverage, not exhaustive validation. The
full goal remains active.

## Latest: 2026-09-25 negotiated stream concurrency

Removed the 64-per-direction incoming, 128-local, 256-total-object, and
257-closed-range caps. Negotiated stream credit now governs concurrency; local
application opening governs outgoing allocation. Public defaults are 100 bidi
and 100 uni streams, negative values disable incoming streams, and values above
2^60 clip without mutating caller configuration, matching the pinned upstream
configuration behavior. The engine still validates protocol stream-count bounds.

A v1/v2 engine regression holds 2,408 streams across both initiators and both
types, transfers data and FIN, releases odd/even streams separately (301 holes
per type), verifies object cleanup, and exhausts replenished credit. Public tests
include 100/100 and 300/300 configurations. The deterministic flight helper now
allows 4,096 packets instead of 100 to handle these larger valid windows.

Full root-module race tests passed (transport 38.408s), and root vet passed.
Independent quic-go v0.62.0 tests exercise configured credit, blocking opens,
actual data on simultaneous streams, and replenishment in both TLS roles.
v1 300/300 and v2 100/100 pass; v2 300/300 times out after peer qlog key phase 1,
consistent with its already documented key-update mismatch. Those two cases
remain explicit opt-in strict failures, not passing coverage; see
interop/KNOWN_FAILURES.md. Receive buffers are
still eagerly allocated per stream, with a 64 KiB window cap; large-concurrency
memory efficiency remains unfinished. The wider conformance and API goal remains
active.

## Latest: 2026-09-25 candidate Initial key caching

Bounded compatible-version candidate keys are now reused across failed packet
authentication and transferred to active keys only after successful opening.
Retry clears the cache before changing the Initial key DCID; original-version
CRYPTO, Initial retirement, and transport cleanup also release it. Regression
tests cover both switch directions, unchanged active handshake state on invalid
input, persistent failure accounting, pre-Retry ciphertext rejection, successful
post-Retry selection, and key release.

The repeated invalid-candidate microbenchmark improved from roughly 3.35 us to
303 ns per packet, with allocations reduced from 101 / 10237 bytes to 2 / 96
bytes. This is a narrow local optimization, not an upstream throughput claim.
See [benchmark evidence](benchmarks/compatible-candidate.md). Full root-module
`go test -race ./...` passed (transport 38.175s), as did `go vet ./...`.
The separate independent-interop module was not rerun for this cache change.
Goal remains active.

## Latest: 2026-09-25 negotiated-version application key updates

Added TestCompatibleVersionKeyUpdates for both switch directions. It verifies
initial application keys and four successive generations against references
derived from in-memory TLS traffic secrets using the negotiated version; original-
version references cannot open the same ciphertext. It alternates update initiators
and drives packet loss, reversed delivery and duplicates, including acknowledged
PINGs so either endpoint satisfies the next-update guard. The initial test correctly
hit that guard when only an ACK-only response had been sent; explicit acknowledged
traffic now exercises the intended alternating sequence. No production fix was
needed. Secrets are not printed or persisted, and synthetic verification packets
are not injected into either engine packet-number space.

Compatible/key-related transport race gates passed (1.504s), and transport vet
passed. Packet RFC-vector tests remain the primitive-level reference; this new gate
checks transport version binding rather than claiming independent cryptographic
validation. Broader conformance/interop/performance work remains. Goal active.

## Latest: 2026-09-25 independent client-role compatible switching

Added Config.InitialVersion to separate first-flight choice from Versions
preference (zero retains existing behavior). Configuration requires the initial
version to be in the supported list. Fallback history now accepts either order of
the two supported preferences, while preserving original-version provenance and
authenticated selection checks. Public IPv4/IPv6 enablement and fallback tests
exercise a nonpreferred first-flight version.

TestPicoquicCompatibleMultipath starts our client in v1 with [v2,v1] preference
and verifies v2 negotiation against the unmodified pinned Picoquic server. The
relay observes the first-flight v1 header; both independent role fixtures require
zero VN packets. IPv4/IPv6 clean/loss gates retain greasing, exact downloads,
abandonment, repeated migration and migration during a response. Final combined
independent switching race gates passed (14 cases, 26.913s; v2-to-v1 peer-limit
skips remain explicit). Full root race suite passed (root 6.606s, transport
38.145s; other packages passed/cached), plus root and interop vet.

Independent v1-to-v2 switching now has evidence in both endpoint roles. V2-to-v1
independent evidence remains unavailable with this pin; broader conformance and
performance requirements remain unfinished. Goal active.

## Latest: 2026-09-25 coalesced packets across compatible switching

Added split-ClientHello gates that force an original-version ACK before server
selection. Both switch directions preserve Initial PN progression and receive/
recovery state across old-first, old-last, separate-old and late-old-prefix layouts.
ACK-only input does not prematurely lock the version; duplicate coalesced inputs
still complete the handshake.

The late-old-prefix cases reproduced a receive bug: first-packet version filtering
dropped fresh negotiated packets later in the datagram. Moved version rejection
to per-packet handling, including confirmed-path and closing receive paths. Path
fixtures accept a valid protected tail after either known version while retaining
unknown-version, truncated-boundary, different-CID and tamper controls. Closing
fixtures process a valid trailing close after a different-known-version prefix.
RFC 9000 section 12.2 was checked for independent coalesced packet processing.

Targeted race gates passed; full root race suite passed (root 6.845s, transport
38.146s; other packages passed or cached), and full root vet passed. See
COMPATIBLE_VERSION.md. The overall conformance/interop/performance goal is active.

## Latest: 2026-09-25 combined version-negotiation downgrade protection

Removed the temporary combined-negotiation configuration rejection and preserved
compatible offers on authenticated fallback attempts. A live forged-VN test first
reproduced an acceptance bug when the server subsequently switched back to the
first version. Validation now compares the replayed selection policy against the
fallback attempt version, separately from the final negotiated version.

Public relay gates reject both v1-to-v2-to-v1 and v2-to-v1-to-v2 forgeries with
0x11 and require an observed switched-back server Initial. Engine tables cover
authenticated lists including/omitting retiring versions, empty lists and valid
unswitched fallback. Ordinary fallback and public session/token reuse still pass.
Full root race suite passed (root 6.533s, transport 38.049s; other packages passed
or cached); full root vet passed. Broader independent switching and conformance
remain open. Goal active.

## Latest: 2026-09-25 independent compatible switching with Picoquic client

Added TestPicoquicClientCompatibleMultipath and a test-driver desired-version
argument using Picoquic public API. Both peers verify the negotiated version.
V1-to-v2 passes IPv4/IPv6, greasing, exact 1 MiB transfer, additional-path
validation, base abandonment and five probe/bulk impairment cases. The dedicated
race gate passed (ten cases, 4.181s). Peer library source remains clean/unmodified;
updated driver hash is in interop/picoquic/README.md.

V2-to-v1 times out: the pinned peer has no upgrade-from list for its v1 entry.
The strict clean diagnostic reproduced in 20.268s. That branch remains opt-in via
QUIC_INTEROP_KNOWN_FAILURES and is not counted as passing independent evidence.
An ordinary non-switching regression case also timed out late in a response; five
focused repetitions passed, but its cause remains unresolved. Both are recorded
separately in interop/KNOWN_FAILURES.md. A final combined ordinary/greased/compatible
reverse-role race run passed (50 cases, 14.350s, unsupported-direction skips);
interop vet passed. Independent opposite-direction and our-client switching,
combined negotiation and broader conformance/performance work remain. Goal active.

## Latest: 2026-09-25 public compatible version negotiation opt-in

Added Config.EnableCompatibleVersionNegotiation (default off). Versions controls
server preference and client offers, with v1/v2 as defaults. Initial attempts can
switch; authenticated incompatible fallback advertises only its chosen version,
so combined negotiation stays disabled pending its downgrade audit. Config version
slices are copied. Public connection state exposes the negotiated version.

Fixed public NEW_TOKEN storage to use the negotiated connection version rather
than the attempted version captured by the dial callback. Public IPv4/IPv6 stream
tests cover both switching directions and every enablement combination. Public
ticket/token tests switch, reuse on the negotiated version, and decline reuse
when initiating in the old version. Existing fallback and forged-downgrade tests
now also run with compatible negotiation enabled.

Full root-module race suite passed (root 6.583s, transport 38.347s; other packages
passed or cached); full root vet passed. Independent switching, explicit coalescing
and combined-negotiation policy remain open. See COMPATIBLE_VERSION.md. Overall
goal remains active.

## Latest: 2026-09-25 compatible switching through Retry and token issuance

Extended Retry handshake tests to both version-switch directions, normal/empty
Retry CIDs and clean/impaired delivery. Alternate-version Retry packets with
valid tags are ignored; subsequent client Initials retain the original version;
Retry tokens reject cross-version validation. Negotiated handshakes preserve
authenticated original and Retry CID bindings. Admission tests now exercise real
Retry issuance and address-bound token validation before switching, retaining
malformed-input and wrong-address controls and checking amplification validation.

NEW_TOKEN issuance/recovery tests now run after both switch directions. Tokens
validate only for the negotiated version; lost first transmissions recover with
the same token bytes, receive acknowledgments, and preserve fresh-token and
queue-full behavior. No production changes were needed for these gates. Targeted
compatible/Retry/address-token race tests passed (1.526s); transport vet passed.
Public configuration/runtime integration and combined-negotiation downgrade
validation remain open. The overall goal remains active.

## Latest: 2026-09-25 internal compatible version switching

Added internal CompatibleVersions configuration and engine selection. Servers
validate peer version/CID parameters before selecting and deriving negotiated
Initial keys. Clients authenticate an offered candidate Initial before committing
and lock the original version on original-version server CRYPTO. Packet-number
spaces, recovery, CRYPTO offsets and connection identity remain in place. Server
original Initial receive keys survive the switch until Initial-space retirement
and are cleared on connection resource release. Authenticated client transport
parameters continue to describe the original first flight.

Both switching directions pass clean/loss and normal/reversed/duplicate handshake
matrices. Dedicated tests cover tamper non-selection, old Initial retransmission
authentication, key retirement and original-CRYPTO version locking. Full transport
race suite passed (37.873s), handshake race suite passed (cached), and scoped vet
passed. Final targeted compatible gates passed (1.503s). Public switching remains
disabled; combined incompatible/compatible configuration is explicitly rejected
pending downgrade-policy integration. Retry, coalescing, resumption, public runtime
and independent switching gates remain. See COMPATIBLE_VERSION.md. Goal active.

## Latest: 2026-09-25 ticket provenance across compatible TLS switching

Added protected TLS ticket sequences for both version-switch directions: original
issuance, resumed handshake that switches, negotiated-version resumption, and
old-version non-offer. Distinct ticket/current transport limits expose stale or
missing state. The test initially failed in both directions: the server selected
a new version before its resume event, then discarded valid original-version
ticket limits. TLS now retains originalVersion for restoring offered tickets;
new ticket records continue using the negotiated version.

A second gate changes only client-local provenance to force a wrong-version
opaque ticket offer. The server independently rejects it, performs a full TLS
handshake and still switches versions. Early-data keys remain absent. Handshake
and transport race suites passed (1.592s / 37.807s), as did scoped vet. See
COMPATIBLE_VERSION.md. Public transport switching integration remains open;
this does not enable it or complete the overall goal.

## Latest: 2026-09-25 TLS boundary for compatible version negotiation

Reviewed RFC 9368/9369 and added a negotiating-server TLS constructor with
deferred transport parameters, a pre-key client version transition, and a version
accessor. Server selection occurs on parsed client parameters before converting
TLS secret events into version-specific packet keys; EncryptedExtensions carries
selected-version parameters. Existing transport paths keep their original behavior.

Protected TLS tests complete v1-to-v2 and v2-to-v1 handshakes with certificate
verification and application packet protection in both directions. Invalid
selection tests cover unoffered/unknown versions, inconsistent chosen versions,
malformed parameters and selector failure, requiring no exposed flight or keys.
Handshake and transport race suites passed (1.506s / 37.805s), and scoped vet
passed. Public compatible switching is still disabled. Detailed remaining
transport/key-retention/Retry/ticket integration is in COMPATIBLE_VERSION.md.
Goal remains active.

## Latest: 2026-09-25 independent IPv6 multipath and greasing

Extended all four Picoquic multipath gates to IPv4 and IPv6. The migration relay
now selects matching loopback socket families; the test-only peer client driver
accepts --ipv6 and preserves the entire local sockaddr while opening path one.
Peer library source remains unmodified. Existing exact payload, bit variation,
path identity, abandonment, migration and loss assertions apply to both families.

Race gates passed: IPv6 greased both roles (14 cases, 27.155s); IPv4 greased
regression (14 cases, 26.830s); ordinary Picoquic both roles/families plus shared-
relay upstream migration and keepalive regression (40 cases, 88.938s). Interop
vet passed. Updated driver hash and reproduction details are in
interop/picoquic/README.md. Positive independent greasing now covers same-family
IPv6 path changes, including our client migrating during a response. Mixed-family
migration, reordering, dedicated PMTU observations and broader RFC/API/performance
requirements remain open. Goal remains active.

## Latest: 2026-09-25 reverse-role independent QUIC-bit greasing

Added TestPicoquicClientGreasedMultipath: our server advertises receive support,
and the test-only external client driver enables greasing through public
Picoquic transport-parameter APIs before starting TLS. No peer library source
changed. v1/v2 each cover clean, request-probe loss, response-probe loss, combined
probe loss and three large-response losses. Every case verifies both bit values
in both directions, exact 1 MiB delivery, path-zero abandonment, distinct NAT
bindings and exact loss counts. Client observations accommodate ACK frequency.

Fresh pinned Picoquic/PicoTLS checkouts were built and verified clean; revisions
and executable hashes are recorded in interop/picoquic/README.md. Forward greased,
reverse ordinary and reverse greased race gates passed together (24 cases,
29.855s). The final reverse greased assertion passed three repetitions (30 cases,
9.290s); interop vet passed. Positive independent IPv4 greasing now covers both
roles. IPv6 independent path changes, reordered traffic and broader protocol,
API and performance requirements remain open. Goal remains active.

## Latest: 2026-09-25 positive Picoquic greasing/multipath/migration interop

Added TestPicoquicGreasedMultipath using the existing external Picoquic server
fixture and our public greasing configuration. Both versions pass with clean and
lost-first-probe paths, exact small/1 MiB downloads, base-path abandonment,
repeated path-1 migration and migration during a 1 MiB response. Every large
transfer must observe both short-header bit values in both directions. Exact
request/response probe-loss counts remain required.

The strengthened race gate passed (24.273s), and interop vet passed. The peer
binary SHA-256 is b6103d609b0497f026720f1333b8103c2bb565ef6886f18760b8ed63cc47f7ca,
at /tmp/quic-pinned-driver-repro/interop-build/picoquic/picoquicdemo. Its old source
checkout is gone; binary identity is recorded but source revision is not freshly
attested. The pinned source's support was independently checked on its official
repository. No peer or production implementation source was changed in this turn.

Positive client-role greasing now has independent IPv4 evidence. Reverse-role
and IPv6 independent migration, reordering, and broader RFC/API/performance
requirements remain open. The full goal remains active.

## Latest: 2026-09-25 greased migration and multipath transfer coverage

Parameterized the existing public primary-migration, additional-path-migration and
large multipath-transfer tests with greasing disabled/enabled. The v1/v2 lifecycle
matrix covers repeated primary migration with base QUIC and multipath, canceled
migration isolation, additional-path migration, base-path abandonment, surviving
path closure and migrated socket release. Existing non-greased coverage is retained.

Both-path 256 KiB transfers now verify substantial traffic on each path, exact data
and observed loss with greasing enabled as well. Per-path relays count cleared
client short-header bits: both paths must show them when enabled, neither when
disabled. These are own-to-own IPv4 tests; positive independent greasing, IPv6 path
movement, reordering and PMTU-specific observations remain open.

The targeted 20-case race matrix passed (2.027s), and root vet passed. No production
change was needed for these scenarios. The full goal remains active.

## Latest: 2026-09-25 IPv6 and packet-loss QUIC-bit wire matrix

Extended the transparent relay to IPv6 and deterministic packet loss. The complete
own-to-own wire matrix now spans v1/v2, IPv4/IPv6, all four receive-support pairs,
and clean/lossy paths (32 cases). Loss discards every seventeenth short-header
UDP datagram per direction, independent of its bit, while preserving forwarded
bytes. Each case checks successful delivery of 48 serial echoes and correct bit
permission behavior; loss cases require observed drops in both directions.

The matrix passed under the race detector (7.413s with the then-existing fallback
cases). The independent upstream fallback gate now also covers IPv6 and passed
its targeted race run. Interop vet passed. Positive independent greasing,
reordering and migration/multipath observations remain outstanding. Goal is active.

## Latest: 2026-09-25 on-wire QUIC-bit directionality and upstream fallback

Added a transparent UDP relay that counts short-header bit values while forwarding
original datagrams. Live v1/v2 tests perform 48 serial stream echoes for all four
client/server support combinations. Permitted send directions must exhibit both
values; unpermitted directions must never clear the bit. The observed run counted
107 short-header datagrams per direction, with 42–65 cleared in permitted directions.

A separate pinned upstream-server gate verifies fallback when our client advertises
support but the peer does not: both directions stay set-bit while stream echoes
succeed. Its v2 workload does not cross the separately documented key-update gate.
Positive independent greasing remains unproven; tests do not mislabel own-to-own
positive observations as independent extension interoperability. IPv6 relay and
impaired/migrating/multipath greasing coverage remain open.

All new tests passed three race repetitions; interop vet passed. Echo and relay
workers are joined on test cleanup. The full goal remains active.

## Latest: 2026-09-25 public QUIC-bit advertisement and randomized sending

Added Config.EnableQUICBitGreasing for opt-in receive advertisement. Packet
builders now randomize the authenticated bit after processed peer parameters grant
permission. A 256-bit crypto/rand cache avoids one random-source call per packet;
permission is checked on every use. The sender does not require its own receive
advertisement. Ordinary/closing long and short headers, multipath/PMTU output,
path closes and migration probes share the bit-selection helper. Stateless output
keeps the bit set; no remembered-token early greasing is implemented.

Live own-to-own public stream tests cover v1/v2 and all four client/server receive
support combinations. Unit tests cover unnegotiated sending, deterministic cached
bit consumption without altering other flags, entropy refill and permission removal.
Existing authentication/tamper tests were made independent of randomized builders.

Validation: full root race suite passed (transport 38.367s), full default nested
interop suite passed (56.365s), and root vet passed. Dedicated independent on-wire
greasing counts and expanded migration/multipath coverage remain; QUIC_BIT.md
records these limitations. The full goal remains active.

## Latest: 2026-09-25 QUIC-bit engine receive and routing integration

Centralized connection header parsing under local GreaseQUICBit policy, including
normal/coalesced input, buffered replay, closing and path receive/probe handling.
CID routers now identify zero-bit packets without normalizing them; the selected
connection retains authentication and policy authority. Unknown Initial admission
and automatic version selection use template policy. Internal engine configuration
can now advertise support; public configuration and randomized sending remain open.

Tests cover v1/v2, both roles, every local/peer permission combination and tampering:
peer advertisement never grants receive permission. CID routing preserves address
identity. Authenticated zero-bit Initials receive Retry only when local policy
allows, and Retry remains set-bit without processed peer support. Full root race
suite passed (transport 38.005s), as did targeted engine tests and root vet.
The unchanged goal remains active; QUIC_BIT.md tracks remaining integration.

## Latest: 2026-09-25 QUIC-bit greasing parser and authentication foundation

Reviewed remaining transport gaps and began RFC 9287 integration. Added an explicit
ParseHeaderWithQUICBit receive-policy entry point while retaining strict default
parsing. No input bits are rewritten; version mappings, CID bounds and packet
length rules remain unchanged. The engine still rejects advertisement until all
receive/routing paths and send permission handling are implemented.

Tests cover Initial/Handshake/0-RTT/1-RTT/Retry headers for v1/v2. Packet tests
seal/open zero-bit long and short packets for all three TLS cipher suites and
reject post-seal bit tampering. Header metamorphic fuzzing passed 1,589,995 inputs
in ten seconds with two workers. Wire/packet race tests and targeted vet passed.
QUIC_BIT.md records the remaining endpoint, negotiation, token and interop work.
The unchanged full implementation goal remains active.

## Latest: 2026-09-25 measured capsule parser allocation reduction

Added reproducible independent/upstream capsule codec benchmarks, with 64 capsules
per batch and empty/64/1200-byte payloads. Reusing wire-reader integer scratch and
avoiding payload-reader boxing during discard reduced independent parser batches
from 195 allocations/~2196 bytes to 2 allocations/112 bytes. Final isolated-run
median parse times were 1629/2231/2949ns versus upstream 2506/3165/3893ns, about
24–35% lower in this local microbenchmark. Nonempty capsule writing remains slower
than upstream. See CAPSULE_BENCHMARK.md and its raw before/after files for methodology,
limitations and the initial after-run timing interference.

Validation: public/connection/wire HTTP race tests passed; public capsule
fragmentation fuzzing passed 125,656 executions in a ten-second, two-worker run;
capsule/CONNECT/datagram independent race interop passed (2.086s); root vet passed.
No network throughput or universal performance gain is claimed. Goal remains active.

## Latest: 2026-09-25 capsule fragmentation and direct truncation errors

Fixed payload readers passing io.ErrUnexpectedEOF through without protocol error
classification. HTTP frame payloads now map it to H3_FRAME_ERROR, and capsule
payloads to request-scoped H3_MESSAGE_ERROR, activating automatic tunnel abort.
Unrelated source failures remain unchanged; exact-length payloads accompanied by
ordinary EOF remain valid. Tests check partial and complete reads, both framing
modes, error scope and sticky terminal state.

Added a public capsule metamorphic fuzzer comparing normal, one-byte, and data-plus-
EOF readers. It checks identical decoded sequences/errors, remaining lengths,
termination and exactly-once automatic abort. A two-worker ten-second run passed
7,381 executions. HTTP public/connection/wire race suites passed
(1.504s/1.498s/1.666s); root vet passed. Goal remains active.

## Latest: 2026-09-25 tunnel close/abort ordering

Fixed ServerTunnel.CloseWithError publishing a new request failure after normal
Close had already completed. Normal close and extension abort now share one
once-only operation, including failure publication, transport cancellation and
joining I/O. An abort no longer emits a redundant write reset with a different
fallback code. Later close/abort calls preserve the winning cleanup outcome.

Added server tests for normal-first, abort-first and concurrent ordering, retained
failure cause and a single write reset. Client tests race tunnel close/abort with
response-body cleanup and verify exactly one underlying close and slot release,
correct winning code, and rejection of late writes. Server close tests passed
20 repetitions under the race detector. Full HTTP public/internal race suites
passed (1.602s/1.478s); root vet passed. Goal remains active.

## Latest: 2026-09-25 automatic capsule abort and client tunnel cleanup

Added client Tunnel.CloseWithError through exchange-owned failure publication and
reserved-body cleanup. It resets the request, joins upload cleanup and releases
its request slot without closing the connection. NewCapsuleParser now binds to a
source's CloseWithError capability, including both public tunnel types, and invokes
it once on malformed capsule framing. Plain readers remain report-only. Original
connection-scoped HTTP frame errors are no longer incorrectly converted into
CapsuleError. Clean EOF and pending-payload errors do not abort.

Live v1/v2 client truncation tests verify peer-observed 0x10e and a subsequent GET
with one request slot, without an explicit client tunnel Close. Server-role tests
now remove the handler's manual abort and prove parser-driven reset and connection
reuse. Unit tests cover single abort, sticky errors and preserved connection errors.
The longer known-failing v2 policy sequence now has the stable subtest name
`sequential`, independent of the number of isolated cases.

Validation: HTTP race suites passed (public 1.373s); capsule interop race suite
passed (1.720s); full root race suite and root vet passed. Capsule extension-specific
semantics, impaired-network coverage and broader goal requirements remain open.

## Latest: 2026-09-25 sequential capsule v2 failure traced across key update

Added a small upstream qlog recorder to the capsule policy interop test. It records
only key phase and 1-RTT sent/received counters, never packet payloads or secrets.
Per-case logs and distinct healthy-request failure text locate the timeout more
precisely than the earlier checkpoint: the malformed informational response is
rejected and the peer reset check succeeds; the subsequent healthy GET times out.

A paired strict race run completed the v1 sequence at peer phase 1 with 109 received
and 66 sent 1-RTT packets. The v2 sequence failed the healthy GET at peer phase 1,
with 114 received and 90 sent packets. Immediately before its last case the peer
was still at phase 0 (90 received/56 sent). The pinned source initiates its first
key update after the early packet threshold, and its derivation still uses the
version-independent `quic ku` label. This trace is consistent with the documented
v2 key-update defect; it does not independently decrypt or compare those packets.

No production workaround or peer source change was made. Strict v2 group7 remains
an unresolved interop gate; default isolated tests still require same-connection
GET success after every malformed response. Goal remains active.

## Latest: 2026-09-25 live client capsule-policy rejection and isolation

Added independent negative response tests against quic-go v0.62.0 for forbidden
Content-Type/Content-Length, success statuses 204/205/206, and Capsule-Protocol on
403 or informational 103. Every isolated v1/v2 case verifies nil tunnel/response,
peer-observed remote request error 0x10e, and subsequent GET success on that same
connection with MaxConcurrentRequests=1. The full sequential v1 group also passes.

The longer sequential v2 group reproducibly times out at the final informational
case (two initial observed failures, eight-second deadlines). Later tracing below
locates the failure in its subsequent healthy GET, after successful reset evidence. It is retained as group7 under QUIC_INTEROP_KNOWN_FAILURES=1. This is an
unresolved validation limitation, not proof of a particular root cause. The pinned
peer's separately documented v2 key-update issue may be relevant, but this fixture
does not diagnose it. No default test result is evidence that this sequence passes.

Default negative-policy race tests passed (1.574s); interop vet passed. Default
capsule race suite also passed. Automatic malformed-payload handling and diagnosis
of the sequential v2 failure remain work toward the unchanged active goal.

## Latest: 2026-09-25 client capsule header/status policy integration

ClientConfig.CapsuleProtocols selects capsule policy by Extended CONNECT token,
independently of header presence or QUIC datagram support. Configuration validates
tokens and copies the map. ExchangeRequest preflights outgoing capsule headers
before concurrent upload/reception and supplies policy plus request protocol to
response validation before HTTP adaptation. This prevents an early successful
response from racing invalid outgoing capsule headers and preserves validation of
response content metadata otherwise ignored for successful CONNECT.

Tests cover prohibited response content metadata/statuses, informational and
rejection Capsule-Protocol, ordinary rejection bodies, request cleanup, no output
for invalid request headers, and configuration copy ownership. Public/internal
HTTP race suites passed (1.652s/1.522s); capsule/CONNECT/datagram independent race
interop passed (1.950s); root vet passed before the final preflight addition.
The live client capsule fixture now enables public policy. Automatic malformed
payload handling and live negative client policy interop remain unfinished.

## Latest: 2026-09-25 server capsule header/status policy integration

Server.CapsuleProtocols now selects header/status policy for registered Extended
CONNECT or HTTP datagram protocol handlers. Configuration rejects unregistered
selected tokens. Validated request :protocol selects policy before adapting or
handing headers to the handler. The response writer inherits that selection and
enforces successful/rejection/informational capsule response restrictions.

Tests exposed a cleanup-order bug: unflushed malformed response headers were
validated after normal body cleanup had already selected receive code 0x100.
ServeRequest now commits/validates pending response headers before that cleanup,
so response protocol errors select 0x10e in both directions. Tests cover prohibited
request/response metadata, statuses 204/205/206, rejection headers, ordinary
rejection content, pre-handler rejection and unselected extension isolation.

Validation: public/internal HTTP race suites passed (1.235s/1.282s); live capsule
and ordinary/Extended CONNECT interop race suites passed (1.817s); root vet passed.
The live server capsule fixture now selects public CapsuleProtocols. Client policy
and automatic capsule payload failure integration remain pending; goal is active.

## Latest: 2026-09-25 server capsule interop and extension request abort

Added ServerTunnel.CloseWithError for extension-selected stream aborts. It uses
request failure publication before transport cancellation, preserves the first
cause, cancels the handler context and joins tunnel I/O through normal cleanup.
It does not terminate the QUIC connection. Unit coverage checks message, datagram
and arbitrary extension codes, closed I/O, context cancellation and repeated aborts.

New live server-role capsule interop against quic-go v0.62.0 passes on v1/v2:
empty/small/7KB echoes, unknown capsule discard, clean FIN, truncated capsule
payload rejection with remote request error 0x10e, and a subsequent successful GET
on the same connection with MaxConcurrentRequests=1. Handshake and capsule-error
handling are explicit in the fixture; automatic endpoint capsule policy is pending.
All capsule interop race tests passed (1.791s); root go vet passed.

## Latest: 2026-09-25 public streaming capsules and independent interop

Added CapsuleType, CapsuleParser, CapsuleReader and WriteCapsule using our existing
independent wire codec. The API supports bounded streaming, shared reader position,
explicit discard, stale-reader rejection, recoverable pending-payload errors and
sticky malformed-input failures. CapsuleError exposes request code 0x10e and wraps
unexpected EOF. Writes accept io.Writer, validate before output and detect short
writes. No declared length drives allocation. Public API shape was checked against
upstream documentation; no upstream implementation was copied.

Unit tests cover fragmented/nonminimal headers, maximum declared lengths, truncated
headers/payloads, reader lifetimes, empty payloads and header/payload write failures.
Both-direction codec interoperability with quic-go v0.62.0 covers varint boundaries,
unknown types and empty/small/69KB payloads. Live client-to-upstream-server capsule
echoes pass over v1/v2 Extended CONNECT, including unknown-type discard and FIN.
The fixture owns handshake validation: automatic capsule endpoint policy and stream
reset integration are still pending, as is live server-role capsule interop.

Validation: HTTP/3 and internal HTTP packages passed race tests; capsule interop
race tests passed (1.615s); targeted go vet passed. The full goal remains active.

## Latest: 2026-09-25 capsule header parsing and explicit message policy

Added public http3.CapsuleProtocol and an independent, allocation-free Boolean
Item parser. It rejects repeated/malformed/non-boolean fields and validates
ignored parameters, including RFC 9651 dates and UTF-8 display strings. Numeric
bounds, quoted escapes, base64, parameter keys and whitespace are validated.

Internal MessageConfig.Capsules explicitly selects capsule header/status checks:
Extended CONNECT is required; successful capsule messages reject content metadata
and prohibited response statuses; unsuccessful responses reject Capsule-Protocol.
Errors retain request scope and sticky message failure. Ordinary message processing
does not infer capsule activation from the header. Public capsule tunnel selection,
streaming integration and independent capsule interoperability remain unfinished.

Validation: boundary and message-policy tests passed; HTTP/3 and HTTP connection
race suites passed (1.527s/1.289s). A two-worker, ten-second parser fuzz run passed
8,385 executions, including a UTF-8 oracle for percent-encoded display strings.
Full root-module tests and go vet also passed (transport tests 34.386s).
Standards checked: RFC 9297 and RFC 9651 on 2026-09-25.

## Latest: 2026-09-25 public server HTTP datagrams

Added Server.DatagramHandlers and server queue/payload configuration. Declared
protocol handlers enable Extended CONNECT and H3_DATAGRAM settings and receive
ServerTunnel datagram methods after AcceptTunnel. EnableDatagrams can advertise
without registered extensions. Invalid/duplicate protocol registrations and limits
are rejected. Serve and ServeQUICConn validate local QUIC datagram reception before
taking ownership; Listener.DatagramsEnabled exposes the immutable local setting.

Server acceptance now registers pending datagram state before parsing headers.
Queues stay bounded and unavailable until validated headers identify supported
semantics. Unsupported requests abort even when an early payload exceeded the
receive limit. Closed receive sides drop pending data. A per-request abort context
cancels handlers that never read the body. Local datagram-reset code 0x33 is retained
when stream errors race the abort observer. Transport calls remain outside locks.

The reverse independent fixture now uses the public Server and ServerTunnel APIs,
with two request queues echoing in reverse order and FIN ending datagram reception
before application EOF. Both public roles cover v1/v2 and IPv4/IPv6. New live tests
verify 0x33 request-only abort, handler cancellation without reads, connection/slot
reuse and preserved listener ownership after configuration rejection. Pending
semantics/ownership/overflow/oversize, abort context and registry validation unit
tests pass. Three datagram interop race repetitions passed (1.654 seconds).

HTTP connection/public HTTP3 race suites passed (1.779/1.435 seconds), both module
vet checks passed, full root race passed (root 6.083 seconds; unchanged packages
cached), and full default interop race passed (59.398 seconds). HTTP_DATAGRAMS.md
was consolidated into current implementation, evidence and remaining work.

Capsule-Protocol semantics and streaming integration, impaired-network datagram
validation, broader parity and performance evidence remain unfinished. The full
project goal remains active; this is not complete RFC or extension conformance.

## Latest: 2026-09-25 public client HTTP datagram tunnels

Added ClientConfig.EnableDatagrams and per-request receive queue/payload limits,
ClientConn.OpenDatagramTunnel, and Tunnel.SendDatagram/ReceiveDatagram. HTTP Dial
enables local QUIC receiving on a copied configuration; NewClientConn validates
local receive capability through Conn.DatagramsEnabled. Open requires peer QUIC
support, Extended CONNECT and bilateral HTTP settings before request HEADERS.
Extensions retain handshake/payload semantics; no capsule fallback is supplied.

Every client request is registered, including ordinary unsupported requests. The
session owns the receive loop; dispatcher abort callbacks reset only the bound
request outside locks. CloseWrite cancels/joins datagram sends before upload EOF;
Close/body cleanup detach request state. Receive completion is observed independently
of application EOF. Queue defaults are 16 payloads and 1200 bytes per payload;
overflow/oversize drop policy is documented. Peer/path send limits stay in QUIC.

The upstream-server datagram fixture now uses public client APIs for v1/v2 and
IPv4/IPv6, exact empty/small/900-byte echoes, FIN/close gates and caller configuration
immutability. New live negative tests cover absent HTTP/QUIC support and a datagram
on GET producing 0x33 without preventing a subsequent request. Three repetitions of
both roles plus negatives passed race (1.660 seconds). HTTP connection/public HTTP3
race suites passed (1.368/1.246 seconds), both modules passed vet, full root race
passed (root 6.154 seconds; unchanged packages cached), and full default interop
race passed (59.473 seconds). Abort/detach and limit validation unit tests passed.

Public server datagram configuration, pre-header/extension semantics handling and
handler lifetime binding remain unfinished; reverse interop still uses internal
assembly. Capsule header/streaming integration and impaired-network validation
remain open. Full parity/performance goals remain active and unproven.

## Latest: 2026-09-25 public Extended CONNECT negotiation and routing

Added ClientConn.OpenExtendedTunnel and ErrExtendedConnectNotNegotiated. Validated
protocol metadata is retained in request context separately from HTTP version and
headers. Header construction includes scheme/authority/path/protocol; Session waits
for SETTINGS and replaces caller-supplied permission with the authenticated server
advertisement before any request HEADERS are written. Ordinary RoundTrip still
requires an explicit CONNECT owner. OpenExtendedTunnel shares byte-tunnel status,
half-close, cancellation and cleanup with OpenTunnel.

Server.ExtendedConnectHandlers enables the local advertisement when nonempty and
routes tokens to extension handlers. Invalid keys/nil handlers fail configuration
validation. Unknown protocols receive 501. Session derives receive permission from
its actual local settings. ExtendedConnectProtocol(request) exposes the validated
token while Request.Proto remains HTTP/3.0. Handlers validate extension handshakes
before AcceptTunnel. This is not a WebSocket implementation or datagram API.

Tests cover exact pseudo-fields/path/query, token validation, metadata preservation,
missing/zero settings, canceled settings waits, refusal to trust a caller permission
flag, disabled receive permission and known/unknown routing. HTTP connection and
public HTTP3 race suites passed (1.354/1.312 seconds), and both modules passed vet.
Independent ordinary/Extended CONNECT tests passed three race repetitions (2.027
seconds) across both roles, v1/v2 and IPv4/IPv6, including 501 rejection and reuse.
Full default interop race passed (59.559 seconds). RFC 9220 was rechecked.

Public datagram settings/configuration, per-extension datagram semantics and tunnel
lifetime binding, Capsule-Protocol semantics and streaming integration remain open.
The previously recorded large v2 peer failure remains unresolved. Full project
scope, parity and performance validation remain unfinished; the goal stays active.

## Latest: 2026-09-25 public ordinary CONNECT server tunnels

Added http3.AcceptTunnel and ServerTunnel, retaining request/handler ownership.
Acceptance requires uncommitted successful CONNECT headers without length or
trailers, switches request framing to established CONNECT, sends the response,
and prevents subsequent ResponseWriter DATA writes. The tunnel exposes DATA I/O,
CloseWrite and interruptible Close. Reads and writes have separate serialization;
Close interrupts before joining active operations. Handler return finishes sending
and closes receiving unless the tunnel was already aborted. The existing request
worker retains admission, cancellation, panic handling and QPACK cleanup.

Unit tests cover normal half-close with unread request data, writes after FIN
without canceling reception, invalid/repeated handoffs, blocked-read interruption,
and connection-scoped H3_FRAME_UNEXPECTED for trailers after acceptance. HTTP
connection race suite passed (1.384 seconds), public HTTP3 passed, and both module
vet checks passed. An upstream HTTP/3 client exercises the public server API for
v1/v2 over IPv4/IPv6, exact echo, both half-close directions and an ordinary request
after tunnel cleanup. Three reverse-role race repetitions passed (1.489 seconds).
Full default interop race suite passed (59.221 seconds). RFC 9114 Section 4.4
rechecked for DATA-only tunnel framing and half-close behavior.

Public Extended CONNECT negotiation, datagram/capsule APIs, pooled client tunnel
opening and deadline methods remain unfinished. The larger upstream v2 transfer
failure recorded above remains unresolved. The full goal remains active.

## Latest: 2026-09-25 public ordinary CONNECT client tunnels

Added ClientConn.OpenTunnel and Tunnel Read/Write/CloseWrite/Close. The URL remains
bound to the authenticated proxy origin; Request.Host supplies the target
host:port. The existing concurrent exchange owns bounded DATA upload, response
framing, QPACK cleanup, GOAWAY admission and request cancellation. Explicit
AllowConnect admission keeps ordinary RoundTrip from accidentally owning tunnels.
Request body/length/trailers are rejected by the tunnel API. Non-2xx responses
remain readable; successful response.Body is the tunnel. Close releases the
request reservation, even after receive EOF. Writes hand bytes to the bounded
upload worker and do not promise delivery; CloseWrite submits EOF to that worker.

New independent upstream-server tests cover exact echo, both half-close directions,
read/write cancellation, rejection bodies and subsequent ordinary requests with a
single available request reservation. v1/v2 and IPv4/IPv6 passed three race
repetitions (1.485 seconds). HTTP connection/public HTTP3 race suites passed
(1.531/1.266 seconds); updated tunnel unit tests passed (1.255 seconds). Both module
vet checks passed. Full default interop race passed (59.318 seconds).

The initial 66,000-byte v2 echo stalled in both families after partial reception,
consistent with the separately documented pinned upstream key-update problem;
this test did not independently diagnose the key phase. Default v2 uses 12,000
bytes to isolate tunnel lifecycle. QUIC_INTEROP_KNOWN_FAILURES=1 retains the larger
v2 gate, which is not claimed passing. v1 still exercises 66,000 bytes. RFC 9114
Section 4.4 was consulted for CONNECT framing.

Public server handoff, Extended CONNECT negotiation, datagram/capsule APIs, pooled
tunnel opening and tunnel deadlines remain unfinished. This is concrete public
client progress, not complete tunnel parity. The full goal remains active.

## Latest: 2026-09-25 receive completion before application EOF

Added transport WatchReceiveClose and public ReceiveStream.ReceiveContext. FIN
completion requires all bytes through the final offset; remote reset, local
cancellation and connection termination also notify. Normal completion preserves
unread data and reports io.EOF without changing send-side lifetime.

DatagramStream now requires independent receive completion and binds it atomically
with request registration. An observer wakes receivers, while synchronous checks
at registration/delivery/dequeue prevent delayed callbacks from admitting late
payloads or aborting already closed unsupported requests. Cleanup stops observers.

Tests cover v1/v2 FIN gaps, completion before acceptance, reset/cancel causes,
one-shot notification, preserved unread bytes, connection shutdown, delayed
callbacks and waiter wakeup. Full root race passed (transport 38.145 seconds);
after the registration refinement, HTTP connection/public HTTP3 race suites passed
(1.496/1.291 seconds). Both independent HTTP datagram roles passed three race
repetitions (1.548 seconds), including upstream FIN closing datagram reception
before application EOF in the reverse-role IPv4/IPv6 and v1/v2 matrix. Root vet
passed. RFC 9297 Section 2.1 rechecked against the RFC Editor text.

Public HTTP tunnel/endpoint integration, capsule semantics and impaired-network
HTTP datagram tests remain open. The full project goal remains active.

## Latest: 2026-09-25 transport-bound HTTP datagram request lifecycle

Added internal DatagramStream binding raw request stream operations to dispatcher
state: observed FIN/reset closes receiving, deadline errors do not; Close and
CancelWrite cancel/join sends before transport termination. Peer STOP_SENDING
cancels pending sends through a transport send-context observer, also checked
synchronously on admission. Release unregisters the request and stops observation.

Lifecycle tests passed three race repetitions (1.477 seconds). The reverse live
upstream-client fixture now uses the wrapper for datagram operations and cleanup;
three repetitions of both roles passed (1.563 seconds). HTTP connection/public
HTTP3 race suites passed (1.289/1.465 seconds), and affected/root HTTP and interop
vet passed.

This is not a public tunnel API. Receive FIN still needs a transport notification
before application EOF consumption to provide strict immediate receive closure.
That limitation and remaining integration work are explicit in HTTP_DATAGRAMS.md.
The full project goal remains active.

## Latest: 2026-09-25 reverse independent HTTP datagrams

Added TestHTTPDatagramsIndependentClient using pinned upstream quic-go's real
HTTP/3 client against our internal server session assembly. Two Extended CONNECT
requests carry distinct datagrams; both are sent before reading and echoed in
reverse request order to verify exact demultiplexing in each implementation.
The fixture covers v1/v2 over IPv4/IPv6 and explicitly performs request lifecycle
cleanup. Five race repetitions of both independent datagram roles passed (1.765
seconds); interop vet passed. Only tests/docs changed this turn.

This is test-only endpoint assembly. Public tunnel dispatch, automatic FIN/reset
integration, capsules and impaired-network tests remain open. The complete
project goal stays active.

## Latest: 2026-09-25 live independent HTTP datagrams

Added TestHTTPDatagramsIndependentServer against pinned upstream quic-go v0.62.0.
Our client uses the independent QUIC transport, internal session/settings/QPACK
and datagram APIs to open a test-only Extended CONNECT echo protocol. It checks
peer settings and decoded 200 response, then verifies exact empty/small/900-byte
datagram echoes. Both v1/v2 and IPv4/IPv6 pass five race repetitions (1.453 seconds).
The full default interop race suite passed (59.191 seconds), and interop vet
passed. The fixture explicitly closes send before FIN and cleans up
request receive state. No production behavior changed.

Public HTTP tunnel/request integration, reverse-role independent datagrams,
capsules and impaired-network coverage remain open. The full project goal remains
active; docs/HTTP_DATAGRAMS.md records the test's precise scope.

## Latest: 2026-09-25 request-send closure under backpressure

A new regression reproduced CloseSend hanging behind a transport send waiting
for queue capacity. Per-request send contexts now cancel that operation before
CloseSend joins its gate. Dispatcher shutdown also cancels request send contexts;
receive-side closure alone still permits sending. Gate waiters observe request,
session or caller cancellation, and send admission checks cancellation again
after acquiring the gate.

The pre-fix regression failed after one second. Five focused post-fix race
repetitions passed (1.473 seconds), including the existing dispatch/session tests.
HTTP connection/public HTTP3 race suites passed (1.428/1.465 seconds); vet passed.
Public HTTP tunnel/request lifecycle integration remains open; the full goal is
still active.

## Latest: 2026-09-25 cancellation-aware HTTP datagram send path

Added Session.SendHTTPDatagram with bilateral negotiation, request ownership,
extension semantics and send-side lifetime checks. A per-request gate serializes
sends with CloseSend while allowing callers waiting behind another send to cancel.
Receive-side closure remains independent. Session cancellation interrupts blocked
transport sends. QUIC Conn.SendDatagramContext exposes cancellation of send-queue
capacity waits; existing SendDatagram delegates with a background context.
QUICTransport implements the HTTP sender interface.

Three targeted race repetitions passed (1.418 seconds), including exact wire
payload, rejected-send isolation, receive-FIN independence, caller timeout behind
a blocked send and preservation of a session cancellation cause. Request owners
must still wire CloseSend before FIN/reset and public HTTP APIs remain disabled.
Full root race tests passed (transport 38.361 seconds) and root vet passed.
The full goal remains active.

## Latest: 2026-09-25 session-owned HTTP datagram receiver

Added optional DatagramSessionConfig tying dispatcher/receive-loop lifetime to
Session. Startup validates local advertisement and complete receiver configuration.
Shutdown cancels and joins the loop, clears queued datagrams and wakes consumers;
malformed wire datagrams abort the connection with H3_DATAGRAM_ERROR. QUICTransport
now adapts public ReceiveDatagram as a source. Request registration, FIN/reset
propagation and public endpoint activation remain unfinished.

Five focused race repetitions of session shutdown/malformed-input and dispatcher
tests passed (1.534 seconds). The lifecycle fixture also now verifies successful
payload delivery before shutdown. HTTP connection/public HTTP3 race suites passed
(1.284/1.499 seconds), and vet passed. The full project goal remains active.

## Latest: 2026-09-25 bounded HTTP datagram receive dispatcher

Added a receive dispatcher with bounded request and queue/payload limits, owned
payload storage, unsupported-request abort decisions and connection-error handling
for malformed datagram IDs. Its source loop calls request abort callbacks outside
the mutex. Blocking receives use lazy broadcast notifications and wake on data,
cancellation, request removal/closure and dispatcher closure. Shutdown clears
queued payloads and rejects subsequent registration. No public API advertises or
routes HTTP datagrams yet; session/request lifecycle wiring remains required.

Tests cover empty datagrams, ownership, overflow/oversize drops, unopened/closed
IDs, budget release, error scope, callback reentrancy, concurrent close, installed
waiter wakeups and preservation of payloads for canceled waits. Five focused race
repetitions passed (1.401 seconds). HTTP connection/public HTTP3 race suites
passed (1.214/1.272 seconds), and vet passed.
The full goal remains active; see docs/HTTP_DATAGRAMS.md for integration gaps.

## Latest: 2026-09-25 HTTP datagram settings negotiation

SettingsState now retains H3_DATAGRAM in local/peer snapshots and provides a
symmetric 1-RTT send-negotiation gate requiring both advertisements. Tests cover
both roles and absent/zero/one values, pre-settings denial, malformed/repeated
settings, live Session control-stream publication and concurrent snapshot access.
Three focused race repetitions passed (1.493 seconds); HTTP connection/public
HTTP3 race suites and vet passed. No public HTTP datagram API is enabled yet:
request semantics/lifetime, transport capability and dispatch are still required.
The full project goal remains active.

## Latest: 2026-09-25 RFC 9297 wire foundation

Implemented HTTP/3 datagram quarter-stream-ID codecs and SETTINGS_H3_DATAGRAM
boolean validation. Added streaming capsule mode to the existing bounded framing
reader: capsule types do not inherit HTTP/2 frame prohibitions, and truncation
uses request-scoped H3_MESSAGE_ERROR. Payload lengths do not drive allocation.
Live HTTP/3 still does not advertise or dispatch datagrams/capsules.

Tests cover boundary stream IDs, nonminimal varints, empty datagrams/capsules,
unknown capsule types, fragmented input, every truncated prefix, huge declared
lengths and transactional invalid settings/IDs. HTTP wire/connection/public
HTTP3 race tests and vet passed. Five-second two-worker fuzz campaigns passed:
1,000,361 datagram inputs and 261,564 capsule inputs. These are bounded smoke
campaigns, not complete robustness evidence.

RFC 9297 was rechecked against the RFC Editor text. docs/HTTP_DATAGRAMS.md tracks
negotiation, request routing/lifetime, Extended CONNECT, header semantics and
independent interoperability still required. The full project goal remains active.

## Latest: 2026-09-21 independent preferred migration after original-path loss

Extended TestPicoquicPreferredServer with a real UDP relay that drops the first
client datagram and the first server datagram, counting both drops. The original
handshake traverses the relay; preferred migration bypasses it and changes the
client source tuple seen by the unmodified Picoquic server. Exact 64 KiB download
and FIN still follow the client's preferred RemoteAddr commit. Clean and lossy
v1/v2 × IPv4/IPv6 cases passed under race (17.790 seconds). Three additional lossy
matrix repetitions passed (37.921 seconds); interop vet passed.

The preferred-path probes and file data are direct and unimpaired. This closes
an original-path-loss-before-migration gate only; preferred validation loss and
post-commit data loss remain separate independent-peer work. No production code
changed. The full project goal remains active.

## Latest: 2026-09-21 local/preferred migration serialization

Added v1/v2 tests for both request orderings and cancellation before/after a probe
is sent. They verify single candidate/CID ownership, unchanged established tuples,
automatic preferred deferral, local-request rejection while busy, and subsequent
admission after cancellation. Sent-probe cancellation retires its CID; an initial
fixture expectation of immediate reuse failed. The final test supplies a protected
NEW_CONNECTION_ID and verifies deferral during exhaustion and subsequent recovery.
Three final race repetitions passed (1.410 seconds); transport vet passed.
Public Migrate now documents pending preferred-migration rejection. No production
behavior changed. Remote rebinding races, competing commits and public runtime
request-race coverage remain open. The full project goal remains active.

## Latest: 2026-09-21 actual preferred UDP reader failure

Added TestPreferredCandidateUDPReaderFailure for v1/v2 over IPv4/IPv6. Following
an authenticated fixture handshake, a real UDP probe creates a preferred server
candidate with a CID reservation. Closing the actual preferred socket lets the
reader report its failure through the runtime. The test waits for candidate
cancellation, sends another authenticated packet to the original socket, verifies
its acceptance, and decrypts a server PING received from that original socket.
Five race repetitions of all four cases passed (1.509 seconds); transport vet
passed. No additional production change was required.

The first fixture attempt omitted runtime migration-admission flags ordinarily
set during server admission, so it timed out waiting for a candidate. Correcting
the pre-established fixture enabled the intended failure transition. This test
uses our implementation in both roles and does not perform its TLS handshake over
UDP. Independent-peer impairment, post-commit socket failure and simultaneous
migration remain open. The project goal remains active.

## Latest: 2026-09-21 preferred candidate socket failure fix

The new v1/v2 TestPreferredCandidateSocketFailure failed because failLocalSocket
retained a peer-initiated candidate using the failed preferred socket. Its
established path used another healthy socket, so the existing affected-path scan
missed it; later candidate output could trigger endpoint removal for an invalid
local binding. Socket failure now clears such candidates before scheduling.
Normal probe scheduling retires used CID reservations with its existing queue-
pressure retry behavior. Original-path output and connection retention are tested.

Three race repetitions of the new regression and existing multi-path socket
failure evacuation test passed (1.388 seconds). The final stronger output
assertion passed three further race repetitions (1.317 seconds). The full root
race suite passed (transport 37.995 seconds), and root vet passed.
Initial fixture construction
errors were corrected before observing the production candidate-retention failure.
This is serialized owner failure-injection coverage, not a real UDP reader failure
or post-commit failover guarantee. The full project goal remains active.

## Latest: 2026-09-21 preferred validation recovery

Added deterministic encrypted-packet recovery tests for v1/v2: two withheld
challenges, delayed first-probe responses after three sends, expired responses,
and reordered responses after commit. Tests assert original-path packet-number
acceptance while probes wait and correct destination/CID state. Three focused
race repetitions passed (1.371 seconds). All transport preferred-address tests
also passed under the race detector (13.522 seconds), and transport vet passed.
An initial fixture panic accessed probe
state after successful validation cleared it; the fixture now saves the delayed
token before delivery. No production code changed.

Independent-peer impairment and broader socket failure/simultaneous migration
coverage remain open; this does not complete the preferred-address feature or
the project goal.

## Latest: 2026-09-21 reverse preferred-address interoperability

Added TestPicoquicPreferredServer: our client connects to the unmodified pinned
Picoquic server's original socket, waits for committed preferred RemoteAddr,
and downloads an exact 64 KiB file with FIN. The final matrix covers v1/v2 and
IPv4/IPv6. Three race repetitions of both independent preferred-address roles
passed (14.967 seconds). The full default interop race suite passed (59.077
seconds), and interop go vet passed. No production code changed.

Initial fixture attempts failed because the peer accepts bare address arguments,
uses the second port in `-p original:preferred` for preferred advertisement, and
opens both sockets. The initial byte-order diagnosis was wrong; its helper stores
the port in host order. Peer qlog and source inspection resolved these setup errors.
A separate confirmed peer encoder issue writes the IPv4 port into the IPv6 field.
The normal fixture advertises both families on the preferred port; strict IPv6-only
v1 still fails after 15 seconds with qlog showing port zero. That gate and the
scope limitation are retained in interop/KNOWN_FAILURES.md.

Independent preferred migration now has successful coverage in both TLS roles;
loss/reordering, socket failure after advertisement and simultaneous migration
remain open. The complete project goal remains active.

## Latest: 2026-09-20 independent preferred-address coverage

Added upstream v0.62.0 coverage for authenticated preferred advertisement and
continued bidirectional stream service on the original address, for v1/v2 over
IPv4/IPv6. The first fixture incorrectly required immediate retirement of CID 1,
based on an upstream comment. All cases timed out on that assertion. Inspecting
AddFromPreferredAddress showed it queues the CID instead; the fixture was corrected
to test observed supported behavior. Three focused race repetitions then passed
(1.415 seconds). Earlier progress wording claiming immediate retirement was wrong.

Added an optional Picoquic demo-client gate for successful preferred migration.
Our listener serves a 1 KiB prefix, waits for preferred-local-address commit, then
sends the remainder and FIN. The peer's exact 64 KiB file and exit status are
checked. The final v1/v2 × IPv4/IPv6 matrix passed three race repetitions (2.668
seconds). Binary: /tmp/quic-pinned-driver-repro/interop-build/picoquic/picoquicdemo.
The associated source checkout is clean at the documented Picoquic revision
8c6bbf82b6b6d98a413a69e6bf2265e7f6a10161; CMake points to that checkout and PicoTLS
is bfa67875982afc4c24f21e146cef4747fa189c2f. No peer source patch was applied.

The full default interop race suite passed (59.077 seconds); interop vet passed
and was rerun after the IPv6 matrix extension. Only tests/docs changed this turn.
The optional external gate is separate from the default suite and skips without
PICOQUIC_DEMO. Successful migration against an independent server in our client
role, loss/reordering and simultaneous migration remain open. The project goal
remains active; docs/PREFERRED_ADDRESS.md and interop/picoquic/README.md contain
coverage scope and commands.

## Latest: 2026-09-20 public preferred-address listener API

Config.PreferredAddresses now accepts up to one concrete netip.AddrPort per
family; port zero allocates an ephemeral port. ListenAddr opens and owns all
sockets, rejects a wildcard primary when preferred addresses are configured,
and transfers them together to the serialized runtime. Construction failures
close every socket already opened. Listener.PreferredAddresses returns a copied
slice of actual bound addresses; Close releases the preferred sockets too.
DialAddr ignores the server-only option.

Public tests perform bidirectional stream request/echo after automatic migration
for v1/v2 over IPv4/IPv6, verify address snapshots and copy ownership, and rebind
ports after listener close and partial construction failure to detect leaks.
The initial tests exposed a missing client RemoteAddr snapshot update at preferred
commit; that is fixed. The IPv6 fixture explicitly authenticates its certificate's
127.0.0.1 identity without disabling certificate verification. Three focused
race repetitions passed after the fix. Full root race passed (transport 38.037
seconds), full interop race passed (59.023 seconds), and vet passed in both modules.

Next: independent preferred-address coverage. Pinned upstream v0.62.0's
connection.go explicitly declines migration and retires the advertised preferred
CID; it can verify advertisement/retirement compatibility, while successful
migration needs another independent peer such as the existing Picoquic harness.
Remaining broader failure and simultaneous-migration tests are listed in
docs/PREFERRED_ADDRESS.md. The full project goal remains active.

## Latest: 2026-09-20 registered-socket preferred advertisement

The internal server transport now accepts PreferredAddresses only through a
verified socket-owner admission gate. Every configured address must be concrete,
registered and unique by family. Multi-socket base migration is supported without
requiring the multipath extension. New admissions omit removed preferred sockets.
Each connection generates its own CID/reset token, reserves sequence 1 in the
local ledger before TLS output, and leaves the listener template unchanged.
Direct wire PreferredAddress injection remains rejected in production NewConn.

Full TLS handshakes over real UDP now exercise automatic client preferred
migration and server validation/commit for v1/v2 over IPv4/IPv6. The matrix also
removes the preferred socket before admission and verifies parameter omission
and continued original-address operation. Separate tests cover missing socket
authorization and per-connection identity ownership. Three focused race
repetitions passed. Full root race passed (transport 38.326 seconds), full interop
race passed (59.331 seconds), and vet passed in both modules.

This is internal transport configuration, not yet a public listener option.
Next: public listener socket allocation/configuration and lifecycle, followed by
independent preferred-address interoperability and broader failure/data tests.
docs/PREFERRED_ADDRESS.md describes the current implementation and open work.
The full project goal remains active; earlier intermittent handshake failure
remains unresolved despite this passing suite.

## Latest: 2026-09-20 preferred-server receive and commit integration

Authenticated candidate reception now recognizes path-zero packets delivered to
the server's exact advertised preferred local address. Probing-only packets can
start the specialized selection state; unadvertised local-address changes remain
rejected. Server output uses a reserved fresh peer CID for the changed source
address. Commit rechecks preferred local-address authorization, requires proof
plus nomination, and resets recovery when the local binding changes.

The v1/v2 encrypted engine test verifies forged/unknown-local rejection,
probing-only admission, preservation during original-address traffic, actual
server response/challenge construction, no switch on proof alone, and commit
on a newer non-probing packet. Three focused race repetitions passed. Full root
race passed (transport 38.008 seconds), full interop race passed (59.024 seconds),
and vet passed in both modules. The earlier intermittent handshake timeout
remains recorded as unresolved; this passing run is not a fix claim.

Preferred server advertisement still enters through the test TLS fixture.
NewConn's production advertisement guard remains until listener socket
registration and per-connection CID issuance are integrated. Those gates and a
full UDP handshake/exchange are next, then independent preferred-address peer
coverage. docs/PREFERRED_ADDRESS.md records current boundaries. Goal stays active.

## Latest: 2026-09-20 preferred-server candidate selection

Added a server-oriented preferred candidate observation primitive. Authorized
probes can allocate bounded validation without nominating application traffic.
Newer original-address non-probing traffic retains preferred reachability proof
but revokes nomination; only a newer preferred non-probing packet can restore
it. Commit requires both proof and nomination before CID/recovery mutations.
A preferred-packet watermark survives expiry, preventing replay from renewing
an expired candidate; competing probing tuples cannot replace the live candidate.

Tests cover both proof/nomination arrival orders, original traffic, nomination
revocation, stale packets, successful commit, competing probes and expiry replay.
Focused selection tests passed three race repetitions. Full root race passed
(transport 38.052 seconds), and vet passed in both modules.

The first full interop race run failed one v2 handshake-loss case with a five-
second upstream idle timeout after dropping the second client-to-server datagram.
Five exact focused repetitions passed (1.489 seconds), followed by a successful
full interop race rerun (59.004 seconds). The failure remains unresolved and is
recorded in interop/KNOWN_FAILURES.md; no fix or root cause is claimed.

The new selection primitive is not yet connected to preferred-server receive
admission. Advertised-local-socket authorization, server commit guards and socket
serving remain next, before enabling server advertisement. The project goal
remains active; docs/PREFERRED_ADDRESS.md records the remaining scope.

## Latest: 2026-09-20 server preferred CID reservation

Added localIDs.reservePreferred to install the transport-parameter CID as
sequence 1 before ordinary issuance. It owns the CID bytes/token, counts toward
the peer's active limit, rejects collision/late installation/invalid routing
length without mutation, and preserves uniqueness history through retirement.
The authenticated preferred-address server fixture now uses this operation
instead of manually editing its CID maps. Ordinary issuance starts at sequence
2; tests cover limits, retirement rules, replacement sequences and input aliasing.

Focused preferred-CID tests passed three race repetitions before the final
oversize-input guard was added. The full root race suite passed afterward
(transport 38.025 seconds), including all real-UDP preferred-address cases;
root vet passed. No new independent-peer interoperability claim is made.

The reservation helper is preparation for server integration, not enabled
advertisement: NewConn still rejects preferred-address parameters. The next
server state-machine change must permit probing-only candidate admission and
preserve that candidate while original-address traffic continues, then require
validation plus newer non-probing traffic before commit. Existing ordinary
migration nomination semantics cannot simply be reused unchanged. Details are
in docs/PREFERRED_ADDRESS.md. The full goal remains active.

## Latest: 2026-09-20 preferred-address probes over real UDP

Added a real UDP matrix for v1/v2, IPv4/IPv6, and successful/unanswered preferred
validation. An authenticated handshake fixture precedes socket-owner startup;
the peer receives and decrypts actual probes, optionally sends an encrypted
response, and observes ordinary PING traffic at the original/preferred sockets.
This is socket integration coverage, not independent-peer or handshake-over-UDP
preferred-address coverage.

The initial run failed all eight cases because ordinary original-path output
was held during validation. Inspection also found that the inherited local-
migration sender added a non-probing PING to preferred probes. Preferred
candidates now omit that PING and permit established-path output while probing.
Ordinary local migration retains its nomination and output-hold behavior.

The matrix passed after both fixes (13.506 seconds under race). It verifies
original-path traffic before proof, preferred-path output after valid response,
and original-path fallback after unanswered probes expire. Full root race
passed (transport 38.442 seconds), full interop race passed (59.078 seconds), and
vet passed in both modules. docs/PREFERRED_ADDRESS.md records coverage limits.

Next: server advertisement/CID ownership and preferred-socket serving, followed
by full handshake-over-UDP and independent preferred-address interoperability.
The project goal remains active.

## Latest: 2026-09-20 automatically initiate preferred-address validation

The endpoint send owner now activates the confirmed connection's path state
before considering preferred-address migration. It selects a compatible live
registered socket, preferring the established address family/socket and otherwise
using deterministic address order. Admission defers if a compatible socket or
unused CID is unavailable. A started attempt is remembered so failed validation
does not restart repeatedly. Validated candidates commit through the existing
owner transition; failed probes leave the original binding.

Endpoint tests for v1/v2 verify automatic probe output, encrypted response
routing, commit and expiry without restart. Socket-selection tests cover an
unavailable family and deterministic alternate-family socket choice. Three race
repetitions passed. Full root and interop race suites passed (interop 59.091
seconds), as did vet in both modules.

The tests use authenticated in-process fixtures, not an independent preferred-
address server. Next: real UDP success/failure validation, followed by server
advertisement/socket integration and independent-peer preferred-address cases.
Automatic allocation of new alternate-family sockets and simultaneous migration
policy also remain open. docs/PREFERRED_ADDRESS.md records the scope and gaps.
The full project goal remains active.

## Latest: 2026-09-20 preferred-destination client validation primitive

The client migration engine now admits an exact destination from this
connection's authenticated preferred_address parameter through a separate
internal entrypoint. It reserves an unused CID, permits keeping the local
socket while changing the remote address, and rechecks destination authorization
at validated commit. Ordinary local migration keeps its existing remote-address
restriction. Disabled active migration still forbids changing local addresses
toward the handshake address, even if it is advertised as preferred.

Tests for v1/v2 and IPv4/IPv6 exercise padded probe output, encrypted responses,
forged and wrong-source proof rejection, validated commit, and expiry preserving
the original binding. Admission rejects absent/unknown/unspecified destinations,
zero ports, wrong families, unconfirmed state and server-role misuse. Tests also
check that commit cannot switch to an unauthorized destination and that rejected
admission leaves no CID reservation. Focused migration tests passed three race
repetitions. Full root and interop race suites passed on the main change (interop
59.388 seconds); a subsequent handshake-address restriction edge case passed
three focused race repetitions. Vet passed in both modules, with root vet rerun
after that final guard adjustment.

The new entrypoint is internal and not automatically invoked by UDP clients yet.
Next: runtime address/socket selection and initiation, then actual preferred-
address packet exchanges and independent peer tests. Server advertisement and
socket serving remain open. docs/PREFERRED_ADDRESS.md tracks remaining work.
The project goal remains active.

## Latest: 2026-09-20 preferred-address CID ownership

An RFC audit found that parsed preferred_address parameters did not register
sequence 1 in the client CID ledger. The authenticated handshake now installs
that CID and reset token before exposing peer parameters, preserving active-CID
limit enforcement, duplicate consistency, retirement, unused-ID selection and
path-zero adoption. Reset recognition remains gated on actual CID use.

New TLS-handshake tests for v1/v2 cover encrypted CID frames, overflow including
the preferred CID, matching/conflicting duplicates, retirement and replay,
reset-token use/retirement, and path history. Three race repetitions passed.
Full root and interop race suites passed (interop 59.289 seconds), as did vet in
both modules. The new preferred-address cases use our own TLS/transport fixture;
independent preferred-address interoperability is still unverified.

This fixes a prerequisite, not the entire feature. Client address selection and
validation/commit, server socket/advertisement integration, and independent peer
coverage remain open. docs/PREFERRED_ADDRESS.md records those requirements and
RFC links. Existing local migration admission and commit explicitly require an
unchanged remote address; those guards need a distinct, authenticated preferred-
address authorization path before they can support destination migration.
The goal remains active.

## Latest: 2026-09-20 retain one deadline context across a stream Write

SendStream.Write previously constructed and canceled a deadline context for
every partial flow-control commit. It now keeps one context for the call,
retaining the committed byte offset across cancellation/retry when a concurrent
deadline change interrupts a blocked write. Contexts still end at call return.

New upstream interop tests establish partial delivery with a fixed receive
window before changing deadlines. Both QUIC versions cover expiry with the
correct partial count and subsequent resumption, and clearing a deadline while
the original Write continues. Exact payload and FIN checks reject missing or
duplicated bytes. These and the existing public-stream tests passed five race
repetitions. Full root and interop race suites passed (interop 59.199 seconds),
as did vet in both modules.

Five matched 20-iteration echo samples reduced median whole-process allocations
7.3% (64,812 to 60,104) and bytes 4.9% (6,554,234 to 6,232,737). Time changed
13.745 to 13.413 ms; no reliable throughput improvement is claimed. Upstream
remains substantially faster at 5.189 ms. Raw results and scope limitations are
in docs/benchmarks/stream-echo-ecn-outbound.md. The goal remains active with the
previously documented protocol, platform and performance gaps still open.

## Latest: 2026-09-20 lazy UDP owner notifications

The UDP owner creates a broadcast notification channel only when a command
actually blocks. Events close the registered channel and clear it; operations
that complete immediately no longer subscribe. Wait and Accept registration
remains serialized with signaling, including shutdown. A regression test covers
shared waiters, absence of mutual wakeups, fresh notification generations, and
shutdown; the UDP tests passed ten race repetitions.

Five 20-iteration stream echo samples reduced median whole-process allocations
from 67,050 to 64,812 (3.3%) and bytes from 6,927,176 to 6,554,234 (5.4%). Timing
changed 13.931 to 13.745 ms, too small to claim reproducible throughput gains;
upstream remains substantially faster at 5.203 ms in this workload. Results are
in docs/benchmarks/stream-echo-ecn-outbound.md. Full root and independent-peer
race suites passed (interop 59.178 seconds), and vet passed in both modules.

The goal remains active. The earlier allocation profile also identifies stream
read/write deadline contexts and command reply channels as remaining costs;
changes there require preserving cancellation and committed-operation behavior.
The protocol and platform gaps recorded below remain unresolved.

## Latest: 2026-09-20 measured stream echo and reused outbound descriptor

Re-measured the 1 MiB request/echo workload against pinned upstream after ECN
and ACK optimizations. Our median remained about 14.0 ms versus upstream 5.21 ms;
the synthetic history improvement did not translate into a material gain here.
A separate profile identified per-packet outbound descriptor allocation among
the remaining costs. The UDP owner now reuses one descriptor across synchronous
write/error callbacks and clears references afterward. Existing NextDatagram
callers retain independently owned descriptors and packet byte ownership.

Five matched 20-iteration samples show median allocations reduced 2.6% and
allocated bytes 3.4%, with no claimed throughput gain. Tests cover descriptor
reuse, stale ECN/MTU metadata, and preservation of earlier owned output for v1/v2.
Full root and interop race suites passed (interop 59.331 seconds), as did vet
in both modules. Raw results and profile location are documented in
docs/benchmarks/stream-echo-ecn-outbound.md. The performance gap remains large.


## Latest: 2026-09-20 synchronize migration address assertion

Pinned upstream code delivers STREAM frames before publishing a newly selected
remote address: handleShortHeaderPacket calls handleUnpackedShortHeaderPacket
before ChangeRemoteAddr. The application can wake between those operations.
The independent migration fixture incorrectly treated request-stream completion
as an address-publication barrier. It now waits at most one second (also bounded
by its existing context) for the exact expected tuple. Closure or a persistent
wrong tuple still fails. No production transport behavior was changed.

All v1/v2 clean/request-loss/response-loss/both-loss migration cases passed
three race repetitions (49.922 seconds), and interop vet passed. The full
independent-peer race suite then passed (59.389 seconds).


## Latest: 2026-09-20 bounded packet and congestion-history ACK lookups

Applied a shared bounded lookup decision to packet retirement and persistent-
congestion ACK history, as well as ECN evidence. Sparse ranges use direct map
lookups; dense/huge ranges scan retained entries. History ordering, prefix
trimming, loss detection, and final ACK event ordering remain unchanged. Added
maximum-PN, dense-range, and cross-space history tests. Full root race tests and
vet in both modules passed using the external build cache.

An equal-iteration (10,000) benchmark comparison reduced median retained-history
send/ACK time from 53,395 ns to 497.4 ns, with seven allocations unchanged. Short
after-samples have substantial warm-up variance; this is only a synthetic
workload result. Raw samples and the isolated baseline method are documented
in docs/ECN_ACK_BENCHMARK.md.

The full interop race suite failed one v2/both-loss migration RemoteAddr check.
Five focused race repetitions passed afterward; the cause is unresolved and
the assertion remains enabled. Recorded in interop/KNOWN_FAILURES.md rather
than treating focused success as a clean full-suite result.


## Latest: 2026-09-20 sparse ECN ACK lookup optimization

ACK processing now chooses between direct packet-number lookup for sparse
validated ranges and scanning the bounded ECN ledger for dense ranges. It
preserves all retained marking evidence and exact late/duplicate accounting.
Tests cover mixed ECT markings, sparse/dense ACKs after loss, and maximum packet
number ranges without unbounded enumeration. Recovery/transport race suites
and vet passed using an external-SSD build cache.

Five before/after benchmark samples with 4,095 retained markings reduced median
send/ACK cycle time from 72,225 to 52,066 ns (27.9%); allocations stayed at seven.
This is a synthetic recovery result, not end-to-end or upstream comparative
performance. See docs/ECN_ACK_BENCHMARK.md for raw samples and methodology.


## Latest: 2026-09-20 Linux validation infrastructure failure diagnosed

Re-polled the original live validation/status handles without launching a
duplicate. Docker VM logs showed disk I/O errors; backend logs reported no
space left on the host volume. The internal disk had only 160 MiB free. Moved
the two unopened updater cache files created by this Docker startup to the
external SSD, then stopped the Docker instance started for validation. The
original validation command is now terminal (exit 125, unexpected EOF), and
the status command is terminal too. Sessions 25559/49660 need no further polling.

The observed Linux root race pass stands; later vet/interop checks have no
verified result. No existing Docker images or volumes were deleted. Recovered
about 450 MiB; the internal disk still has only 615 MiB free. Added a 4 GiB
macOS Docker storage preflight and read-only root/tmpfs build directories to
the Linux script. Syntax and the refusal path passed; the revised container
configuration remains untested. See docs/LINUX_VALIDATION.md for exact evidence
and updater-file recovery location. Linux validation is unavailable until
storage is repaired, but other implementation work can continue.


## Latest: 2026-09-20 native Linux validation started

Started the installed Docker Desktop engine and added scripts/test-linux.sh,
with a pinned Go 1.26.2 image, read-only source/module-cache mounts, two CPUs,
and 4 GiB. Native Linux arm64 under kernel 6.12.76-linuxkit passed the entire
root race suite, including transport/socket tests (26.697 seconds). This replaces
cross-compilation-only evidence for that specific Linux environment.

The validation command remains live (exec session 25559), but Docker API checks
returned EOF and then hung; the follow-up status command is session 49660.
No completed Linux independent-peer or vet result has been observed. Re-poll
these handles and inspect authoritative state before restarting anything. See
docs/LINUX_VALIDATION.md for the reproducible command and scope limits.


## Latest: 2026-09-20 independent ECN across active migration

Added TestECNSenderRevalidatesAfterMigrationUpstream for v1/v2 and IPv4/IPv6.
Our client performs two active migrations against native upstream quic-go.
Each of the three path rounds completes sequential DATAGRAM echoes and
requires more than ten upstream-observed ECT(0) data packets. The test also
checks that the local tuple changes and upstream reports that new peer tuple.
This exercises marking with cumulative feedback across real UDP migration;
separate recovery tests inspect the fresh-testing policy transition itself.

The original 24-packet-per-round matrix passed v1 but failed every v2 run.
A diagnostic upstream qlog trace placed a v2 failure at round 1, packet 17,
after upstream reached key phase 1, consistent with the documented key-update
interop gate. Default v2 coverage uses 12 packets per round, still exceeding
the ten-probe budget; QUIC_INTEROP_KNOWN_FAILURES=1 retains the 24-packet v2
case. The resulting default matrix passed three race repetitions and vet.
This is not a resolution of the longer v2 failure.


## Latest: 2026-09-20 ECN probe deadline after local rejection

Corrected a deadline edge case: if the only marked probe was rejected locally,
the expired deadline could stop further testing even though no marked packet
reached the network. Deadline checks now require retained probe evidence. The
next committed probe establishes its own deadline. Tests cover ordinary and
MTU local rejection, preserving the original deadline when an earlier probe
remains, and allowing testing after the only probe space is discarded.

Recovery and transport race suites and vet passed. A Linux runtime check found
Docker installed but no running daemon/socket at ~/.docker/run/docker.sock;
no Linux runtime validation was performed.


## Latest: 2026-09-20 bound ECN testing during handshake packet loss

Added a time bound in addition to the ten-packet ECN test budget. The first
marked commit fixes a deadline at three base PTO durations without backoff; later PTO
backoff and RTT changes cannot extend it. Production serializers check this
deadline before selecting each marking. Expiry stops ECT while still accepting
valid late feedback. A new path starts with a fresh deadline. Loss of every
probe can fail the path after either budget ends.

Without this bound, exponentially spaced handshake probes could remain marked
until the handshake timeout on a network dropping every ECT packet. The real
UDP relay now has a mode that drops every client ECT(0) packet while forwarding
Not-ECT unchanged to upstream. All four v1/v2 and IPv4/IPv6 cases completed
handshake and 24 data echoes in the targeted race run (13.493 seconds total).
Recovery/transport race suites and vet passed, including fixed-deadline, late
feedback, and migration-reset assertions. The full independent-peer race suite
also passed (59.365 seconds), along with interop vet.


## Latest: 2026-09-20 independent ECN bleaching relay

Added a real UDP relay interop test that forwards unchanged protected packets
using default Not-ECT sockets in both directions. It independently records our
client markings before forwarding to a native ECN-capable upstream server.
For v1/v2 and IPv4/IPv6, the test observes initial ECT(0), completes 24 DATAGRAM
echoes, and requires the final eight client packets to be Not-ECT. Upstream
qlog confirms no marked data packets arrive through the relay.

The first run exposed a fixture assumption: upstream v0.62.0 toQlogECN maps
Not-ECT to its empty ECNUnsupported value. Corrected that expected representation
and allowed it as unmarked in the prior successful-validation test, which still
requires more than ten explicitly ECT(0) observations. Both test matrices then
passed five race repetitions; interop vet passed. These remain macOS results.
Independent congestion-response, migration, and total-probe-loss scenarios and
Linux runtime validation are outstanding.


## Latest: 2026-09-20 independent missing-ECN-feedback fallback

Added TestECNSenderStopsWithoutPeerFeedback for both roles, v1/v2, IPv4/IPv6.
A PacketConn wrapper reads and independently records kernel ECN metadata, then
withholds it from upstream quic-go while preserving payloads and addresses.
Upstream therefore generates ordinary ACKs without ECN feedback. Each case
requires actual initial ECT(0) probes, completes 24 sequential DATAGRAM echoes,
and checks the last eight observed packets are all Not-ECT. This verifies
continued delivery after marking stops without synthesizing ACK frames or
reading private sender state.

All eight cases passed five race repetitions on macOS; interop vet passed.
This covers missing peer reporting, not an on-path bleaching device or malformed
authenticated counters. Independent CE, migration, and total-probe-loss coverage
and Linux runtime tests remain outstanding.


## Latest: 2026-09-20 independent sender ECN validation evidence

Added TestECNSenderContinuesAfterValidationUpstream. For our client and server
roles, v1/v2, and IPv4/IPv6, it exchanges 24 sequential DATAGRAM echoes with
pinned upstream quic-go. Upstream qlog reports its kernel-observed ECN marking
on each received data packet. Each case requires more than ten ECT(0) packets,
proving our sender continued beyond its initial bounded probe budget using
ordinary independent-peer feedback. No synthetic ACKs or private sender state
are used. Payload checks verify delivery alongside marking observations.

All eight cases passed five race repetitions on macOS, and interop vet passed.
This covers successful validation and continued marking. Independent malformed
feedback, bleaching, total probe loss, migration, and CE response scenarios
remain to be tested; Linux runtime testing is also still absent.


## Latest: 2026-09-20 automatic UDP sender ECN activation

Linux/macOS UDP runtimes now enable the internal ECN sender gate for existing
client connections and the server admission template. Normal base/multipath
serializers lazily start each recovery owner testing; failed states cannot be
revived by repeated selection. Path-zero adoption retains its existing state.
Unsupported platforms retain Not-ECT behavior. OS marking support is attempted
through sendmsg, with the preceding synchronous local-error fallback handling
unsupported ancillary data instead of a separate network capability probe.

The real UDP receive-feedback fixture now also asserts outgoing ECT(0) from
kernel metadata for v1/v2 over IPv4/IPv6 on macOS. Five race repetitions passed.
The full root-module and independent-peer race suites passed with activation
enabled (independent suite 46.640 seconds), as did vet in both modules. This
proves the existing interop scenarios remain working, not exhaustive independent
validation of every ECN sender transition. Linux runtime testing remains absent.


## Latest: 2026-09-20 local ECN write fallback

Outbound now retains the exact recovery owner/space/number for its serialized
packet. For EINVAL, ENOPROTOOPT, or EOPNOTSUPP with zero bytes reported on a
marked write, the UDP writer removes that latest packet marking from recovery,
disables ECN on the path, and attempts the identical ciphertext once without
marking. It preserves packet number, flight bytes, frame ownership, and normal
ACK eligibility. Missing/stale marking evidence prevents retry. The rollback
requires synchronous send-result handling before any other send or receive.

Other write outcomes follow the existing error path, including EMSGSIZE,
ordinary network errors, successful writes, and any reported bytes sent. Tests
inject wrapped kernel errors at the writer boundary and verify exact marking
sequence, one retry, retained ciphertext, ECT rollback, and ACK retirement.
Recovery/transport race suites, vet, and Linux amd64 cross-compilation passed.
Production automatic ECN activation is still pending.


## Latest: 2026-09-20 serialized packet ECN metadata

Normal base and multipath packet serializers now select ECNMarking from their
recovery owner and commit that exact marking with the packet. The endpoint
copies the committed metadata into Outbound for the UDP writer. Every endpoint
scheduling attempt clears prior metadata, including before close/migration
control paths that still send Not-ECT. Blocked normal serialization also clears
metadata without consuming a marking or packet number.

Five race-test repetitions for v1/v2 check base endpoint metadata against sent
ECT counts and the receiving peer ACK_ECN, fallback after missing feedback,
multipath marking/path isolation, and blocked-send accounting. Socket capability
detection and automatic runtime activation remain pending; ordinary connections
still leave ECN testing disabled by default. Full transport race tests and vet
passed.


## Latest: 2026-09-20 recovery owns ECN path policy

Sent now owns ECNPath and exposes explicit EnableECN plus capacity-aware
ECNMarking selection. Marked commits consume the probe budget; checked ACKs
advance capability or fail the entire path; loss declarations identify loss of
the full test flight. Synchronous local send rejection returns probe budget,
and encryption-space discard removes inconclusive probes. Ordinary migration
starts fresh testing while retaining cumulative feedback and marking evidence.
A previously failed context stays disabled across resets until an explicit
revalidation/baseline design exists; EnableECN cannot silently revive it.

Tests drive these transitions through real recovery callbacks, including
capacity per space, failure across spaces, local rejection, migration, old-path
ACKs, and total probe loss. This remains disabled in production until the engine
selects markings and copies them consistently into recovery and Outbound.
Recovery and transport race suites and vet passed.


## Latest: 2026-09-20 per-datagram UDP ECN marking

Outbound datagrams now carry their recovery ECN marking, and the UDP writer
uses per-message IP_TOS/IPV6_TCLASS ancillary data for nonzero markings on
Linux/macOS. The helper rejects originating CE or invalid values and never
changes shared socket options. Unsupported platforms reject marked sends;
Not-ECT retains the existing write path. DSCP in marked writes is currently zero.
No connection engine producer selects ECT yet: path-policy/send-accounting
integration and socket capability handling are still required before activation.

Real macOS IPv4 and IPv6 loopback tests switch ECT(0), Not-ECT, and ECT(1) on one
socket and inspect received kernel metadata. Five helper-test race repetitions
passed before routing the same test through UDPServer.writeOutbound. The full
transport race suite and vet passed afterward. All root-module packages compile,
and the transport tests cross-compile for Linux amd64; Linux runtime behavior
has not been tested.


## Latest: 2026-09-20 validated ECN congestion response in transport

Base connection and multipath ACK handlers now feed validated CE increases to
NewReno before ordinary loss/ACK handling. Recovery events supply the largest
acknowledged packet send time when available in current-path recovery evidence.
Marked-packet evidence retains timestamps beyond ordinary metadata retirement.
Migration preserves cumulative counts but marks retained ECN transmissions as
old-path evidence, preventing their late ACKs from reducing the new path window.
No congestion event is emitted without a retained current-path send timestamp.

Tests exercise v1/v2 base window reduction, invalid/duplicate CE suppression,
multipath isolation, retained marked timestamps after pure-ACK eviction, and
old/new path congestion eligibility across migration. The invalid-ECN fixture
initially expected no window growth; corrected it to expect ordinary growth
from the valid packet ACK.
Sender marking and runtime path-testing policy integration remain unfinished;
production packets still originate Not-ECT. Recovery and transport race suites
and vet passed.


## Latest: 2026-09-20 ECN feedback wired into recovery ACK processing

Sent.OnACK now validates ECN feedback using its own cumulative marking ledger
and exact newly acknowledged ECT counts, after rejecting malformed ranges and
ACKs of unsent/rejected packets. Recovery events expose the checked ECN result.
A feedback failure does not invalidate packet acknowledgments or retain their
flight bytes; the transport owner must disable marking instead of closing.
Per-space feedback state survives ordinary migration alongside marking evidence
and resets on key-space discard or Retry. Keeping the migration baseline avoids
reporting historical CE counts again as new congestion.

Regression tests cover invalid ACK isolation, duplicate CE suppression, bad
feedback with successful data retirement, space isolation, migration baseline,
and discard. The path-policy integration test now consumes actual OnACK output.
Transport still does not originate ECT markings or act on these ECN events;
socket marking and congestion/path-policy integration remain unfinished.
Recovery and transport race suites and vet passed.


## Latest: 2026-09-20 sender ECN path-testing policy

Added recovery.ECNPath, separate from cumulative packet-number-space feedback.
Its zero value disables marking; explicit construction tests with up to ten
ECT(0) packets, then waits without marking. Validated feedback acknowledging a
probe on this network path establishes capability. Probe identity includes the
packet-number space, so old-path ACKs and same-number ACKs in another space
cannot establish support. Validation failure in any space disables the path;
loss of all ten probes also fails it. Duplicate commits/losses do not consume
extra budget. State uses a fixed ten-entry array.

Tests combine sent recovery, cumulative feedback validation, and path policy
for successful CE feedback and bleaching, plus policy isolation and loss cases.
Recovery race tests and vet passed. This is not yet called by the transport or
UDP writer: sender ECN remains disabled pending that integration.


## Latest: 2026-09-20 preserve ECN accounting across migration

Corrected the preceding recovery marking implementation: ordinary migration
keeps the packet-number space, so its peer ECN counters remain cumulative.
ResetForPathChange now retains sent ECT totals and pending marking evidence
while continuing to reset flight accounting, RTT, and congestion-loss history.
Late old-path ACKs still account for their markings without becoming new-path
RTT samples or flight acknowledgments. Discard and Retry still clear evidence.

A combined ledger/feedback regression exercises marked sends, ACK, migration,
late old-path ACK, and new-path CE feedback against cumulative peer counts.
New-path capability testing must use its own baseline/probe evidence; a late
old-path ACK is not proof the new path supports ECN. Sender runtime integration
and that path policy remain unfinished. Recovery and transport race suites
and vet passed.


## Latest: 2026-09-20 recovery ECN marking history

Recovery packets now carry an outgoing Not-ECT/ECT(0)/ECT(1) marking. Each
packet-number space retains bounded marking evidence independently of ordinary
recovery metadata, including after loss and pure-ACK eviction. ACK processing
returns exact newly acknowledged ECT totals, including late ACKs, once only.
Invalid ACKs leave evidence untouched. Cumulative sent totals retain acknowledged
and lost transmissions, while synchronous local send rejection rolls them back.
Discard, Retry, and migration reset the context; future validator integration
must reset corresponding feedback state. A full marking ledger rejects further
marked sends without consuming a packet number and still permits Not-ECT sends.

Tests cover late/duplicate ACKs, metadata eviction, capacity, invalid ACKs,
space isolation, local MTU rejection, resets, and invalid outgoing CE marking.
Recovery and transport race suites and vet passed. Sender marking and runtime
ECN validation remain unfinished and disabled.


## Latest: 2026-09-20 sender ECN feedback validation component

Added recovery.ECNFeedback, isolated per packet-number space and path. After
normal ACK-range/unsent validation, its caller supplies cumulative sent ECT(0/1)
totals and exact newly acknowledged ECT(0/1) counts. Advancing ACKs reject missing
feedback for marked packets, decreasing counts, ECT totals beyond transmissions,
impossible total marked counts, and insufficient increments. The combined
increment check prevents counting one CE increment for two newly acknowledged
packets with different ECT markings. Valid excess increments remain allowed for
lost ACKs. Equal/lower-largest ACKs leave the state unchanged.

Failure is sticky. Results expose successfully checked CE increases and whether
the ACK newly acknowledges marked packets. Tests cover lost/reordered ACKs,
bleaching/remarking, range/sum boundaries, count ownership, failure persistence
and independent state. Recovery race tests and vet pass; 5,828,557 fuzz
executions completed without failure. This is not yet a full path validation
state machine.

Production transport does not call this component yet. Remaining sender work:
bounded ECT test flights, total-loss detection, marking metadata on sent/rejected
packets and late ACKs, per-path state/revalidation, socket marking, and connecting
validated congestion events to NewReno. No sender ECN support is claimed yet.

Reference: https://www.rfc-editor.org/rfc/rfc9000.html#section-13.4.2.1
Full implementation goal remains active.

## Previous: 2026-09-20 NewReno response to validated CE

Added NewReno.OnECN as the congestion response operation for a new increase in
validated peer CE feedback. It accepts the associated largest-acknowledged
packet's send time and the current time. ECN validation and reordered/duplicate
counter handling remain caller responsibilities; unvalidated counts must not
call it.

Loss and CE now share a single congestion-event operation. Tests cover both
CE-before-loss and loss-before-CE, recovery-boundary timestamps, new recovery
epochs, minimum window, additive-growth credit reset, application-limited state
and independent path controllers. Recovery and full transport race tests and recovery/transport
vet pass. The existing loss filtering for ACK-only packets and MTU probes is
preserved.

The new operation is not yet connected to incoming ACKs. Outgoing packet marking,
per-path sender validation/revalidation and recovery metadata integration are
still needed before enabling sender ECN. Full implementation goal remains active.

Reference: https://www.rfc-editor.org/rfc/rfc9002.html#appendix-B.6

## Previous: 2026-09-20 ECN wire feedback and independent decoding

Extended real UDP runtime tests to decrypt returned ACK packets. V1/v2 over
IPv4/IPv6 verify CE=1 after a CE-marked packet, unchanged counts after replay,
and ECT(0)=1/CE=1 after a fresh ECT(0) packet. Ten race repetitions pass. These
tests now cover actual ACK output, beyond observing internal receive counters.

Added independent upstream-client tests for ECT(0), ECT(1), and CE on v1/v2.
A wrapper directs upstream output through a socket with fixed IP marking; the
upstream qlog callback inspects its independently decoded 1-RTT ACK frames.
The test checks only the expected counter is nonzero and verifies DATAGRAM
delivery. Five race repetitions and both modules' relevant vet checks pass.
This is receive-feedback interoperability, not upstream ECN validation evidence.

No production changes were needed. Linux runtime validation, outgoing marking,
sender validation/revalidation, and congestion response remain unfinished.
Full implementation goal remains active.

## Previous: 2026-09-20 UDP ECN reception

Linux/macOS sockets now request IPv4 TOS and IPv6 traffic-class control metadata
before readers start, including sockets added for migration. Unsupported option
setup is optional: absent metadata remains Unknown. UDP reads use ReadMsgUDP,
carry parsed markings through the actor queue, and call metadata-aware endpoint
input. Payload truncation drops the datagram; truncated, malformed or conflicting
control metadata reports Unknown. Control-message parsing avoids allocating a
slice of messages, and each reader reuses its ancillary buffer.

Real macOS IPv4/IPv6 socket tests exercise Not-ECT, ECT(0), ECT(1), and CE with
nonzero DSCP bits. Encrypted v1/v2 runtime tests verify CE reaches the connection
through the actual reader/actor/endpoint pipeline. Five race repetitions pass.
The initial tests exposed macOS's IP_RECVTOS control-message type (Linux uses
IP_TOS), and negative ReadMsg lengths on socket-close errors; both are fixed.
Full root and interop race suites and root vet pass. Linux amd64 cross-compiles; Linux runtime
behavior has not been tested here.

This enables receive feedback, not outgoing ECN marking or validated congestion
response. Sender validation, revalidation on migration, independent ECN-specific
interop, and broader platform coverage remain unfinished. Full goal stays active.

## Previous: 2026-09-20 endpoint ECN routing and admission

Added metadata-aware endpoint input, server Initial admission and network-owner
receive operations. Existing methods delegate with ECNUnknown. The original
datagram marking is passed to newly admitted connections and existing routes,
including the active bound-path receive operation. Invalid endpoint metadata
is rejected before routing or admission. Stateless Retry does not create
receive accounting state.

V1/v2 tests cover token-based server admission, repeated Initials routed to the
same connection, a fresh marked Initial, and invalid metadata. Another test
delivers marked application packets before and after migration-capable base-path
activation and verifies counts survive the owner handoff. Five race repetitions, the full transport race suite
and transport vet pass.

Socket ancillary-data acquisition is still absent: the UDP reader does not call
the metadata-aware endpoint yet. Sender marking, validation and congestion
response also remain unfinished. Full implementation goal remains active.

## Previous: 2026-09-20 admitted multipath ECN metadata

Added ECN-aware multipath authentication, decoding and bound-datagram entry
points, retaining Unknown behavior for existing callers. Authenticated packet
metadata carries its marking until the owner commits receipt. The commit step
updates the receiving path's tracker, so authentication without address admission
does not count. Remote migration's committing packet preserves the same metadata.
Local migration's validation-only packets remain unrecorded/unacknowledged.

V1/v2 encrypted-packet tests cover authentication without admission, rejected
server tuples, tampered ciphertext, repeated packet numbers with different marks,
independent counts on paths zero/one, staged metadata retention and PATH_ACK
ECN serialization. Five focused ECN race repetitions, the full transport race suite and transport
vet pass.

This remains an internal metadata path: UDP ancillary-data reception, endpoint
propagation and sender ECN validation/congestion response are still unfinished.
No on-wire ECN capability or independent multipath ECN interop is claimed.
Full implementation goal remains active.

## Previous: 2026-09-20 connection and deferred-packet ECN metadata

Added HandleDatagramECN to the internal connection engine. Existing callers of
HandleDatagram retain Unknown metadata behavior. One datagram marking reaches
each authenticated packet's number-space tracker. Invalid local metadata is
rejected before touching connection state, while failed packet authentication
does not increment counts.

Deferred packets now own their original ECN marking alongside bytes and receipt
time. Key-availability replay uses that marking, not the marking on the later
datagram that unlocked decryption. Preactivation multipath retention also records
the supplied marking in the existing path-zero receive tracker.

V1/v2 tests use encrypted packets and actual TLS flights: tampered ciphertext,
duplicate packets with changed marks, unavailable-key Handshake flights duplicated
before Initial, and coalesced Initial/Handshake packets. Tests verify original
mark retention, separate space counts, and suppression of duplicate counting.
Five focused race repetitions, the full transport race suite and transport vet pass.

UDP ancillary-data input, endpoint metadata propagation, active multipath packet
input, sender marking/validation and congestion response remain unfinished.
This does not yet enable ECN on the public socket API. Full goal remains active.

## Previous: 2026-09-20 receive ECN accounting component

Began ECN support with packet-number-space accounting in Received. RecordECN
accepts the IP two-bit marking, with a separate Unknown value for unavailable
metadata. Existing Record calls preserve prior behavior by supplying Unknown.
Only authenticated, newly processed packets increment ECT(0), ECT(1), or CE;
duplicates and packets below the retained floor do not. Counts persist across
ACK-range eviction, and ACK snapshots own their count copies. Observed Not-ECT
permits a zero-count ACK_ECN; unavailable metadata alone does not invent feedback.
Invalid metadata is rejected before modifying packet-number state.

Tests cover reordered/repeated markings, forgotten packets, snapshot ownership,
wire serialization, invalid inputs, and independent number-space counts for
coalesced packets. Five receive/ACK race repetitions, the full transport race suite and vet pass.
This is an accounting component, not yet runtime ECN support.

The UDP reader currently uses ReadFromUDPAddrPort and discards IP metadata.
Remaining integration: ancillary-data socket setup/parsing, metadata transport
through UDP/endpoint/connection and deferred/coalesced packet paths, multipath
per-path accounting, sender marking/validation and congestion response. None of
those are claimed complete by these component tests.

Reference: https://www.rfc-editor.org/rfc/rfc9000.html#section-13.4.1
Full implementation goal remains active.

## Previous: 2026-09-20 packet-number exhaustion through owners

The previous turn fixed silent exhaustion at the send boundary. New v1/v2 tests
exercise exhaustion through cross-path borrowing, the multipath scheduler,
bound-address scheduling, and endpoint output.

The scheduler begins on idle path 1 and borrows STREAM data from path 0 at the
packet-number limit. Tests retain both former path-state pointers and verify
terminal cleanup releases both queues; rollback does not restore payload after
the connection drops its multipath owner. Bound scheduling returns no packet
without dereferencing removed path state.

Endpoint coverage checks the terminal callback's original packet-number cause,
removal from the connection list, router and accept queue, and no output on later
scheduling. These are deterministic owner tests, not a real UDP transmission of
2^62 packets. Ten race repetitions and transport vet pass. Production code was
unchanged; no further cleanup defect was found in the exercised paths.
The full implementation goal remains active.

## Previous: 2026-09-20 silent packet-number exhaustion

Auditing RFC 9000 section 12.3 found send checks allowed 2^62-1 and failed only
above it. Connection sends, multipath sends, migration probes, and both close
serializers now stop at that value and close the engine without emitting a
CONNECTION_CLOSE or other packet. The original packet-number error is retained;
transport buffers and multipath state are released through normal Close.

New v1/v2 boundary tests cover Initial/application sends, base/multipath close
attempts, multipath validation sends, and local/remote migration response probes.
They assert no output, immediate terminal closure, no closing period, and no
subsequent output. A close-serialization test permits 2^62-2 and rejects the next
number. The first fixture attempted to skip the recovery ledger to that boundary
and failed its contiguous-send invariant; it was corrected to test serialization
without fabricating prior recovery history.

Five boundary-test race repetitions, the full transport race suite, and vet pass.

Reference: https://www.rfc-editor.org/rfc/rfc9000.html#section-12.3
The full implementation goal remains active.

## Previous: 2026-09-20 packet protection scratch allocation

The preceding turn established independent invalid-token interoperability.
Packet protection now groups header-mask and nonce scratch into one per-call
object, removing an interface-induced heap allocation on both Open and Seal.
Keys remain immutable and safe for concurrent use. Existing ownership and packet
wire behavior remain unchanged.

Five before/after benchmark samples show 3 → 2 allocations per operation and
unchanged allocated bytes (1440 for OpenOwnedPacket, 1312 for InitialSeal).
Median times improve approximately 2%, but samples include timing outliers;
no end-to-end or quic-go-relative performance claim is made.
Reproducible commands and all timing samples are in PACKET_SCRATCH_BENCHMARK.md.

Added concurrent shared-key protection tests for v1/v2, all three cipher suites,
16 workers and four path IDs. Packet race tests pass, including existing RFC
vectors, output ownership, tamper rejection, key updates and multipath coverage.
Full root and interop race suites and both modules’ vet checks pass.
Packet-open fuzzing completed 3,686,315 executions without failure. Full goal
remains active.

## Previous: 2026-09-20 independent invalid-token and foreign-token interoperability

The prior turn implemented stateless INVALID_TOKEN responses and unreadable-token
Retry fallback. New independent-peer tests now verify both behaviors for v1/v2.

TestStatelessInvalidTokenUpstream uses the real admission endpoint over UDP with
an upstream quic-go client. Only the admission clock advances to expire the Retry;
no wall-clock sleep is required. The endpoint returns no Conn, emits a bounded
response addressed to the actual peer, and has no accepted connection. Upstream
reports a remote transport error with code 0x0b rather than a handshake timeout.

TestForeignNewTokenUpstream obtains NEW_TOKEN from an upstream server, then
replaces that server with our public listener at the same address and TLS
authority. Packet observation verifies a token-bearing first Initial and Retry.
The upstream client completes the replacement handshake, delivers an exact
DATAGRAM payload, and receives a new token. This validates cross-implementation
unreadable-token recovery, beyond own-codec damaged-token fixtures.

The initial replacement fixture failed because closing the upstream listener
retained its UDP binding. The fixture now explicitly owns and closes both the
upstream Transport and socket before rebinding. Ten race repetitions of both
tests pass for both versions; interop vet passes. Production code was unchanged.
The full implementation goal remains active.

## Previous: 2026-09-20 stateless INVALID_TOKEN and unreadable-token fallback

RFC 9000 verified erratum 7861 clarifies that unreadable tokens cannot be treated
as identified Retry tokens. Retry validation now distinguishes authenticated
contents rejected for expiry/future time or other content checks from tokens
that cannot be authenticated. The new error wraps ErrRetryToken for callers.

Admission returns an encrypted Initial CONNECTION_CLOSE with INVALID_TOKEN for
authenticated but rejected Retry contents. It allocates no connection/TLS state,
uses the received destination CID for Initial keys, targets the client source
CID, and sends only a small non-ack-eliciting packet. Endpoint tests confirm
response routing and no connection/router/accept-queue capacity consumption.
Client tests for both versions verify the wire error is received immediately.

Unreadable, foreign and truncated tokens now fall back to bounded Retry rather
than being discarded. This supersedes the preceding checkpoint's unknown-token
limitation. With this codec's address-bound AAD, a token returned from a changed
address is unreadable and also follows this fallback.

Full root race suite and vet pass. Five focused token/admission race repetitions
and five upstream token/Retry interoperability repetitions pass. Additional
tests distinguish authenticated expiry from every single-byte token mutation.
Independent-peer decoding of the new stateless close is not yet directly tested.

Reference: https://www.rfc-editor.org/errata/eid7861
The full implementation goal remains active.

## Previous: 2026-09-20 multipath token receipt and damaged-envelope fallback

The previous turn connected client token storage. Added v1/v2 encrypted-packet
tests for NEW_TOKEN on an additional path, authenticated receipt time and owned
bytes, damaged-packet rejection, packet replay, duplicate token frames in fresh
packets across both paths, and delivery of a distinct token on path zero.
Ten race repetitions pass. The fixture installs engine paths; it does not claim
UDP path discovery or independent-peer multipath coverage.

Admission audit found that corrupting either address-token format byte caused
ErrRetryToken/discard rather than Retry fallback. Four new v1/v2 regression cases
failed before the fix. IsAddressToken now classifies the fixed 54-byte envelope
by size, disjoint from Retry's 58–70-byte envelopes; AddressTokens.Validate still
requires the exact format and valid authentication before granting address credit.
Invalid tokens produce a bounded Retry without allocating a connection. Tests
complete the resulting Retry handshake. Arbitrary/truncated envelopes outside
this fixed size retain the existing unknown-token behavior.

The full token and transport race suites and their vet checks pass.
Five focused race repetitions pass for tokens and admission. Classification
tests cover input lengths 0–512 and actual Retry tokens for every supported
original destination CID length. This follows RFC 9000 section 8.1.3's invalid
NEW_TOKEN fallback guidance:
https://www.rfc-editor.org/rfc/rfc9000.html#section-8.1.3

Full goal remains active; early data, ECN, full standards/API parity and measured
performance targets are incomplete.

## Previous: 2026-09-20 client NEW_TOKEN receipt and reuse

Config.TokenStore now connects client packet receipt to future DialAddr Initials.
Keys include normalized TLS server name, destination port and QUIC version.
Each attempt consumes one eligible token, checks its version, size and age, and
copies its bytes into the engine. Retry still replaces this Initial token.

Both base and multipath frame dispatch store NEW_TOKEN after TLS completion.
The owner callback receives an owned copy. Per-connection SHA-256 history retains
at most 64 unique tokens and then ignores additional tokens; retransmissions
cannot restore consumed tokens through cache-history eviction on that connection.
Custom TokenStore callbacks must return promptly and not reenter connection APIs.

Five race repetitions cover receipt bounds, duplicate/ownership handling,
server/pre-completion gates, public v1/v2 UDP cache reuse and authority isolation.
The full root and existing interop race suites pass; both modules pass vet.
Five independent upstream-server
test repetitions pass for each version: packet observation proves an empty first
Initial and Retry, then token-bearing first Initials with no Retry on two further
connections, with exact DATAGRAM delivery. Existing upstream-client coverage
exercises the reverse direction and listener key replacement.

The full implementation goal remains active. Multipath-specific token receipt
has shared processing but has not yet received a dedicated on-wire test.
Persistent server token keys, broader token policy/API parity, early data, ECN,
full standards conformance and performance targets remain unfinished.


## Previous: 2026-09-20 bounded client token store

The preceding turn enabled public listener issuance and verified upstream reuse.
Added opaque ClientToken, the concurrency-safe TokenStore contract, and
NewLRUTokenStore. Pop atomically consumes the newest eligible token. Put owns its
byte copy; returned consumed tokens cannot mutate retained cache state. Origin
LRU and per-origin record limits bound storage. Consumed records retain hashes
for bounded duplicate suppression; eviction also evicts duplicate history.

Store policy rejects unsupported token versions, future/expired receipt times,
empty values and payloads above 512 bytes. The 24-hour client retention and payload
cap are implementation policies, not server lifetime decoding. Server/version
key construction is delegated to the forthcoming client integration. Positive
capacity arguments are required, matching the constructor's documented contract.

Ten race repetitions pass for ownership, consume-on-use, version-key separation,
expiry, bounds/LRU ordering, maximum payload, invalid inputs, concurrent Pops and
concurrent duplicate Put/Pop. The latter delivers one token across 64 consumers;
a separate test consumes 64 distinct tokens across 128 readers. Root vet passes.
No runtime receive or dialing path uses this store yet, so no new interoperability
claim is made. Wiring Config, NEW_TOKEN receipt and Initial reuse remains next,
along with the broader protocol/API/performance objective. The full goal is active.

## Previous: 2026-09-20 public listener NEW_TOKEN and upstream reuse

The preceding turn automated issuance for internally configured endpoints.
ListenAddr now configures address tokens using a separately generated random key
and a 24-hour lifetime. Key material is local to each listener; restarting retires
old address tokens, which correctly fall back to Retry. No new public knob is
needed for issuance; persistent/shared keys remain future API work.

An upstream client uses its TokenStore against our public listener. A packet
observer checks the first Initial and received Retry packets. Four connections
per version prove: no-token Retry, token reuse from a different port without
Retry, old-token fallback after restarting the listener on the same address,
and renewed-token reuse without Retry. All connections store a fresh token and
transfer exact DATAGRAM bytes. Five focused race repetitions pass in both
versions. The wrapper's lack of UDP buffer tuning support produces the expected
upstream diagnostic and does not bypass verification.

The initial fixture incorrectly required exactly one first-connection Retry;
multiple Initial datagrams can elicit multiple replies. The corrected assertion
requires at least one Retry without usable address evidence and zero on valid
reuse. The first full upstream run used the old compiled fixture and failed only
that assertion. Root race and both vet gates pass. The corrected full upstream
run passes in 46.793 seconds; the final restart extension has separate
focused race coverage above.

Our client token cache and Initial reuse, persistent key policy, single-use
server policy and broader protocol/API/performance work remain unfinished.
The full goal stays active.

## Previous: 2026-09-20 automatic endpoint NEW_TOKEN issuance

The preceding turn added reliable token issuance. Timestamped server endpoint
events now queue one NEW_TOKEN after confirmation when AddressTokens is configured.
Issuance uses the router's committed peer address and the event's clock. A per-
connection flag prevents duplicate issuance, while normal frame recovery retains
the issued bytes. A full send queue defers issuance to a later event without
preventing application acceptance or failing the connection.

The existing two-client endpoint regression now configures the codec and checks
both versions: distinct tokens authenticate for each client's observed address,
handshakes converge with all reliable output acknowledged, and repeated sync
cannot issue another token. A separate full-queue test verifies deferral and
retry. Focused and full root race tests, the full default upstream race suite
(46.499 seconds), and both vet checks pass.

Public ListenAddr does not yet configure the new codec, so this is automatic
issuance for internally configured endpoints. Public listener integration,
client token storage/consume-on-use and independent token-reuse interoperability
remain unfinished. The full goal remains active and incomplete.

## Previous: 2026-09-20 reliable NEW_TOKEN issuance operation

The previous turn connected NEW_TOKEN validation to internal server admission.
Added Conn.IssueAddressToken for the endpoint to queue a fresh token after
handshake confirmation, using the actual validated client address and configured
codec. Clients and unconfirmed servers cannot issue tokens. Tokens enter the
ordinary reliable application frame queue, including its multipath selection.
Optional issuance checks capacity before enqueue so a full send queue does not
fail the connection.

The v1/v2 regression drops the original packet, triggers the actual recovery
timer, delivers recovery output, and processes ACKs with a monotonic simulated
clock. The original token must remain unchanged and become acknowledged; a fresh
issuance must differ. It also checks role/confirmation gating and full-queue
behavior. Focused and full root race tests and root vet pass. No network caller invokes this new operation yet,
so no new independent-peer issuance result is claimed.

Automatic endpoint issuance, client storage/public configuration, token reuse
policy and independent NEW_TOKEN interop remain unfinished. The full goal stays
active and incomplete.

## Previous: 2026-09-20 NEW_TOKEN server admission integration

The preceding turn added the address-token codec. Internal transport Config now
accepts an optional AddressTokens validator. Stateless admission recognizes its
fixed envelope, validates the actual client IP and Initial version, and admits
valid tokens without Retry. The original destination CID remains the first
Initial's DCID, and no retry_source_connection_id is sent. Only authenticated
address evidence removes the amplification limit.

Recognized NEW_TOKEN values with expired lifetime, wrong IP/version, invalid
authentication or unavailable keys now issue Retry instead of being handled as
invalid Retry tokens. The existing invalid-Retry behavior is unchanged. Envelope
recognition is explicitly not authentication. Unrecognized or malformed envelopes
still follow the existing unknown/Retry-token handling; this is not a general
foreign-token format interoperability claim.

Fourteen admission cases cover both versions, valid tokens, changed ports,
wrong addresses/versions, expiry, damaged authentication and unavailable keys.
Each valid path and Retry fallback completes its TLS/QUIC handshake; valid
NEW_TOKEN admission checks address validation and absence of Retry CID state.
Focused race, full root and default upstream race suites and both vet checks pass.
The default upstream suite completed in 46.528 seconds.

This is internal admission support. Server issuance/retransmission, client token
storage/consume-on-use, public configuration, replay policy and independent
NEW_TOKEN interoperability remain unfinished. The full goal remains active.

## Previous: 2026-09-20 NEW_TOKEN protection component

The preceding turn fixed completed public reads after runtime shutdown. Added
internal/token.AddressTokens as the protection layer for NEW_TOKEN integration.
It uses XChaCha20-Poly1305 with random 192-bit nonces and a domain separate from
Retry tokens, binding the canonical client IP and QUIC version while allowing
an ephemeral port change. The fixed 54-byte format carries protected issuance
time and lifetime. Current and previous keys support rotation; validation applies
the shorter of the issued lifetime and current policy. The configurable one-second
to seven-day lifetime range is implementation policy, not an RFC requirement.

Token race tests and root vet pass. Fuzzing completed 3,133,988 executions.
Tests cover both versions, IPv4/IPv6/mapped IPv4, changed port, wrong IP/version,
key overlap/retirement, expiry/future timestamps, changed lifetime policy, all
single-byte mutations, every truncation, trailing data and Retry separation.

RFC 9000 sections 8.1.1, 8.1.3 and 8.1.4 were checked:
https://www.rfc-editor.org/rfc/rfc9000.html#section-8.1.3
This is not yet connected to transport issuance/admission or client storage.
Tokens remain reusable within their bounded lifetime and address scope; server
single-use policy, client consume-on-use/deduplication, authoritative-server
scoping, retransmission and independent interop remain required integration work.
No NEW_TOKEN or early-data support is claimed complete. The full goal stays active.

## Previous: 2026-09-20 completed public reads after UDP shutdown

The preceding turn fixed original client-lifetime cancellation after peer close.
A new real-UDP regression confirmed that all completed streams returned
ErrEndpointClosed after the socket owner exited, despite engine-retained bytes.
Public receive operations now retry retained reads only after UDPServer.Done
closes, joining endpoint cleanup. A connection mutex serializes the transferred
receive ownership across reads, cancellation and release callbacks. No socket or
network task is kept alive for unread data; existing deadlines remain effective.

Five race repetitions of the v1/v2 public test pass with four concurrent streams,
including cancellation, expired/reset deadlines, exact bytes and EOF/release.
The HTTP/3 default suite now includes reading after observed endpoint shutdown;
the fixture polls actual closed state after peer close, without assuming a fixed
draining duration. Five focused HTTP/3 race repetitions and the full default
upstream race suite pass (46.686 seconds). Root race and both vet checks pass.

This addresses the demonstrated buffered-body ownership gap. Trailer/QPACK,
abnormal-close and additional independent-peer combinations still need coverage.
The full protocol/API/performance goal remains active and incomplete.

## Previous: 2026-09-20 client lifetime cancellation after peer close

The previous turn fixed explicit client Close after graceful peer closure. The
new default-suite regression cancels the original NewClientConn lifetime with a
distinct cause after observing peer close. Before the fix, v1 returned the entire
buffered response and nil error. ClientConn now retains that original lifetime;
active requests observe it independently of the peer-canceled session context.
Their cleanup stops or joins the observer, preserving the bounded registry.
Response Body.Read checks request and owner cancellation synchronously before
reading, so a queued cancellation callback cannot allow buffered data through.

Ten race repetitions of client lifetime cancellation, explicit Close, GOAWAY and
client-close cases pass in both versions. Full root and default upstream race
suites and both vet checks pass; upstream completed in 46.646 seconds. No public API addition or transport timing delay was needed.

Buffered response reads after UDP owner shutdown still need validation, along
with the remaining protocol/API/performance requirements. The full goal remains
active and incomplete.

## Previous: 2026-09-20 explicit client close after graceful peer close

The previous turn fixed the synchronized GOAWAY response-loss case. A new test
now exposed that explicit ClientConn.Close could subsequently leave an unread
response usable: the pre-fix v1 case returned all `startend` bytes and nil error.
The first session-context cancellation could not be replaced by ErrClientClosed.

ClientConn now tracks independently cancelable request lifetimes after acquiring
a concurrency slot. Close synchronously cancels them with ErrClientClosed, even
if the session already records graceful peer close. Each failed exchange removes
its registration; successful responses retain it until Body.Close. Request
cancellation remains the parent and original response.Request identity is kept.
The existing concurrency budget bounds the registry.

Ten repetitions of explicit client close, GOAWAY and client-close race cases pass
after the final adjustment to acquire request slots before registration. Full
root race and both vet checks passed; the adjusted HTTP/3 package race and root
vet checks also pass. The full upstream race suite passed in 46.433 seconds
before the final registration-placement adjustment; the focused repetitions above
validate that final adjustment.

Reads after UDP owner shutdown and cancellation of the original client-lifetime
context after peer close still need coverage. The full protocol/API/performance
goal remains active and incomplete.

## Previous: 2026-09-20 HTTP/3 completed response drain after GOAWAY

The preceding turn retained completed transport receive buffers. HTTP/3 managed
exchanges now keep a request-derived context and observe their session lease
separately. A peer application close 0x100 after GOAWAY permits lower-ID response
reads to drain retained data; rejected IDs, other session failures and explicit
request cancellation still abort. Peer close also stops unfinished upload work.
This removes the synchronized response-loss regression in both QUIC versions.

The strict test passed five race repetitions and is now enabled by default.
A new case cancels the request after observing peer close and verifies that
cancellation still wins. Full upstream race passed in 46.472 seconds. The first
root race run exposed an interrupted read returning io.ErrClosedPipe instead of
ErrRequestRejected; normalization now preserves the causal error when Body.Close
has not been called. Thirty managed-exchange race repetitions and the subsequent
full root race suite pass; both vet checks pass. Ten post-fix upstream repetitions of HTTP/3 client close and GOAWAY cases pass.

The historical failure record now distinguishes passing synchronized regression
coverage from outstanding lifecycle questions: response reads after the UDP
owner stops, and explicit client Close after prior graceful peer close. These
need additional validation. The server-resumption timeout and other known
interoperability failures also remain separate work. The full goal stays active.

## Previous: 2026-09-20 completed receive retention on peer close

The preceding turn produced a deterministic HTTP/3 shutdown failure. The first
production correction now transfers accepted, completed, unread transport
receive buffers into a separate bounded map before draining clears active state.
It retains the existing allocation, not a payload copy; send/packet/crypto state
is still released. ReadStream can consume these buffers after peer close and
routing Close without generating flow-control traffic. EOF or CancelRead removes
the retained entry and invokes the existing release callback. Incomplete,
unaccepted, canceled and already-drained receives are not retained.

Engine tests cover v1/v2, reads in three-byte chunks after routing Close, EOF
callbacks, cancellation cleanup, and rejection of incomplete/unaccepted retention.
Root race, focused transport race and both vet checks pass. The strict HTTP/3
regression still fails in both versions: session cancellation triggers CancelRead,
MessageBody checks its canceled context, and client shutdown requests closure.
This is not yet a public API or HTTP/3 response-delivery fix. Remaining work must
preserve explicit caller cancellation while letting completed responses drain.

The full default upstream race suite passes in 46.459 seconds. Existing known
failures, including the opt-in strict HTTP/3 case, remain unresolved. The full protocol/API/performance goal stays active.

## Previous: 2026-09-20 deterministic GOAWAY response-loss reproduction

The previous server ticket-version work made progress and passed its checks.
This turn investigated the recurring HTTP/3 response truncation. A new opt-in
fixture waits for successful graceful server shutdown and observed peer close
before reading the active response. Both versions failed in all three race
repetitions, returning no body bytes and close code 0x100. Ten repetitions of
the existing normal-timing fixture pass; nested interop vet passes. An initial
command accidentally ran the nested test name in the root module and matched
nothing; only the subsequent nested-module executions count as evidence.

The fixture now obtains the raw QUIC connection explicitly and wraps it with
NewClientConn so it can synchronize on connection termination. It continues to
check queued-request rejection and handler admission. There are no sleeps and
no production changes. The deterministic case is guarded by the existing
QUIC_INTEROP_KNOWN_FAILURES opt-in; the intermittent case remains enabled.

Code inspection identifies receive-buffer destruction on transport draining,
ReadStream's connection-error precedence, and response-body context cancellation
as relevant paths. The fix needs bounded retention of completed received data
and compatible HTTP/3 body lifetime/cleanup; it must not disguise the issue with
a server delay. interop/KNOWN_FAILURES.md contains the exact reproduction.
The full protocol/API/performance objective remains active and incomplete.

## Previous: 2026-09-20 server ticket version enforcement

Server UnwrapSession now checks the authenticated originating-version record
before returning a session to TLS for PSK selection. Absent, duplicate or
cross-version origin yields a cache miss/full handshake; callback errors retain
their existing fatal behavior. The check also wraps a privately cloned config
returned by GetConfigForClient. Default wrapping/decryption stays bound to the
source configuration's ticket keys, preserving live rotation. Known-version
legacy or malformed transport payloads can still resume without early data.

Added upstream-client tests for both versions, base/selected TLS configurations
and valid/missing/duplicate/wrong-version records. A wrap callback rewrites the
fixture's protected metadata while preserving its TLS identity. The unwrap call
count proves the second handshake offers the ticket; both endpoints must agree
on full versus resumed state and deliver exact DATAGRAM bytes. The unversioned
external early-ticket test now correctly expects a full handshake and still
verifies rejection rollback and no early payload delivery.

Root race, focused server interop and both vet checks pass. The full default
upstream race suite, including the new 16-case matrix, passes in 46.196 seconds. An initial focused command ran in the root
module and matched no tests; it is not counted as validation of the new nested
interop test. Existing intermittent GOAWAY and dial-timeout observations remain
unresolved, regardless of later passing runs.

The preceding turn fixed client ticket selection. Compatible version switching,
NEW_TOKEN version handling, early-data acceptance and the broader protocol/API/
performance objective remain incomplete. The full goal stays active.

## Previous: 2026-09-20 client ticket version enforcement

The RFC 9369 section 5 audit found that merely discarding cross-version early
transport metadata is insufficient: tickets must not be offered across versions
for ordinary resumption either. Client cache selection now requires a unique,
supported, matching originating version. Missing/unrelated/ambiguous metadata is
a cache miss. The version gate is independent of transport schema so a known
origin with legacy transport state can still support 1-RTT resumption.
https://www.rfc-editor.org/rfc/rfc9369.html#section-5

Upstream client-resumption coverage now switches versions in both directions,
expects a full handshake after the switch and successful resumption within the
new version. Direct cache checks assert the other version cannot obtain the
ticket. The shared-cache mutation fixture now seeds via our client so version
provenance exists, then explicitly enables its local EarlyData flag before
concurrent readers; it no longer relies on accepting unmarked upstream cache
entries. Existing byte-for-byte ownership assertions remain.

Root race, focused handshake race and both vet checks pass. The first full
upstream race run failed with the known GOAWAY response-loss symptom and a
server-resumption dial timeout. Five focused repetitions of the affected and
changed tests passed. A second full upstream race run passed in 46.336 seconds.
Both unresolved observations are recorded in
interop/KNOWN_FAILURES.md; neither passing repeats nor this change resolve them.

The preceding turn expanded remembered transport fields. Server-side version
validation before PSK selection, compatible negotiation and NEW_TOKEN version
handling remain outstanding, along with early acceptance and the broader goal.
The full goal remains active.

## Previous: 2026-09-20 expanded remembered transport state

Replaced the credit-only ticket payload with schema-2 EarlyTransport: the eight
credit limits plus max_idle_timeout, max_udp_payload_size and
disable_active_migration. Both client cache storage and server ticket issuance
now snapshot these fields. Internal ResumedTransport exposes the decoded state;
credit comparison remains a separate check, and early data remains disabled.
The bounded codec rejects invalid UDP limits, invalid flags, invalid nested
lengths, truncation, trailing data and unknown/old schemas. Old credit-only
metadata still permits ordinary 1-RTT resumption but cannot restore this state.

Full root and default upstream race suites and both vet checks pass. Codec fuzzing
completed 2,866,716 executions. Handshake tests verify refreshed policy fields,
legacy ticket fallback, client/server separation and concurrent cache ownership.
The default upstream suite took 47.103 seconds; opt-in known failures remain.

The preceding turn added client credit snapshots. The new EARLY_DATA.md records
remaining extension-state audits, application/replay policy and early packet
processing requirements. RFC 9000, RFC 9287 and RFC 9368 were checked for the
relevant retention and pre-handshake greasing rules. The full goal remains active
and incomplete, including broader protocol/API/performance requirements.

## Previous: 2026-09-20 client cached ticket limit snapshots

Client QUICStoreSession events now attach the issuing connection's server limits
and QUIC version to local SessionState.Extra. This metadata is independent of the
opaque server ticket. Both roles restore the snapshot at QUICResumeSession;
clients continue to use a privately decoded cache entry. A refreshed ticket uses
the current peer limits, rather than perpetuating the previous session snapshot.
Storage requires received, parsed peer parameters. EarlyData remains false.

The handshake tests now run three connections for each version and server ticket
metadata policy. They verify original limits on the first resumption, updated
limits on the second, and that missing/malformed server-private metadata does
not remove the client's local snapshot. Eight concurrent readers mutate private
Extra and ticket bytes and EarlyData; the shared cached object remains byte-for-
byte unchanged. Full root and default upstream race suites and both vet gates
pass (default upstream suite 46.001 seconds; known opt-in failures remain).

This remembers only the eight credit limits. RFC 9000 section 7.4.1 also requires
other applicable understood parameters for clients attempting early data:
https://www.rfc-editor.org/rfc/rfc9000.html#section-7.4.1
The remaining parameter/extension audit, application bindings, replay policy,
early packet processing and broader RFC/API/performance work remain unfinished.
The preceding turn bound server ticket metadata. The full goal stays active.

## Previous: 2026-09-20 server ticket limit binding

Server TLS ticket issuance now embeds the local eight-limit snapshot and QUIC
version in a namespaced SessionState.Extra record. The limits codec retains its
schema byte; the default TLS ticket protection authenticates the enclosing state.
Custom ticket callbacks remain responsible for protecting/preserving their state.
The server resume event restores only a valid, unique, version-matched record.
Missing, malformed, duplicate and cross-version metadata yield no usable limits
without rejecting ordinary 1-RTT resumption. ResumedLimits is internal evidence,
not an early-data acceptance decision. Early data remains explicitly disabled.

Full root and default pinned-upstream race suites and vet pass. Additional
handshake race tests cover both versions across valid, missing, malformed,
duplicate and cross-version records. They complete a full and then resumed
handshake, including reduced current credit, and assert no early traffic keys.
Unit tests cover truncation, trailing bytes and preserving unrelated Extra data.
The default upstream suite still excludes documented opt-in known failures.

The preceding turn added the standalone limit codec. Client remembered state,
application/replay policies and early packet processing remain unfinished, along
with the broader RFC/API/performance work. The full goal remains active.

## Previous: 2026-09-20 remembered 0-RTT limit component

Added wire.EarlyLimits, a scalar snapshot and comparison for the seven minimum
credit limits from RFC 9000 section 7.4.1 plus max_datagram_frame_size from
RFC 9221 section 3. Every remembered limit must be supported by the current
configuration; invalid snapshots fail comparison. A versioned private codec is
bounded at 65 bytes and rejects unknown schemas, truncation, trailing bytes and
invalid limit ranges. Snapshotting excludes connection IDs, reset tokens,
preferred addresses, ACK policy and multipath negotiation.

This component is not connected to ticket issuance or early-data acceptance yet.
It is specifically the limit comparison, not all remembered transport parameters:
version/application bindings, optional policies, replay decisions, remembered
client state and packet processing remain required. Existing 0-RTT rejection
behavior remains in force. The codec must be protected by its enclosing TLS
ticket when integrated.

Wire race tests, root vet and 3,250,425 fuzz executions pass. Tests cover each
independent credit reduction/increase, DATAGRAM disablement, scalar ownership,
default encoding and maximum representable limits. RFC sources were checked:
https://www.rfc-editor.org/rfc/rfc9000.html#section-7.4.1
https://www.rfc-editor.org/rfc/rfc9221.html#section-3
The preceding turn made progress through callback-failure isolation tests.
The full goal remains active and incomplete.

## Previous: 2026-09-20 ticket callback failure isolation

Added v1/v2 upstream tests for injected WrapSession and UnwrapSession errors.
A healthy connection first populates the frozen cache. The failed connection
must receive a remote transport close, invoke the failing callback exactly once,
and never appear through server Accept. Clearing the fault on the same listener
must then permit a resumed handshake and exact DATAGRAM payload delivery.
All four cases pass under race instrumentation; interop vet passes. Production
code is unchanged; exact TLS alert mappings and broader callback policies remain
separate coverage gaps.

The preceding turn made progress through real 0-RTT rejection evidence. Accepting
0-RTT, automatic clock-driven ticket rotation, and broader protocol/API/performance
work remain unfinished. The full goal stays active and incomplete.

## Previous: 2026-09-20 external 0-RTT ticket rejection

Added upstream-client v1/v2 tests that seed a genuine early-data-capable ticket
from an upstream server, then reuse the test ticket key at our server. A custom
PacketConn blocks response reads until a real 0-RTT packet has been written, so
the test cannot pass merely because the handshake completed before the early
write. Both peers resume, upstream reports Err0RTTRejected and Used0RTT=false,
and after resetting rejected streams via NextConnection only the new exact
1-RTT payload and FIN reach our application.

Both cases pass under race instrumentation; interop vet passes. Initial fixture
errors (unused import and overriding the certificate's configured DNS name with
an IP) were corrected; no certificate verification bypass was introduced. The
wrapper's cleanup unblocks reads before transport shutdown. Production code is
unchanged. The preceding turn made progress by fixing live ticket-key rotation.
Accepting 0-RTT, replay policy and remembered transport/application parameters
remain unfinished; the full goal stays active and incomplete.

## Previous: 2026-09-20 live server ticket-key rotation fix

A new upstream-client v1/v2 test exposed that SetSessionTicketKeys on the original
TLS configuration did not reach the running listener: old tickets still resumed
after their key was retired. NewServerEndpoint cloned the configuration before
the handshake adapter installed default callbacks, so those callbacks used the
listener snapshot's isolated key state. The endpoint now binds default ticket
encryption/decryption to the original shared configuration while retaining its
snapshot for other fields and preserving custom callbacks.

The test keeps the listener running, freezes an original ticket, retains its key
under a new primary key (resumption succeeds), then removes the old key (full
handshake required). All three established connections continue delivering exact
DATAGRAM payloads after rotation. Both versions failed before the fix and pass
now. Existing server-resumption/custom/selected-policy gates, full root/default
interop race suites, and vet pass. Clock-driven automatic rotation and callback
error-path coverage remain outstanding. The prior turn made progress through
selected TLS configuration tests. The overall goal remains active and incomplete.

## Previous: 2026-09-20 selected TLS ticket configuration

Expanded the upstream server ticket-policy matrix with GetConfigForClient
selection. The base listener has unrelated ticket keys and the opposite disabled
policy; it must not override the selected configuration. Each selected case
requires exactly one selection callback and zero base wrapping callbacks. All
retained-key, retired-key and disabled-policy stages still verify both peers'
resumption decisions and exact application bytes/FIN, with default and custom
selected wrapping in v1/v2.

All thirty-two connections pass under race instrumentation; nested-module vet
passes. Production code is unchanged. This covers alternate configuration policy
selection but not callback errors, live key rotation, or 0-RTT. The preceding turn
made progress through ticket-key and wrapping policy tests. The overall goal
remains active and incomplete.

## Previous: 2026-09-20 server ticket-policy interoperability

Added a sixteen-connection v1/v2 matrix against the upstream client with default
and custom WrapSession/UnwrapSession callbacks. The client ticket is deliberately
frozen while listeners restart with explicit key sets. Retaining its old key
permits resumption; retiring it produces a full handshake; disabling server
tickets rejects it even with the original valid key restored. Custom wrapping
preserves an Extra marker, with exact wrap/unwrap/restore call counts and no
callback calls under the disabled policy. Every connection transfers exact
application bytes and FIN.

The first matrix passed but its disabled stage also lacked the old key; corrected
that stage to retain the valid key so the test isolates policy rejection. The
final matrix passes under race instrumentation; interop vet passes. Production
code is unchanged. This covers explicit keys across restarts, not live or
clock-driven rotation, callback error paths or GetConfigForClient selection.
The preceding turn made progress by enabling server ticket issuance. The full
protocol/API/performance objective remains active and incomplete.

## Previous: 2026-09-20 server 1-RTT session tickets

Servers now request one TLS ticket at the handshake-complete event, with
EarlyData=false. crypto/tls honors the effective SessionTicketsDisabled setting.
Per-connection clones use default EncryptTicket/DecryptTicket methods bound to
the shared server TLS configuration, preserving its automatic ticket-key lifecycle
instead of generating isolated keys per connection. Existing custom wrapping
callbacks remain in place. Session events are enabled for both roles and disable
early data on resumption, including when a server reads an older early-capable
ticket. Full 0-RTT support remains unfinished.

Added reverse-role upstream resumption gates for v1/v2: initial handshake,
resumed handshake, and a client with tickets explicitly disabled. Both peers'
DidResume values, verified identity, application bytes/FIN and non-early ticket
state are checked. Focused reverse-role tests, full root and default upstream
interop race suites, and vet pass. These new gates do not yet establish custom
callback, server-disable, key-rotation or alternate TLS configuration lifecycle
coverage; those remain follow-up work. The prior turn made progress by testing
shared external client cache ownership. The full goal stays active and incomplete.

## Previous: 2026-09-20 concurrent external session-cache ownership

Added a real upstream-issued EarlyData=true ticket fixture, frozen so six client
connections share the same cache object concurrently. Both v1/v2 resume at both
peers, retain verified certificate identity, transfer exact bytes/FIN and report
no upstream 0-RTT use. Source session serialization and ticket bytes remain
unchanged, including its EarlyData=true flag. This directly checks the private
copy path with an externally populated cache, beyond our own EarlyData=false
stored tickets. Deliberate ticket reuse is confined to this ownership stress test.

The first test run paired sockets by a wildcard local address and failed to find
the matching upstream peer; binding test clients to concrete loopback addresses
corrected that fixture error. The final twelve resumed connections pass under
race instrumentation, and interop vet passes. Production code is unchanged.
The preceding turn made progress by implementing client resumption; server ticket
issuance, 0-RTT, the unresolved GOAWAY observation and broader objective remain.
The full goal stays active and incomplete.

## Previous: 2026-09-20 client 1-RTT session resumption

Enabled the caller's TLS session cache on clients, with explicit Go QUIC
resume/store session-event handling that disables early data. A cache adapter
privately serializes/decodes cached state before use, retaining opaque Extra data
without mutating shared session entries. Caller TLS configuration and explicit
SessionTicketsDisabled are preserved. Server ticket issuance remains disabled;
0-RTT, its remembered transport/application parameters and replay policy remain
unfinished. RFC 9001 section 4.5 permits resumption without 0-RTT and requires no
remembered QUIC transport state in that mode.

New upstream v1/v2 tests exercise initial, resumed and explicitly disabled
handshakes. Both peers' DidResume values, verified identity, exact data/FIN,
stored EarlyData=false and upstream Used0RTT=false pass under race instrumentation,
with the upstream server permitting early data. Full root race tests and vet pass.
The first full interop race run failed a cache-free HTTP/3 GOAWAY active-response
test. Twenty focused repetitions and a subsequent full interop race run pass;
the observed failure remains unresolved and recorded in KNOWN_FAILURES.md.

The prior turn made progress by fixing keepalive admission across blocked paths.
Server resumption, concurrent/external cache coverage, 0-RTT and the broader
protocol/API/performance objective remain unfinished. The full goal stays active.

## Previous: 2026-09-20 keepalive admission with blocked path queues

Found that queueKeepAlive returned immediately for any queued frame on any path,
including the legacy path-zero alias, before checking whether that path could
send. A regression initially failed when local migration held path zero but a
validated empty path one was available. Multipath admission now selects an
eligible empty path independently of other paths' queues. The legacy single-path
queue check and global memory admission bound remain in place; actual output
still goes through ordinary send gates.

The regression covers v1/v2, a migration-held path zero and an unvalidated path
one, preserving the blocked item and its charge while queuing exactly one PING
on the other path and authenticating its output. This uses synthetic blocked-path
state to isolate selection, not a live migration/loss simulation. Focused tests,
full root race tests, vet and independent basic/migration keepalive race tests
pass. The prior turn made progress through live surviving-path coverage. The full
objective remains active and incomplete.

## Previous: 2026-09-20 live multipath keepalive

Extended TestPublicKeepAlive with negotiated multipath over a distinct client
socket, validation of path one and public abandonment of path zero. Both v1/v2
and client/server keepalive roles survive 3.5 seconds without application traffic
with a one-second idle timeout and MTU discovery disabled. The test now checks
exact stream bytes and FIN in both directions after the quiet interval.

All eight base/multipath cases pass under race instrumentation; root vet passes.
Production code is unchanged. This closes the live two-endpoint surviving-path
keepalive gap, not independent-peer draft interoperability or per-path NAT
retention. Independent multipath keepalive and injected keepalive loss remain
outstanding. The preceding turn made progress through migration/loss evidence
and documented the unresolved pinned-peer update issue. The full objective
remains active and incomplete.

## Previous: 2026-09-20 keepalive across independent-peer migration

Refactored the existing upstream migration gate into a shared helper and added
four keepalive cases: v1/v2, clean or first-packet loss in both directions on
each new binding. Each connection stays idle before migration and after two
successive local socket changes, then exchanges exact request/reply bytes and
FIN and checks the peer-observed address. Loss counts remain exactly two in
each direction across the two migrations. All eight original cases and four
new default cases pass together under race instrumentation; interop vet passes.

Initial failures were investigated rather than counted as successes. A one-second
idle budget expired during validation with both initial flights lost; loss cases
now explicitly use five seconds and wait 5.5 seconds without application traffic.
Clean cases retain one second and 3.5-second quiet periods. A 100 ms v2 keepalive
run failed near packet 105; temporary diagnostics found eight authentication
failures and no accepted key-phase transition. This is consistent with the pinned
peer's known update defect, not independent proof of that packet's derivation.
The default period is 500 ms. The unresolved 100 ms v2 reproduction remains in
QUIC_INTEROP_KNOWN_FAILURES=1 mode and is documented in KNOWN_FAILURES.md.

All temporary diagnostic production edits were restored byte-for-byte. No
production fix or upstream workaround was introduced. This verifies keepalive
across completed migration within the stated validation budget, not survival
when the peer's idle deadline expires during a loss-induced validation delay.
The prior turn made progress via independent basic keepalive/control tests.
Live multipath keepalive coverage and broader protocol/API work remain; the full
goal stays active and incomplete.

## Previous: 2026-09-20 independent keepalive interoperability

Added eight real-UDP cases against pinned quic-go v0.62.0, covering v1/v2,
independent client/server roles and enabled/disabled keepalive. Our endpoint
advertises five seconds and configures a ten-second keepalive period; the peer
advertises one second. With upstream keepalive and both MTU discovery mechanisms
disabled, enabled cases stay open without application traffic for 3.5 seconds
and then transfer exact payload/FIN. Disabled controls require explicit idle
errors at both endpoints (observed about 1.02 seconds). This independently checks
the negotiated-timeout clamp, not just our two-endpoint timer behavior.

The entire new matrix passes under race instrumentation; nested-module vet passes.
Production code is unchanged, so prior full root/default interop regression
evidence remains applicable. Loss, live migration and multipath keepalive tests
remain outstanding. The preceding turn made progress by implementing keepalive;
this turn adds independent evidence for its basic operation and disabled control.
The full objective remains active and incomplete.

## Previous: 2026-09-20 connection keepalive

Added public/internal KeepAlivePeriod configuration, disabled by default with
negative values rejected. Confirmed idle connections queue ordinary PINGs, with
the period capped at half the negotiated nonzero idle timeout. Deadline queries
are side-effect free. Authenticated activity postpones the deadline; after an
ack-eliciting send, recovery owns probes until another peer packet arrives.
Attempts at full queues or without a validated usable path rearm without changing
idle activity. Multipath chooses a validated surviving path; this is connection
keepalive, not independent NAT-binding refresh for every path.

Timer tests cover period negotiation, confirmation, encrypted v1/v2 PING/ACK,
exactly one queued probe, silent-peer idle expiry and full-queue retry. A synthetic
multipath fixture verifies authenticated output after abandoning path zero and
rejects an unvalidated survivor. Live v1/v2 tests in both keepalive roles remain
usable after 3.5 seconds without app data with a one-second advertised timeout,
then deliver exact stream bytes/FIN. Full root/default upstream interop race
suites and vet pass. Existing upstream tests do not yet enable this new option;
independent-peer keepalive and live multipath/migration keepalive gates remain.

The initial backpressure test caught enqueue marking a full legacy queue fatal;
preflight capacity admission now avoids invoking that helper on a full queue.
RFC 9000 sections 10.1–10.1.2 were checked against the RFC Editor text and linked
in README. The prior turn made progress with independent stream-credit coverage.
The full objective remains active and incomplete.

## Previous: 2026-09-20 independent incoming-stream credit interoperability

Added a twenty-case real-UDP gate using pinned quic-go v0.62.0 as the stream
sender and our implementation in both client/server roles. It covers v1/v2,
asymmetric incoming limits, disabled directions and each 64-stream boundary.
Checks require upstream's specific credit-exhaustion error, synchronous-open
context expiry, exact payload/FIN delivery and exactly one replenished credit
after completing each bidi/uni stream. The full new matrix passes under race
instrumentation. The first command ran from the root module and selected no
tests; the actual passing matrix was then run from the nested interop module.

Production code is unchanged, so the previous full root/default interop suite
evidence remains applicable. The preceding turn made progress by exposing and
testing the public configuration; this turn closes its independent-peer evidence
gap. Higher stream capacities and upstream default/API parity remain unfinished.
The overall goal stays active and incomplete.

## Previous: 2026-09-20 public incoming stream limits

Added Config.MaxIncomingStreams and MaxIncomingUniStreams, carried through the
shared client/server configuration into transport parameters. Zero retains the
existing default 16; negative values prohibit the corresponding peer-initiated
stream direction. Values above the engine's current per-direction capacity of
64 fail configuration validation. This exposes existing engine capabilities;
upstream's default 100 and higher supported limits remain a parity gap.

New real-UDP v1/v2 tests exercise both client and server initiation, independent
bidi/uni limits, disabled directions, the 64-stream boundary, blocked synchronous
open cancellation and credit replenishment after consuming a finished stream.
The initial 64+64 boundary case reached the separate local 128-object ceiling;
final cases use 64+1 and 1+64 to distinguish peer credit from local capacity.
README documents both bounds. Configuration validation/default tests, full root
and default upstream interop race suites, and vet pass. The new nondefault-limit
behavior is tested between our endpoints, not yet against an independent peer.
The previous turn made progress through a tested packet allocation change.
The overall implementation and compatibility goal remains active and incomplete.

## Previous: 2026-09-20 decoded packet allocation ownership

Combined header/plaintext storage into one fresh allocation per packet open,
with disjoint regions and capped header capacity. Added tests across v1/v2 and
all suites for append isolation, retained outputs across subsequent opens,
input immutability and failed authentication. Header-only retention now retains
the entire decoded allocation; transport does not retain those headers separately.

Focused 1400-byte packet-open samples show 4 to 3 allocations and 1448 to 1440
bytes per open. Full echo samples show 1.2% fewer allocations but slightly higher
bytes/time; no end-to-end speedup is claimed. Full root/default upstream interop
race suites, vet, RFC vector tests and 2,208,001 packet-open fuzz executions pass.
Raw measurements and limitations are recorded in the stream-echo benchmark report.
The prior turn was progress: its benchmark/profile evidence selected this ownership
change. The full goal remains active and incomplete.

## Previous: 2026-09-20 refreshed upstream echo comparison

Ran the existing public server benchmark against pinned quic-go v0.62.0 with
five twenty-operation samples. Current median exchange time is 13.70 ms versus
5.49 ms upstream; allocated bytes are 6,973,466 versus 715,830 per operation.
The independent implementation remains about 2.50 times slower here and uses
9.74 times as many allocated bytes. Byte/FIN checks pass, as do both benchmark
variants under a one-operation race smoke. No production code changed.

Captured a separate 300-operation CPU and allocation profile. Sealing, AEAD
buffers and retained send data lead sampled allocation costs. Darwin syscall
and runtime wait samples dominate CPU output, which is insufficient to assign
an active CPU bottleneck. Raw data, profiles and limitations are retained in
`docs/benchmarks/stream-echo.md`. Buffer ownership is a candidate for further
work, not an established safe optimization yet. The overall goal stays active
and incomplete.

## Previous: 2026-09-20 MTU-sized serialization scratch

A refreshed allocation profile after lazy split metadata showed StreamFrame.Append
at 13.72% of sampled allocated bytes. The reusable buffer still had a 1200-byte
capacity. Increased it to the shared 1452-byte automatic-discovery ceiling, adding
252 inline bytes per connection while avoiding normal larger-frame encoding
allocations. Updated the existing close-clearing assertion for the new capacity.

Final full root race tests, vet and fresh default upstream interop race tests pass
(the initial compile required updating that old fixed-array test assertion).
Five ten-iteration benchmark samples show another 20.6%/17.6% reduction in median
automatic-mode allocated bytes for v1/v2 compared with lazy metadata alone.
V1 elapsed median improved about 3.3%; v2 was essentially unchanged. A follow-up
profile no longer lists StreamFrame.Append above its reporting threshold. Raw
measurements, profiles, fixed-memory tradeoff and limits are in the MTU startup
benchmark report. The overall goal remains active and incomplete.

## Previous: 2026-09-20 deferred reservation transition regression

Added focused coverage of the new deferred allocation path for 1141-, 2500- and
65527-byte items. A failed unsplit completion callback leaves the item live;
invalid split requests preserve descriptor/queue identity, metadata absence and
charge. Subsequent portable splitting succeeds with the application queue full,
and a late ACK of the original completes all fragments and refunds exactly once.

The new test and existing full-buffer, unsplit-refund and split-completion-error
tests pass under race instrumentation; vet passes. Production code is unchanged
this turn, so prior full race/interop and benchmark evidence remains applicable.
This covers explicit error paths, not runtime OOM recovery. The overall goal
remains active and incomplete.

## Previous: 2026-09-20 deferred STREAM split metadata

Larger STREAM admission still charges full fallback capacity, but now allocates
only a small reservation descriptor. Coverage, branch map and fragment table are
materialized on the first real split, consuming that reservation. Unsplit ACK
and cancellation bypass the ledger and refund the full logical charge. Split
initialization remains unpublished until capacity checks succeed.

Full root race tests, vet and fresh upstream interop race tests pass, including
full-buffer splitting and live MTU shrink. New coverage checks lazy metadata and
unsplit ACK/cancellation refunds. Five 10-iteration public transfer samples show
automatic-mode median allocated bytes falling 32.9%/34.8% versus the earlier
v1/v2 baseline; elapsed medians fell 7.2%/8.4%, subject to timing noise and control
variation. Automatic mode still uses more bytes than its disabled control.
Commands, raw output and limits are recorded in the MTU startup benchmark report.
The complete protocol/API/platform/performance goal remains active.

## Previous: 2026-09-20 measured MTU startup allocation regression

Refactored the existing public transfer benchmark into a shared harness and added
v1/v2 automatic-versus-disabled discovery cases. Five 10-iteration samples measure
short post-handshake 256 KiB transfers, with no convergence wait. Automatic mode
was about 7.5% slower for v1 and 5.1% for v2 by sample median, with roughly 82–83%
more allocated bytes. Raw data, limitations and commands are documented in
`docs/benchmarks/mtu-startup-transfer.md`.

A separate 100-iteration allocation profile attributes 24.54% of sampled bytes
flat (26.14% cumulative) to reserveStreamItem. Eager split metadata allocation is
therefore the next measured optimization target, while preserving reservation,
fragment ACK and cancellation invariants. All benchmark cases pass race smoke
runs and vet passes. No production code changed or improvement is claimed; the
overall implementation and measured-performance goal remains active.

## Previous: 2026-09-20 candidate replay-history fuzz model

Added FuzzCandidateChallengeReplayHistory with an independent per-number
membership oracle, separate from the implementation's range insertion/merging.
Inputs mix candidate challenges, normal packet admission and irrelevant frames;
checks cover freshness, bounded eviction, membership and unchanged candidate
ACK/admission state. The domain is 512 packet numbers and at most 512 operations
per input, with explicit eviction/reordering seeds.

A 10-second local campaign using four workers completed 190226 executions and
passed. Focused race tests for the fuzz seeds, challenge/CID replay bounds and
encrypted peer migration passed, as did vet. Production code was unchanged, so
prior full race/interop evidence remains applicable. Wider fuzz domains and the
full protocol/API/platform/performance objective remain open; goal active.

## Previous: 2026-09-20 decoding reference across owner handoff

Moved authenticatedExpected into the shared numberSpace so both legacy packet
receipt and address-aware path authentication update/use the same reference.
Path-zero adoption retains it by aliasing the existing space; migration recovery
reset preserves it. This also covers authenticated packets dropped because the
pre-activation retention buffer is full, without recording receipt or ACKs.

The initial test incorrectly assumed ordinary retention withholds receipt and
failed. Inspection showed that only saturated retention does so; the corrected
v1/v2 test explicitly fills the accounting limit, crosses a packet-number
boundary, restores capacity and verifies handoff plus later admission. Candidate
coverage now includes a recovery reset. Final full root race tests and vet pass;
fresh upstream interop race tests also pass for this production change. The
broader goal remains active and incomplete.

## Previous: 2026-09-20 authenticated packet-number reconstruction

Path authentication previously reconstructed truncated packet numbers only from
normal admitted receive history. Candidate authentication intentionally leaves
that history unchanged. Added a per-path authenticated expected-number reference
and use the larger of it and normal receipt for decoding, updating it only after
successful packet authentication. ACK/admission history remains independent.
RFC 9000 section 17.1 was checked for the authenticated-number reference rule.

Receiver-boundary v1/v2 tests cross a 16-bit boundary with short encodings,
exercise reordering, reject a forged higher number and check path isolation and
unchanged receive/ACK state. Full root race tests, vet and fresh default upstream
interop race tests pass. This is authentication-boundary coverage, not a complete
peer packet-number encoding-policy scenario. The overall goal remains active;
full protocol/API/platform/performance completion has not been established.

## Previous: 2026-09-20 encrypted migration control replay coverage

Expanded the encrypted v1/v2 peer migration test with a CID replacement during
validation and an obsolete CID advertisement alongside challenges and withheld
STREAM data. Sixteen encrypted candidate replays preserve retirement queue count
and charges. After binding commit, replay admits the withheld data without
repeating CID controls or replies. Local mutual-validation tests now also replay
a pending challenge sixteen times and verify response identity/tail, charges,
normal receive history and ACK state.

Full root race tests and vet pass for the first test extension; a focused race
run passes the subsequently added local replay assertions and peer regression.
Only tests/documentation changed; the previous production revision's upstream
interop run remains current evidence. This closes decoded-only coverage for these
replay cases, not independent-peer migration or the complete protocol/API/
platform/performance objective. The goal remains active and incomplete.

## Previous: 2026-09-20 candidate CID control replay suppression

CID handlers preserve state on duplicate frames, but obsolete NEW_CONNECTION_ID
frames can still enqueue retirement responses. Candidate packets bypass normal
receive-history admission, allowing replays to repeatedly charge that queue.
Added a separate 64-range per-path history for candidate CID controls, checked
against normal receipt too. Normal dispatch after commit skips controls already
applied during validation while still admitting withheld STREAM data.

V1/v2 decoded-control regression tests cover repeated obsolete CID packets,
unchanged queue charges and receipt state, subsequent stream admission, history
bounds and replay after eviction. Full root race tests, vet and fresh default
upstream interop race tests pass. This is decoded-boundary coverage, not an
independent peer migration wire test. The full objective remains active, with
broader migration, platform, API and performance validation still outstanding.

## Previous: 2026-09-20 candidate challenge replay suppression

Candidate PATH_CHALLENGE processing now uses a separate per-path 64-range history
in both local and peer-initiated migration. Repeated or forgotten challenge
packets do not queue another response. Established packet receipt is checked as
well. The candidate history does not feed ACKs or normal receive admission and
survives binding commit; ordinary dispatch suppresses only already-handled
challenges while allowing previously withheld STREAM data to be admitted.

The encrypted v1/v2 peer regression now replays a candidate packet sixteen times
before commit, then replays it afterward with withheld data. Response counts and
order remain correct, normal receive history stays unchanged before commit, and
the withheld data becomes readable afterward. A separate test verifies history
bounds, eviction and retained gaps. Full root race tests, vet and fresh default
upstream interop race tests pass. Other migration control-frame replay cases and
the full protocol/API/platform/performance objective remain unfinished; goal active.

## Previous: 2026-09-20 peer migration response preservation

Peer-initiated candidate handling now queues challenge responses using the same
bounded FIFO as local migration. Rebinding commit transfers the pending head and
tail into the established path instead of discarding them. Normal dispatch of
the committing packet skips challenges already handled by candidate admission;
other frames continue through ordinary processing.

A v1/v2 encrypted regression test nominates a changed tuple, exchanges validation,
retains two earlier response tokens and commits with a third challenge plus
STREAM data. Exactly three ordered replies are sent and stream data is admitted.
Full root race tests, vet and fresh default upstream interop race tests pass.
Candidate duplicate-packet suppression remains outstanding. The broader protocol,
API, platform and performance objective stays active and incomplete.

## Previous: 2026-09-20 local migration response queue

Local candidate challenge handling now appends to an eight-token response queue
instead of overwriting a pending token. The existing head pointer remains the
send identity and a bounded tail preserves order. Committed sends advance the
queue; blocked sends do not. Overflow returns ErrSendBuffer atomically and
candidate expiry clears all pending responses. Local migration commit transfers
the head plus tail to the established path with no application-buffer charge.

Encrypted v1/v2 tests verify ordered 1200-byte replies, blocked-send identity and
packet numbers, overflow and expiry. Existing local commit tests now also verify
queued-tail preservation at full application capacity. Full root race tests, vet
and fresh upstream interop race tests pass. Peer-initiated candidate accumulation
and candidate duplicate-packet suppression remain open audit items; the overall
goal stays active and incomplete.

## Previous: 2026-09-20 response priority across ACK/PTO scheduling

Pending responses now receive a scheduler pass before ordinary ACK/data work.
On their own path, queued responses precede ACK selection; PTO reuses pending
responses. A blocked response can fall back to a pending ACK without consuming
its queue item. Pacing deadlines are restored after that fallback, so congestion
or pacing cannot turn response priority into an ACK stall.

V1/v2 tests cover response selection ahead of pending ACKs on both paths, token
integrity, retained ACK state and PTO completion. Separate congestion/pacing
checks verify ACK fallback and retained response ownership/deadlines. The baseline
proof test now sends the ACK on the other path before the response, preserving
its assertion that an ACK alone is insufficient. Full root race tests, vet and
fresh default upstream interop race tests pass. Candidate migration senders and
the remaining protocol/API/performance scope still require work; goal active.

## Previous: 2026-09-20 response ordering before local challenges

Inspection showed ordinary PATH_RESPONSE frames already bypassed STREAM backlog,
but both responses and locally generated challenges were inserted at the queue
head. Updated admission to preserve the pending response prefix: response tokens
retain arrival order and new challenges follow them. Existing application memory
charges, saturation reserve, send gates and completion handling are unchanged.
V1/v2 encrypted tests verify three 1200-byte responses in token order before the
new challenge, retained queued STREAM data and exact response-charge release.

Full root race tests, vet and fresh default interop race tests pass. RFC 9000
section 8.2.2 was checked as the prompt-response requirement; this is a queue-order
fix, not proof of complete scheduler conformance. ACK and migration-specific
response priorities remain separate audit work. The overall goal remains active.

## Previous: 2026-09-20 live UDP MTU shrink recovery

Extended the real UDP relay fixture with an atomic size limit and a v1/v2 shrink
scenario. Both peers first discover 1388 bytes; the relay then drops everything
above 1200 while the sender writes exactly three full-size STREAM chunks (3984
bytes), with no small trailing STREAM. No extra driver PINGs or recovery edits
are made after the shrink. Runtime PTO, ACK/loss detection, baseline validation
and fragment retransmission complete exact payload/FIN and sender acknowledgment.
Assertions cover actual oversized drops, the validated 1200-byte fallback,
upward-search cooldown and released send-buffer charges.

The initial 5000-byte shrink experiment passed; the retained test strengthens it
by removing the small trailing packet. Full root race tests and vet pass. No
production code changed. Kernel route-MTU/ICMP/EMSGSIZE testing and the broader
implementation/compatibility/performance objective remain unfinished and active.

## Previous: 2026-09-20 constrained UDP relay discovery

Added real-socket v1/v2 coverage through a bidirectional UDP relay that drops
payloads above 1388 bytes. Automatic discovery on both peers converges exactly
to that limit using production timers and authenticated ACK/loss processing.
Small PING traffic supplies later packets for normal loss detection; no internal
recovery or discovery state is edited. A subsequent 5000-byte STREAM verifies
exact data, FIN and sender acknowledgment completion. The relay records actual
oversized drops and its goroutine is joined on teardown.

The first run confirmed convergence but caught a test-driver mistake: it read
before accepting the stream. The corrected test uses AcceptStream. Full root
race tests and vet now pass; production code was unchanged this turn. This is
userspace size-filter evidence, not kernel route-MTU/ICMP/EMSGSIZE or namespace
coverage. Those validations and the broader implementation goal remain open.

## Previous: 2026-09-20 automatic runtime MTU discovery

Concrete-address UDP runtimes on Linux/macOS now enable discovery after socket
nonfragmentation configuration succeeds. Output selection initializes searches
for bound, confirmed paths and subsequently validated additional paths. The
ceiling is min(1452, peer max_udp_payload_size), with initial usable size 1200.
Existing search state is preserved. Public DisablePathMTUDiscovery is passed
through DialAddr and ListenAddr; unsupported platforms, wildcard sockets and
non-runtime engines retain portable behavior.

Real loopback UDP tests verify v1/v2 authenticated size increases on both peers
without directly initializing discovery, and verify the disable setting.
Admission tests cover unbound/unvalidated paths, peer limits and idempotence.
Full root race tests, vet, Linux amd64 cross-build and fresh default upstream
interop race tests pass. Documentation now describes activation and its limits.
Actual constrained-route/kernel MTU failure testing, wildcard/platform coverage
and full RFC 8899 audit remain open; no throughput improvement is claimed from
these changes. The overall goal remains active and incomplete.

## Previous: 2026-09-20 concurrent unequal-MTU scheduling

Added deterministic encrypted v1/v2 coverage with both paths active. Discovery
confirms a 3000-byte path and three 2500-byte STREAM writes are admitted there.
After both paths become equally preferred, the production multipath scheduler
borrows onto the 1200-byte path, splits to fit, and continues larger sends on the
original path. Delaying the first small-path fragment exercises reordering.
Tests assert traffic on both paths, each packet-size limit, exact bytes/FIN,
stream completion, zero logical send charges and zero flight on both live paths.

Full root race tests and vet pass. This turn changes tests/documentation only.
The fixture uses prepared multipath contexts and an encrypted in-memory link;
real constrained socket behavior and automatic discovery remain open. Public
configuration currently has no MTU-discovery controls, and activation remains an
internal staging hook. The overall goal remains active and incomplete.

## Previous: 2026-09-20 unequal-MTU path failover transfer

Added encrypted v1/v2 transfer tests for a failed large path and a smaller
surviving path. Real probe/ACK exchange confirms 3000 bytes, WriteStream admits
three 2500-byte chunks, and one large packet is transmitted before the failure
API abandons its path. The 1200-byte survivor delivers both outstanding and
queued data through ordinary packet construction and encrypted peer ACKs.
Variants discard the original or deliver its delayed ACK after survivor
fragment transmissions begin. Tests assert exact payload/FIN, stream completion,
released logical charges and zero survivor flight bytes.

Full root race tests and vet pass. Only tests/documentation changed; no new
upstream interoperability claim is made. Failure detection is injected, and real
kernel MTU tests, simultaneous unequal-path scheduling and automatic discovery
remain open. The full implementation goal remains active and incomplete.

## Previous: 2026-09-20 endpoint MTU fallback transfer validation

A deterministic v1/v2 packet-exchange test now completes a STREAM transfer after
an injected synchronous EMSGSIZE rejection through the actual endpoint write
callback. Encrypted discovery first confirms a 3000-byte path; the rejected
2500-byte STREAM is retained, baseline validation exchanges encrypted packets,
and endpoint routing carries the smaller retransmissions and peer ACKs. Exact
payload reassembly, FIN, StreamAcknowledged completion, released queue charges,
released flight accounting and rejection-map removal are asserted.

Both versions also pass with the first baseline challenge dropped. That variant
uses endpoint Deadline and HandleTimeout to trigger the retry. Full root race
tests and vet pass. This turn changes tests and documentation only; the previous
production revision's full default interop run remains the interop evidence.
Real kernel MTU behavior, unequal-path transfer coverage and automatic discovery
activation remain outstanding; the overall goal stays active.

## Previous: 2026-09-20 oversized STREAM local-write fallback

The endpoint now recognizes committed large STREAM output rejected synchronously
with wrapped EMSGSIZE. Recovery removes the rejected transmission without a
congestion-loss event while retaining consumed packet numbers and rejecting ACKs
for them, including after migration. Reliable leaves are requeued before the
rejected packet mapping is removed. Idle activity is restored; MTU resets to
1200 with baseline validation required and the existing upward-search cooldown.

V1/v2 endpoint tests cover connection survival, unchanged congestion window,
flight release, packet-number continuity, rejected ACKs, baseline gating and
reserved splitting at full application capacity. Recovery tests also cover size
eligibility and synchronous latest-packet restrictions. Full root race tests,
vet and fresh default interop race tests pass. These tests inject the socket
error; complete endpoint fallback-transfer and real constrained-kernel tests
remain necessary. Automatic discovery remains disabled. The overall goal is
active and substantially incomplete.

## Previous: 2026-09-20 saturated responder control reserve

Established-path PATH_RESPONSE admission now falls back to an eight-entry
per-path FIFO when application capacity is exhausted. Reserved responses retain
their challenge tokens and take send priority without consuming application
charges. Blocked sends retain the FIFO and packet number; completed sends pop
one response. PTO reuses pending response work, migration clears old-tuple
responses, and reserve exhaustion preserves the existing resource-error policy.

V1/v2 tests cover FIFO token order, duplicate packet suppression, amplification
blocking, pending-response PTO selection and bounded overflow. The baseline
control test now runs with both application queues saturated and validates
successfully. Full root race tests and vet passed. An initial default interop
race run hit a v2 handshake timeout after one dropped client datagram; five
isolated repetitions passed, so the cause remains unconfirmed. A subsequent full
run exposed an HTTP/3 active-request Close race returning the underlying local
QUIC close error. ClientConn now normalizes failed requests to its explicit close
cause, preserving an already-present request cancellation. Thirty focused race
repetitions pass (the pre-fix thirty repetitions also passed, so the original
full-run observation remains the reproducer). Final root race tests, vet and a
fresh complete default interop race run pass.
Ordinary large-packet
EMSGSIZE recovery, automatic discovery and real constrained-network validation
remain unfinished. The full goal stays active.

## Previous: 2026-09-20 bounded baseline challenge slot

Baseline revalidation challenges now use a single per-path control slot, allowing
initiation and retries with a full application queue. PTO can initiate/reuse the
same validation machinery without allocating a queued PING. Blocked sends retain
the challenge and consume no packet number or probe-history entry. Successful
response, validation expiry and migration reset clear the slot.

A saturation test covers PTO initiation, amplification blocking, first-send loss,
retry and encrypted completion with unchanged application memory charge. Existing
baseline and simulated-black-hole tests pass. Full root race tests and vet passed.
Responder-side full-queue handling, ordinary large-packet write-error recovery,
automatic activation and real network validation remain unfinished. The full
goal stays active.

## Previous: 2026-09-20 fallback baseline revalidation

Loss-driven MTU fallback now requests the existing expanded PATH_CHALLENGE /
PATH_RESPONSE exchange before resuming application output on that path. The gate
includes path zero and shared borrowing; packet ACKs alone cannot satisfy it.
Matching responses clear the request, while validation expiry uses existing path
abandonment and reliable-data evacuation. Migration resets obsolete requests.

Tests cover both path-zero/secondary-path gating, expanded response confirmation,
failure evacuation and encrypted v1/v2 simulated-black-hole recovery with the
additional validation exchange. Root race tests, vet and default quic-go interop
race tests passed. Automatic
discovery remains disabled pending saturated control-queue handling, ordinary
large-packet write-error recovery and broader constrained-network validation.
The full implementation and validation goal remains active.

## Previous: 2026-09-20 encrypted simulated-link black-hole recovery

Added v1/v2 tests that confirm a 3000-byte path with encrypted probes, lower a
deterministic link's datagram limit to 1200, and drop three large STREAM packets.
Normal PTO output, encrypted ACKs and recovery loss detection trigger the MTU
heuristic without a manual reset or injected ACK/loss metadata. The reserved
split path retransmits and completes a 7.5 KiB WriteStream transfer with exact
payload, FIN, stream completion and memory release; no further oversized packets
are emitted. Normal congestion reduction is also checked.

Full root race tests and vet passed; the subsequent explicit receiver EOF check
passed in the focused race test. This is simulated-link engine evidence, not
kernel/ICMP or network-namespace validation. Baseline revalidation, automatic
discovery and broader constrained-network validation remain unfinished. The
full implementation and validation goal remains active.

## Previous: 2026-09-20 reserved larger-write admission

WriteStream now selects the enqueue path's confirmed payload size and reserves
all portable fallback metadata before committing flow-control credit. Splits
consume that reservation with no extra send-buffer capacity, and completion or
cancellation refunds it with the root item. If the larger admission will not
fit, a portable partial write can still progress. Payload sizing accounts for
the STREAM length field growing to four bytes on very large paths.

Tests cover full-buffer splitting through maximum UDP payload size, cancellation
refunds, blocked admission without offset/FIN changes, portable fallback and
worst-case wire overhead. Encrypted v1/v2 fallback tests now exercise WriteStream
and its actual completion barrier. Final root race tests and vet passed; default
quic-go interop race tests passed before the length-width correction (the
correction affects only staged very-large-MTU paths).

Automatic discovery is still disabled, so default public operation remains
portable. Baseline revalidation, full black-hole network recovery and constrained
network evidence remain unfinished. The full implementation goal stays active.

## Previous: 2026-09-20 encrypted STREAM fallback packetization

The per-path builder now uses authenticated confirmed MTU limits for ordinary
frames and automatically splits oversized STREAM items into portable prefixes.
Shared borrowing splits in the source queue so blocked sends preserve the tail.
Using 1140-byte prefixes avoids repeated subdivision as path limits shrink.

Encrypted v1/v2 tests confirm a 3000-byte size, emit a 2500-byte STREAM frame,
simulate loss and a return to the 1200-byte baseline, then verify smaller
retransmissions, full reassembly and exactly-once release. Both fragment-only
ACK completion and a late original ACK are covered, along with blocked shared
borrowing. Root race tests, vet and default quic-go interop race tests passed.

Public stream writes remain portable until larger write admission reserves
fallback metadata before committing stream credit. The staged low-level builder
fails on split-allocation exhaustion. Automatic discovery, baseline revalidation,
full black-hole network recovery and constrained-network evidence remain
unfinished. The full implementation and validation goal stays active.

## Previous: 2026-09-20 split-aware recovery selection

Added a bounded, memory-charged split tree to shared STREAM groups. Loss queues,
PTO selection, explicit probe scheduling, detachment and path evacuation now
select unacknowledged leaves while retaining original packet mappings. Duplicate
loss references do not enqueue fragments twice. ACKing an intermediate parent
retires its descendants even before the full logical item completes.

Tests cover nested fragments, duplicate loss callbacks, partial-parent delivery,
other-path queue ownership, multipath PTO selection and repeated path evacuation
followed by a late original ACK. Full root race tests and vet passed; the later
added evacuation test passed with the focused race suite. Automatic splitting
in packet construction and larger dynamic packetization remain unfinished.
The full implementation and validation objective stays active.

## Previous: 2026-09-20 split send-item ownership

Connected shared STREAM delivery to reliable ACK processing and cancellation.
A staged owner operation splits queued items while preserving original packet
mappings and a single logical completion callback. It reserves ledger/member/
fragment memory charges before queue changes and caps retained members at 128.
ACK of fragments or a late original ACK releases the root charge once; canceled
groups retire together without claiming delivery.

Tests cover nested splitting, original/fragment completion order, duplicate
ACKs, partial-ACK cancellation, failed split admission, failed completion and the
real stream outstanding-data/FIN completion barrier. Full root race tests and
vet passed; the subsequently added stream-barrier test passed under race in the
focused split suite. Automatic packet-builder splitting and retransmission queue
selection still need integration. Larger dynamic output remains disabled and
the full implementation goal stays active.

## Previous: 2026-09-20 shared STREAM delivery primitive

Added a bounded byte/FIN delivery ledger for an original STREAM frame plus a
zero-copy splitting helper. Fragment ACKs can overlap or arrive out of order;
a late original-packet ACK can cover all fragments. Final completion runs once,
and a failed callback leaves that final ACK unapplied. FIN-only completion,
maximum offsets and invalid ranges are checked. The bitmap is bounded by one
maximum UDP payload and split FIN remains on the tail.

Unit tests pass, the five-second byte-oracle fuzz run completed 246802 executions
without failure, and full root race tests and vet passed. This is a staged
component, not sender activation: send-item ownership, queue memory charges,
cancellation and retransmission maps must still use the shared ledger before
larger frames can be emitted and split across unequal paths. The full goal
remains active.

## Previous: 2026-09-20 portable STREAM payload utilization

Raised the STREAM write payload cap from 900 to 1140 bytes, with send-buffer
reservation updated to match. The bound reserves worst-case short-header CID,
packet number, authentication tag and STREAM ID/offset/length encodings, so each
frame still fits every validated 1200-byte path after migration or fallback.
Tests verify the exact worst-case packet size and partial-write/FIN behavior in
both versions. Root race tests, vet and default quic-go interop race tests passed.

A small sequential loopback before/after experiment recorded roughly 16.8%
lower median time and 20.6% fewer allocations for 256 KiB v1 base-path transfers.
See benchmarks/portable-stream-payload.md for exact samples, command and limits.
This is preliminary workload-specific evidence, not a quic-go performance claim.

Inspection confirmed larger dynamic STREAM frames require shared delivery
accounting when split for smaller retransmission paths. That splitting, dynamic
packetization, baseline revalidation and constrained-network tests remain
unfinished. The full implementation and performance goal stays active.

## Previous: 2026-09-20 staged large-data loss fallback

Added a conservative per-path MTU fallback heuristic to ACK and timer-loss
feedback: three ordinary large-packet losses and a later acknowledged small
packet return the size estimate to 1200 and defer upward probing ten minutes.
Probe losses are excluded. Newer large-packet success or a sufficiently large
successful probe invalidates older failure evidence; packet-number boundaries
isolate resets. Normal congestion response and recovery accounting are retained.

Tests cover v1/v2 real recovery records, ordinary congestion response, probe
exclusion, newer success, small-ACK ordering and stale migration evidence. Full
root race tests and vet passed. These tests inject metadata for hypothetical
larger ordinary packets because builders still cap them at 1200; they do not
prove end-to-end black-hole recovery. Baseline revalidation, variable-size
packetization, automatic activation and constrained-network evidence remain.
The full conformance, compatibility and performance goal stays active.

## Previous: 2026-09-20 completed-search upward rechecks

Searches that settle below their configured ceiling now arm one ten-minute
upward-recheck deadline. Owner expiry reopens the upper interval without losing
the confirmed size or packet-number history. Duplicate feedback cannot postpone
the deadline; fresh probe evidence remains necessary for an increase. Searches
at the configured ceiling do not arm fruitless upward checks. Both migration
commit paths now clear discovery wait/maintenance timers through a common reset
while retaining the minimum send spacing across tuple changes.

Tests cover v1/v2 completion following local oversized-write rejection, deadline
visibility/expiry, unchanged confirmed size, fresh probe numbers, old ACK
isolation, ceiling confirmation, and migration reset cancellation. Full root
race tests and vet passed. Confirmed-size validation under traffic, black-hole
recovery, variable-size packetization and constrained-network evidence remain
unfinished; automatic activation is still disabled. The full goal stays active.

## Previous: 2026-09-20 bounded discovery ACK waits

Dedicated MTU probes now arm an independent ACK-wait deadline of max(16 seconds,
three base path PTOs), integrated into owner deadline selection and expiry.
Timeout releases the pending search attempt without declaring packet loss,
freeing flight bytes, changing congestion state or reusing packet numbers.
Fresh retries restart the wait; late ACKs can still validate a retained candidate.
ACK/loss, local write rejection and migration reset make stale deadlines inert.
Three failed attempts narrow the bounded search.

Tests cover v1/v2 timer-driven retries, recovery preservation, late ACKs,
attempt bounds, slow-path PTO scaling and reset cancellation. Full root race
tests and vet passed. Completed-search maintenance, revalidation, black-hole
recovery, variable-size packetization and constrained-network validation remain
unfinished. Automatic activation remains disabled; the full goal stays active.

## Previous: 2026-09-20 discovery probe spacing

Added per-path discovery spacing of max(one second, smoothed RTT at commit).
Rapid ACKs and local oversized-write feedback cannot immediately emit another
size probe. The limit is retained across migration and leaves ordinary output
eligible. Once a candidate is ready, its remaining delay enters the existing
path pacing deadline so the owner can wake and resume discovery without input.
Tests cover short/long RTTs, early ACKs, unchanged blocked-send state, owner
deadline visibility, ordinary output and expiry. Full root race tests and vet
passed.

RFC 8899 section 5.1.1 was reviewed. This added spacing is an implementation
rate policy, not its PROBE_TIMER or the complete DPLPMTUD state machine. Probe
loss still uses QUIC recovery. Automatic activation, complete timeout and
revalidation policy, black-hole handling, variable-size packetization and
constrained-network evidence remain unfinished. The full goal remains active.

## Previous: 2026-09-20 local migration MTU integration coverage

Added v1/v2 integration tests in base and multipath modes that confirm a larger
MTU with encrypted packets, leave another size probe outstanding, and perform
mutual local-migration validation. Unvalidated commit attempts preserve search
state. Successful commits clear size/flight evidence while retaining packet
number continuity. A delayed pre-move ACK injected at the recovery boundary
cannot restore old evidence; a fresh encrypted probe/ACK exchange confirms the
initial larger proposal on the new tuple. The fixture completes both endpoints'
validation before testing ordinary post-move output.

Full root race tests and vet passed. This turn adds validation evidence rather
than enabling discovery. Probe rate/revalidation policy, black-hole recovery,
ordinary variable-size packetization, and actual constrained-network tests remain
required. The full conformance, compatibility and performance goal stays active.

## Previous: 2026-09-20 MTU scheduling and rebinding evidence

The multipath scheduler now defers discovery probes until ordinary per-path
output and shared application data have been tried on all available and backup
paths. This fixes an idle path emitting padded discovery before borrowing queued
stream data. Packet construction retains the existing send gates and direct
per-path staging hook. Tests exercise both versions and both path roles, verify
shared stream output precedes probing, and verify discovery resumes afterward.

New rebinding tests emit actual v1/v2 MTU probes, confirm an initial size, leave
a larger probe pending, and commit peer-port rebinding. Search evidence resets
while packet numbers remain monotonic. A delayed ACK of the old probe cannot
confirm the new tuple; an ACK of a fresh probe can. Full root race tests and vet
passed. Local migration reset integration coverage, probe rate/revalidation
policy, black-hole detection and variable-sized ordinary packetization remain
unfinished. Automatic discovery remains disabled and the full goal stays active.

## Previous: 2026-09-20 nonfatal oversized-probe write handling

The UDP owner routes failed writes through endpoint feedback. A dedicated MTU
probe rejected with EMSGSIZE now narrows the search, releases flight bytes,
restores idle activity and rearms recovery without removing the connection.
Fresh smaller probes use new packet numbers. Ordinary packets and other write
errors keep the existing connection-removal behavior. Recovery retains a bounded
ledger of locally unsent probe numbers and rejects their ACKs across path resets;
ledger exhaustion safely falls back to connection removal.

Endpoint tests cover v1/v2 output metadata, wrapped EMSGSIZE, smaller fresh
probes, timer/flight/idle accounting and ordinary/unrelated error handling.
Recovery tests cover rejection atomicity, history cleanup, packet-number
continuity, ACK rejection across resets and ledger capacity. Full root race and
vet checks pass; default independent quic-go interop race checks also passed
before the final idle/PTO timestamp restoration adjustment. No actual low-MTU
kernel rejection test is claimed. Automatic MTU discovery remains disabled;
revalidation, black-hole recovery, scheduling and ordinary packetization remain.

## Previous: 2026-09-20 staged per-path MTU probes

Connected the bounded MTU search to encrypted per-path PING-plus-padding packets
and ACK/loss feedback through an internal staging hook. Peer payload limits and
path validation gate initialization; amplification, congestion and pacing gate
sends without consuming search attempts. Lost probes are retried with new packet
numbers; authenticated late ACKs can confirm a current candidate after recovery
has discarded packet metadata. Migration commits reset size evidence.

New tests pass for v1/v2 encrypted convergence, initialization/send gates,
packet-threshold probe loss without a congestion cut, and late ACK confirmation.
Full root race tests and vet passed.

Automatic/public activation remains disabled. Nonfatal probe write-error handling,
revalidation and black-hole recovery, shared-data scheduling, migration reset
integration tests and variable-size ordinary packetization remain unfinished.
The broader implementation and performance objective remains active.

## Previous: 2026-09-20 MTU probe recovery metadata

Added a dedicated MTUProbe marker to recovery packet metadata. It survives
ACK/loss processing, retains normal bytes-in-flight and successful-ACK accounting,
and excludes probe-only loss from NewReno window reduction. Probe loss cannot
anchor a persistent-congestion interval; ordinary lost packets still can span
it, while an acknowledged probe remains a barrier. Invalid probe metadata
(wrong number space, insufficient size or non-eliciting packet) is rejected
before flight accounting changes.

Tests exercise real sent/ACK/loss processing, mixed ordinary/probe losses,
flight release, persistent-congestion boundaries and metadata rejection.
The recovery race suite, full root race suite and root vet passed.

No production sender marks or emits MTU probes yet. Sender/scheduler integration,
probe EMSGSIZE handling, search feedback, black-hole recovery and migration
revalidation remain required. The broader conformance, compatibility and
performance objective stays active.

## Previous: 2026-09-20 bounded MTU size-search component

Added mtuSearch as a standalone DPLPMTUD size-search component for eventual
per-path ownership. It starts from a caller-validated 1200-byte baseline and a
bounded local/peer ceiling, proposes binary-search sizes without committing them,
and records only emitted probe packet numbers. One probe may be pending; a size
gets up to three attempts before the search narrows. A matching late ACK for a
current-size attempt can confirm that size, while unrelated or abandoned feedback
cannot. Reset discards size evidence but retains the packet-number high-water
mark to prevent pre-migration feedback from validating a new tuple.

Tests cover convergence across MTU ceilings/support limits, loss/reordering,
proposal and invalid-commit atomicity, packet-number exhaustion and reset.
The fuzz pass completed 10,555 executions without a failure. Focused tests,
the full root race suite and root vet passed.

This component is not wired into production packet emission or recovery yet.
The 1200-byte sender ceiling remains in force. Next integration must distinguish
MTU probes from congestion-loss signals, schedule and pad them within send
budgets, handle probe EMSGSIZE without killing the connection, and reset search
on migration. Revalidation, black-hole handling and full RFC 8899 state-machine
behavior remain unfinished. The full goal stays active.

## Previous: 2026-09-20 UDP nonfragmentation configuration

Packetization/MTU review found that UDP sockets did not explicitly disable IP
fragmentation. Added Linux and macOS setup before initial runtime ownership and
dynamic socket registration, covering listener, client, path and migration
sockets. Linux uses IPv4/IPv6 PMTU PROBE options; macOS uses DONTFRAG. The live
dual-stack test exposed EINVAL for the IPv4 option on a macOS IPv6 socket; Apple
kernel source confirms the IPv6 option also governs mapped IPv4 output, and the
implementation now handles that platform distinction. Setup failures propagate
without registering or taking ownership of the failing socket.

Added kernel-option tests for IPv4, IPv6, dual-stack, initial runtime sockets,
dynamic registration and closed-socket rejection. Root race tests, root vet and
the complete default interop race suite passed on macOS arm64. Linux amd64 test
cross-compilation passed; Linux runtime and packet-capture verification remain
open. x/sys is now a direct dependency.

MTU.md documents this prerequisite and the remaining DPLPMTUD implementation:
per-path search, padded probe recovery, nonfatal EMSGSIZE, black-hole recovery,
migration and unequal-MTU packetization tests. Other platforms retain an explicit
unimplemented setup fallback. The packet-size ceiling is unchanged. The full
compatibility, conformance and performance objective remains active.

## Previous: 2026-09-20 multipath idle lifecycle verification

Added v1/v2 regression tests covering connection-wide idle activity after path
zero is abandoned. Authenticated encrypted traffic on path one refreshes the
deadline; corrupted ciphertext does not. Deadline queries cannot fabricate
activity. Expiry remains silent, returns ErrIdleTimeout and clears recovery
timers even when application recovery is held in per-path state.

Separate coverage verifies that a slow abandoned path contributes to the idle
PTO floor while retained, and ceases contributing after disposal without changing
the last-activity timestamp. Five focused race repeats passed before adding the
final expiry assertions; the complete root race suite and vet passed afterward.
No further production change was needed for these lifecycle cases.

The prior turn changed production behavior and evidence; this turn strengthens
validation of that behavior. Full conformance, independent network matrices and
performance targets remain incomplete. The goal stays active.

## Previous: 2026-09-20 multipath review and idle PTO floor

Rechecked the IETF record: multipath remains revision 21 in the RFC Editor queue.
Replaced the obsolete PLAN table entry saying live multipath was gated off and
added MULTIPATH.md with section-level implementation evidence and explicit gaps.
The optional Picoquic binaries were absent at the documented default paths;
historical interop results are not presented as fresh validation.

The review found that IdleDeadline still used the original connection recovery
RTT after activation of per-path application recovery. It now uses the largest
retained application-path base PTO for its three-PTO floor, conservatively
accounting for slower paths. Probe backoff does not inflate that floor; larger
negotiated timeouts and disabled idle expiry retain their existing behavior.
This is an implementation policy applying the connection idle floor to multipath,
not a claim that the draft explicitly mandates a largest-PTO idle formula.

New v1/v2 regression coverage verifies the slow-path floor, real PTO backoff,
negotiated timeout precedence and disabled expiry. The focused multipath review
suite, focused idle tests, complete root race suite and root vet passed.
Full DPLPMTUD, ECN, broader network/interop matrices and performance goals remain
open. The full objective stays active.

## Previous: 2026-09-20 HTTP3 replay cancellation and shutdown

Replay checks cancellation before and after GetBody. Cancellation during body
reconstruction now preserves its custom cause instead of returning a competing
GetBody error, and closes any returned body exactly once. Transport closure is
checked before invoking GetBody; closure during reconstruction is rejected by
normal admission, which closes the unused replacement body. GetBody remains
synchronous and must return promptly; no detached callback worker is introduced.

Unit tests verify cancellation before reconstruction skips GetBody and that
cancellation during reconstruction wins over a simultaneous callback error.
Live v1/v2 GOAWAY fixtures verify cancellation and transport closure from GetBody
cause no second connection or handler invocation and release both bodies once.
Five focused race repeats, root race tests, three HTTP3 interop race repeats and
root/interop vet passed.

The preceding turn made implementation and validation progress. The broader
compatibility, conformance and performance objective remains incomplete and active.

## Previous: 2026-09-20 HTTP3 request rejection retry evidence

Expanded the existing single retry to locally refused stream admission and peer
H3_REQUEST_REJECTED stream errors. Session reports GOAWAY admission refusal before
request bytes are sent, including allocated IDs below the GOAWAY boundary.
ClientConn collects remote rejection evidence at the concrete response-stream
Read boundary or from its transport-owned send context. An application-supplied
error with the same type/code cannot authorize replay. Tracking is omitted for
direct single-exchange clients and the final retry attempt.

Stream rejection can reuse the same connection; GOAWAY still retires it. Body
reconstruction, upload-trailer exclusion, cancellation, DisableRetries and the
two-attempt bound remain unchanged. The transport does not retry failures after
returning a response to its caller.

Unit coverage verifies below-boundary admission refusal without writing bytes.
Live v1/v2 tests exercise RESET_STREAM rejection alone, rejection in both stream
directions, ordinary cancellation, repeated rejection, and original-body cleanup
before reconstruction. Forged application StreamError and sentinel errors do
not trigger replay. Five focused retry race repeats, root race tests, three full
HTTP3 interop race repeats and root/interop vet passed.

Generic connection-failure retries, capacity queuing, full compatibility,
conformance and performance gates remain unfinished. The full goal stays active.

## Previous: 2026-09-20 HTTP3 bounded GOAWAY retry

Transport now permits one retry when received GOAWAY state proves a failed
request was unprocessed: either no request stream was allocated or the allocated
stream is at/above the peer's rejection boundary. ClientConn records that proof
separately from returned errors, avoiding application-supplied sentinel errors
as retry authority. Direct ClientConn.RoundTrip remains a single exchange.

Replay requires no body or GetBody, no upload trailers, and a live request
context. The original attempt closes its body before reconstruction. GetBody
errors/nil bodies are checked, rejected replacement bodies are closed, and
Response.Request remains the original pointer. DisableRetries opts out.
There is no retry after returning response headers or after a second failure.

Unit tests cover body reconstruction and ownership. Live raw/upstream HTTP3
tests on v1/v2 verify successful POST replay after GOAWAY(0), nonreplayable bodies,
opt-out, a two-attempt bound, and no retry for an application body returning the
same rejection sentinel without protocol evidence. Five focused race repeats,
root race tests, three full HTTP3 interop race repeats and root/interop vet passed.

Standalone H3_REQUEST_REJECTED resets, generic failure retry policy, and locally
rejected allocated streams below the GOAWAY boundary are not yet replayed.
Capacity queuing, full compatibility, conformance and performance gates remain
unfinished. The full goal stays active.

## Previous: 2026-09-20 HTTP3 transport gzip decoding

Transport now advertises gzip automatically unless DisableCompression is set,
an Accept-Encoding or Range header key is supplied, or the method is HEAD or
CONNECT. It copies request headers without copying mutable upload trailers and
preserves Response.Request identity. Automatically negotiated single-gzip bodies
are decoded lazily; metadata is updated through Uncompressed, ContentLength=-1
and removal of encoded Content-Length/Content-Encoding. Bodyless response
metadata and explicitly requested or stacked encodings remain unchanged.

The reader preserves framing validation and trailers underneath gzip, reports
malformed headers/truncation/checksum failures, and closes the underlying body
once. Concurrent Close interrupts initialization or payload reads before joining
decoder access. EOF and decoding failure retain reservations until Body.Close.

Unit tests cover negotiation policy, response metadata, decoder failures and
blocked-read interruption. Live upstream v1/v2 tests cover negotiation/opt-outs,
request immutability, trailers, and lazy headers by withholding payload until
RoundTrip returns. Root race tests, three full HTTP3 interop race repeats, final
three focused compression race repeats and root/interop vet passed.

Automatic retries, capacity queuing, full compatibility, protocol conformance
and performance gates remain unfinished. The full goal stays active.

## Previous: 2026-09-20 HTTP3 pool idle timeout

Transport now expires unused connections through IdleConnTimeout, defaulting
to 90 seconds; negative values disable expiry. The connection worker owns the
timer and stops it on exit. Reservation changes notify that worker through a
bounded channel. Expiry rechecks reservation count and the latest idle timestamp
under the pool lock, protecting reuse and subsequent idle periods from stale
timer notifications. Active requests and unclosed response bodies remain exempt.

Live upstream tests on v1/v2 verify reuse cancels the previous idle period,
EOF without Body.Close retains the connection, releasing the body permits
expiry, disabled expiry preserves idle connections, manual idle closure still
works, and subsequent requests dial a replacement. Three focused race repeats,
root race tests, three full HTTP3 interop race repeats and root/interop vet passed.

Automatic retries, capacity queuing, full compatibility, protocol conformance
and performance gates remain unfinished. The full goal stays active.

## Previous: 2026-09-20 HTTP3 pool capacity eviction

At capacity Transport now retires the least recently idle connection, waits for
its worker to join, and retries admission. Cleanup remains charged against the
connection limit; cancellation interrupts the admission wait and preserves its
cause. The pool lock is released during cleanup, and admission rechecks closure,
reuse and capacity afterward. Connections with reserved requests or unclosed
response bodies remain protected, even when the response has reached EOF.
If every connection is reserved, admission still returns ErrConnectionLimit.

A deterministic unit test covers eviction choice, cancellation and capacity
accounting during cleanup. Live tests across three origins on v1/v2 verify LRU
reuse, replacement, active-body protection, rejection body ownership and recovery
after releasing reservations. Ten focused live race repeats, root race tests,
three full HTTP3 interop race repeats and root/interop vet passed.

Idle timeout, queuing for active capacity, automatic retries, full compatibility,
protocol conformance and performance gates remain unfinished. The goal is active.

## Previous: 2026-09-20 HTTP3 pooled transport

Added public Transport implementing http.RoundTripper with one accepting
connection per normalized HTTPS origin. Concurrent callers share dialing;
reservations cover dial waiters, queued requests and response bodies until
Close. Cancellation of the last dial waiter cancels setup. DialTimeout defaults
to ten seconds; MaxConnections defaults to 256 and counts dialing/draining
connections, returning ErrConnectionLimit when full.

GOAWAY retires a connection without interrupting earlier responses; subsequent
requests can dial a replacement. CloseIdleConnections joins unused connections,
and Close permanently stops admission and joins owned connection workers.
Request bodies close on validation, dialing and exchange failure. Configuration
is immutable while in use. No automatic replay or origin coalescing is performed.

Unit tests cover validation/body ownership, shared dialing reservations, capacity,
abandoned dialing, timeout, closure and repeated Close. Live v1/v2 tests verify
concurrent reuse, active response survival across idle closure, fresh dialing
after idle closure and GOAWAY replacement while the old response remains active.
Ten focused live race repeats, root race tests, three HTTP3 interop race repeats,
and root/interop vet passed.

Capacity waiting/idle eviction, idle timeout, automatic retries, decompression,
broader API parity, protocol conformance and performance gates remain unfinished.
The full goal stays active.

## Previous: 2026-09-20 HTTP3 client shutdown lifecycle

Added live v1/v2 coverage for closing a client with a blocked upload and a
queued request: both return ErrClientClosed, request bodies close once, and
the queued request never reaches the peer. A graceful GOAWAY test verifies
that queued requests return ErrPeerDraining while an earlier response remains
readable through completion.

The latter exposed a cleanup race: response Body.Close attempted QPACK
cancellation after validated EOF, sometimes reporting the gracefully closed
peer as an error. Completed responses now skip that unnecessary cancellation;
abandoned input still cancels QPACK work, and late upload failures remain
observable. Added a deterministic decoder-shutdown regression test.

Root race tests, root/interop vet, ten focused lifecycle race repeats and three
HTTP3 interop race repeats passed. Connection pooling is the next client
milestone; broad protocol conformance, API parity and performance gates remain
unfinished. The full goal stays active.

## Previous: 2026-09-20 HTTP3 client dialing

Added public http3.Dial with shared origin/client-option validation before
network work. It clones TLS settings, selects h3 ALPN and preserves explicit
verification policy; nil TLS configuration uses system roots. The context
bounds DNS, handshake and critical-stream initialization without becoming the
successful client lifetime. Setup cancellation interrupts connection writes,
joins its callback and closes partially initialized clients/connections.

The live early-response fixture now uses Dial against upstream on v1/v2, checks
actual hostname-verification failure, confirms caller ALPN is unchanged and
cancels the setup context before successfully exchanging the request. Unit tests
cover malformed origins/options and preserved pre-cancellation causes. Root race
tests and root/interop vet passed; three HTTP3 interop race repeats and three
focused dial/early-response repeats passed.

Connection pooling, automatic retries, multi-address racing, IDNA and the broader
protocol/compatibility/performance gates remain unfinished. The full goal stays
active.

## Previous: 2026-09-20 public established-connection HTTP3 client

Added NewClientConn, ClientConfig and ClientConn.RoundTrip/Close. The client
initializes critical/QPACK streams on an authenticated h3 connection, pins the
request URL origin, bounds request allocation, owns request-body closure and
retains reservations until response-body cleanup. Response TLS state is populated.
Close cancels active work and joins connection processing. Request cancellation
and GOAWAY wake queued/allocation waits; lower-ID active exchanges remain governed
by their leases. Exported peer-draining/rejection errors preserve retry evidence
without implementing automatic retries.

Normal and early-response upstream-server fixtures now use the public client
on v1/v2. Tests verify origin rejection and body ownership, cancellation while
queued, reservation retention after response EOF, origin normalization, and
GOAWAY notification before/after subscription. Root race tests, root/interop vet,
three HTTP3 interop race repeats and final focused package race checks passed.

The client is for an already dialed connection. Dialing/pooling, authentication
policy selection, retries, decompression, Expect/continue and CONNECT tunnels
remain unfinished. It preserves Go Request.Host overrides while pinning URL
origins. The large v2 peer gate and overall conformance/performance objective
remain unresolved; the full goal stays active.

## Previous: 2026-09-20 session-managed client exchanges

Added Session.ExchangeRequest connecting request admission to the concurrent
exchange lifecycle. It combines request cancellation with the GOAWAY-aware lease,
waits for peer settings/encoder readiness, assigns session QPACK/error ownership,
and retains admission through response cleanup and upload joins. Admission and
preflight failures close the body and reset the unused stream without sending
request fields. Rejection causes survive upload/response interruption.

Tests verify admission remains charged after response EOF until Body.Close,
GOAWAY rejects blocked upload/response work at the boundary while preserving
lower-ID requests, and draining prohibits new requests with complete cleanup.
Both live independent-client fixtures (normal exchanges and blocked-upload early
responses) now use session-managed exchanges against upstream on v1/v2.

Root race tests and root/interop vet passed, three HTTP3 interop race repeats
passed, and ten managed-exchange race repeats passed. Public client construction,
stream allocation/release ownership, dialing/pooling/retries and full protocol
compatibility/performance work remain unfinished. The complete goal stays active.

## Previous: 2026-09-20 concurrent client request exchange

Added internal ExchangeRequest coordinating upload and response reception on a
fresh stream. Response headers can arrive while Body.Read is blocked. Remote
H3_NO_ERROR STOP_SENDING cancels only the upload, preserving a complete early
response. Response Body.Close cancels unfinished upload, interrupts unread data,
joins upload/watchers, cancels QPACK work and releases endpoint ownership.
Preflight failures close the request body and reset the allocated stream.
Protocol errors apply connection scope before potentially blocking QPACK cleanup.
Cancellation causes and late upload failures remain observable.

Tests cover blocked uploads, early responses, remote upload cancellation, source
failures, response framing errors, parent cancellation, late failure during Close
and preflight cleanup. The existing live client fixture now uses ExchangeRequest.
A new live upstream-server test verifies that peer STOP_SENDING closes a blocked
request body before local response closure, while the full 403 response remains
readable, on both v1/v2.

Root race tests and root/interop vet passed. Ten exchange race repeats and three
HTTP3 interop race repeats passed; the final focused race run passed after adding
preflight cleanup. Public client APIs, managed session admission integration,
pooling/retries, Expect scheduling and CONNECT tunnel ownership remain unfinished.
The full QUIC goal remains active.

## Previous: 2026-09-20 streaming HTTP request upload

Added WriteHTTPRequest over a fresh MessageWriter. It preflights metadata and
trailer permission, streams bounded body chunks, reads trailer values only after
EOF, rejects newly introduced trailer names, and sends FIN only after successful
body/trailer validation. It closes Body exactly once on success and failure.
Context cancellation interrupts transport writes through the owner callback and
closes the body to wake blocked reads; callbacks are joined before return and
context cancellation causes are preserved. Repeated empty reads are bounded.

Tests cover EOF-with-data and trailer publication, short/long declared lengths,
body read/close errors, preflight failures without wire output, write failure,
no-progress readers, cancellation during blocked body reads and header writes.
The live independent-client fixture now uses this upload adapter against upstream
HTTP3 servers on v1/v2. Root race tests and root/interop vet passed; three HTTP3
interop race repeats and ten focused upload race repeats passed.

Endpoint work still must run upload and response reception concurrently, map
errors to reset/connection closure, and handle early responses and Expect/continue.
Public client pooling/dialing and the full compatibility/performance goal remain
unfinished; the complete QUIC goal stays active.

## Previous: 2026-09-20 outbound Go HTTP request headers

Added HTTPRequestHeaders to convert client http.Request metadata into validated
HTTP3/QPACK fields without reading the body or mutating the request. It applies
Host override, default GET, escaped target/query and OPTIONS *, ordinary CONNECT,
net/http body-length semantics, declared trailers, URL-userinfo Basic authorization
with explicit-header precedence, and sensitive indexing for authorization/cookies.
Header Host/Content-Length/Trailer values do not override the corresponding
request fields. Invalid metadata, connection-specific fields and malformed
authorities/trailer names are rejected before encoding.

Round-trip tests cover cookie/length/trailer mapping, escaped targets, metadata
immutability, authorization precedence, unknown/empty bodies, CONNECT and malformed
inputs. The independent-client live fixture now converts a real http.Request and
streams its Body against upstream HTTP3 servers on v1/v2. Root race tests,
root/interop vet and three HTTP3 interop race repeats passed.

The adapter currently targets HTTPS with ASCII authorities; IDNA, opaque URLs,
extended CONNECT and public client dialing/pooling/upload-response lifecycle
remain unfinished. It does not synthesize User-Agent or compression negotiation.
The complete QUIC goal remains active.

## Previous: 2026-09-20 public HTTP3 listener serving

Added Server.Serve(ctx, *quic.Listener), with listener ownership, duplicate
registration checks, bounded connection tasks and worker joins. Shutdown stops
acceptance without closing the shared UDP socket until active connections finish;
Close closes listeners and aborts connections. Pending connection workers are
included in the listener join, including workers racing shutdown admission.
Serve returns ErrServerClosed after shutdown and continues through individual
connection failures. Invalid configuration/admission leaves listeners caller-owned.

The primary upstream-client fixture now uses listener serving for concurrent
v1/v2 requests and graceful shutdown. A new live test holds a streaming response
open across a Shutdown timeout with MaxConnections=1, verifies the handler stays
alive and the socket remains usable, then completes the response, joins serving
and checks permanent admission closure. Duplicate listener registration is also
checked. Accepted-connection ownership/lifecycle tests remain in place.

Root race tests and root/interop vet passed; three final HTTP3 interop race
repeats passed, including the new listener lifecycle case. HTTP client APIs,
convenience listening/TLS helpers, Expect/tunnel/push/0-RTT features and full
compatibility/performance validation remain unfinished. The full goal stays active.

## Previous: 2026-09-20 public HTTP3 accepted-connection server

Added experimental http3.Server with ServeQUICConn, Shutdown and Close. It
validates ALPN/configuration, bounds connection admission and per-connection
requests, initializes control/QPACK ownership, passes TLS/remote metadata to
handlers, and joins all protocol/request workers before serving returns.
Shutdown permanently stops admission and gracefully drains owned connections;
its timeout leaves handlers running. Close cancels connections/handlers.
Rejected connections remain caller-owned. CANCEL_PUSH is rejected because this
server never promises pushes. Public documentation describes ownership, limits,
trailer permission and unfinished APIs/features.

The live upstream-client test now uses the public server instead of internal
assembly, with six concurrent requests, a two-task bound and graceful shutdown
on v1/v2. New lifecycle tests cover duplicate/over-limit admission, retained
caller ownership, shutdown timeout without handler cancellation, permanent
admission closure, Close cancellation, worker joins and repeated shutdown.
Root race tests and root/interop vet passed; three HTTP3 interop race repeats
passed including the new public lifecycle cases.

Public listener helpers and HTTP client APIs, Expect handling, extended CONNECT,
push, 0-RTT, the large v2 peer gate and full compatibility/performance objectives
remain unfinished. The complete QUIC goal stays active.

## Previous: 2026-09-20 graceful server request drain

Added an optional Drain signal to Session.ServeRequests. Drain interrupts acceptance
and settings waits, sends GOAWAY above the highest admitted request ID, rejects
a stream returned concurrently with draining, and preserves active handlers. It
waits for GOAWAY acknowledgment and request stream release before local close.
Parent cancellation interrupts critical writes and bounds draining. Task slots
now include transport release, so response bytes/FIN are acknowledged before
local graceful closure. The loop owns outgoing GOAWAY while running.

Added SendStream.WaitAcknowledged and the engine delivery query for open streams,
with reset distinguished from successful delivery. Public/engine tests cover
v1/v2 delivery, continued writes, FIN, release and reset. Drain tests cover cutoff,
active handler preservation, delayed response acknowledgment, cancellation before
settings, blocked acknowledgment and acceptance racing shutdown.

Live concurrent HTTP3 tests now initiate drain before the last handler returns.
Upstream sometimes closes normally after consuming all responses, before final
ACKs. Added typed PeerCloseError preserving application/transport namespace, code,
trigger frame and reason, and fixed critical-stream dispatch to retain peer-close
evidence instead of relabeling it H3_INTERNAL_ERROR. Such peer closure remains a
returned error; it is not proof of transport acknowledgment. Live tests require
all response bodies/trailers and accept only typed application H3_NO_ERROR as the
alternative to local completed drain.

Root race tests and root/interop vet passed; three final default HTTP3 interop
race repeats passed. One intermediate blocked-QPACK test timed out during peer
dial; the final repeated suite passed. The large v2 key-update gate remains
open. Public HTTP3 endpoint APIs, broader shutdown/stream-exhaustion audits,
Expect/tunnel dispatch and the complete QUIC objective remain unfinished.

## Previous: 2026-09-20 bounded server request scheduling

Added Session.ServeRequests and the QUIC request acceptor. The server has one
accept loop per session, waits for peer settings/encoder readiness, and acquires
a bounded task slot before accepting a stream. Slots cover header and QPACK
waits, handler execution, cleanup and optional error reporting. Request errors
remain isolated; protocol failure cancels the session and other handlers.
Termination interrupts critical writes and transport I/O before joining all
accepted tasks. The caller subsequently joins critical tasks with Session.Stop.
This is abortive shutdown; graceful GOAWAY admission/draining remains unfinished.

The upstream HTTP3 client fixture now runs six concurrent requests through the
production scheduler with a two-task limit, checking bodies, trailers and request
metadata. Large v1 exchanges pass. Large v2 exchanges repeatedly stalled,
consistent with the recorded upstream key-update defect; that gate remains
available under QUIC_INTEROP_KNOWN_FAILURES=1 and is documented in KNOWN_FAILURES.
The default v2 concurrency case uses smaller payloads and passes.

Root race tests and root/interop vet passed. Ten scheduler race repeats passed
for bounds/slot reuse, joins, stream-failure isolation, protocol-failure fanout,
invalid IDs and cancellation before settings. Three final default HTTP3 interop
race repeats passed. Public endpoint APIs, graceful shutdown, and the large v2
interop gate remain incomplete; the complete QUIC goal stays active.

## Previous: 2026-09-20 server request lifecycle

Added internal ServeRequest owning one accepted request: bounded header parsing,
handler execution, TLS/remote address metadata, response completion, body cleanup
and QPACK cancellation. It recovers handler panics (preserving ErrAbortHandler),
cancels the request context on return, interrupts transport I/O on cancellation,
and applies stream versus connection error scope even when a handler ignores a
body-read failure. Early responses stop unread requests with H3_NO_ERROR per
RFC 9114 section 4.1. Closing a body concurrently with its read does not turn the
expected read interruption into a failed response.

Tests exposed and fixed cancellation classification and a delayed cancellation
callback allowing response FIN after a handler observed cancellation. Original
protocol errors survive connection-abort cancellation. Root race tests/vet and
interop vet passed; three HTTP3 interoperability race repeats passed. Ten focused
request lifecycle race repeats passed, including concurrent body close. The live
upstream-client v1/v2 fixture now uses ServeRequest.

ServeRequest is still internal and relies on the owner to bound/join request
tasks and interrupt shared QPACK writers through the session. Public server
accept/scheduling, graceful shutdown/admission, Expect handling, deadline APIs
and CONNECT tunnel dispatch remain unfinished. The full QUIC goal stays active.

## Previous: 2026-09-20 handler response writer

Added internal HTTPResponseWriter implementing http.ResponseWriter, http.Flusher
and FlushError over MessageWriter. It snapshots final headers, supports repeated
informational responses and implicit 200, sniffs content type on first body write,
adds suppressible Date, discards HEAD payloads, rejects prohibited response bodies,
and sends declared or TrailerPrefix trailers under the existing permission policy.
Finish validates body completion and sends FIN once; transport/semantic failures
are sticky. The live server fixture now invokes an ordinary http.Handler against
upstream HTTP3 clients on v1/v2.

Root race tests and root/interop vet passed; three HTTP3 interop race repeats
passed. Additional focused race tests passed for flush/header freezing, header
suppression, incomplete body rejection and invalid status.

This remains an internal adapter: DATA is unbuffered, automatic small-response
Content-Length and Expect handling are unfinished, and public handler scheduling,
panic recovery, deadlines, stream failure policy and server/client lifecycle still
need implementation. The complete QUIC goal remains active.

## Previous: 2026-09-20 net/http request adapter

Added ReadHTTPRequest mapping validated pseudo-fields into server http.Request,
including escaped path/query, authority/Host, cookies, lengths, streaming body and
separate trailers. Ordinary and negotiated extended CONNECT are supported, with
ControlData retaining the protocol value. Invalid target/authority mappings fail
with cleanup. Tests include OPTIONS *, Host fallback, malformed escapes/fragments,
CONNECT and body/trailer ownership.

The live server fixture now uses the adapter against upstream HTTP3 clients on
v1/v2. Root race tests/vet passed, three combined HTTP3/ls-qpack interop race repeats
passed, and interop vet passed. A final focused race run passed after adding URI
edge coverage. Public handler dispatch, TLS/RemoteAddr population and client/server
APIs remain unfinished; the complete QUIC goal remains active.

## Previous: 2026-09-20 net/http response adapter

Added ReadHTTPResponse for bounded informational responses and final http.Response
construction with MessageBody. It preserves repeated fields, canonicalizes length,
keeps trailers separate, handles unrepresentably large metadata without overflow,
and ignores successful CONNECT lengths. Cleanup is transferred to Body.Close on
success and runs on failures before final headers. Tests cover these mappings,
interim callback/limit errors, bodyless statuses and cleanup ownership.

The live independent client now uses the adapter against upstream HTTP3 on v1/v2,
checking status/protocol/length, body, informational responses and trailers. Root
race tests/vet and three HTTP3/ls-qpack interop race repeats passed; interop vet
passed. A final focused race run passed after refining bodyless length mapping.
Public transport/server APIs, TLS metadata, pooling and retry integration remain
unfinished; the full standards/features/performance goal remains active.

## Previous: 2026-09-20 outgoing Content-Length correctness

Inspection before net/http adaptation found MessageWriter reusing receiver
CONNECT tolerance on send. It now rejects Content-Length on successful CONNECT
responses before QPACK/framing writes; incoming successful CONNECT continues to
ignore the field. Outgoing equivalent repeated/list Content-Length values are
normalized to a single decimal field, retaining NeverIndex and leaving caller
fields untouched. This addresses sender requirements in RFC 9110 section 8.6.

Tests cover successful CONNECT statuses 200/201/299 with zero/nonzero/invalid
lengths, receiver tolerance and wire-round-trip normalization. Three package race
repeats, full root race tests/vet, three HTTP3/ls-qpack interop race repeats and
interop vet passed. Public net/http adaptation has not started; the complete
standards/features/performance goal remains active and incomplete.

## Previous: 2026-09-20 preserve body cancellation causes

Regression tests reproduced MessageBody losing GOAWAY rejection as plain
context.Canceled, both before reading and while blocked on QPACK trailers. Fixed
the adapter to retain context.Cause for matching context-cancellation errors.
Tests verify the retained cause, sticky subsequent errors and one QPACK cancellation
through cleanup. Separate injected protocol/transport failures concurrent with
rejection remain unchanged, so they do not acquire false retry assurances.

Three package race repeats passed after the fix; final root race tests/vet and
three HTTP3/ls-qpack interop race repeats passed. Interop vet passed. Public endpoint
cleanup/retry integration and the wider QUIC standards/features/performance goal
remain unfinished and active.

## Previous: 2026-09-20 streaming HTTP3 body adapter

Added MessageBody implementing io.ReadCloser after final HEADERS. It streams DATA
across frames, skips unknown frames, delivers trailers separately and requires
explicit push policy. Concurrent Close cancels QPACK waits and runs mandatory
endpoint cleanup once, with completion true only after valid FIN. Endpoint cleanup
still owns I/O interruption, stream/QPACK cancellation and request release.

Tests cover one-byte reads across DATA/empty/unknown frames, trailers, incomplete
content, push rejection, early close, cleanup errors and concurrent Close during
blocked source I/O. The live leased-request GOAWAY test now reads the accepted
response through MessageBody and releases its lease in the completion callback.
Three package race repeats, full root race tests/vet, three HTTP3/ls-qpack interop
race repeats and interop vet passed. Public net/http endpoint integration and full
QUIC standards/features/performance parity remain unfinished; goal stays active.

## Previous: 2026-09-20 live leased-request GOAWAY rejection

Added live v1/v2 coverage for two admitted GET streams against upstream QUIC.
GOAWAY(4) rejects stream 4, preserves stream 0 and blocks further admission. The
fixture explicitly cancels rejected QPACK work and sends STOP_SENDING; the peer
must observe remote H3_REQUEST_CANCELLED. Stream 0 subsequently completes status,
DATA and FIN through MessageReader. Both request leases are explicitly finished.
The fixture session helper now exposes Session and configures a bounded request
budget, while retaining its existing dispatcher convenience wrapper.

Three repeats of the new test passed under race on both versions. Three repeats
of the combined HTTP3/ls-qpack suite and interop vet also passed. Production code
and unrelated transport suites were unchanged and not rerun. Peer HTTP3 framing
and endpoint cleanup remain fixture-owned; public admission/draining/retry APIs
and full QUIC feature/performance parity remain incomplete. Goal stays active.

## Previous: 2026-09-20 bounded request admission and GOAWAY rejection

Added client request leases with explicit MaxRequests budget (zero disables),
admission serialized with GOAWAY, rejection contexts for IDs at/above received
limits, and idempotent Finish. Rejected leases remain charged until cleanup;
session failure propagates its cause and clears registry references. The endpoint
must register each allocated ID once, reset failed admissions, cancel QPACK/stream
work and select retry policy. Public/live endpoint use is not implemented yet.

Tests cover limits, invalid/duplicate IDs, release, lowered GOAWAY boundaries,
publication before callbacks, connection failure and admission/GOAWAY races.
The full root run exposed a pre-existing accept/abort race masking protocol causes
with context.Canceled. Fixed sessionIncoming.AcceptUni to preserve the dispatcher's
cause and added deterministic coverage. Twenty package race repeats passed after
the fix; final root race tests/vet and three HTTP3/ls-qpack interop race repeats
passed. Interop vet passed. The full standards/features/performance goal remains
active, with public endpoints and complete draining still unfinished.

## Previous: 2026-09-20 live bidirectional GOAWAY exchange

Added TestHTTP3LiveGoaway over QUIC v1 and v2 using upstream QUIC as client and
our Session as server. It checks outgoing maximum/repeated/decreasing/zero
request limits, incoming client push limits 5/5/0 and their published snapshots.
An increasing client limit must terminate with remote H3_ID_ERROR, including
when our endpoint has already sent its final zero GOAWAY. Session termination is
also awaited. The peer owns test framing, not a full independent HTTP3 endpoint.

Three race-enabled repeats passed on both versions; interop vet passed. Production
code and unrelated suites were unchanged and not rerun. Full request admission,
accepted-work draining, retries and public endpoint integration remain unfinished.
The full QUIC standards/features/performance goal stays active.

## Previous: 2026-09-20 outgoing GOAWAY and control-write shutdown

Added Session.SendGoaway with role/range checks, serialized control-stream writes
and repeated/decreasing limits. Invalid local IDs are rejected before transmission;
short/error writes terminate the session and abort the connection without retry.
Session now owns all local critical writers, interrupts the control writer on
shutdown and joins an in-flight SendGoaway before publishing completion.

Tests parse emitted frames for both roles and exercise maximum/repeated/decreasing/
zero limits, invalid IDs, short writes and Stop during a blocked control write.
Three package race repeats passed, followed by full root race tests/vet and three
HTTP3/ls-qpack interop race repeats. Interop vet passed. No live GOAWAY exchange or
full admission/draining policy is claimed yet; public endpoints and wider
standards/features/performance parity remain unfinished. The goal stays active.

## Previous: 2026-09-20 session GOAWAY publication

Added immutable PeerGoaway snapshots published before endpoint control callbacks.
Existing wire validation already enforces role/range/non-increasing IDs. The
snapshot distinguishes request and push IDs and classifies previously initiated
work at/above the rejection limit. Documentation explicitly prohibits all new
requests/promises after receipt, even below the limit; earlier work must not be
assumed unprocessed. Endpoint admission serialization, resets/retries and outgoing
graceful shutdown remain unfinished.

Tests cover both roles, maximum/repeated/decreasing/zero limits, publication order
and returned-copy isolation. Three package race repeats passed, followed by full
root race tests/vet and three HTTP3/ls-qpack interop race repeats. Interop vet
passed. Unchanged unrelated transport interop was not rerun. Public endpoints and
broader standards/features/performance parity remain incomplete; goal stays active.

## Previous: 2026-09-20 pre-SETTINGS QPACK cancellation handling

Fixed Session rejecting all QPACK feedback before encoder publication. Stream
Cancellation is now accepted when no encoded references can exist yet; Section
Acknowledgment and Insert Count Increment still cause QPACK_DECODER_STREAM_ERROR.
Tests hold the decoder stream open, confirm cancellation processing before sending
SETTINGS and verify later encoder publication/use. Canceling a SETTINGS waiter
also leaves the session usable. Separate cases check early ACK/increment errors.

Three package race repeats passed, followed by full root race tests and vet.
Three HTTP3/ls-qpack interop repeats and interop vet passed. Unrelated non-HTTP
transport interop was unchanged and not rerun. Public endpoint request ownership,
connection policy and net/http integration remain unfinished; the full standards,
features and performance objective remains active.

## Previous: 2026-09-20 HTTP3 critical-stream session owner

Added Session owning peer SETTINGS, encoder publication, shared QPACK routing and
critical-stream dispatcher lifecycle after OpenLocal. Configuration supplies exact
local settings, bounded budgets and endpoint control/push callbacks. Stop/parent
cancellation interrupts critical writes, joins workers and fails decoder waiters.
The transport wrapper cancels before dispatcher abort/join, preventing blocked
encoder writes from stranding shutdown.

Live HTTP3/ls-qpack fixtures now use this owner instead of manual lifecycle code.
Three interop repeats passed under race. Focused tests force encoder initialization
to block and verify parent cancellation and duplicate-critical-stream failure
interrupt it and terminate the session; three package race repeats passed. Full
root race tests and root/interop vet passed. Unchanged non-HTTP transport interop
was not rerun. The owner is internal and starts after critical streams are opened;
request ownership, transport shutdown, endpoint control policy and net/http APIs
remain unfinished. The full QUIC standards/features/performance goal stays active.

## Previous: 2026-09-20 shared request and response paths in live interop

Switched the live independent client's request writes to MessageWriter and the
independent server's request reads to MessageReader. Together with the previous
response integration, both roles now exercise shared production message paths
in both directions against upstream HTTP3. Removed redundant fixture framing,
manual semantics calls and the unused direct QPACK decode helper. Existing POST
body/trailer, informational response, unknown-frame and v1/v2 assertions remain.

Three repeats of HTTP3 and ls-qpack interop passed under race. Root and interop
vet passed. Production code was unchanged; unrelated transport tests were not
rerun. Connection dispatch/settings policy and net/http adaptation are still
fixture-owned. Public endpoint ownership remains the next substantial gap, and
the complete standards/features/performance objective remains active.

## Previous: 2026-09-20 integrated HTTP3 message writer

Added MessageWriter for serialized HEADERS/DATA/trailer/FIN transmission through
the shared FieldEncoder and message semantics. Body length and placement are
checked before transmission; trailers require a field-definition permission
policy. Write/FIN errors are sticky and never retried; successful Close sends FIN
once. DATA progress counts payload bytes only. The endpoint retains responsibility
for resetting failures, QPACK reference lifecycle and interruptible I/O.

The live server fixture now uses this writer against upstream's HTTP3 client,
retaining informational/final headers, DATA, trailers and unknown-frame coverage.
Tests cover reader/writer round trips, invalid-message rejection without writes,
short frame-header and payload writes, retained progress, failed FIN and no retry.
Full root race tests/vet passed. Three repeats of HTTP3/ls-qpack interop passed
under race; interop vet passed. Unchanged unrelated transport interop was not
rerun. Public HTTP3 endpoint ownership and broader standards/features/performance
parity remain unfinished; the full goal stays active.

## Previous: 2026-09-20 message reader blocked-QPACK lifecycle

Added deterministic asynchronous coverage for MessageReader after its section is
observably pending in FieldDecoder. Insertion resumes valid headers followed by
DATA/FIN. Context cancellation emits exactly one cancellation, clears pending
ownership and remains sticky on Next/Read. A connection decoder failure also
releases the waiter and preserves its exact cause. Late inserts after cancellation
produce only insertion feedback, not a section acknowledgment or message revival.

Three race-enabled repeats of internal/http3conn passed; package vet passed. No
production change was needed. Unchanged transport/interop suites were not rerun.
Arbitrary blocking I/O still requires owner deadlines or closure. Public endpoint
ownership remains the next substantial HTTP3 gap; the full QUIC standards,
features and measured-performance goal remains active and incomplete.

## Previous: 2026-09-20 integrated HTTP3 message reader

Added reusable internal MessageReader connecting wire framing, bounded HEADERS
allocation, FieldStream QPACK decoding and Message semantics. HEADERS return
classified decoded events; DATA remains streamed and byte-count validated before
delivery. Clean FIN checks completion; malformed/truncated frames and message
errors are sticky. Unconsumed payload is retryable local backpressure. Unknown
frames and PUSH_PROMISE stay visible for endpoint-specific validation.

The live client fixture now uses this reader against upstream's HTTP3 server for
informational/final headers, body and trailers. Unit tests cover partial DATA
reads, unknown payloads, trailers, repeated EOF, framing/placement errors, header
limits, body underflow/overflow and terminal error retention. Full root race tests
and vet passed. Three repeats of HTTP3/ls-qpack interop passed under race; interop
vet passed. Unchanged unrelated transport interop was not rerun.

Context controls QPACK blocking; transport I/O interruption, cancellation/reset,
push policy and connection shutdown remain endpoint-owner responsibilities. Public
HTTP3 endpoints and the wider standards/features/performance goal are unfinished.
The full goal remains active.

## Previous: 2026-09-20 live independent QPACK cancellation

Extended the v1/v2 ls-qpack fixture with three cancellations among eight blocked
response sections. FieldStream cancellation is called twice, later Decode is
rejected, and the upstream transport must observe remote H3_REQUEST_CANCELLED.
Actual QPACK feedback must identify canceled IDs correctly and acknowledge only
surviving sections. Subsequent encoder updates still advance insertion feedback;
ls-qpack accepts every batch and the five surviving responses decode on the same
connection. Both modes acknowledge ten insertions per connection.

All four live cases passed under race. Three repeats of the HTTP3 and ls-qpack
interop suites passed under race, and interop vet passed. Production code and
unrelated transport tests were unchanged and not rerun. Coverage uses explicit
cancellation after blocked Submit, not a concurrent Decode/context race. Broader
concurrent request handling and public HTTP3 endpoint ownership remain gaps;
the full feature, standards and performance goal remains active and incomplete.

## Previous: 2026-09-20 live independent dynamic QPACK over QUIC

Added TestLSQPACKLive for v1 and v2. An unmodified ls-qpack encoder supplies response
sections over an upstream QUIC server; our client sends valid GET sections and
receives eight dynamic responses. Each response blocks before encoder updates,
then resumes with exact fields. Message semantics and FIN are checked. Actual
feedback travels over the decoder stream and returns unchanged to ls-qpack before
the next response is encoded. Ten insertions are acknowledged per connection.

The test dispatch helper now accepts the exact locally advertised SETTINGS so the
256-byte maximum matches the peer's encoding modulus. Existing fixtures retain
their defaults. The peer disables duplication because of its documented accounting
issue. HTTP3 framing is test-owned: this is independent live QPACK/QUIC evidence,
not a complete independent HTTP3 endpoint implementation.

Both new version cases passed under race. Three repeats of all HTTP3 and ls-qpack
interop tests passed under race; interop vet passed. Production code and unrelated
transport tests were unchanged and not rerun. Live cancellation/concurrency and
public HTTP3 endpoint ownership remain next gaps. The full QUIC feature and
performance goal remains active and substantially incomplete.

## Previous: 2026-09-20 isolated ls-qpack feedback accounting mismatch

Added a decoder-independent two-section probe and read-only peer counter query.
The unmodified pinned encoder accepts ACK raising Known Received Count to 2, then
accepts increment 1 after a third insertion but incorrectly retains count 2.
The probe exits 1 and documents the expected value 3 from RFC 9204 section 2.1.4.

A separate temporary peer build with the retained accounting-experiment.patch
changes increment bounds/addition to use Known Received Count. The probe then
passes; all four duplicate-enabled interactive cases pass the strict 80-section
gate under race, including 27 cancellations and fragmented feedback. This isolates
the fallback cause in this fixture. The canonical build script never patches the
peer, and its tracked source diff remains empty. Production code is unchanged.

The canonical ls-qpack test suite passed with race and cache disabled after the
driver rebuild. Existing independent encoder ACK/increment/bounds tests passed
under race. No unrelated transport tests were rerun. Next substantial work remains
independent live dynamic HTTP3 interoperability and public endpoint ownership;
the full QUIC feature/performance goal remains active and incomplete.

## Previous: 2026-09-20 interactive independent QPACK feedback

Added a test-only C framing driver linked to the unmodified pinned ls-qpack
library. Our FieldDecoder's actual acknowledgment, cancellation and insert-count
feedback is returned to its encoder API; errors are checked directly. Eight cases
cover duplicate-enabled/disabled encoding, cancellation and one-byte feedback.
Without duplication all 80 sections block; cancellation mode cancels 27. Every
surviving section decodes correctly and every feedback API call succeeds.

The duplication-enabled peer falls back to literals after three blocked sections.
This remains an unresolved sustained-compression gate: default tests explicitly
check observed coverage and fields, while QUIC_INTEROP_KNOWN_FAILURES=1 requires
80 blocked sections and reproduces failure. A possible mismatch in the peer's
ACK/increment accounting is documented in interop/KNOWN_FAILURES.md; root cause
has not yet been established. Neither peer library nor production decoder was
patched to bypass this behavior.

The external peer was freshly cloned and built via the checked-in script; the
final driver was rebuilt against that library with compiler warnings as errors.
All ls-qpack tests passed three race-enabled repeats, followed by a passing final
run after adding the strict gate. Interop vet passed; the strict duplicate-mode
reproduction failed as documented. Root production code and unrelated transport
interop tests were unchanged and not rerun. Next: isolate peer feedback accounting,
then independent live dynamic HTTP3 and public endpoint ownership. The full goal
remains active and substantially incomplete.

## Previous: 2026-09-20 independent QPACK delayed-update and cancellation coverage

Extended ls-qpack interoperability to feed its field sections through FieldDecoder
before each section's encoder updates. The pin's 80 sections all block and resume
correctly; a second pass cancels every third section. Assertions require exactly
80 acknowledgments normally, or 26 cancellations and 54 acknowledgments, with
correct fields and no missing/duplicate feedback. Encoder instruction order stays
intact; cross-stream delivery is deliberately reordered. Feedback is inspected
locally, not exchanged interactively with the offline peer.

Three repeats of all ls-qpack codec tests passed under race, followed by a passing
rerun after tightening blocked/cancellation counts to their exact expected values.
Interop vet passed. Root production code and unrelated interop tests were unchanged
and not rerun. Independent interactive feedback/live HTTP3 validation and public
endpoint ownership remain unfinished, along with broader QUIC parity. The full
goal stays active.

## Previous: 2026-09-20 independent dynamic QPACK codec interoperability

Added optional bidirectional codec tests using unmodified ls-qpack CLI tools at
91567706c41c0d97ab8dc576873ecd472d7869fa (2.7.0), with a checked-in external build
script and test instructions. Our decoder accepted 80 dynamic sections and 108
insertions from the peer; the peer decoded 80 of our dynamic sections with
one-byte reads in ordered and header/update-swapped modes. Our selected capacity
is 128 against the peer maximum 256, exercising eviction and RIC wraparound.

The first reverse-direction attempt identified a CLI format convention: its
encoder discards initial capacity signaling. The offline test now sets that
out-of-band capacity explicitly; production behavior was not relaxed. Feedback
is modeled by the tools rather than exchanged live. Dynamic references are
asserted so static-only success cannot satisfy these tests.

The script rebuilt the exact pin in /tmp/quic-lsqpack-repro. The full interop suite
with LSQPACK_BIN=/tmp/quic-lsqpack-repro/build/bin passed race tests and vet (19.262
seconds), retaining existing unrelated skips. Production code was unchanged.
Independent live dynamic HTTP/3, feedback/cancellation interoperability and public
endpoint APIs remain unfinished, as does broader QUIC parity. The full goal stays
active and substantially incomplete.

## Previous: 2026-09-20 QPACK cancellation failure propagation

Fixed FieldStream masking a terminal cancellation feedback-write failure as an
ordinary cancellation on later Decode calls. The regression test cancels an
observably blocked section with a short-writing feedback sink, verifies release
and error fanout to another blocked request, preserves the same error across
later calls and confirms no retry. Arbitrary blocked writers still require the
transport owner's deadline/closure to interrupt them; no asynchronous writer
interruption is claimed.

Full root race tests and vet passed. Three repeated live HTTP/3 suites under race
and interop vet passed (1.630 seconds); unchanged non-HTTP interop was not rerun.
Independent dynamic-QPACK interoperability and public HTTP/3 endpoint ownership
remain next substantial gaps. Broader QUIC parity remains unfinished and the full
goal stays active.

## Previous: 2026-09-20 per-stream QPACK cancellation lifecycle

Added FieldStream coordinating sequential section decode, context cancellation
and idempotent per-stream QPACK cancellation. It prevents post-cancellation
submission without a connection-lifetime tombstone map. QUIC reset and critical
writer interruption remain transport-owner responsibilities. Both live HTTP/3
roles now use the wrapper. Extended handcrafted dynamic QPACK tests to cover a
blocked section canceled before insertion and the peer's exact Stream Cancellation
feedback, plus rejection of later submission.

Tests verify context cancellation after observable blocking, repeated/concurrent
Cancel emitting one instruction, and sequential static sections. Five live repeats
passed (20 ordinary exchanges plus 20 blocked resume/cancel cases, 1.610 seconds).
Full root/default interop race tests and vet passed (interop 18.964 seconds),
retaining existing skips. Independent dynamic-QPACK peer testing, public HTTP/3
endpoint ownership and broader QUIC parity remain unfinished. The full goal stays
active and substantially incomplete.

## Previous: 2026-09-20 live QPACK decoder ownership and blocked sections

Integrated FieldDecoder into both live roles, sharing receive table state across
encoder-stream updates and request section decoding, writing feedback on the
actual decoder stream. Fixtures now advertise 4096-byte capacity/eight blocked
streams and wait for per-section results. Dispatch termination interrupts writer
deadlines, joins the cancellation callback and closes the owner to release waiters.
Removed independent per-request DecoderTable instances.

Added v1/v2 handcrafted dynamic QPACK over upstream QUIC: HEADERS arrives before
its insertion, remains blocked, resumes with exact x:y fields and emits the
expected Section Acknowledgment byte. This is wire/transport integration evidence,
not independent HTTP/3 dynamic compression interoperability. Five repeats passed
for 20 ordinary role/version exchanges plus ten blocked-section cases (1.681
seconds). Full default interop race tests and vet passed (18.904 seconds), retaining
existing skips. Root production code was unchanged; prior race/vet evidence applies.
Independent dynamic-peer validation, public HTTP/3 APIs and broader QUIC parity
remain unfinished. The complete goal stays active.

## Previous: 2026-09-20 receive-side QPACK ownership

Added FieldDecoder serializing receive QPACK state, buffered completion channels
and decoder-stream feedback. SettingsState construction honors local advertised
capacity/blocked limits and rejects insufficient budgets. Encoder updates resume
pending sections only after feedback succeeds. Cancel releases per-stream pending
work; Close broadcasts terminal failure and releases table state. Malformed
instructions versus resumed decompression failures retain distinct QPACK codes.
Short/failed feedback writes poison the owner without retry. Connection owners
must interrupt blocked writes before closure, call Close, serialize per-stream
sections, and prevent repeat cancellation/submission after cancellation.

Unit/race tests cover multi-section blocking/resumption, ACK order, local duplicate
submission backpressure, cancellation/closure, canceled-versus-resumed races,
error-code mapping, short writes and advertised-limit enforcement. Root/default
interop race tests and vet passed (interop 18.904 seconds), retaining existing
skips. The new decoder owner has not yet replaced live fixture decoding; that
integration and live dynamic sections are next. Public HTTP APIs and broader
QUIC parity remain incomplete; the full goal stays active.

## Previous: 2026-09-20 live HTTP/3 encoder ownership integration

Both live HTTP/3 roles now construct FieldEncoder from received SETTINGS and the
actual local encoder stream, sharing that owner for section encoding and decoder
feedback. Removed the disconnected feedback table. Atomic publication coordinates
control/decoder workers, and encoder writes use the test connection deadline.
This establishes live ownership, not live dynamic compression interoperability.

Expanded live critical failures with invalid ENABLE_CONNECT_PROTOCOL and impossible
Insert Count Increment feedback, checking local errors and the independent peer's
wire close codes on QUIC v1/v2. Five repeats passed for live exchanges (20 cases,
1.714 seconds) and critical failures (40 cases, 1.515 seconds). Full default interop
race tests and vet passed (18.916 seconds), retaining existing skips. Root production
code was unchanged this turn; its previous full race/vet result remains applicable.
Receive-side QPACK ownership and dynamic live sections are next. Public HTTP APIs
and broader QUIC parity remain unfinished; the complete goal stays active.

## Previous: 2026-09-20 settings-bound QPACK encoder ownership

Added FieldEncoder owning serialized encoder state, feedback and critical-stream
writes. SettingsState construction waits for received peer settings, applying
peer/local capacity, blocked-stream and field-section limits while preserving the
peer maximum for Required Insert Count wraparound. Insertion remains explicit;
NeverIndex and safe literal fallback remain intact. Short/failed writes poison
the owner without retry; invalid feedback maps to QPACK_DECODER_STREAM_ERROR.
The connection owner must close on those failures and supply interruptible writes.

Tests cover 80 insertion/eviction/ACK cycles with differing local/peer capacities,
zero blocked streams, field limits including zero, sensitive fields, concurrent
calls, uncertain writes, feedback failures and settings readiness. Root/default
interop race tests and vet passed (interop 18.911 seconds), retaining existing
skips. This new owner is not yet wired into live fixtures; unifying their section
encoding and feedback state is next. Receive-side dynamic QPACK ownership, public
HTTP APIs and broader QUIC parity remain unfinished. The full goal stays active.

## Previous: 2026-09-20 HTTP/3 settings state and extended CONNECT permission

Recognized ENABLE_CONNECT_PROTOCOL in wire settings with boolean value checks
and transactional encoding rejection. Added concurrent SettingsState snapshots
for understood peer/local settings, protocol defaults, one-time peer publication,
and sticky failure. Extended CONNECT permission follows server advertisement
semantics: client settings do not grant server permission. Live control callbacks
publish settings; independent server request validation reads local permission.

Tests cover local/peer/client/server combinations, atomic failure, duplicate
settings, concurrent publication, independent snapshots and invalid wire values
through the largest QUIC varint. Root/default interop race tests and vet passed
(interop 18.749 seconds), retaining existing skips. QPACK/field limit enforcement,
public request sending, remembered settings/0-RTT and WebSocket APIs still need
integration. The complete independent QUIC goal remains active and incomplete.

## Previous: 2026-09-20 HTTP/3 trailer status and sending policy

Message now rejects trailer sections on 204/304 responses as sticky stream-scoped
malformed-message errors, including empty sections. Added ValidateOutgoingTrailers
with a required field-definition permission callback: a Trailer declaration does
not imply permission. Structural/framing/routing checks cannot be overridden.
Incoming unknown trailers remain separate, not merged into initial headers.
Both independent live senders apply the policy to their custom trailer fields.

Tests cover prohibited/permitted status cases, headers-only completion, CONNECT
204 tunnel behavior, unknown fields, missing policy, duplicate preservation and
attempted policy bypass of core checks. Root/default interop race tests and vet
passed (interop 18.960 seconds), retaining existing skips. Public APIs still need
to own field definitions, sending status/stream policy, connection QPACK state and
full lifecycle. Complete URI validation and broader QUIC parity remain unfinished.
The full independent implementation goal remains active and substantially incomplete.

## Previous: 2026-09-20 HTTP/3 receive message ordering

Added Message combining field/control validation and body length accounting.
It handles informational/final/trailer sections, DATA placement, completion,
unknown frames, PUSH_PROMISE placement and successful CONNECT tunnel transitions.
Ordering errors are connection-scoped; malformed fields/lengths remain stream
errors. Protocol failures are sticky. Trailer checks prohibit pseudo-headers and
content-length/host/trailer fields but are not a complete field/status policy.

Both live roles now use Message, send 103 Early Hints and exercise unknown frame
skipping. Root/default interop race tests and vet passed (interop 19.051 seconds).
After adding an assertion that the independent client actually received Early
Hints, five repeated live runs passed (20 role/version exchanges, 1.488 seconds).
Sequence fuzzing completed 3,059,835 executions without failure. Existing skips
remain. Complete trailer policy, URI validation, dynamic QPACK connection
ownership, public HTTP/3 APIs and broader QUIC parity remain unfinished. The full
goal remains active and substantially incomplete.

## Previous: 2026-09-20 HTTP/3 content length accounting

Added canonical decimal Content-Length parsing with equal duplicate/list handling
and no integer conversion limit. Added receive-side BodyLength for exact DATA
accounting, early overrun rejection, completion underruns and sticky protocol
errors. HEAD/304 representation lengths are metadata, bodyless statuses reject
content, and successful CONNECT responses ignore Content-Length as required.
CONNECT DATA is tunnel data. The caller must supply validated fields/control data
and enforce frame order; this helper does not implement an outbound policy.

Both live roles now account body bytes and explicit lengths are sent by the
fixtures. Root/default interop race tests and vet passed (interop 19.042 seconds),
retaining existing skips. Parsing fuzzing completed 3,837,165 executions without
failure. Tests exercise duplicate/conflicting values, huge decimals, exact/short/
long bodies, overflow, terminal errors, bodyless responses and CONNECT exceptions.
Full stream message ordering, trailer eligibility, complete URI validation,
extension settings ownership and public HTTP/3 APIs remain unfinished. The full
independent QUIC goal remains active and substantially incomplete.

## Previous: 2026-09-20 HTTP/3 control data validation

Added ValidateControlData above structural field validation. It checks required
pseudo-headers, method tokens, scheme syntax, basic path rules, HTTP authority
presence/userinfo, Host consistency, ordinary CONNECT host/port and omitted
scheme/path, and explicitly enabled extended CONNECT. Responses require a
three-digit status in 100-599 excluding 101. It returns control metadata only
on success; malformed messages remain stream-scoped errors. Both live HTTP/3
roles now apply it to decoded fields.

Root and default interop race tests and vet passed (interop 18.813 seconds),
retaining existing skips. The control-data fuzz target completed 3,767,066
executions without failure. Full URI grammar, non-HTTP scheme-specific authority
policy, actual extended CONNECT settings ownership/WebSocket handshakes,
content-length/body consistency, trailer eligibility and message lifecycle remain
unfinished. Extended CONNECT coverage here is validation, not a usable tunnel API.
The full independent QUIC goal remains active and substantially incomplete.

## Previous: 2026-09-20 HTTP/3 field structure validation

Added HTTP-layer validation of lowercase token names, value octets, pseudo-header
context/order/duplicates and prohibited connection-specific fields. TE is limited
to request trailers capability; :protocol requires explicit extension permission.
Ordered fields and regular duplicates are preserved. Malformed fields produce a
stream-scoped MessageError instead of a critical-stream connection error.

Tests cover valid duplicates, all value octets, malformed names/values, pseudo
ordering/contexts and extension gating. Fuzzing completed 1,297,592 executions
without failure. Both live roles validate incoming headers/trailers; ten repeats
across roles and versions passed (40 exchanges). Full root/default interop race
tests and vet passed (interop 18.912 seconds), retaining existing skips.
Required pseudo-headers, method/status/URI semantics, content length, trailer
eligibility and complete message lifecycle still need implementation. The full
goal remains active.

## 2026-09-20 live critical-stream dispatch and handlers

Added reusable control/QPACK stream readers with context-aware semantic callbacks
and protocol/resource error mapping. Transport failures remain distinguishable.
Both live HTTP/3 roles now use ongoing bounded dispatch through teardown, with
zero-capacity compression callbacks supplied by the fixture. Unit tests cover
framing, callback delivery, malformed instructions, resource limits and source
failure preservation.

Ten repetitions of both live roles/versions passed (40 exchanges), followed by
the full interop race suite (18.831 seconds). New live critical-failure tests then
passed five repetitions across v1/v2 and control-FIN/duplicate-control cases
(20 failures), checking both local and independently observed wire close codes.
Full root race tests and both module vets pass. Shared dynamic-QPACK ownership,
full HTTP semantics and public endpoint APIs remain unfinished. Next: connection
state coordination and message validation. The full goal stays active.

## 2026-09-20 bounded HTTP/3 incoming dispatch

Added RunIncoming with bounded concurrent prefix readers/handlers, first-failure
connection abort and complete worker joining. Worker-limit exhaustion maps to
H3_EXCESSIVE_LOAD; critical handler closure maps to H3_CLOSED_CRITICAL_STREAM.
Parent cancellation stops acceptance and handlers without selecting a protocol
error. Cancellation deadlines interrupt blocked handler reads, and callbacks are
joined before cleanup. Handlers must cooperate with context outside stream I/O.

Repeated race tests cover cancellation/joining, duplicate critical claims,
critical handler return, worker exhaustion and a handler blocked inside Read.
Full root race tests and vet pass; the final targeted suite passed ten repetitions.
The loop is not yet connected to live control/QPACK handlers or the HTTP/3 test
fixtures. Next: wire those handlers and validate lifecycle failures over live
connections. Public HTTP/3 APIs and full semantics remain unfinished; the full
implementation goal stays active.

## 2026-09-20 reusable incoming HTTP/3 classification

Added cancellable stream-type reading, concurrent critical identity registration,
unknown-stream STOP_SENDING and peer closure classification. Incomplete prefixes
ending in peer FIN/reset are tolerated; duplicate critical streams and client
push streams fail. Confirmed critical FIN/reset maps to H3_CLOSED_CRITICAL_STREAM,
while local deadlines/cancellations preserve their causes. Prefix reads own and
clear temporary deadlines after joining cancellation callbacks.

Race tests cover classification boundaries, unknown types, truncated/reset
prefixes, cancellation, closure causes and 32 simultaneous claims for one critical
type (exactly one succeeds). Both live roles use the component; ten repeated runs
across both roles and versions passed (40 exchanges). Full root/default interop
race tests and vet pass (interop 18.866 seconds), retaining existing skips.
Bounded acceptance/dispatch, connection error application, message semantics and
public APIs remain unfinished. The full goal stays active.

## 2026-09-20 reusable HTTP/3 critical-stream initialization

Added internal/http3conn.OpenLocal and the public-QUIC transport adapter. The
component opens all three local critical streams, writes their type prefixes and
control SETTINGS, and returns sole-writer handles without FIN. Local validation
and already-canceled contexts fail before mutation. Partial setup failures abort
the connection. Cancellation interrupts blocked writes with a temporary deadline;
callbacks are joined and setup deadlines cleared before ownership transfer.

Race tests cover successful prefixes/settings, every partial-open failure, short
writes, write errors, local validation and blocked-write cancellation. Both live
interop roles now use this shared component. Ten repetitions across both roles
and versions passed (40 exchanges). Full root/default interop race tests and vet
pass (interop 18.912 seconds), retaining existing skips. The connection owner
still must enforce exactly-once startup and implement incoming critical-stream
dispatch, HTTP semantics and public APIs. The full implementation goal stays active.

## 2026-09-20 live HTTP/3 reverse-role interoperability

Added upstream HTTP/3 Transport against our server-side QUIC/framing/QPACK
components. Both v1 and v2 verify an 11 KiB POST, custom headers and request
trailers; upstream validates response headers, exact echoed bytes and response
trailers through its net/http interface. Initial SETTINGS are decoded, critical
streams remain open, and the client goroutine is canceled/joined on every exit.

Ten race-enabled repetitions of both live roles and versions passed (40 exchanges).
The full default interop race suite and vet passed (18.798 seconds), retaining
existing skips. Both gates are test-only assemblies with zero QPACK capacity;
production HTTP/3 connection ownership, full message validation/lifecycle,
critical-stream dispatch and independent dynamic compression remain unfinished.
Next: build production connection/stream ownership around these verified codec
and transport paths. The full implementation goal remains active.

## 2026-09-20 first live HTTP/3 independent-server gate

Added a live test assembling our QUIC transport, HTTP/3 framing/SETTINGS,
critical-stream registry and QPACK codecs against pinned upstream HTTP/3 Server.
Both v1 and v2 deliver a POST with an exact 11 KiB body and request trailers;
our client validates response headers, echoed bytes and response trailers.
TLS uses trusted generated certificates. Critical streams stay open until
teardown, and initial server SETTINGS are decoded with our control reader.

Ten race-enabled repetitions passed for both versions (20 exchanges). The full
default interop race suite and vet pass (18.855 seconds), retaining existing
skips. Importing upstream HTTP/3 adds x/text v0.42.0 only to the isolated test
module; production remains independent. This is a test-only protocol assembly,
not a public HTTP/3 API. QPACK capacity is zero; reverse-role/dynamic compression,
full HTTP validation and ongoing critical-stream dispatch remain outstanding.
Next: reverse-role live evidence and a production HTTP/3 connection owner.
The full implementation goal remains active.

## 2026-09-20 QPACK insertion policy

Added opt-in eager field admission with compact instruction selection. Never-index
fields, exact matches, oversized entries, protected/unreceived victims and updates
exceeding the encoded budget are skipped without mutation. Literal/static/dynamic
name encodings compete by size, with Huffman only when smaller. Returned updates
are committed independently from field sections and must still be sent if a later
section fails. There is no reuse predictor or performance/compression claim.

Tests cover all insertion forms, sensitive/existing-field skips, exact wire
budgets, eviction protection and the full local insertion/selection/blocked decode/
acknowledgment cycle. Full root race tests and vet pass; ten-second fuzzing
completed 1,022,203 executions without failure. Independent dynamic QPACK coverage
and live HTTP/3 stream ownership/error integration remain unfinished. The full
implementation goal remains active; the next phase is HTTP/3 integration.

## 2026-09-20 QPACK representation selection

Added EncodeFields to select static/dynamic references or literals from ordered
fields, preserving duplicates and never-index flags. It compares per-field wire
sizes and uses Huffman only when smaller. Dynamic references obey blocking and
tracking budgets; literals remain available when either budget is exhausted.
Selection does not insert entries automatically or claim whole-section optimality.
The dynamic lookup is currently a bounded linear scan.

Tests cover static/dynamic/sensitive forms, literal fallbacks, reference budgets
and decoding equivalence. A ten-second fuzz run completed 1,138,238 executions;
follow-up race tests verify the static name-reference/Huffman selection refinement.
The independent decoder accepts a 298-field selected section. Full root/default
interop race tests and vet pass (interop 18.843 seconds), retaining existing skips.
Automatic insertion policy, independent dynamic QPACK and live HTTP/3 integration
remain unfinished. The full implementation and performance goals stay active.

## 2026-09-20 QPACK section encoding and tracking

Added complete field-section encoding from caller-selected representations,
coordinated with encoder reference tracking. References and exact decoded/wire
sizes are validated before output allocation; Required Insert Count is derived
from references. Tracking commits only after successful encoding. Invalid fields,
resource limits and blocked-stream limits produce no output or tracking changes.
Successful output must be committed in stream order by the endpoint.

Tests reproduce RFC 9204 Appendix B.2 bytes, run the local blocked-section/ACK
cycle and verify atomic size/blocking failures. The independent static/literal
matrix now exercises this whole-section encoder (503 cases). Full root and
interop race tests and vet pass (interop 18.739 seconds); section fuzzing completed
1,062,799 executions without failure. Independent dynamic QPACK remains untested.
Next: representation selection/insertion policy and HTTP/3 integration. The full
implementation and performance goals remain active and unproven.

## 2026-09-20 QPACK atomic encoder table updates

Added an encoder table owner coordinating contents, reference protection and
instruction bytes. It validates the entire eviction plan and encodes the wire
instruction before mutating state. Protected/unreceived entries prevent capacity
reductions or insertions; partial eviction cannot occur when a later victim is
protected. Name-reference and duplicate sources resolve before self-eviction.
Successful changes update table and lifetime reference counters together.

Tests cover blocked multi-entry eviction, capacity clearing/restoring, receipt
requirements, self-duplication/name-reference eviction, invalid updates and
emitted-wire consistency against our decoder. Full root race tests and vet pass;
a ten-second fuzz campaign completed 281,057 executions without failure. The
wire comparison uses our own decoder and is not independent interoperability.
Field selection, whole-section encoding/tracking, reliable stream scheduling
and HTTP/3 integration remain unfinished. Next: section encoding coordinated
with reference tracking. The full implementation goal stays active.

## 2026-09-20 QPACK encoder reference tracking

Added bounded encoder-side reference bookkeeping: FIFO outstanding sections,
per-entry protection counts, Known Received Count, potentially blocked stream
limits, and oldest-first eviction guards. ACKs release one dynamic section;
cancellation releases all sections for a stream without acknowledging insertions.
Insert Count Increments validate against sent insertions and do not release field
references. Static-only sections consume no tracking slot. Invalid operations
preserve state; input references are copied and released storage is cleared.

Tests cover FIFO ACKs, blocked-stream counting, repeated references, duplicate
ACK rejection, bounds, receipt versus eviction safety, and our decoder's actual
feedback sequence. Full root race tests and vet pass. A ten-second state fuzz
campaign completed 126,781 executions without failure. The tracker is not yet
connected to an encoder table or field-selection policy; atomic coordination
with table and wire changes is the next step. Live HTTP/3 and independent dynamic
QPACK interoperability remain unfinished. The full goal stays active.

## 2026-09-20 QPACK feedback stream I/O

Added fixed-storage decoder-instruction reading and ordered feedback writing.
Readers distinguish boundary EOF, truncation and other source failures without
reading ahead. Writers validate whole batches before transmission and use fixed
scratch storage. Short writes and source errors become sticky, preventing retry
of potentially delivered acknowledgments. Local invalid batches send no bytes
and do not poison the writer. Callers still own stream ordering, type prefixes,
deadlines and critical-stream error handling.

Tests cover fragmentation, maximum integers, malformed instructions, resets,
short/invalid writer counts, partial/full writes with errors and local validation.
A decoder-to-writer-to-reader sequence verifies ACK/increment/cancellation order.
Full root race tests and vet pass. A ten-second feedback reader fuzz campaign
completed 604,464 executions without failure. This is internal stream I/O support,
not live HTTP/3 interoperability. Next: encoder-side reference/acknowledgment
tracking and HTTP/3 connection integration. The full goal remains active.

## 2026-09-20 QPACK encoder-stream instruction reader

Added bounded instruction framing from io.Reader, with no read-ahead into the
next instruction. Advertised string lengths are checked before allocation, and
buffer capacity is explicitly capped. Fragmented bodies are read once rather
than repeatedly parsed. The borrowed instruction bytes feed the existing decoder
for semantic validation and updates. Boundary EOF, truncated EOF and source
errors remain distinct; failures are sticky and release buffered storage.

Tests cover one-byte fragmentation, every truncated prefix, exact instruction
boundaries, oversized advertised bodies, integer length limits, resets and a
reader-to-decoder sequence that unblocks a section and emits acknowledgment and
speculative insertion feedback. Full root race tests and vet pass. The final
ten-second framing fuzz run completed 432,557 executions without failure.
Next: HTTP/3 critical-stream/error integration and feedback delivery, alongside
encoder-side reference tracking. Full implementation and performance goals
remain active and unproven.

## 2026-09-20 QPACK decoder blocking and feedback

Added a serialized receive-side decoder owning its table and blocked sections,
with limits on blocked streams, total retained encoded bytes, individual sections,
field count and instruction literals. Copied blocked bodies preserve receipt-time
prefixes. Each insertion resumes ready sections before another update can evict
entries. Dynamic sections emit individual acknowledgments; speculative insertions
emit nonzero increments after accounting for implicit acknowledgments. Cancellation
releases buffered work and can cover reset-before-HEADERS. The caller supplies
stream ordering, at-most-once cancellation and reliable ordered feedback delivery.

Tests cover multiple blocked streams, owned buffers, local backpressure, repeated
ACKs, speculative insertions, cancellation, resource caps, fragmented instructions
and sticky failures. State fuzzing completed 505,175 executions without failure;
full root race tests and vet pass. The first full interop run timed out during
upstream dialing in TestPublicStreamSmoke/v2 (public_stream_test.go:186). Five
isolated race-enabled repetitions passed. No production transport imports QPACK;
the timeout is recorded as an unresolved intermittent transport observation.
The subsequent full interop race suite and vet passed (18.799 seconds), retaining
existing skips.

Live encoder-stream buffering, HTTP/3 error mapping, encoder policy and socket
integration remain unfinished. The full implementation goal remains active.

## 2026-09-20 QPACK whole-section decoding

Added bounded whole-section decoding with ordered fields, duplicate occurrences
and never-index preservation. Indexed and literal fields both count toward total
name/value bytes plus 32 bytes per occurrence, with a separate field-count cap.
Required Insert Count must match the highest reference; rejection of excessive
counts uses the strict behavior permitted by RFC 9204 section 2.2.3. Errors expose
no partial fields and leave table state unchanged. Blocked sections return their
receipt-time reconstructed prefix for resumption without modulo reinterpretation.

RFC dynamic-section fixtures cover waiting/resumption, invalid counts, eviction
and saved-prefix behavior across count wrapping. The independent upstream encoder
produces a passing 200-field section covering every static entry, custom values,
literal names and duplicates. Root fuzzing completed 194,778 executions and
independent section fuzzing 84,433 without failures. Full root/default interop
race tests and vet pass (interop 18.664 seconds), retaining existing skips.
Next: bounded blocked-stream ownership, instruction stream dispatch and decoder
acknowledgments. HTTP validation, encoder policy and live HTTP/3 remain unfinished.
The full implementation goal remains active.

## 2026-09-20 QPACK field-line representations

Added all five RFC 9204 field-line forms, with raw/Huffman literals and preserved
never-index bits. Relative and post-base resolution validates arithmetic,
Required Insert Count bounds, static indices and dynamic entry availability.
Invalid encodes preserve destination bytes; parse errors consume no input.
Resolved references report their insert-count contribution for future section
validation. Per-line literal limits do not replace whole-section size accounting.

Wire fixtures, truncation at every boundary, Huffman/never-index combinations,
eviction and reference boundaries pass. A ten-second fuzz campaign completed
1,923,830 executions without failure. The independent QPACK decoder agrees on
503 static/literal combinations; dynamic reference checks remain local because
the peer lacks that support. Full root and default interop race tests and vet
pass (interop 18.880 seconds), retaining existing skips. Next: whole-section
decoding with exact Required Insert Count and size checks, then blocked-stream
state and HTTP/3 integration. The full goal remains active.

## 2026-09-20 QPACK encoder instructions and table updates

Added independent codecs for all four RFC 9204 encoder instruction forms and
atomic application to the decoder dynamic table. Static and encoder-relative
name references are resolved before insertion/eviction. Combined literal name
and value bytes have an explicit decode limit. Incomplete or invalid updates
consume no bytes and leave table state unchanged; invalid encoding arguments
leave destination bytes unchanged. Live encoder-stream buffering, error mapping
and blocked-section notification remain unimplemented.

Tests cover the Appendix B capacity/insertion/duplicate sequence, every truncation
boundary, Huffman combinations, capacity and index failures, combined limits and
self-eviction. A ten-second instruction-sequence fuzz campaign completed 198,626
executions without failure. Full root race tests and vet pass. Next: complete
field representations, reference validation and field-section decoding, followed
by bounded stream integration. The full implementation goal remains active.

## 2026-09-20 QPACK Huffman and string codecs

Added independent Huffman encoding/decoding with a generated RFC 7541 Appendix B
codebook, cross-checking its binary/hex columns. Decoding enforces explicit output
limits and rejects EOS symbols, excessive padding and invalid padding bits.
Raw/Huffman QPACK strings support variable prefixes, consume no input on error,
and preserve caller bytes on invalid encoding arguments. All octets are opaque,
including invalid UTF-8. Added RFC-data license notices for both generated tables.

RFC examples, all 256 symbols, truncation, invalid padding and output limits pass.
The independent HPACK comparison agrees on all 65,792 one/two-byte wire inputs
and byte-exact encodings of all symbols. Root fuzzing completed 381,605 executions;
independent differential fuzzing completed 163,888, both without failures. Full
root and default interop race tests and vet pass (interop 18.936 seconds), with
existing skips retained. The decoder currently uses a bitwise trie; no throughput
advantage is claimed. Next: encoder instructions and complete field representations,
then blocked-stream integration and live HTTP/3. The full goal remains active.

## 2026-09-20 QPACK static table

Added all 99 RFC 9204 Appendix A entries with checked indexed lookup and
exact-value/name-only reverse lookup. A checked-in XML generator reproduces the
formatted table from the published RFC; regeneration matched byte-for-byte.
Table tests cover all indices, bounds, copied fields and concurrent readers.
The isolated interop module now pins QPACK v0.6.0 alongside unchanged quic-go
v0.62.0 and decodes every static index through that independent decoder, checking
exact names/values and rejection of index 99. No upstream QPACK dependency was
added to the implementation module.

Full root and default interop race tests and vet pass; the interop suite took
18.965 seconds, with existing optional-peer and known upstream skips unchanged.
Full field representations, Huffman strings, encoder policy, blocked-stream
state and live HTTP/3 endpoints remain unfinished. Next: independent bounded
string/Huffman codecs, then encoder instructions and complete field sections.
The full implementation goal remains active; no performance-parity claim.

## 2026-09-20 QPACK decoder dynamic table

Implemented decoder-side capacity management, entry insertion/duplication,
oldest-entry eviction and absolute/encoder-relative lookup. Size accounting
includes the RFC's 32-byte entry overhead. Failed oversized insertions and
capacity changes do not evict existing entries. Clearing preserves lifetime
insert counts; maximum-count overflow is rejected before mutation. Owned string
copies prevent retaining oversized backing buffers, and cleared/compacted slots
bound history storage by retained entries rather than lifetime insert counts.

Race tests cover eviction, duplicate-self eviction, capacity clearing/restoring,
invalid updates and 10,000 sequential inserts with bounded storage. A model-based
ten-second fuzz run completed 218,213 executions without failure. Full root race
tests and vet pass. Encoder reference protection, instruction parsing, static
table, strings/Huffman, field decoding and HTTP/3 endpoints remain unfinished.
The full implementation goal remains active.

## 2026-09-20 QPACK field-section prefixes

Added Required Insert Count modulo encoding/reconstruction and signed Delta Base
encoding/decoding. Reconstruction uses negotiated maximum table capacity rather
than current capacity. Overflow-safe intermediate arithmetic permits valid
counts near the 62-bit boundary; invalid modulo values, zero aliases, negative
bases and oversized reconstructed results fail without consuming input.
Future insert counts are retained for the unfinished blocked-decoder machinery.

RFC examples, an independent exhaustive small-window reconstruction oracle,
truncated prefixes, maximum counts, invalid bases and transactional encodes pass
race-enabled tests. The ten-second prefix fuzz run completed 2,358,100 executions
without failure. Full root race tests and vet pass; additional targeted boundary
tests also pass. Strings/Huffman, reference tables, field representations,
blocked decoding and live HTTP/3 endpoints remain open. Full goal active.

## 2026-09-20 QPACK integers and decoder instructions

Started independent internal/qpack primitives: prefixed integers with all
prefix widths 1–8, 62-bit values, pre-arithmetic overflow checks, transactional
errors, and bounded nonminimal encodings (16-byte limit). Added wire codecs for
Section Acknowledgment, Stream Cancellation and Insert Count Increment, including
zero-increment rejection. Table-state validity and QPACK connection-error mapping
remain the responsibility of the unfinished encoder/decoder integration.

RFC 7541 Appendix C.1 vectors, maximum values, prefix boundaries, truncation,
opcode preservation, overflow, redundant encodings and invalid instructions
pass race-enabled tests. A ten-second fuzz run completed 2,357,964 executions
without failure. Full root race tests and vet pass. Field sections, strings,
Huffman coding, static/dynamic tables, blocking and HTTP/3 endpoints remain open.
The full implementation goal remains active.

## 2026-09-20 HTTP/3 critical-stream ownership

Added unidirectional stream-type parsing and a fixed-size incoming critical
stream registry for control, QPACK encoder and QPACK decoder streams. The
registry verifies stream direction/initiator, rejects duplicate critical types
and client push streams, and retains critical identities after closure. Unknown
types do not allocate entries and are left for endpoint discard/abort handling.
EOF/partial EOF/reset before a complete type is preserved without manufacturing
an HTTP/3 connection error, as required by RFC 9114 section 6.2.

Tests cover fragmented/nonminimal prefixes, payload boundaries, pretype closure,
both endpoint roles, rejected registration rollback, unknown stream floods,
critical closure/replacement rejection, and 32 concurrent claims producing
exactly one owner. Package and full root race tests plus vet pass. The registry
is ready for endpoint integration; live stream dispatch, QPACK instructions,
request/response handling and the full protocol/API/performance goal remain
unfinished. Goal active.

## 2026-09-20 HTTP/3 control-stream validation

Added a streaming control reader with explicit SETTINGS byte/entry limits,
first-and-only SETTINGS enforcement, unknown-frame skipping, critical-stream
EOF detection, GOAWAY role/monotonicity checks, and MAX_PUSH_ID role/monotonicity
checks. Added the RFC 9114 frame-placement table for control/request/push streams.
Malformed input leaves the reader terminal, and SETTINGS state is committed
only after complete validation. Non-EOF transport errors remain available for
the future endpoint's reset/cancellation classification.

Race-enabled tests cover valid and invalid control sequences, placement in both
directions, limits, fragmented input, truncated frames and abrupt source errors.
A ten-second fuzz campaign completed 796,556 executions without failure. Full
root race tests and vet pass. No live HTTP/3 endpoint exists yet: critical-stream
ownership, request/response sequencing, QPACK, push lifecycle and HTTP adapters
remain open. The full implementation goal remains active.

## 2026-09-20 HTTP/3 framing foundation

Started RFC 9114 implementation in internal/http3wire. The streaming reader
parses type/length integers without allocating from peer-declared lengths,
preserves frame boundaries, supports unknown types and nonminimal integers,
and distinguishes clean truncated FIN from abrupt stream errors. Prohibited
HTTP/2-only frame types are rejected. Added exact single-ID payload parsing,
bounded SETTINGS parsing with duplicate/reserved-ID checks, and transactional
frame-header/SETTINGS encoding. Stream placement and endpoint state remain
separate unfinished work; this is not an HTTP/3 client/server implementation.

Race-enabled tests cover fragmented input, EOF/error behavior, maximum lengths,
frame boundaries, unknown and reserved identifiers, SETTINGS limits and round
trips. A ten-second fuzz run completed 7,388 executions without a failure;
this is a short initial smoke campaign. Full root race tests and vet pass.
No existing transport behavior changed. docs/HTTP3.md records coverage and
remaining endpoint/QPACK work. The full implementation goal remains active.

## 2026-09-20 reverse-role probe and bulk loss

Expanded the external Picoquic-client/independent-server matrix to five cases
per version: clean, first request loss, first response loss, both probe losses,
and a three-large-datagram response burst loss. The UDP relay assigns distinct
NAT bindings to the two client sockets. Bulk loss is armed on path one's exact
server-facing binding after validation and path-zero abandonment, before the
remaining 1 MiB response is written. The relay counts datagrams of at least
512 bytes without decrypting or classifying frames. Exact drop counters, exactly
two bindings, and complete downloaded bytes are required.

All ten cases pass across three race-enabled repetitions. The full interop race
suite with both external executables enabled and vet also pass. The initial 1000-byte
bulk filter caught no packets and failed the counter assertion; the actual data
packets fit below that threshold. Lowering the filter to 512 bytes exercised the
intended bounded loss without changing production code or the external peer.
Reverse-role established-path migration, arbitrary loss/reordering, and the
broader protocol/API/performance objective remain open. Goal active.

## 2026-09-20 reverse-role interoperability with Picoquic library

Added a small C application driver linked to the pinned, unmodified Picoquic
library. It waits explicitly for path one's peer CID and local CID list before
requesting the additional path, avoiding the original demo's premature attempt.
The two internal CID queries are read-only; the library still performs all
transport processing. Certificate verification uses the test root and localhost
SNI. The driver reports failure unless the requested path was opened and the
response reached stream FIN successfully.

TestPicoquicClientMultipath now uses PICOQUIC_CLIENT by default. Our server
sends a 1 KiB prefix, validates the client-initiated additional path, abandons
path zero, and sends the rest of a 1 MiB response. The test checks the distinct
client address for path one and exact downloaded bytes. Both v1 and v2 pass,
including ten race-enabled repetitions. The original demo diagnostic remains
selectable with PICOQUIC_REVERSE_DIAGNOSTIC=1 and is still failing; no peer patch
or production workaround was introduced.

The build helper now builds both demo and driver through a CMake wrapper.
A complete fresh clone/configure/build succeeded with both pinned source
revisions, and the resulting driver passed both reverse-role race cases.
The full interop race suite with both peer executables enabled and vet pass.
Reverse-role imposed loss and established-path migration remain unverified,
along with the broader protocol/API/performance requirements. Goal active.

## 2026-09-20 reverse-role Picoquic diagnostic

Added TestPicoquicClientMultipath, explicitly enabled by both PICOQUIC_DEMO
and PICOQUIC_REVERSE_DIAGNOSTIC=1. It verifies TLS with the pinned external
client, requests an additional socket, and attempts server abandonment of path
zero before sending most of a 1 MiB response. Both versions currently fail:
the external demo reports path-probe error -1 and exits before validation.
This is a retained failing diagnostic, not a reverse-role compatibility pass.

Peer qlog and source point to a CID-readiness mismatch: the availability check
uses next ID zero; path creation skips the existing zero and selects one, for
which no peer CID has arrived yet. The demo treats that failure as terminal.
Our runtime already advertises additional-path CIDs during activation. Sending
a response prefix does not avoid the failure. The peer is unmodified. Detailed
reproduction and interpretation are in interop/picoquic/README.md; an independent
client driver or verified peer fix is the next step. No production behavior was
changed to work around this ordering. The full goal remains active.
Default interop race tests and vet pass with this diagnostic explicitly skipped;
the opt-in diagnostic reproduces the failure in both versions under the race
detector. The peer's zero process exit status does not mask the test failure.

## 2026-09-20 Picoquic migration during an unfinished response

The external v1/v2 multipath matrix now performs a third migration after reading
and verifying the first 1 KiB of a new 1 MiB response. Application reads pause
during migration; the current 64 KiB stream receive window and consumption-based
credit updates prevent the peer from finishing the response before the move.
The remaining bytes and EOF must arrive intact on the same stream afterward.
Path zero remains abandoned throughout. The path ID remains one while its local
socket address changes.

Clean and lossy cases pass with the race detector, including two further
repetitions of all four cases. Loss cases now verify four
drops in each direction: opening the additional path and all three migrations.
The first full matrix took 22.8 seconds, including about 9.8 seconds per lossy
case. Default interop race tests and vet pass. No production implementation or
external peer patch was needed. Reverse-role multipath interop and arbitrary
loss/reordering coverage remain open, as does the full feature/performance goal.

## 2026-09-20 Picoquic additional-path migration and probe loss

Expanded the external Picoquic matrix to v1/v2 with clean and lossy relay
bindings. After opening path 1 and abandoning path 0, each case downloads an
832-byte file and a 1 MiB patterned binary, migrates path 1 twice, and downloads
the binary after each migration. Exact bytes and stream EOF are required;
migration retains path ID 1 while changing its local socket address. Responses
are bounded to expected length plus one byte to reject oversized peer output.

The relay gives each client socket a distinct server-facing NAT binding. Loss
cases drop the first client and server packet on path opening and both
migrations: exactly three drops per direction per case. This exercises retries
with an independent peer while the primary path remains abandoned. No transport
or peer changes were needed.

All four race-enabled cases pass, including three repeated runs. The default
interop race suite and vet pass. The external matrix takes roughly 18 seconds
per run (clean cases around 1.5 seconds, lossy cases around 7.6 seconds); these
are validation timings, not comparative performance measurements. The pinned
peer revision is unchanged. Reverse-role multipath interop, arbitrary loss and
reordering, and migration during an active transfer remain unverified. The
full protocol/API/performance goal remains active.

## 2026-09-20 independent Picoquic multipath interoperability

Added an optional external-process interop gate using unmodified Picoquic
revision 8c6bbf82b6b6d98a413a69e6bf2265e7f6a10161 and PicoTLS revision
bfa67875982afc4c24f21e146cef4747fa189c2f. Both v1 and v2 negotiate multipath,
validate an additional client socket as path 1, abandon path 0, and download an
exact 832-byte file. The client verifies the temporary server certificate;
the application is HTTP/0.9 hq-interop, not HTTP/3. External processes and logs
are cleaned up on success or failure. No peer patch or production dependency
was introduced.

The initial explicit race run and three subsequent repetitions passed for both
versions. Root and full interop race tests and vet pass (the final full runs
reused passing Go test cache entries). Seven upstream compatibility skips remain;
this additional external gate skips only when PICOQUIC_DEMO is unset. Build pins,
dependencies, commands and coverage limits are in interop/picoquic/README.md.
The helper script passes shell syntax checking; the verified external binary
was built with the equivalent CMake configuration before the helper was added.

This is the first independent multipath peer result. Reverse-role coverage,
large transfers, established-path migration and imposed loss against this peer
remain open, along with the broader protocol/API/performance objective. The
goal remains active.

## 2026-09-20 live migration loss interoperability

Extended the public-client/upstream-server migration matrix with a live UDP
relay that allocates a separate server-facing socket for each client source
tuple. For each of two migrations, scenarios drop the first client packet, first
server packet, or both. The original clean scenario remains. Exact drop counts,
changed peer-address observations, and bidirectional streams before/after each
migration verify that traffic crossed the intended bindings. Cleanup stops new
relay reader creation before closing sockets and joining all readers.

All eight v1/v2 clean/loss cases pass with the race detector against unmodified
upstream v0.62.0. Each one-direction-loss case takes about two seconds for two
migrations; both-direction loss takes about four, consistent with probe retries.
These timings are test behavior, not a performance comparison. Root race tests
and vet pass. The full default interop race suite and interop vet also pass;
seven existing interop skips remain unchanged.

This adds real-socket/recovery evidence beyond the deterministic engine loss
tests. Live reordering, independent multipath peers, cross-family/wildcard
migration and the remaining protocol/API/performance objective remain open.

## 2026-09-20 public additional-path migration

Added Path.Migrate(ctx, local), retaining the path ID and using the same socket
registration, CID reservation, validation, cancellation and cleanup operation
as primary migration. The runtime operation now accepts an established path ID
and rejects missing/abandoned paths before allocation. Conn.Migrate remains the
path-zero wrapper. Cleanup handles path state collected after abandonment.

Path.Close now resolves its latest bound socket on the serialized owner, rather
than using the handle's original socket address. It preserves last-viable-path
protection and repeated-close behavior after migration. Address accessors remain
immutable snapshots; the handle needs no concurrently mutated address fields.

Live public v1/v2 tests cover canceled and successful path-one migration,
unchanged primary identity, bidirectional streams after abandoning the primary,
the server's migrated address, repeated close, OS-level rebinding of the released
migrated socket, rejection of migration after abandonment, and stream continuity
on another additional path. Ten race-enabled repetitions pass. Root race tests,
vet and default interop race tests pass; seven known skips remain.

Independent multipath interop, cross-family/wildcard migration, live network
impairment tests and the remaining full protocol/API/performance work remain
open. The goal remains active.

## 2026-09-20 multipath response path-ID review

Rechecked the current IETF document, still draft-ietf-quic-multipath-21.
[Section 3.1](https://datatracker.ietf.org/doc/html/draft-ietf-quic-multipath-21#section-3.1)
requires responses to use the challenge's path ID; section 5.5 excludes validation
frames from general cross-path frame delivery. Our interpretation retains token
matching within a path ID, while accepting changed arrival addresses within that
ID under RFC 9000. No production matching change was needed. This is a documented
draft interpretation, not a claim of exhaustive standards conformance.

Added encrypted v1/v2 migration tests for both path zero and additional path one.
Proof carried under the other path ID cannot validate or authorize commit; the
same token on its own path ID, arriving on the old address tuple, validates the
new candidate. Commit must preserve the other path's address and selected CID.
These supplement the existing initial-validation path isolation and response
affinity tests. Root race tests, vet and default interop race tests pass; seven
known interop skips remain.

Independent multipath interoperability and live impaired-network migration
testing remain open, alongside the remaining full protocol/API/performance goal.

## 2026-09-20 cross-address migration response proof

Separated authenticated token matching from candidate tuple admission. Responses
received on the established tuple now reach pending local/remote migration
validation. Client candidates also accept proof on another registered local
tuple with the known server address; unknown server addresses remain discarded.
Server candidates accept proof on a different arrival tuple and commit the
original probed tuple without admitting unrelated application frames from the
other unvalidated address. Local proof still requires a live CID reservation and
the socket owner's later commit. Expiration/token/MTU checks remain shared.

This implements the distinction in [RFC 9000 sections 8.2.2–8.2.3](https://www.rfc-editor.org/rfc/rfc9000.html#section-8.2.2):
the responder must use the incoming path, but the initiator must not require the
response to arrive on the probed path. Existing nominated-tuple receive helpers
remain separate from the cross-address proof handler.

Sixteen encrypted tests cover v1/v2, base/multipath packet modes, local/remote
migration, and established/other arrival tuples. They reject forged and incorrect
tokens, check the committed tuple, and withhold application frames on the other
unvalidated tuple. Root race tests, vet and default interop race tests pass;
seven existing interop skips remain.

The new matching is within the packet's path-ID context. Responses carried under
a different multipath path ID need a separate latest-draft review and tests;
this is not a claim of complete multipath validation conformance. Live lossy
migration interop and the rest of the full objective remain open and active.

## 2026-09-20 migration reordering and expired proof replay

Expanded the encrypted impairment matrix from 20 to 44 cases. New scenarios
drop stream data, delay either endpoint's validation response or client stream
data by 1.5 simulated seconds, and deliver a client challenge or server response
4.5 seconds late after suppressing all corresponding validation packets. The
late cases must expire and retain/recover the old address. Delayed cases verify
that newer packets were admitted first, then replay the delayed packet twice.
All cases require exact stream contents and reject an extra accepted stream;
late proof must not resurrect the canceled migration.

The simulator now initializes the peer address snapshot as the real network
owner does, covering the case where no migration ever committed. No production
change was needed for these additional scenarios. All 44 cases pass ten
race-enabled repetitions (440 scenarios); root race tests, vet and the default
interop race suite pass with the same seven known skips.

This remains deterministic engine evidence. Live lossy/reordered migration
interop, cross-tuple PATH_RESPONSE conformance, and the other outstanding
protocol/API/performance work remain open. The full goal remains active.

## 2026-09-20 migration validation loss and fallback

Added deterministic encrypted packet-loss simulations for v1/v2 in base and
multipath packet modes. Each drops the first client challenge, server challenge,
client response or server response; a fifth case drops every server response to
the client's validation probe. The driver advances recovery timers, transfers
packets using their selected tuples, and requires the queued reliable stream to
finish. Single-loss cases must commit; complete response loss must expire the
attempt and recover the old peer address without committing locally.

The complete-loss case exposed a CID lifecycle deadlock during fallback. Server
validation could finish while the client's proof remained lost. Retirement and
replacement frames arriving on candidate tuples were ignored, exhausting the
spare IDs needed to validate the fallback address. Authenticated candidate input
now applies only CID lifecycle frames in addition to validation frames, using
the existing idempotent handlers. Application frames remain withheld and the
candidate packet is not recorded or ACKed. Local reservations are rechecked after
control processing in case a peer retirement revoked them.

All 20 cases pass, including ten race-enabled repetitions. Root race tests, vet
and default interop race tests pass; seven existing skips remain. These are
engine simulations, not live lossy-network interoperability measurements.
Cross-tuple response conformance, reordering and live migration loss tests remain
open along with the full protocol/API/performance objective.

## 2026-09-20 migration cancellation and concurrent-call stress

Added live v1/v2 actor tests racing cancellation at four delays against migration,
plus eight simultaneously started migration callers. Checks relate the returned
result to the committed address, require one remaining registered socket and
reader with no pending candidate, and complete a bidirectional stream afterward.
Thirty race-enabled repetitions pass: 300 scenarios comprising 240 cancellation
attempts and 480 concurrent migration calls. Timing coverage is stress evidence,
not exhaustive proof of every cancellation interleaving.

Review found that Migrate captured its predecessor address during preflight,
before opening its socket. A concurrent migration could commit in that gap,
making cleanup target an obsolete predecessor. Capture now occurs atomically
with successful candidate preparation on the owner. Root race tests, vet and
default interop race tests pass; seven existing interop skips remain. The final
expanded bidirectional stress test also passes its thirty repetitions.

Cross-tuple PATH_RESPONSE conformance and migration loss/reordering coverage
remain open, along with additional-path/cross-family/wildcard migration and the
rest of the full protocol/API/performance objective. The goal remains active.

## 2026-09-20 independent migration interoperability

Added a live public-API test against the unmodified pinned upstream quic-go
v0.62.0 server: small bidirectional streams before migration and after two
successive client local-port migrations, checking the server's observed address
in v1 and v2. Ten race-enabled repetitions pass. Root race tests, vet and the
complete default interop race suite also pass; the seven existing skips remain.

The test initially exposed CID starvation on the second migration. Debug packet
traces showed successful retirement of server sequence zero but no replacement;
the pinned upstream conn_id_generator.go explicitly skips that replacement.
Public configuration now advertises four active peer CIDs instead of inheriting
the wire default of two, providing migration spares. Internal explicit limits
and wire defaults are unchanged. No upstream patch or new test skip was used.
This is a local resource policy consistent with keeping unused IDs available
for migration ([RFC 9000 section 5.1.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-5.1.1)).

Evidence covers two migrations per connection and small streams, not unlimited
migrations, loss/reordering during migration, cross-family changes, or v2 key
updates. Cancellation/commit stress and cross-tuple response conformance remain
open, alongside the rest of the full protocol/API/performance goal.

## 2026-09-20 migration commit under queue saturation

Removed the application-queue allocation requirement from validated local
migration commit. The candidate's pending PATH_RESPONSE transfers into one fixed
per-path slot, preserving the existing bounded storage independently of queued
application bytes. Packet construction prioritizes that response on its own path,
uses normal encryption/amplification/congestion/pacing/recovery gates, and clears
the slot only after successful output. Sending it does not consume pending ACK
work. Abandonment, closing and draining clear the slot.

The v1/v2 base/multipath mutual-validation regression now commits with both a
full application queue and full old congestion window, checks recovery reset,
checks that new congestion blocking preserves the response and packet number,
decrypts the response sent despite queue saturation, and verifies unchanged
queue accounting and retained ACK work. Root race tests, vet and default interop
race tests pass; seven documented interop skips remain.

Cancellation/commit race stress, independent migration interoperability and
cross-tuple response conformance remain next. The full goal remains active.

## 2026-09-20 public primary migration operation

Added Conn.Migrate(ctx, local) and its UDP-owner operation. It opens/registers
a concrete client socket, waits for an unused CID, validates and commits path
zero, and retires the old socket only if no connection/path/candidate still
references it. Owner-serialized finalization resolves cancellation against a
committed flag: commit wins; otherwise the candidate is marked failed, canceled,
and its socket removed. A blocked retirement retains failed state for retry.

Public tests exposed a real migration race: ordinary old-tuple output with a
higher packet number canceled the peer's candidate after a new-tuple probe,
consuming its reserved CID and stalling repeated migration. Ordinary output on
the migrating path is now held until commit/cancellation; candidate probes and
responses have their own sender. Other paths remain schedulable. The operation
waits for replacement CIDs instead of intermittently rejecting rapid migrations.

Ten repeated public test runs pass across v1/v2 and ordinary/multipath modes,
including two migrations and bidirectional streams, pre-canceled requests and
server-role rejection. Live actor tests now exercise the operation, including
timeout cleanup against a peer that ignores migration. Root race tests, vet and
default interop race tests pass; seven existing skips remain.

Next: exercise/fix saturated send-queue plus congestion interactions at commit
(holding old-path output could prevent progress while a pending response cannot
be queued or sent), cancellation/commit race stress, external migration interop,
and cross-tuple response conformance. Additional-path local migration, wildcard
packet-info and cross-family migration remain unfinished. Full scope remains active.

## 2026-09-20 socket-owner migration servicing

The endpoint now services local migrations before output: validated candidates
commit only while their local socket remains registered, revoked CID reservations
cancel, and missing candidate sockets enter failed validation and cancellation.
Queue pressure preserves retryable cancellation without permitting further
candidate output. Socket failure also runs this transition before identifying
affected established paths, allowing a validated healthy replacement to survive
loss of the original socket.

Live UDP actor tests cover v1/v2 with ordinary and negotiated multipath owners:
register a new client socket, validate and commit migration, observe the server's
new remote address, remove the original socket, and deliver stream data. Failure
tests cover candidate loss, queue-blocked spent-CID cancellation, and original
socket loss after replacement validation. Root race tests, vet and default
interop race tests pass; seven documented interop skips remain.

Socket opening/ownership and cancellation still need a public migration operation;
the existing internal prepare helper is the live test entry point. Response
acceptance across tuples still needs the conformance review described below.
The full protocol/API/interoperability/performance goal remains active.

## 2026-09-20 local migration commit

The engine now commits a validated local candidate by selecting its reserved
used CID, changing the bound tuple, publishing the address snapshot and resetting
network recovery while preserving packet numbers and reliable transmission
references. Pending candidate responses transfer to the new binding; stale
old-tuple challenge/response work is retired. Send-queue pressure leaves the
commit retryable without changing ownership. Client output now services deferred
CID retirement, and public LocalAddr reads the committed primary snapshot.

Encrypted mutual-validation tests now continue through commit and application
delivery for v1/v2 in base and multipath modes. They check blocked-commit atomicity,
recovery reset, new-tuple output, CID retirement and stale-commit rejection.
Additional tests reject unvalidated, unsent-CID and revoked-CID commits.
Root race tests, vet and default interop race tests pass with the same seven
documented interop skips.

The socket owner does not yet invoke this transition in a public migration
operation. Socket registration/lifetime, automatic validation-to-commit servicing,
and public API integration remain next. Full migration conformance also requires
reviewing response acceptance across tuples: RFC 9000 section 8.2.2 prohibits
the initiator from enforcing the responder's same-path requirement; current
candidate response admission is more restrictive and is not a conformance claim.
The full implementation and performance goal remains active.

## 2026-09-20 mutual local migration validation

Candidate-local input now authenticates and handles PATH_RESPONSE and
PATH_CHALLENGE only for the nominated tuple and live peer-CID reservation.
It leaves the established binding, receive/ACK history and application delivery
unchanged until a future local commit. Remote candidates also answer challenges,
allowing both endpoints to validate the new tuple. A single replaceable pending
response bounds storage; sending shares CID, amplification, congestion, pacing,
packet-number and key-use gates with candidate probes. Blocked sends preserve the
response, and validated local candidates retain response pacing wakeups.

Encrypted v1/v2 tests in base and multipath modes complete mutual validation,
reject forged and wrong-tuple proof, withhold candidate stream data, preserve
blocked responses and check post-validation pacing deadlines. This follows the
response-on-received-path requirement in [RFC 9000 section 8.2.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-8.2.2).
Root race tests, vet and default interop race tests pass; the seven documented
interop skips are unchanged.

Local binding commit, socket opening/cleanup and public API integration remain
unfinished. The complete implementation and performance goal remains active.

## 2026-09-20 local migration probe output and timers

Local migration candidates now produce expanded PING plus PATH_CHALLENGE packets
from their reserved local tuple and peer CID, using shared packet-number/AEAD
counters and established congestion/pacing gates. PING permits remote non-probing
address nomination while the client retains its established ordinary-output
binding. Candidate retry/pacing/absolute expiry deadlines participate in path
timers; expired or revoked reservations are canceled and reliably retired. A
queue-blocked cancellation retains retryable ownership without keeping a past
candidate deadline armed. Remote and local probe construction share one output
implementation. A UDP-owner preparation helper checks connection registry and
registered-local-socket ownership before engine admission.

Encrypted v1/v2 tests in base and multipath modes cover blocked output, 1200-byte
probes, reserved CID/source tuple, shared counters, unchanged established binding,
server-side nomination, deadline servicing and queue-blocked expiry/retry.
Runtime tests reject unregistered sockets and foreign connections. Root race
tests, vet and default interop race tests pass (seven known skips).

Response handling on the candidate local tuple, validated commit, socket opening
and cleanup, and public API integration remain unfinished. No public operation
yet initiates local migration. The complete protocol/multipath/API/interop/
performance objective remains active.

## 2026-09-20 local migration admission and cancellation

Added owner-confined preparation of client local-address migration. Admission
requires a live confirmed client, an existing validated path, a different concrete
local address in the same family, peer permission at its handshake address, no
concurrent candidate and an unused peer CID. The reservation preserves established
address/CID, packet numbers, queue charges and recovery. Local candidates receive
client amplification exemption separately from their still-pending reachability
validation; no fictional received-byte credit is created.

Cancellation releases unsent reservations or atomically queues reliable retirement
for spent IDs. Queue pressure restores the pending operation for retry. General
reservation cleanup preserves a live local candidate; abandonment/closing/draining
clear pending local operation references. V1/v2 tests cover reservation isolation,
unsent/spent cancellation and full-queue rollback. Policy tests cover role,
confirmation, peer prohibition, unchanged/invalid/cross-family addresses, invalid
time, CID exhaustion and unvalidated paths. Root race tests, vet and default
interop race tests pass (seven known skips).

No runtime or public caller creates these local candidates yet. Registered-socket
ownership, probe/timer integration, responses on the candidate local tuple, commit
and socket cleanup remain to implement before exposing actual client migration.
The complete protocol/multipath/API/interop/performance goal stays active.

## 2026-09-20 active-migration policy conformance

Local socket-migration admission review found an existing multipath policy gap:
draft-21 section 2.2 forbids new paths to the handshake address when the peer sends
disable_active_migration. Path initiation now enforces that rule before allocating
state, consuming an ID or queuing a probe. The original handshake remote address
is retained independently of current bindings. A different peer address remains
eligible under the draft's rule; unknown handshake identity is rejected when the
restriction applies.

Server advertisement now reflects capability: a concrete migration owner or
multipath-capable server can omit disable_active_migration, while legacy/wildcard
owners and explicit disable requests retain it. Client advertisement remains
restricted. Synthetic multipath fixtures now explicitly model migration permission.
V1/v2 tests verify policy rejection without state mutation, unchanged handshake
identity after a current-binding change, permitted alternate addresses and
capability/explicit-disable advertisement. Root race tests, vet and default
interop race tests pass (seven known skips); the corrected fixture's transport
race rerun passes.

Actual client socket-migration admission/output remains to implement. This policy
fix is a prerequisite and closes an existing standards gap, not a completed local
migration feature. The full QUIC/multipath/API/interop/performance goal stays active.

## 2026-09-20 concrete-address ordinary client ownership

Dial now requests base-protocol migration ownership, and the UDP runtime checks
client connection capabilities as well as server endpoint configuration when
selecting a concrete-address owner. Ordinary clients with a concrete LocalAddr
hand off path zero after confirmation; wildcard clients retain legacy ownership.
This does not enable client acceptance of unknown server addresses or negotiate
multipath. It establishes the owner boundary needed for a future local socket
migration transaction.

Public IPv4 and IPv6 tests verify client/server path-zero snapshots, unchanged
SupportsMultipath=false, no additional paths, and rejection of multipath backup
status. The IPv6 case verifies the existing certificate fixture identity through
an explicit ServerName. Upstream handshake-loss cases now use concrete independent
client bindings and assert base ownership while preserving the sixteen-case loss
matrix. Root race tests, vet and default interop race tests pass (seven known
skips); focused IPv4/IPv6 ownership/rebinding and updated loss tests also pass.

Actual client socket migration remains unimplemented. Existing performance
samples describe the earlier server-only base owner and should be refreshed when
evaluating this client handoff. The full protocol/multipath/API/interop/performance
goal remains active.

## 2026-09-20 base-only scheduler fast path

Base-protocol ownership now schedules path zero directly instead of collecting,
sorting and scanning a multipath candidate set. Existing packet construction
still enforces all output gates. A same-state microbenchmark reports median
idle-scheduler time of 59.04 ns through generic scheduling versus 13.34 ns through
the shortcut, with zero allocations for both. Isolated completed 256 KiB public
transfers remain around 3.3–3.4 ms/op; the short samples do not establish an
end-to-end throughput improvement. Raw measurements and reproduction commands
are in docs/benchmarks/base-owner-scheduler.txt and path-transfer.md. An overlapping
benchmark run was discarded and rerun sequentially.

Root race tests, vet and default interop race tests pass (seven known skips).
The full protocol, migration, multipath, HTTP/3, 0-RTT, API/interoperability and
performance goal remains active; this is a measured local CPU optimization,
not a claim of upstream or universal performance superiority.

## 2026-09-20 coalesced input after path handoff

Address-aware input now uses parseable long-header packet lengths to reach a
trailing 1-RTT packet after confirmation, when Initial/Handshake processing has
been retired. It checks the full datagram size and first-packet version, requires
matching destination CIDs, and stops at malformed framing without scanning for
packet-like bytes. The common short-header path delegates directly without extra
header parsing. Once the trailing packet authenticates, established or candidate
amplification accounting receives the full datagram size once; candidate data is
still withheld until validation. Closing receive now also honors base-protocol
frame restrictions instead of always permitting multipath frames.

V1/v2 tests in base and multipath modes cover valid coalescing, candidate credit
and admission isolation, forged tails, CID mismatch, wrong versions, truncated
prefixes and oversized datagrams. Root race tests, vet and default interop race
tests pass (seven known skips); transport race tests also pass after the fast-path
adjustment. Local socket migration, wildcard/zero-CID policy, broader migration
interop, HTTP/3, 0-RTT and the full API/RFC/performance objective remain active.

## 2026-09-20 ordinary QUIC server rebinding

Concrete-address public listeners now activate base-protocol path-zero ownership
after confirmation when both CIDs are nonzero and legacy retirement history has
not already advanced. Wildcard listeners, zero-length peer CIDs and pre-handoff
CID retirement retain the existing engine. The handoff installs no multipath
transport parameter and emits ordinary RFC 9000 frames. Server validation policy
now covers address rebinding in this mode as well as negotiated multipath.

The public v1/v2 NAT relay matrix now runs with and without multipath, covering
port/IP changes and reused/fresh CIDs. A separate upstream quic-go client test
uses its shared UDP transport with nonzero CIDs and verifies bidirectional streams
before rebinding, on the new port and after returning to the old port. Both
versions pass, including ten race repetitions. Initial interop regressions exposed
zero-length peer CID handoff; fallback guards fixed these, with regression tests
for zero-CID and pre-retired-history cases. Root race tests, vet and the complete
interop race suite now pass (seven known skips).

Remaining work includes ordinary zero-CID/wildcard address policy, local socket
migration, coalesced post-confirmation long-header handling, broader migration
interop and performance comparisons for this new base owner. The full protocol,
multipath, HTTP/3, 0-RTT, API and performance objective remains active.

## 2026-09-20 base-protocol path-zero ownership

Added an explicit baseOnly path-table mode and activateBasePath handoff for
confirmed ordinary QUIC connections. It preserves the existing packet-number,
recovery and CID objects without claiming multipath negotiation. Internal path-zero
ACK/CID feedback converts to RFC 9000 ACK, NEW_CONNECTION_ID and
RETIRE_CONNECTION_ID frames. Base-mode queue/output validation rejects other
extension controls; authenticated receive validates with multipath disabled.
Nonzero internal path IDs are never silently converted to path zero.

V1/v2 tests in both endpoint roles exchange encrypted packets with a peer still
using the legacy ordinary engine, including CID rotation/replenishment and ACK
completion. They verify preserved ownership/negotiation, rejection of extension
output without queue mutation, and rejection of authenticated unnegotiated input.
Root race tests, vet and default interop race tests pass (seven known skips).

Runtime activation is not enabled yet. Next: integrate the base-mode handoff in
the concrete-address owner, verify handshake/long-header handling after handoff,
and add ordinary QUIC rebinding interoperability tests before changing public
behavior. Local socket migration, wildcard packet-info, full multipath interop,
HTTP/3, 0-RTT and the remaining RFC/API/performance goal remain active.

## 2026-09-20 live handshake-loss interoperability

The unexplained TestPublicListener timeout did not recur in 100 race-enabled
repetitions. Added a UDP relay interoperability test that drops exactly one early
datagram and checks handshake completion followed by exact unidirectional stream
bytes and FIN. Its sixteen cases cover independent client/server roles, QUIC v1/v2,
both drop directions, and the first or second datagram in that direction, without
assuming either implementation's packet coalescing. All sixteen cases pass under
the race detector. The complete interop race suite and interop vet pass, with
seven known skips unchanged.

This adds loss coverage and evidence about the timeout, not a claimed fix: its
cause remains unknown. No transport implementation changes were needed for these
cases. Continue migration work (ordinary non-multipath rebinding and local socket
moves), independent multipath interoperability, remaining RFC/API features and
performance evaluation. The full goal remains active.

## 2026-09-20 live UDP peer-IP rebinding

Enabled peer-IP rebinding for public negotiated multipath servers. Receive and
commit both reject local socket/address changes and cross-family remote changes.
The public NAT relay test now covers v1/v2, reused/fresh CIDs and port/IP changes,
including bidirectional streams, return to the original address and snapshot
retention after shutdown. For peer-IP cases it first tries a second loopback
address, then an already assigned local IPv4 address; it changes no interface
configuration and sends only between local sockets. All eight cases executed and
passed on this Mac, including ten race-enabled repetitions. Unit tests verify
unsupported local/family transitions leave binding and recovery state untouched.

Root race tests and vet pass. The first interop race run had one unexplained
TestPublicListener upstream dial handshake timeout (multipath disabled). Ten
isolated race repetitions and a subsequent full interop race run passed; the
seven documented skips remain. The failure message now includes the version.
The timeout is not considered diagnosed or fixed and warrants further evidence
if it recurs.

Remaining migration work includes local socket moves, ordinary non-multipath
migration, address-family changes, wildcard packet-info, and broader independent
interop/recovery validation. The full HTTP/3, 0-RTT, API/RFC and performance goal
also remains active.

## 2026-09-20 gated peer-IP migration integration

Peer-address commit now distinguishes port-only rebinding from IP changes. An
internal allowIPMigration gate admits remote-IP candidates on the existing local
socket. Successful address validation invokes the recovery reset for IP changes
before publishing the binding; port-only changes retain prior RTT/congestion
state. CID reservation/revocation and packet-number continuity remain shared
with the fresh-CID integration. The commit helper is renamed commitPeerRebinding.

Encrypted v1/v2 probe exchanges now cover both address-change modes and both CID
reuse/fresh-CID modes, with explicit assertions that IP changes clear old RTT,
flight and congestion state. The reliable-late-ACK test now runs through an actual
primary IP binding commit and verifies address publication, retained aliases,
requeue without double charging, and late completion without RTT/window changes.
Root race tests, vet and default interop race tests pass (seven known skips).

Public IP migration remains gated pending live UDP validation and socket-family
policy coverage. This Mac cannot bind alternate loopback 127.0.0.2 (EADDRNOTAVAIL),
so a portable test should select a second locally owned IPv4 address where
available, or run on a host with alternate loopback support. No host interface
configuration was changed. Local-address migration still needs a separate owner
transaction. The complete QUIC/multipath/API/interop/performance goal stays active.

## 2026-09-20 recovery transition for IP migration

Added Sent.ResetForPathChange, which returns outstanding transmissions for frame
recovery and resets RTT, flight, loss and persistent-congestion evidence while
preserving packet-number continuity and ACK-of-unsent validation. Old packet ACKs
produce no recovery events or RTT samples in the new context. Added a path-level
transition that requeues reliable items without additional memory charges and
resets NewReno, PTO, pacing and pending probe state in place, preserving path-zero
aliases. Existing logical transmission references remain available for late ACK
completion independently of the reset recovery ledger.

Recovery tests verify that old ACKs do not affect new flight/RTT/congestion
history, new ACKs provide fresh samples, and unsent ACKs remain invalid. A transport
test verifies outstanding reliable-item requeue, object/packet-number preservation,
and late completion without changing the initial congestion window or RTT state.
Root race tests, vet and default interop race tests pass (seven known skips).

This transition is not yet wired into address admission; public rebinding remains
port-only. Next: integrate it with validated peer-IP changes, test mixed old/new
ACKs and reliable transfers through that transition, then extend live UDP tests.
The full QUIC/multipath/API/interoperability/performance goal remains active.

## 2026-09-20 fresh-CID lifecycle integration coverage

Added authenticated v1/v2 nomination tests spanning candidate replacement and
absolute expiry, each before and after a validation probe is emitted. They verify
that unsent reservations are released/reusable, spent reservations produce one
reliable retirement, no unvalidated binding becomes visible, and ordinary encrypted
output continues with the established CID. An exhaustion test first constructs
packets with every available peer CID, then verifies a fresh-address nomination
cannot reserve one: repeated output attempts consume no packet numbers or AEAD
usage, and expiry preserves the established binding and connection state.

All scenarios pass 25 race-enabled repetitions. Root race tests and vet pass.
This turn adds verification rather than new wire behavior; the existing lifecycle
implementation satisfied these cases. Live UDP candidate replacement and peer
retirement remain useful interop coverage, and general IP/local-address migration
still requires a separate recovery/CID transition. The complete protocol, multipath,
HTTP/3, 0-RTT, API/interoperability and performance goal remains active.

## 2026-09-20 fresh-CID port rebinding integration

Authenticated fresh-DCID nominations now mark candidates for exclusive peer-CID
reservation. Candidate probe output reserves an unused CID without changing the
established sending CID, preserves it across blocked output, and checks continued
ownership before every probe. Validation commits the address and reserved CID
together; previously used IDs are excluded from the new tuple and retired through
the reliable cleanup path. Peer retirement revokes a candidate's proof rather
than silently switching its probes to a replacement CID. Path-zero commits also
update the legacy peerCID mirror. Public EnableMultipath now enables this policy
on servers alongside existing observed-CID port rebinding.

Encrypted v1/v2 tests cover both CID modes, short and expanded probes, blocked
amplification credit, shared packet-number/key counters, ordinary output with the
established CID during validation, fresh-CID commit, and deferred stream delivery.
A retirement test verifies failed commit leaves both the network binding and
published address unchanged. Public three-socket NAT relay tests rotate the client
CID on its owner and exercise rebinding plus return to the original port.

Root race tests, vet, default interop race tests (seven known skips), ten race
repetitions of the public rebinding test, and 25 repetitions of the core exchange
and peer-retirement tests pass. Test fixtures now tolerate out-of-order initial
CID advertisements and issue a replacement CID when needed.

The full goal remains active. General IP/local-address migration, wildcard
packet-info, independent multipath interoperability, HTTP/3, 0-RTT and broader
API/RFC/performance requirements remain unfinished. Further migration coverage
should exercise CID exhaustion, candidate replacement and peer retirement during
live UDP traffic, then address IP-change recovery and local socket migration.

## 2026-09-20 reliable retirement of canceled candidate CIDs

Added selective retirement of spent candidate CID reservations. Sparse bounded
retirement tombstones preserve lower established IDs while preventing delayed
NEW_CONNECTION_ID advertisements from reviving selectively retired IDs; advancing
RETIRE_PRIOR_TO collects obsolete tombstones. Migration output servicing now
releases unsent canceled reservations and stages reliable retirement frames for
spent reservations, preserving the active candidate. CID deletion and the entire
retirement batch commit only after queue/destination admission. Buffer pressure
leaves ownership unchanged and allows ordinary output to drain before retry.

Tests cover sparse retirement and replay, retirement-floor collection, preserved
current CID, active-candidate exclusion, one-byte-short queue rejection, duplicate
cleanup, and actual encrypted v1/v2 retirement delivery to the peer's local CID
manager. Root race tests, vet and default interop race tests pass (seven known
skips).

Fresh-CID receive/probe/address-commit integration remains pending; reservations
are not yet created by packet receive, so public fresh-CID rebinding stays gated.
Next: reserve on authenticated fresh-DCID nomination, send candidate probes with
that reservation, recheck peer retirement before commit, and test established
and candidate output together. The full implementation goal remains active.

## 2026-09-20 candidate CID ownership primitives

Added owner-confined peer-CID reservations for migration candidates. Reservation
selects an unused non-current ID without changing the established CID; blocked
output can release an unsent reservation. Committed probe use marks the ID used,
and cancellation then requires reliable retirement while keeping the ID excluded
from ordinary selection. Validated commit selects the candidate CID and excludes
previously used IDs from automatic selection on the new tuple. Duplicate peer
advertisements preserve reservations; RETIRE_PRIOR_TO revokes them. Rotation and
automatic current selection skip reserved IDs, including when the only remaining
ID has the maximum legal sequence. Unsent cancellation restores a current ID
when peer retirement temporarily left no ordinary CID available.

Tests cover ownership, duplicate advertisement, output-use accounting, cancellation,
validation and double-commit guards, peer retirement, exhaustion, and maximum
sequence handling. Root race tests, vet and default interop race tests pass
(seven known skips).

These primitives are not yet connected to candidate receive/probe/commit output;
fresh-CID rebinding remains gated. Next: wire reservations into that transaction,
reliably retire spent reservations on replacement/expiry, and test encrypted
fresh-CID rebinding while ordinary output continues on the established tuple.
The full implementation and validation goal remains active.

## 2026-09-20 transactional peer CID rotation

Fresh-CID rebinding review exposed two prerequisites in existing same-path CID
rotation: it ignored the used flag when selecting an unused CID, and mutated
CID state before retirement queue admission. Selection now deterministically
skips used IDs. RotatePeerID stages the CID manager and the complete reliable
RETIRE_CONNECTION_ID batch, checks capacity and a viable destination first,
and commits without replacing the manager pointer shared by multipath path
zero. Queue exhaustion returns ErrSendBuffer without closing the connection
or changing CID/queue state. Abandoned path-zero and closed-connection guards
prevent inappropriate rotation requests/readiness reports.

Tests exercise used-ID exclusion, unchanged state when no unused ID remains,
queue rejection one byte below required capacity, exact-capacity success,
complete retirement batches, and path-zero pointer preservation for legacy
and multipath connections. Root race tests, vet, and default interop race tests
pass (seven known skips).

Fresh-CID rebinding is still gated: a candidate needs exclusive ownership of an
unused peer CID while the established tuple continues using its existing CID.
Retirement, replacement, cancellation and commit must preserve that ownership;
a global rotation before validation would send the new CID on both tuples.
Continue with candidate-specific CID ownership and encrypted rebinding tests.
The full migration, multipath interop, HTTP/3, 0-RTT, API/RFC and performance
goal remains active.

## 2026-09-20 per-path address snapshots

Added owner-published immutable address-pair snapshots for primary activation,
client path initiation, authenticated server path acceptance, prepared bindings,
and port-rebinding commits. Readers do not access mutable engine state. Snapshots
retain the last binding after abandonment/shutdown, bounded by the engine's
lifetime CID/path limits. Conn.PathAddresses exposes these pairs on either
endpoint; Path address methods use the same snapshots and return fresh UDPAddr
values. Presence is deliberately documented as binding history, not liveness.

Tests cover concurrent pair consistency, unknown IDs, shutdown retention,
secondary-path rebinding publication without changing primary identity, public
v1/v2 client/server additional-path reporting, copy isolation, and primary
rebinding through the public NAT relay. Root race tests, vet and default interop
race tests pass (seven known skips).

The full goal remains active. Fresh-CID rebinding and general IP/local-address
migration still need CID/recovery policy; wildcard packet-info, independent
multipath interop, HTTP/3, 0-RTT, API/RFC conformance and performance requirements
remain unfinished.

## 2026-09-20 public client NAT port rebinding

Config.EnableMultipath now enables reactive client port rebinding on servers
when multipath is negotiated. Admission remains limited to the same local
address and peer IP with authenticated CID-use evidence on the established
path. Candidate validation precedes application admission and primary-path
RemoteAddr publication. RFC 9000 section 9 allows NAT rebinding despite
advertising disable_active_migration, but requires clients to discard an
unknown server address; the receive entry point now enforces that role rule.

Public v1/v2 tests use a three-socket NAT relay, rebind before the first stream,
verify bidirectional stream delivery and RemoteAddr, return to the original
port, and verify address retention after listener shutdown. A core encrypted
packet test verifies client rejection even with the internal gate enabled.
Root race tests, vet, default interop race tests (seven known skips), and ten
race-enabled repetitions of the public rebinding test pass.

The full goal remains active. General IP/local-address migration, fresh-CID
rebinding, secondary-path address reporting, wildcard packet-info, independent
multipath interoperability, HTTP/3, 0-RTT and API/RFC/performance requirements
remain unfinished. Next migration work should address CID rotation and the
lifetime/reporting of additional path bindings before broadening admission.

## 2026-09-20 concurrent committed-address reporting

Added an immutable atomic peer-address snapshot initialized by the address-aware
owner and updated on primary-path port-rebinding commit. Public Conn.RemoteAddr
reads it without calling into the actor and retains the last value after shutdown.
Additional path changes do not redefine the connection's primary identity.
Legacy connections fall back to their initial peer address. LocalAddr and
RemoteAddr return independent UDP address copies to prevent caller mutation.

Tests cover concurrent snapshot access, shutdown retention, public address-copy
isolation, and snapshot updates during real UDP rebinding and return to the old
port. Root race tests, vet and default interop race tests pass (seven known skips).

Public rebinding is still internally gated pending final role/parameter policy
and public integration tests. General IP/local-address migration, fresh-CID
migration, wildcard packet-info, independent multipath interop, HTTP/3, 0-RTT
and remaining API/RFC/performance requirements remain in the active goal.

## 2026-09-20 authenticated CID evidence across handshake handoff

Legacy Handshake and 1-RTT receive now retain authenticated local-CID-use
evidence before the address-aware owner handoff. Activation binds only that
existing evidence to its fixed primary tuple. Issued-but-unused CIDs are not
marked, and previously bound observations cannot be overwritten by handoff.
Initial packets are excluded because their protection keys are publicly derived.

V1/v2 handshake/activation tests verify evidence preservation and unused-path
CID exclusion. Additional tests verify bound observations remain intact and an
actually processed Initial packet creates no evidence. Root race tests, focused
handoff/UDP rebinding race tests, vet and default interop race tests pass (seven
known skips).

Public rebinding remains gated. Next: finish role/transport-parameter policy and
public current-address reporting, then expose and independently validate the
supported rebinding behavior. Fresh-CID/general IP/local-address migration,
wildcard packet-info, HTTP/3, 0-RTT, independent multipath interop and remaining
API/RFC/performance scope stay in the active goal.

## 2026-09-20 CID evidence for reactive rebinding

Local CID owners now retain authenticated established-tuple observations, bounded
by active CID count and removed on retirement. Before reusing a current sending
CID toward a candidate address, internal rebinding admission requires the incoming
DCID to have been used on the established tuple (RFC 9000 section 9.5 exception).
Observations update after normal admitted receive and on validated commit.
The implemented candidate policy also rejects IP/local-address changes before
probing; those still need a separate transition rather than port-only commit.

Tests cover a spare CID that has not appeared on the established tuple, subsequent
authenticated established use, accepted rebinding afterward, and retirement
cleanup. The v1/v2 encrypted and real UDP rebinding tests still pass. Root race
tests, focused CID/migration race tests, vet, and default interop race tests pass
(seven known skips).

Before public enablement, seed authenticated CID evidence across the legacy
handshake/owner handoff so early NAT rebinding does not require an extra packet
on the old tuple. Fresh-DCID migration/rotation, role/parameter policy, general
IP/local-address recovery and public address reporting remain unfinished.
The full goal stays active, including wildcard packet-info, independent
multipath interop, HTTP/3, 0-RTT, API/RFC coverage and performance work.

## 2026-09-20 real UDP actor NAT-rebinding regression

Added a v1/v2 UDP integration test with a fixed client-facing relay socket and
two server-facing sockets. Switching the active back socket changes only the
source port observed by the server, without changing the client's socket or
peer address. The internal migration gate is enabled on the server endpoint.

The test transfers streams before and after rebinding, checks committed address
and validation state, and verifies return traffic on the new relay socket. It
then switches back to the original port and verifies another validated commit
and bidirectional stream delivery. This exercises actual socket routing,
serialized actors, timers, encrypted probes, retransmission and stream dispatch,
rather than manually delivering sealed packets.

Root race tests, vet and default interop race tests pass (seven known skips).
Public migration remains gated pending CID-use and role/parameter policy.
General IP/local-address migration needs its recovery transition. Wildcard
packet-info, independent multipath interop, HTTP/3, 0-RTT and full API/RFC and
performance coverage remain part of the active goal.

## 2026-09-20 internal validated port-rebinding commit

Added commit for validated remote-port changes on the same local socket and peer
IP. It preserves packet-number/nonce history, outstanding reliable items, flight
accounting, RTT and congestion objects using RFC 9000 section 9.4's port-only
exception. Candidate credit replaces old address credit through existing aliases,
validation becomes complete, and old-tuple queued challenge/response frames are
retired. The validation-completing packet is recorded/dispatched after commit;
earlier candidate stream data remains unacknowledged and can be retransmitted.

V1/v2 encrypted tests now complete rebinding and deliver previously withheld
stream data. A recovery regression verifies old outstanding transmissions remain
ACKable after commit, stale probes are removed, and IP changes cannot use this
port-only policy. Root race tests, focused migration/rebinding race tests, vet
and default interop race tests pass (seven known skips).

Public migration remains gated off. Before enabling it, finish CID-use policy,
role/transport-parameter constraints and real UDP rebinding integration. General
IP/local-address migration still requires recovery/congestion transition.
Wildcard packet-info, independent multipath interop, HTTP/3, 0-RTT and full
API/RFC/performance coverage remain in the active goal.

## 2026-09-20 candidate-tuple migration probes

Added internal candidate probe output through the bound sender. PATH_CHALLENGE
packets share the existing path's PN/nonce/key-usage domain and in-flight ledger,
but use the candidate tuple and amplification budget. Probes are expanded to
1200 bytes when credit permits, use fresh tokens for retries, and do not enter
reliable-data retransmission queues. Candidate pacing/retry deadlines join path
timers. Established-path congestion/pacing limits remain a conservative gate
pending migration-specific recovery integration.

V1/v2 encrypted exchange tests simulate NAT translation, complete a short address
probe and padded MTU follow-up, verify shared PN/AEAD counters, and preserve the
established binding and budget. Zero candidate credit consumes no PN, AEAD
usage, or sent-token history. Root race tests, vet, and default interop race tests
pass (seven known skips).

Public migration remains gated off. Validation proof is now reachable internally;
commit must still update binding, CID policy, congestion/RTT/recovery and pending
application receive semantics. Full migration, wildcard packet-info, independent
multipath interop, HTTP/3, 0-RTT, API/RFC coverage and measured performance remain
part of the active goal.

## 2026-09-20 gated authenticated candidate receive

Connected candidate selection to changed-remote-address multipath receive behind
an internal configuration gate (off publicly). Authentication precedes allocation
and credit; stale or probing-only initial evidence cannot nominate an address.
Each path owns one selector/candidate, seeded from established traffic. Candidate
packets do not enter receive history, ACK scheduling, or application dispatch.
Established bindings and credit remain unchanged. Newer established traffic
cancels nomination. Candidate expiry joins path deadlines/timeouts, and
abandonment/closing/draining discard selection state.

V1/v2 encrypted-packet tests cover the default gate, forged/stale packets,
separate credit/receipt accounting, established-address return, timeout, and
replay after expiry. Root race tests, vet and default interop race tests pass
(seven known skips).

The internal implementation currently admits remote rebinding candidates on the
same local socket only. Candidate-tuple probe output and validation/commit with
CID, congestion and recovery handling are next. Public migration remains off.
Full RFC migration, wildcard packet-info, independent multipath interop, HTTP/3,
0-RTT, API/conformance and performance work remain part of the active goal.

## 2026-09-20 authentication before address admission

Split multipath packet receive into authentication/frame parsing and explicit
receipt recording. The ordinary receive wrapper still performs both in order.
Authentication retains packet number, CID, frame evidence and ACK-eliciting
classification without touching receive history, ACK pending state, amplification
credit, migration watermark or stream dispatch. Key-phase/integrity/idle effects
remain in authentication as before. Receipt recording copies metadata and strips
frames from duplicates without erasing the original authentication evidence.

V1/v2 encrypted-stream tests verify forgery rejection, no delivery/credit/ACK
before admission, repeat authentication after a declined admission, successful
admission/dispatch, and duplicate suppression. Root race tests, vet and default
interop race tests pass (seven known skips).

This enables candidate-address policy to authenticate before deciding whether
to record a packet. New-address receive is still gated off: candidate admission,
candidate-tuple probe output, and CID/congestion/recovery commit are next.
The full objective remains active, including completed migration, wildcard
packet-info, independent multipath interop, HTTP/3, 0-RTT and API/RFC/performance
coverage.

## 2026-09-20 live authenticated migration ordering watermark

Connected highest non-probing packet tracking to legacy authenticated 1-RTT
receive and bound multipath receive. Tracking follows successful frame parsing
and uses the existing probing-frame classification. State belongs to the packet
number space, so path-zero adoption preserves pre-activation ordering and
secondary paths remain independent. A candidate selector can now be seeded from
that established-path history instead of starting with an empty watermark.

V1/v2 encrypted-packet tests cover probing-only traffic, forged authentication,
out-of-order packets, duplicates, stale candidate rejection, and newer candidate
admission after seeding. A handshake/adoption test verifies legacy ordering
survives handoff. Root race tests, vet, and default interop race tests pass
(seven known skips).

Live new-address packets are still rejected by the fixed binding. Next work is
authenticated candidate admission and candidate-tuple probe output; candidate
decoding must not advance established-address ordering before selection.
CID/recovery/congestion commit, wildcard packet-info, independent multipath
interop, HTTP/3, 0-RTT and broader API/RFC/performance scope remain unfinished.
The full goal remains active.

## 2026-09-20 ordered migration candidate selection

Added owner-confined migrationSelection with one current candidate and a retained
validated fallback tuple. Only newer authenticated non-probing evidence can
nominate/replace an address. Candidate traffic may add amplification credit
without restarting validation. Newer non-probing traffic on the validated tuple
cancels the candidate; older/probing-only traffic cannot redirect selection.
Expiry preserves the packet-number watermark, preventing replay from restarting
the same nomination. Commit requires the exact current candidate and completed
address/MTU validation, returning the previous tuple for owner bookkeeping.

Tests cover packet number zero, reordered nominations, duplicate credit,
bounded replacement, primary-address return, expiry/replay, unvalidated/stale
commit rejection and successful single commit. Root race tests, vet and default
interop race tests pass (seven known skips).

These components are not yet connected to live receive/output. Next integration
must seed/update the watermark from established-path authenticated traffic,
authenticate candidate packets before admission, classify probing frames,
route probe output to the candidate tuple, and update CID/congestion/recovery
state atomically at commit. Migration remains disabled in the public transport.
The full goal remains active, including migration integration, wildcard packet-info,
independent multipath interop, HTTP/3, 0-RTT, API/RFC coverage and performance.

## 2026-09-20 isolated migration-candidate validation

Started migration/NAT rebinding support with a separate internal candidate
validator: canonical tuple, independent amplification budget, cryptographically
random probe tokens, sent-token matching, at most three probes per validation
phase, and a fixed three-PTO deadline. Unsent tokens and wrong-tuple responses
cannot validate. Short probes prove address reachability but require a padded
follow-up before minimum-size validation. Blocked attempts preserve pending
token/credit state; new input cannot extend the candidate lifetime.

Reviewed RFC 9000 sections 8.2 and 9.3:
https://www.rfc-editor.org/rfc/rfc9000.html#section-9.3
The validator is deliberately not wired into live packet processing yet.
Existing bindings, packet numbers, and recovery remain separate. Next integration
must authenticate before candidate admission, bound concurrent candidates,
enforce highest non-probing packet ordering, send probes on candidate tuples,
and commit or revert with correct CID/congestion/recovery treatment.
Public migration remains disabled; current fixed-tuple drops are unchanged.

Root race tests, vet and default interop race tests pass (seven known skips).
Candidate tests cover credit exhaustion, unsent/mismatched tokens, MTU follow-up,
retry bounds, deadline-edge responses and expiry without any sent probe.
The full goal remains active, including completed migration integration,
wildcard packet-info, independent multipath interop, HTTP/3, 0-RTT, full
API/conformance and measured performance improvements.

## 2026-09-20 allocation-free idle multipath scheduling

Inspected packet sealing: its output allocation is already correctly sized and
owned by callers, so removing it safely requires a lifetime contract change.
Instead removed measured scheduler scratch allocations with a local 64-ID array
and typed sorting; path ordering and scheduling policy are unchanged. Idle
scheduler allocation regression verifies zero allocations versus four before.

Root race tests, vet, and default interop race tests pass (seven known skips).
Repeated public two-path benchmark samples reduce allocation counts from about
26,400 to 19,600 per transfer (26%). Times remain around 6.3 ms, insufficient
evidence for a meaningful throughput improvement over the preceding short run.
Raw data and limitations are recorded in docs/benchmarks/path-transfer.md.

The full goal remains active. Major remaining scope includes migration/rebinding,
wildcard packet-info, independent multipath interop, HTTP/3, 0-RTT, full API/RFC
coverage, and further measured performance work.

## 2026-09-20 allocation-free stable multipath route synchronization

Router.Sync now snapshots per-path local CID owners, highest issued sequences,
and active counts after successful collision preflight and route commit.
Unchanged state returns without rebuilding maps/strings. Path membership/table
replacement invalidates independently of path count. Legacy transition checks
also account for the multipath owner. Immutable CID and monotonic issuance
invariants are the same ones already used by the legacy route cache.

Tests verify zero allocations for stable Sync, same-count CID replacement,
owner replacement, same-count path replacement/retirement, collision atomicity,
pre-send reservation and retention. Root race tests, vet, focused routing race
tests and default interop race tests pass (seven existing skips).

Repeated public benchmark medians improved two-path 256 KiB transfer from about
8.5 ms to 6.4 ms, with about 53% fewer allocated bytes and 69% fewer allocations.
Raw samples and post-change profile are linked in docs/benchmarks/path-transfer.md.
This remains roughly twice ordinary QUIC's time on this loopback workload.
Packet sealing/opening and stream/socket buffers are now prominent sampled
allocation sites; there is no universal performance claim.

Migration/rebinding, wildcard packet-info, independent multipath interop, HTTP/3,
0-RTT and full API/conformance remain unfinished. The full goal stays active.

## 2026-09-20 public path performance baseline

Added BenchmarkPublicPathTransfer for completed, byte-verified 256 KiB transfers
through the public API: v1/v2 ordinary QUIC, negotiated multipath with one active
path, and two active paths. Setup is excluded; receiver completion and FIN are
required. Three non-race samples of 25 operations and a sampled memory profile
are recorded in docs/benchmarks/path-transfer.md with commands and raw artifacts.

Two-path medians were about 8.5 ms versus 3.2–3.3 ms for ordinary QUIC on Apple M4
loopback. This is not an independent-link aggregation or upstream comparison.
Router.Sync accounted for 48.75% of sampled allocated bytes in a separate v1
two-path profile. Next: reduce repeated multipath CID synchronization allocations
while preserving pre-send reservation/collision and retirement semantics.

Race-enabled one-operation benchmark cases, the root race suite, and vet pass.
This turn adds measurement/harness changes only; the implementation is unchanged.
The full goal remains active, including migration/rebinding, independent
multipath interoperability, HTTP/3, 0-RTT, full API/conformance and performance work.

## 2026-09-20 idle paths share queued application data

The packet scheduler now lets an idle validated path send queued stream or
datagram frames from another path. Control frames and pending path probes keep
their affinity. Borrowing commits only after packet construction succeeds;
blocked attempts leave the source queue intact. Logical send-item identity and
memory charges are preserved for recovery and acknowledgment.

V1/v2 tests cover validation/amplification rejection without mutation, committed
ownership transfer, ACK completion without double charging, and a scheduler
sending a blocked primary's backlog through a secondary. Public loss tests
revealed their old 25%-per-path requirement encoded the previous fixed placement
policy: secondary share decreased when the healthy path drained its backlog.
The tests now require substantial traffic on each path (16 KiB of a 256 KiB
transfer), exact payload/FIN, and actual induced drops without imposing fairness.
Root race tests, vet and default interop race tests pass; seven skips remain.

This is an initial adaptive policy, without a measured throughput advantage or
RTT-aware/coupled congestion scheduling. Migration/rebinding, wildcard packet-info,
independent multipath interoperability, HTTP/3, 0-RTT and full conformance/API
coverage remain unfinished. The full goal remains active.

## 2026-09-20 local path initiation order

Added a client-local highest-initiated ID, committed only after a validation
probe is successfully queued. Both OpenPath readiness and the final serialized
initiation check reject lower/equal IDs. This prevents waiting/concurrent calls
from bypassing the order check; failed admission leaves the ordering unchanged.
Cancellation after a probe consumes the ID and skipped lower IDs remain
unavailable. Public tests now initiate path 2 before their canceled path 3.

The local policy is distinct from receive ordering: draft-21 section 3.2.1
warns that CID advertisements and initiation packets can arrive out of order.
A v1/v2 encrypted-packet test initiates 1 then 2 locally and delivers 2 then 1
to the server, confirming both are accepted. Other tests cover skipped lower
IDs, abandonment, full-queue rollback and subsequent valid higher initiation.
Root race tests, focused initiation race tests, vet and default interop race
tests pass; seven known interop skips remain.

Migration/rebinding, wildcard packet-info, independent multipath interoperability,
HTTP/3, 0-RTT, full API/conformance coverage and performance work remain open.
The full goal remains active.

## 2026-09-20 outgoing path-status control

Added Conn.SetPathBackup to advertise backup/available receive preferences.
Local status sequences and preferences are separate from received preferences
used by the outgoing scheduler. Changes queue reliably with monotonically
increasing per-path sequence numbers; repeated preferences are idempotent.
Superseded status frames are retired from queue/recovery references. Queue
admission failure and sequence exhaustion do not change advertised state.

Checked draft-21 section 4.3:
https://www.ietf.org/archive/id/draft-ietf-quic-multipath-21.html#section-4.3
Core tests cover encrypted peer delivery, reordered old status rejection,
supersession, duplicate calls, queue failure, sequence exhaustion, and inactive
path rejection. Public v1/v2 tests exercise both roles. Root race tests,
focused public race tests, vet and default interop race tests pass (seven skips).

Next conformance audit should cover monotonically generated new path IDs:
the public API currently accepts caller-supplied unused IDs without enforcing
initiation order. Migration/rebinding, wildcard packet-info, independent
multipath interoperability, HTTP/3, 0-RTT and performance work remain open.
The full goal remains active.

## 2026-09-20 public multipath transfer under loss

Added public v1/v2 UDP integration tests transferring a 256 KiB stream with
both paths active, beyond the initial stream receive window. Separate UDP relays
measure each path independently: both must carry at least one quarter of the
payload size after validation, ruling out probe-only secondary-path activity.
The receiver must obtain the exact payload and FIN. A second scenario drops
every thirteenth client datagram on the secondary path and verifies recovery,
with an assertion that drops actually occurred.

Root race suite and vet pass; the final two-relay strengthening also passes
under race. Default interop race tests pass with seven existing skips.
This establishes local runtime delivery under this deterministic loss pattern,
not independent multipath interoperability or a throughput advantage.
Migration/rebinding, wildcard packet-info, HTTP/3, 0-RTT, broader conformance/API
coverage and performance work remain unfinished. The full goal remains active.

## 2026-09-20 application traffic on multiple paths

New stream/datagram frames now select validated paths in round-robin order,
preferring available paths over backups. Previously all shared application
output went to the lowest-numbered surviving path, leaving additional paths
primarily useful for failover. Control-frame affinity and existing per-path
congestion, pacing, amplification, packet-number and recovery accounting remain
in place. The enqueue cursor advances only after admission.

V1/v2 tests write a stream through the ordinary engine API, verify even frame
placement across two paths, transmit encrypted packets over both packet-number
spaces, and verify ordered reassembly and FIN. Eligibility tests cover an
unvalidated secondary, backup preference, and fallback after abandonment.
Root race tests, vet and default interop race tests pass (seven known skips).

This initial policy does not move queued data away from a congested/slow path,
measure aggregate throughput gains, or implement coupled congestion control.
Independent multipath interoperability, migration/rebinding, HTTP/3, 0-RTT,
broader API/conformance work and performance evaluation remain unfinished.
The full goal remains active.

## 2026-09-20 safe path-handle closure

Fixed Path.Close after primary abandonment: registered socket count alone
allowed removal of the only viable path because the abandoned primary socket
remained open. Path handles now carry their connection identity. The owner
validates identity/binding, requires a validated alternative through planned
abandonment, and retires the socket in the same serialized command. Rejected
close leaves the socket and connection usable. Internal cancellation and
network-failure socket removal retains its separate failure semantics.

V1/v2 public tests reject closing the sole surviving path and then transfer
stream data. A concurrent-close regression opens two alternatives, abandons
primary, closes both handles concurrently, checks exactly one succeeds, and
verifies delivery over the survivor. Root race suite, focused race regressions,
vet and default interop race suite pass; seven known interop skips remain.

The full objective remains active. Migration/rebinding, wildcard packet-info,
independent multipath interoperability, HTTP/3, 0-RTT, full compatibility and
performance improvements remain unfinished.

## 2026-09-20 public planned path abandonment

Added Conn.AbandonPath(ctx, id), including primary path zero. The actor checks
connection ownership and the engine requires a validated alternative before
committing PATH_ABANDON and transferring queued/outstanding reliable items.
Rejected requests (unknown/uninitiated paths, no validated alternative, full
send queue) leave the live connection unchanged. Repeated abandonment is
idempotent. Sockets stay open, preserving shared listener ownership and retained
in-flight packet routing.

Public v1/v2 failover tests now retire the primary path through this API rather
than the internal socket-removal hook. A server test abandons an additional path,
verifies that its primary remains usable, and establishes another connection
through the same listener socket. Core tests cover rejection atomicity,
evacuation without duplicate charging, and final-path protection.
Root race tests, vet, and default interop race tests pass (seven known skips).

Actual address migration/NAT rebinding, wildcard packet-info, independent
multipath interoperability, broader API parity, HTTP/3, 0-RTT and performance
work remain open. The full goal remains active.

## 2026-09-20 public experimental multipath

Added opt-in Config.EnableMultipath, concrete client Config.LocalAddr,
ConnectionState.SupportsMultipath, and Conn.OpenPath with Path handles.
The UDP owner automatically advertises CIDs for IDs 1–3 after mutual
confirmation; default connections retain their previous behavior. OpenPath
waits for CID exchange and path validation, enforces client-only initiation,
and cleans up its socket on cancellation or failure. Path.Close retires the
socket and abandons its path, with idempotent repeated removal. The last
registered socket must be terminated through connection shutdown instead.

Public v1/v2 tests cover negotiation, validation, duplicate/abandoned-ID
rejection, server-initiation rejection, cancellation before initiation and
after an observed probe, socket release verified by rebinding, and stream
delivery after primary-socket removal through the internal test hook.
Unilateral negotiation falls back; wildcard multipath binds are rejected.
Root race tests and vet pass; the final cancellation-test refinement also
passes under race. Default interop race tests pass with seven known skips.

This is experimental draft-21 support, bounded to four path IDs. Public
primary-path control, wildcard packet-info, migration/rebinding, retired
address reuse, and independent multipath interoperability remain unfinished.
HTTP/3, resumption/0-RTT, full compatibility/conformance and performance
improvements also remain open. The complete goal remains active.

## 2026-09-20 runtime-checked path initiation

Added a serialized runtime path-initiation method that validates connection
ownership and registered local sockets before the core can allocate state or
queue validation. Foreign connection fields are never read before owner-registry
validation, avoiding cross-actor access. Invalid local addresses return a local
request error without closing the connection or consuming the path ID.

The v1/v2 actor tests now use this method and verify unregistered-socket and
foreign-connection rejection, unchanged path state, and subsequent successful
initiation/failover. Root race tests, vet, and default interop race tests pass;
seven known interop skips remain.

Public configuration/path APIs, retired address reuse, wildcard packet-info,
and migration/rebinding remain unfinished. Public multipath advertisement stays
disabled; the complete goal remains active.

## 2026-09-20 explicit local socket removal

Added internal socket removal through the serialized command loop. It closes
the socket, retires its local address, and reuses path abandonment/evacuation
to preserve connections with a surviving path. Repeated removal is harmless;
unknown addresses and removal of the last registered socket are rejected (use
runtime Close for full shutdown). Late terminal reader events for an already
retired address no longer repeat failure handling.

The v1/v2 actor test now runs both unexpected reader failure and explicit
removal after dynamic registration/validation, then verifies stream delivery
over the surviving address pair. It also checks idempotence and last-socket
protection. Root race tests, vet, and default interop race tests pass; seven
known interop skips remain.

Retired address reuse, wildcard packet-info support, public configuration/path
APIs, and migration/rebinding remain unfinished. Public multipath advertisement
stays disabled; the full goal stays active.

## 2026-09-20 socket registration after handshake

Added internal dynamic socket registration through the actor command loop.
Registration validates a concrete local address, rejects duplicate addresses
and lifetime sets over 64, updates the endpoint registry, and starts a reader.
Socket-set mutation, shutdown, and last-reader completion now synchronize so
registration cannot race channel closure or socket disposal. Rejected additions
do not change socket ownership; retired addresses are not yet reusable.

The v1/v2 actor test now starts with one socket per endpoint, registers secondary
sockets after confirmation, validates the new path, and survives failure of the
original socket. It also checks duplicate rejection, canceled registration,
preserved ownership of a rejected socket, and shutdown of the dynamically added
socket/reader. Root race tests, vet, and default interop race tests pass; seven
known interop skips remain.

Explicit socket removal/address reuse, wildcard packet-info support, public
configuration/path APIs, and migration/rebinding remain unfinished. Public
multipath advertisement stays disabled; the full goal stays active.

## 2026-09-20 surviving a local UDP reader failure

A failed socket reader now retires that local address and stops every affected
multipath context before evacuating data. Surviving paths carry PATH_ABANDON
and the transferred logical items. Connections without a surviving path are
removed; closing connections whose cached close uses the failed socket are
disposed. Other sockets and their reader/owner work continue. Failure of the
last registered local socket still terminates the runtime.

The v1/v2 actor test now injects an actual primary-socket close and verifies
stream delivery through the secondary socket. Another regression covers two
paths sharing the failed socket, exactly-once evacuation onto a third path,
and connection/router cleanup after the last socket fails. Root race tests,
vet, and default interop race tests pass; seven known interop skips remain.

Dynamic socket registration/removal, wildcard packet-info support, public
configuration/path APIs, and migration/rebinding remain unfinished. Write
failures still use the existing per-connection removal policy. Public multipath
advertisement stays disabled; the full goal stays active.

## 2026-09-20 multiple local sockets in the serialized UDP actor

Added an internal fixed-socket-set constructor, bounded at 64 sockets. Each
reader tags input with its local address and feeds the same serialized owner;
output selects the socket matching the packet's local tuple. All sockets share
the endpoint's CID reservation space. Admission responses preserve the receiving
local address. Close shuts every socket and waits for all readers; any reader
failure currently terminates the runtime. Invalid/duplicate socket sets fail
before ownership transfer.

The v1/v2 actor test now uses two actual sockets per endpoint and establishes
path one on a distinct local/remote port pair. Retry, negotiation, validation,
primary abandonment, and stream delivery run through the concurrent readers
and serialized Do/Wait interface. Root race tests, vet, and default interop race
tests pass; seven known interop skips remain.

Dynamic socket registration/removal, individual socket-failure recovery,
wildcard packet-info support, public configuration/path APIs, and
migration/rebinding remain unfinished. Public advertisement stays disabled;
the full goal remains active.

## 2026-09-20 address-aware integration in the UDP actor

The existing serialized UDPServer/ServerEndpoint runtime now installs network
owners when its internal multipath capability is enabled. It routes incoming
packets through the address-aware router, activates at confirmation, reserves
CIDs before output, and sends to the selected remote address. Owners are removed
with their connections. Concrete local addresses are required; this one-socket
runtime rejects committed output selecting a different local socket.

An actor-level v1/v2 test performs Retry admission, negotiation, CID exchange,
path-one initiation/validation, primary-path abandonment, and stream delivery
through Do/Wait and the actual reader/owner goroutines. It uses two path IDs on
the same UDP tuple; the existing four-socket test separately covers distinct
address pairs through the engine owner and routers.

Root race tests, vet, and default interop race tests pass; seven known interop
skips remain. Runtime ownership of multiple local sockets, wildcard packet-info
support, public configuration/path APIs, and migration/rebinding remain
unfinished. Public multipath advertisement stays disabled; the full goal remains
active.

## 2026-09-20 routed output across multiple sockets

Added an owner output method that synchronizes CID reservations in every
supplied socket router before constructing a packet. Reservation failure returns
no bytes and consumes no packet number or AEAD operation. Earlier successful
router reservations remain safe if a later router rejects a collision; the
runtime can remove the connection or resolve the conflict before retrying.

The full v1/v2 UDP handshake/failover test now reserves routes before output
and demultiplexes every received datagram through its destination socket router.
A regression injects a collision in the second router and verifies no send
commit, no CID activation, and successful reservation after resolving it. Root
race tests, vet, and default interop race tests pass; seven known skips remain.

The runtime must supply all routers associated with the connection. Concurrent
socket ownership, public listener/dialer integration, and migration/rebinding
remain unfinished. Public advertisement stays disabled; the full goal stays
active.

## 2026-09-20 multipath CID reservation and routing

Router.Sync now reserves local CIDs across the attached path table, including
unsent advertisements for collision prevention and abandoned paths still in
retention. Collection removes those reservations. The single-path fast path
remains unchanged; multipath currently rebuilds the bounded reservation set.

Added RouteNetwork for address-aware owners. Known multipath CIDs select the
connection without treating their source address as validated; the bound engine
must still authenticate and enforce path/tuple rules. Legacy Route retains its
fixed-peer behavior. Otherwise-unroutable resets from secondary bound tuples
can reach their connection through a bounded fallback scan.

Tests cover pre-send reservation versus engine acceptance, retention/disposal,
atomic collision failure, separate legacy address policy, and secondary-tuple
reset routing. Root race tests, vet, default interop race tests, and focused
router race tests pass; seven known interop skips remain.

Public multi-socket runtime integration and migration/rebinding remain
unfinished. A tuple index can replace the reset fallback scan once the runtime
owns route updates. Public advertisement stays disabled; the full goal remains
active.

## 2026-09-20 address-aware owner and full UDP handshake

Added a serialized network-owner adapter with a supplied primary address pair,
uniform receive/output operations, automatic negotiated activation at handshake
confirmation, and single-path fallback. It switches to bound multipath packet
processing and prepares close output for typed receive failures. Socket
allocation, CID demultiplexing, and concurrent application calls remain the
socket runtime's responsibility.

v1/v2 tests exercise automatic handoff and unilateral fallback without waiting
for handshake traffic to quiesce. The four-socket loopback test now also carries
the entire TLS handshake through actual UDP, followed by automatic activation,
encrypted CID exchange, secondary-path validation, abandonment, and stream
failover. No manual handoff or in-memory handshake remains in that test.

Root race tests, vet, and default interop race tests pass; seven known interop
skips remain. Public listener/dialer integration, concurrent multi-socket runtime
ownership, and migration/rebinding remain unfinished. Public multipath
advertisement stays disabled; the full goal remains active.

## 2026-09-20 authenticated input before owner activation

With mutually negotiated multipath, the original receive interface now accepts
the extension's frame syntax and retains authenticated path-zero frames until
owner activation. HANDSHAKE_DONE and connection close remain immediate;
path-zero ACKs update recovery immediately so handshake-completion output can
finish. Other frames retain packet order and the authenticated destination CID
for replay through the multipath dispatcher. This also preserves pre-activation
CID retirement history by deferring those mutations until the table owns them.

Retention is bounded by 64 packets and a 64-KiB charge including frame metadata.
Capacity-dropped packets are not recorded for acknowledgment. Duplicate packets
are not retained twice, and activation/shutdown releases the retained queue.
The initial implementation stalled handshake completion by delaying ACKs; the
immediate path-zero ACK handling resolves that regression.

Tests cover a peer sending encrypted PATH_ACK/CID advertisements before the
other owner activates, duplicate suppression, post-activation application of
CID state, capacity behavior, TLS negotiation, and loopback UDP failover. Root
race tests, vet, and default interop race tests pass; seven known skips remain.

Public runtime address-aware dispatch/activation and migration/rebinding remain
unfinished. Public multipath advertisement stays disabled; the full goal remains
active.

## 2026-09-20 internal TLS negotiation and explicit activation

Added an internal owner-capability gate for multipath transport-parameter
advertisement and an explicit address-aware activation transaction after
confirmation. Both advertised parameter sets determine path limits; a present
zero negotiates support and unilateral advertisement falls back to the original
engine. Invalid binding leaves ownership unchanged, and duplicate activation is
rejected. The public UDP adapters cannot set this private capability gate and
continue to reject local multipath advertisement.

TLS v1/v2 tests cover mutual/present-zero/unilateral negotiation and activation
guards. The four-socket loopback failover test now uses actual negotiated TLS
parameters and activation rather than installing path limits directly. Updated
the existing advertisement-guard test for the more specific rejection message.
Root race tests, vet, and default interop race tests pass; seven known interop
skips remain.

The test owner waits for handshake traffic to quiesce before switching interfaces.
Confirmation-boundary handling (including early multipath frames), public UDP
runtime dispatch/activation, and migration/rebinding remain unfinished. This
internal opt-in is not public runtime multipath support. The full goal remains
active.

## 2026-09-20 real loopback UDP multipath failover test

Added a v1/v2 integration test using four actual loopback UDP sockets. It
exchanges encrypted CID advertisements over the primary socket pair, initiates
and validates a distinct secondary pair, exchanges primary-path abandonment,
and delivers stream data/FIN through the surviving pair. The test checks source
socket ownership, observed UDP source addresses, bidirectional validation, and
that no primary-path packets are emitted during subsequent stream delivery.

The TLS handshake and path-zero adoption remain explicit in-memory fixture
steps; subsequent multipath exchanges use actual UDP through the bound engine
entry points. This is evidence for that boundary, not public runtime multipath
support or interoperability with another multipath implementation.

Root race tests, vet, and default interop race tests pass; seven known interop
skips remain. Negotiated activation, migration/rebinding, and public UDP runtime
integration remain unfinished. Multipath advertisement stays disabled; the
complete project goal remains active.

## 2026-09-20 pending-probe continuity during path-zero handoff

Path-zero adoption now preserves an already pending application PTO probe and
its pacing deadline, in addition to the existing shared packet/recovery/CID
state. Previously, losing the probe allowance could strand the queued probe
behind a full congestion window. Duplicate handoff attempts now fail instead
of creating a second path table over already-owned connection state.

A regression expires the original recovery timer, fills the congestion window,
performs handoff, and verifies the probe sends with a fresh packet number and
rearms recovery. Root race tests, vet, and default interop race tests pass;
seven known interop skips remain.

The handoff still rejects retired legacy CID history that cannot be recovered.
Negotiated activation, migration/rebinding, and live UDP integration remain
unfinished. Multipath advertisement stays disabled; the full goal stays active.

## 2026-09-20 final-path abandonment close output

Receiving PATH_ABANDON for the last usable prepared path now validates the
frame, caches a NO_VIABLE_PATH connection close before peer-CID retirement,
and enters closing. The abandonment is then recorded and active peer CIDs are
cleared; subsequent output uses only the cached close ciphertext and retained
address/budget. No unsendable PATH_ABANDON response is claimed as queued.

Encrypted v1/v2 tests cover this transition through bound packet input/output,
CID retirement, abandonment flags, and decryption of the final close with the
expected error code and triggering frame. Root race tests, vet, and default
interop race tests pass; seven known interop skips remain.

Negotiated handoff, migration/rebinding, and live UDP integration remain
unfinished. The runtime still must deliver unroutable packets from known tuples
for stateless-reset detection. Multipath advertisement stays disabled; the full
goal remains active.

## 2026-09-20 unroutable multipath stateless-reset detection

The bound receive entry point now checks stateless-reset tokens after normal
processing fails to authenticate, including unknown CIDs and unparseable
headers. Detection requires an existing non-abandoned bound address pair and
still uses only used, active peer CID tokens. Closing input now records
successful authentication so a valid ciphertext suffix cannot trigger reset
detection. Reset recognition transitions normal or closing connections into
draining without accepting new path state.

Tests cover unroutable reset packets, mismatched addresses, normal/closing
states, and authenticated ciphertext ending in a configured token. Root race
tests, vet, and default interop race tests pass; seven known interop skips remain.

The future UDP router must deliver otherwise-unroutable packets from known
address pairs to this entry point. Final-path abandonment close output,
negotiated handoff, migration/rebinding, and live UDP integration remain
unfinished. Multipath advertisement stays disabled; the full goal stays active.

## 2026-09-20 bound multipath closing-state input

Added closing-state input for existing bound multipath contexts. It authenticates
using the receiving path's nonce/key state, credits only authenticated packets,
and recognizes peer CONNECTION_CLOSE without dispatching stream/control work or
accepting new paths. Authenticated traffic can request a rate-limited cached
close response; retransmission preserves ciphertext, packet number, and closing
deadline. Credit accrues on the arrival path, even if close output uses another.

Encrypted v1/v2 tests cover response rate limiting, forged packets, mismatched
addresses, identical cached retransmissions, stable packet numbers/deadlines,
and peer-close transition to draining with the original deadline. Root race
tests, vet, and default interop race tests pass; seven known interop skips remain.

Final-path abandonment close output, negotiated handoff, migration/rebinding,
stateless-reset routing for packets without recognizable destination CIDs, and
live UDP integration remain unfinished. Multipath advertisement stays disabled;
the complete project goal remains active.

## 2026-09-20 multipath connection-close packet output

Connection-close construction now selects a surviving multipath context and
seals with its CID, packet number, key phase, and path nonce. Closing retains
the selected address pair and amplification ledger, and the bound output entry
point can return cached close ciphertext despite the connection's terminal
application error. Entering closing clears every path's queued/recovery work
while retaining keys and routing. The existing largest-path-PTO deadline is
preserved.

Encrypted v1/v2 tests deliver a close on path one after path-zero abandonment,
verify the peer enters draining, and check packet-number/resource accounting.
Another test verifies amplification blocking and later emission without a new
AEAD operation or packet number. Root race tests, vet, and default interop race
tests pass; seven known interop skips remain.

Multipath closing-state input still needs authenticated response triggering and
peer-close recognition. Final-path abandonment close output also needs an
explicit policy before retiring its last peer CID. Negotiated handoff,
migration/rebinding, and live UDP integration remain unfinished. Multipath
advertisement stays disabled; the full goal remains active.

## 2026-09-20 application output after primary-path disposal

The shared application enqueue path now selects a surviving prepared multipath
context, preserving existing size/DATAGRAM accounting. New stream data and
connection controls no longer remain in the original queue after path zero is
abandoned or collected. Frame retirement for stream cancellation and superseded
flow-control updates now visits all application paths, accounting shared items
only once and removing their queued/sent references across paths.

Encrypted v1/v2 tests open and write a stream after primary-path collection and
deliver its FIN/data over path one. A cancellation regression covers a logical
stream item referenced by both paths and verifies single memory-charge release.
Root race tests, vet, and default interop race tests pass; seven known interop
skips remain.

Queue placement currently chooses the lowest usable prepared path ID; packet
validation, congestion and pacing gates still apply. Negotiated handoff,
migration/rebinding, final-path close output, and live UDP integration remain
unfinished. Multipath advertisement stays disabled; the full goal stays active.

## 2026-09-20 multipath shutdown resource release

Shutdown now releases secondary-path send queues, receive ranges, recovery
ledgers, validation probes, and key-phase/probe/pacing metadata along with the
original connection resources. Draining retains CID/address routing until its
deadline; immediate close and draining completion drop the multipath table.

Tests hold references to secondary-path state to verify immediate payload and
timer release, retained draining routes, exact deadline disposal, and final
connection cleanup. Updated the idle precedence test to inspect its saved path
table rather than dereferencing the now-released connection field. Root race
tests, vet, and default interop race tests pass; seven known interop skips remain.

Negotiated handoff, migration/rebinding, final-path close packet output, and
live UDP routing remain unfinished. Multipath transport-parameter advertisement
remains disabled, and the full project goal remains active.

## 2026-09-20 encrypted CID advertisement and path establishment

Added transactional internal CID advertisement: it stages CID issuance and
queue charges, then commits the lifetime registry and reliable send items
together. Missing send capacity cannot consume CID state. Newly queued CIDs
become routable only when their encrypted advertisements are emitted; repeated
calls do not requeue already prepared active CIDs.

A new v1/v2 integration test performs the base handshake, attaches path-zero
state, sends both peers' CID advertisements as encrypted packets over path
zero, and uses the received CIDs for client initiation, server acceptance, and
bidirectional path validation. It verifies unused CID-only state, pre-send
routing exclusion, and post-send routing. Additional tests cover full-queue
rollback and repeated advertisement calls. Root race tests, vet, and default
interop race tests pass; seven known interop skips remain.

Path-zero handoff is still explicitly installed by the test. Negotiated
handoff, migration/rebinding, final-path close output, and live UDP routing
remain unfinished. Multipath advertisement remains disabled in transport
parameters; the full project goal remains active.

## 2026-09-20 authenticated server path acceptance

The bound receive entry point now accepts a server's first authenticated packet
on an unused, CID-prepared path. It uses a tentative packet context for nonce
and key-phase processing, removes it when decoding fails, and binds addresses
only after authentication. Acceptance credits the received packet, dispatches
its frames, and starts server reachability/MTU validation with amplification
limits still enforced. Clients do not accept peer-initiated paths. Existing
path bindings still require their established address pair.

Encrypted v1/v2 tests now establish and validate both sides through client
initiation and automatic server acceptance, without preparing server packet
state or manually granting its credit. Tests also cover forged first packets,
PING-only initiation, replay without reinitialization, and client rejection of
peer initiation. Root race tests, vet, and default interop race tests pass;
seven known interop skips remain.

CID exchange and path-zero handoff are still installed explicitly by these
internal fixtures. Negotiated handoff, NAT rebinding/migration, final-path close
output, and live UDP routing remain unfinished. Multipath advertisement stays
disabled; the full goal remains active.

## 2026-09-20 client-side multipath initiation

Added internal client initiation for unused CID-prepared path IDs. It enforces
handshake confirmation, client role, both path limits, active peer CIDs, and an
actually advertised local CID. It binds the address pair, creates fresh path
packet/recovery state, applies the client's amplification exemption, and queues
validation. Application data still waits for reachability/MTU validation.
Failure before queuing a probe rolls back the allocation; a successfully started
attempt cannot reuse its path ID. Validation failure follows abandonment.

The initiation rules use
[multipath draft-21 section 3.1](https://www.ietf.org/archive/id/draft-ietf-quic-multipath-21.txt).
Tests cover rejection/rollback and encrypted v1/v2 validation exchanges against
an explicitly prepared server context. Root race tests, vet, and default interop
race tests pass; seven known interop skips remain.

Automatic authenticated server acceptance of an unused path is next. Negotiated
handoff, NAT rebinding/migration, final-path close output, and live UDP routing
remain unfinished. Multipath advertisement stays disabled; the full project
goal remains active.

## 2026-09-20 prepared-path network address binding

Added immutable local/remote address bindings to prepared application paths,
with IPv4-mapped address normalization and rejection of unspecified/multicast
addresses or zero ports. Binding grants neither reachability validation nor
amplification credit and cannot silently replace an existing tuple.

A bound receive entry point checks CID/path/address association before packet
decryption or state changes, credits authenticated packets to the receiving
path, and dispatches frames only once. A bound output entry point returns the
selected path's address pair with committed bytes and rejects missing bindings
before consuming packet numbers. UDP writers must use that source/destination
pair and terminate the connection if a committed write fails.

Tests cover v1/v2 authenticated packets, wrong source/destination addresses,
forged payloads, replay credit without duplicate frame dispatch, canonical
binding, prohibited rebinding, and unbound-send accounting. Root race tests,
vet, and default interop race tests pass; seven known interop skips remain.

This is an internal boundary for already prepared paths. Client initiation,
authenticated server acceptance of a new tuple, NAT rebinding/migration,
negotiated handoff, final-path close output, and live UDP routing remain
unfinished. Multipath advertisement stays disabled; the full goal stays active.

## 2026-09-20 connection timeout integration for multipath

RecoveryDeadline now combines attached multipath recovery/pacing, validation,
and retention deadlines with connection idle and key-retirement deadlines.
Legacy primary-path timer fields no longer override the attached path table.
HandleTimeout preserves connection closing/draining/idle precedence, then
services validation failure and retention before per-path loss/PTO. Timer
rearming also dispatches through the attached path table. Output still needs
to be drained through the internal multipath packet scheduler.

Tests cover v1/v2 timeout dispatch and probe rearming, read-only deadline
observation, exclusion of stale legacy pacing, simultaneous validation/PTO,
retention collection through HandleTimeout, and idle-timeout precedence.
Root race tests, vet, and default interop race tests pass; seven known interop
skips remain.

This integrates the existing socket-independent connection interface. Network
tuple binding/initiation, final-path close output, negotiated handoff, and live
UDP multipath packet routing remain unfinished. Multipath advertisement stays
disabled; the full project goal remains active.

## 2026-09-20 failed-path evacuation and retention servicing

Added internal validation/retention deadline observation and expiry servicing.
Validation failure queues PATH_ABANDON on a surviving prepared path and moves
queued/outstanding logical items there without charging their memory twice.
Received abandonment now also evacuates the stopped path immediately. Old sent
references remain available for late ACK completion. Path-affine probes and
obsolete CID advertisements are discarded; stopped recovery/pacing state is
cleared. Retention expiry collects old resources after transferring ownership,
while path-ID tombstones persist. A missing survivor yields NO_VIABLE_PATH.

Tests verify validation expiry, retained late-ACK completion, single transfer,
probe affinity, retention boundaries, and preserved queued data after disposal.
Encrypted v1/v2 tests deliver a queued stream through the surviving path after
abandonment. Root race tests, vet, and default interop race tests pass; seven
known interop skips remain.

The network owner still needs combined connection/path timeout dispatch, tuple
binding and initiation, final-path close output, and live UDP negotiation.
Survivor selection currently chooses the lowest usable prepared path ID; normal
validation and sending gates still apply. Multipath advertisement remains
disabled, and the complete project goal remains active.

## 2026-09-20 multipath loss and PTO timer servicing

Added internal per-path recovery deadline observation and timeout servicing.
Time-threshold loss applies congestion/persistent-congestion handling and queues
shared reliable items for retransmission. PTO selects queued or outstanding
ack-eliciting data, falling back to PING, without declaring packets lost. One
pending probe can bypass congestion/pacing gates but cannot bypass amplification
credit or recovery capacity. Actual transmission consumes its allowance and
rearms the timer. Repeated callbacks cannot duplicate a pending probe; a late
ACK completing its selected item triggers selection of another item or PING.
Abandoned paths do not run recovery timers. ACK dispatch now rearms using the
named path's amplification credit rather than assuming it is unrestricted.

Tests cover v1/v2 encrypted probes, congestion-window bypass, retained flight
and window on PTO, backoff/rearm, repeated callbacks, amplification suppression,
abandonment, late ACKs, and time-threshold loss with shared retransmission
identity. Root race tests, vet, and default interop race tests pass; seven known
interop skips remain.

The recovery policy follows the per-path recovery model in
[multipath draft-21 section 5.7](https://www.ietf.org/archive/id/draft-ietf-quic-multipath-21.txt).
These internal helpers still need the multipath network owner: combined
validation/retention/connection deadlines, failed-path abandonment and data
redistribution, tuple binding, and live UDP negotiation are unfinished.
Multipath advertisement remains disabled; the full goal remains active.

## 2026-09-20 path scheduling and cross-path ACKs

Added internal packet selection across prepared paths. Pending ACKs prefer their
source path but can use another path with amplification credit, including when
the source has been abandoned and its receive state is retained. The ACK names
the source path while encryption, packet numbers, and send accounting belong to
the sending path. Blocked attempts preserve pending ACKs and packet numbers.
Other queued output uses round robin among available paths before backup paths.

Encrypted v1/v2 tests verify that a surviving path carries an abandoned path's
ACK and updates the original recovery ledger. Additional tests cover blocked
credit, ACK fallback, round robin, and backup preference. Root race tests, vet,
and default interop race tests pass; seven known interop skips remain.

This scheduler does not redistribute queued data between paths. Timer-driven
probes/retransmissions, tuple binding, network initiation, and live UDP multipath
negotiation remain unfinished. Multipath advertisement stays disabled; the full
project goal remains active.

## 2026-09-20 per-path 1-RTT packet construction

Added nextPacketOnPath with path-specific CID/packet number/nonce selection,
shared key-phase and confidentiality accounting, bounded serialization, and
per-path amplification, congestion, recovery-capacity, and pacing gates. It
produces PATH_ACK, commits sent-probe history, marks advertised CIDs sent only
when sealed output is returned, and distinguishes reliable data from DATAGRAM
and challenge/response frames. Abandoned paths cannot send. Additional-path
STREAM/DATAGRAM traffic waits for validation; probes are prioritized ahead of
blocked application data.

Two independent engine instances now validate secondary paths by exchanging
actual encrypted challenge/response packets in v1/v2 tests, without synthetic
probe commits. Further tests cover zero amplification credit, padded probes,
packet/counter accounting, CID advertisement commits, congestion/pacing blocks,
and validation-before-data. Root race/vet and default interop race pass; seven
known interop skips remain.

This remains a socket-independent internal sender. The owner must transmit each
returned packet on the bound tuple or terminate on write failure. Automatic path
selection, cross-path ACK scheduling, timer-driven probes/retransmissions,
network tuple binding/initiation, and live UDP negotiation are unfinished.
Multipath advertisement remains disabled and the full goal stays active.

## 2026-09-20 per-path challenge/response validation

Added validation and amplification state to each application path, sharing the
existing primary-path state during handoff. Established contexts can queue
random challenges, record only actually sent probes, retry within a bounded
three-probe window, and match responses in the receiving path's state. A small
probe validates reachability but queues another probe for the 1200-byte minimum;
only a matched expanded probe marks full path validation. Expiry wins at the
exact deadline. Probe history is bounded even under repeated send callbacks.

Encrypted PATH_CHALLENGE dispatch queues its response strictly on the receiving
path, without fallback. PATH_RESPONSE dispatch applies only to that path's sent
challenge history. Tests cover unsent/wrong-path tokens, valid padded responses,
small probes, expiry, retries, strict response affinity, and the fact that an
incoming challenge does not validate its sender. Root race/vet and default
interop race pass; seven known interop skips remain.

The tests use explicit send commits for probes. The outgoing scheduler must
perform those commits after packet construction, enforce per-path amplification,
and service validation timers. Network tuple binding, client-only initiation,
address changes, failed-new-path abandonment, and UDP integration remain next.
Multipath advertisement stays disabled and the full goal remains active.

## 2026-09-20 encrypted abandonment and multipath reset detection

The established-path dispatcher now processes PATH_ABANDON. It retires peer
CIDs immediately, calculates retention from the named path's PTO, and queues a
single NO_ERROR response on a surviving prepared path before recording local
abandonment. Duplicate frames neither queue extra responses nor extend the
deadline. No available response path produces NO_VIABLE_PATH for the future
connection-close integration; it does not falsely record a sent response.

Stateless-reset detection now checks used, active peer CIDs across attached
paths, excludes abandoned paths, and never consults retired uniqueness history.
Tests exercise encrypted abandonment arriving on the path being closed,
response routing, duplicate handling, delayed packet decoding/ACK pending state,
CID disposal, final-path failure, and unused/retired/abandoned reset tokens.
Root race/vet and default interop race pass; seven known interop skips remain.

The live driver is still not attached to this dispatcher. It must schedule
PATH_ACK for retained abandoned paths on surviving paths, send/retransmit
responses, perform final-path close output, and collect expired path resources.
Challenge/response network validation and the outgoing scheduler remain next.
Multipath advertisement stays disabled and the complete goal remains active.

## 2026-09-20 encrypted multipath CID/status/limit dispatch

The established-path dispatcher now applies base and multipath CID issuance/
retirement, path status, MAX_PATH_ID, PATHS_BLOCKED, and PATH_CIDS_BLOCKED.
Retirement checks use the actual authenticated packet destination CID.
Retirement and replacement-CID responses enter bounded reliable queues on a
prepared path with a usable peer CID, preferring the arrival path and otherwise
choosing the lowest usable ID. Final send permission remains the scheduler's
responsibility. Path-zero peer CID changes update the legacy send destination.

Blocked frames validate path/sequence limits without automatically allocating
state or granting capacity. Tests deliver encrypted controls, verify per-path
retirement/replacement, duplicate handling, unsent-CID routing exclusion,
reordered status/limit updates, informational behavior, protocol error frame
types, response fallback, and queue bounds. Root race/vet and default interop
race checks pass; seven known interop skips remain.

The dispatcher still needs PATH_ABANDON and challenge/response integration.
Outgoing path-specific packet construction must mark queued CIDs sent, enforce
amplification/congestion/pacing, and route to the validated network address.
Negotiation/UDP integration stays disabled until those pieces are ready. The
complete project goal remains active.

## 2026-09-20 encrypted multipath data/ACK dispatch

Added handleMultipathPacket for established internal path contexts. Decoded
STREAM/flow-control/cancellation frames use shared connection stream state;
DATAGRAM delivery uses the existing bounded queue. ACK/PATH_ACK validates and
updates the named path's recovery, completes shared reliable items once, updates
key-phase acknowledgment, and retains lost data for the outgoing scheduler.
Connection-close and post-handshake TLS processing reuse existing handlers.

Tests pass encrypted packets through CID lookup, decryption, replay tracking,
and dispatch after v1/v2 handshakes. Stream fragments on two paths (with equal
packet numbers) reassemble despite reordering; replayed DATAGRAMs deliver once.
An ACK arriving on another path completes the correct transmission and releases
bytes once. Cross-path traffic cannot bypass connection flow control and reports
FLOW_CONTROL_ERROR. Root race/vet and default interop race pass; seven known
interop skips remain.

The handler is not yet invoked by the UDP engine. Path management frames,
challenge/response routing, initiation/address validation, amplification-aware
timers, and the outgoing scheduler still need integration. Until then it is
limited to pre-established test contexts and rejects unsupported control frames.
Multipath advertisement stays disabled and the full goal remains active.

## 2026-09-20 path-aware authenticated packet decoder

Added decodeMultipathPacket joining sent/local CID lookup, path-specific nonce
and key-phase processing, shared integrity accounting, complete frame syntax/
context validation, and per-path ACK/replay tracking. Unknown or unsent CIDs and
paths without application state are dropped without allocation. Equal packet
numbers on different paths are independent. Duplicate input returns no frames
to apply, but ack-eliciting duplicates can schedule replacement acknowledgments.

The live single-path receiver now shares frame parsing and authentication-error
handling with the new decoder. Failed key trials count once per packet across
all paths/generations. Malformed authenticated payloads cannot commit receive
packet-number state. Successful decoding updates shared authenticated activity.

Tests cover v1/v2, path-zero/secondary packet-number reuse, wrong-path nonces,
replays, unknown/unsent CIDs, unactivated paths, every truncated packet prefix,
invalid/empty payloads, and shared AEAD integrity exhaustion. Root race/vet and
default interop race checks pass; seven known interop skips remain.

This decoder is not yet called by the UDP engine. It requires an established
path context and returns validated frames for the future path-aware dispatcher.
New-path initiation must authenticate/validate addresses, account amplification,
and establish context; frame application, response routing and scheduling still
need integration. Multipath remains disabled and the complete goal stays active.

## 2026-09-20 multipath key-update spacing

Added InitiateKeyUpdateAt for owner-supplied timestamps. With attached
multipath state, subsequent local updates wait until three times the largest
path PTO after the first valid acknowledgment of the current sending keys.
PATH_ACK selects its own path's first-sent threshold. Duplicate acknowledgments
do not move the deadline, old-phase acknowledgments do not start it, and each
new key generation clears the prior acknowledgment/deadline. Peer-initiated
updates are still checked by the protocol acknowledgment rules, not rejected
solely for failing this local timing recommendation.

Automatic updates at the confidentiality limit now use NextDatagram's supplied
time. The existing no-argument helper uses wall-clock time; deterministic callers
can use the explicit-time method. Early or zero-time requests do not change
keys, queues, or counters. Encryption never proceeds beyond its usage limit if
an update cannot be performed.

Tests run v1/v2 handshakes and real engine ACK processing with a slower attached
path, check duplicate ACKs, exact deadline boundaries, failure atomicity, and
PATH_ACK thresholds. Root race/vet and default interop race checks pass with
seven known skips unchanged. Live multipath packet routing, initiation/network
validation, scheduling, and final integration remain unfinished. Advertisement
stays disabled and the full goal remains active.

## 2026-09-20 per-path key-phase packet observations

Separated key-phase packet-number observations from shared key state. Each
allocated additional path tracks its own first/current/previous packet numbers
and write-phase threshold. Existing path-zero receive processing uses the same
refactored code. Key derivation, phase changes, acknowledgment gates, expiry,
and AEAD usage counters remain connection-wide. Path-aware opening applies the
path nonce and rejects unknown paths without allocating state.

Current/previous-key packet boundaries now use the receiving path, including a
path that has not yet received the current generation. All path observations
advance on a global key update. PATH_ACK can confirm reception/sending of the
new phase using the acknowledged path's thresholds. Late old-key packets also
advance that path's previous-generation high-water mark; this closes an overlap
validation gap in the former single-path logic.

Tests cover v1/v2, differently sized packet-number spaces, repeated updates,
local and peer-initiated updates, delayed previous-key packets, overlapping
phase rejection, unknown paths, and path-specific ACK thresholds. Root race/vet,
focused transport race/vet after ACK-helper integration, and default interop
race checks pass; seven known skips remain.

Multipath receive routing and reliable ACK handlers still need to call these
path-aware entry points. Update spacing after acknowledgment, path initiation,
network validation, and scheduling remain unfinished. Advertisement stays
disabled and the full project goal remains active.

## 2026-09-20 connection-wide multipath retention PTO

Added retentionPTO selection across attached application paths and routed the
existing old-key, closing, and draining retention calculations through it.
CID-only paths have no RTT contribution; abandoned paths still in retention
contribute until disposed. The calculation excludes exponential probe backoff
and saturates the three-PTO duration instead of overflowing. Single-path
behavior remains covered by the existing key-update and closure tests.

Tests attach prepared path state to completed v1/v2 connections, add a slower
secondary path, verify key/closing/draining deadlines, and check abandonment,
disposal, and duration saturation. Full root race/vet and default interop race
checks pass; seven known interop skips remain.

The Conn multipath pointer is not yet populated by negotiation. Current key
phase packet-number comparisons still assume one path and must move to per-path
observations while keeping key derivation and AEAD usage limits shared. The
three-largest-PTO spacing after update acknowledgment also remains to be wired
into local key-update policy. Live multipath stays disabled and the full goal
remains active.

## 2026-09-20 shared reliable transmission bookkeeping

The live single-path engine now uses common ACK-completion and loss-queue
helpers. Multipath recovery can validate an ACK on its named path, complete the
shared logical frame item once, and prune transmission references across paths.
Cross-path loss requeue keeps the original packet mapping for late ACKs and
queues the item at most once. Reliable-data completion does not change another
path's packet flight accounting, which remains owned by packet recovery.

Abandonment cleanup now returns deduplicated unacknowledged reliable items as
well as outstanding packet loss. This fixes loss of queued or previously-lost
data absent from the recovery ledger. Detachment clears the disposed path's
references and preserves queue ownership for items already queued elsewhere.
The future scheduler must select which returned items remain retransmittable;
path-specific control frames need their own lifecycle handling.

Tests cover ACK of either path's copy, duplicate ACKs, late ACK after declared
loss, queued-copy cancellation, invalid ACKs, completion errors, and abandonment
of sent/queued/previously-lost items. Root race/vet and default interop race pass;
seven known interop skips remain. Live multipath scheduling and packet processing
are still unfinished, advertisement remains disabled, and the full goal active.

## 2026-09-20 original-path state handoff

Added adoptPathZero to build multipath state around a confirmed connection's
existing CID managers, application number space, recovery ledger, congestion
controller, timer, and pacer. Application state now references numberSpace,
including queued/reliable frames and sent-number bookkeeping. Handoff seeds
peer CID history with all active sequence/token bindings and retains the local
lifetime uniqueness registry. Additional paths still start with fresh state.

Handoff rejects unconfirmed/closed connections and legacy peer CID retirement:
the old manager lacks retired binding history, so eventual negotiation must
perform this handoff before rotation. This is an explicit internal constraint,
not a complete negotiation implementation. The owner must transfer operations
to the table; it cannot continue independently mutating legacy CID state.

Tests complete actual client/server v1/v2 TLS handshakes, leave outstanding and
queued application transmissions, and verify packet numbers, receive ACK state,
RTT/flight/window, timers, pacing ownership, reliable-frame maps, and CID tokens
survive handoff. Subsequent path-zero sends/ACKs use the original ledger; new
paths start at zero independently. Full root race/vet and default interop race
checks pass, with seven known interop skips unchanged.

Next: integrate path-aware packet processing and reliable transmission tracking,
then initiation/address validation, scheduling and shared key-update policy.
The handoff helper is not yet called by the live engine. Multipath advertisement
remains disabled and the complete goal remains active.

## 2026-09-20 per-path application recovery primitives

Added lazy per-path application state for sent/received packet numbers, ACK
ranges, RTT, loss recovery, NewReno, pacing storage, and PTO timers. CID/status
advertisement alone does not allocate it. ACK processing selects the explicit
PATH_ACK path or path zero for base ACK, independently of the carrying path.
Errors preserve the offending ACK frame type. Initial/Handshake and shared
TLS/AEAD state are not moved into these additional-path instances.

Abandonment collection now drains retained outstanding transmissions as loss
and cancels their timer, returning path ID plus lost packets to the future frame
recovery integration. Late ACKs during retention remove acknowledged packets;
ACKs after disposal are ignored. Recovery abandonment preserves packet-number
continuity and leaves other number spaces untouched.

Tests exercise equal packet numbers across paths, isolated ACK/loss/RTT/window
updates, receive replay tracking, independent PTO deadlines/backoff, invalid
ACK/send atomicity, and late ACK/disposal accounting. Full root race/vet and
default interop race checks pass; seven known interop skips remain.

These primitives are still outside live packet processing. Path-zero adoption
must preserve the existing handshake recovery history, additional-path initiation
must enforce role/address validation, and reliable frames need (path, packet)
transmission mapping. Timer expiry, pacing, scheduling, and connection-wide key
update/AEAD policy still need integration. Live multipath remains disabled;
the full implementation and validation goal remains active.

## 2026-09-20 multipath abandonment and status state

Added CID-table abandonment transitions. Local or received abandonment clears
active peer CIDs/reset tokens immediately. Local CIDs remain routable until
both directions have abandoned and three path PTOs have elapsed from the first
received abandonment. Duplicate frames cannot extend the deadline. Collection
returns disposed path IDs so the future engine can declare outstanding packets
lost and release recovery state; this table does not yet own those resources.

Bounded lifetime tombstones prevent path-ID reuse after CID slot reclamation.
Late CID/status frames for abandoned paths are ignored. Path zero can be
collected without breaking later CID issuance or the shared uniqueness history.
Status updates retain independent per-path sequence numbers, including valid
sequence zero, and ignore equal/older updates across available/backup variants.
Receiving status for an unused path creates no CIDs or network traffic.

Tests cover both abandonment orders, exact retention boundaries, duplicate
frames, immediate token retirement, path-zero disposal, slot reuse with a new
ID, forbidden ID reuse, resource bounds, invalid timers, and reordered status.
Root race/vet and default interop race checks pass with seven known interop skips.

Live integration remains unfinished: the owner must reliably queue abandonment
responses/retransmissions, decide how to handle loss of the final usable path,
and retain per-path packet-number/recovery state for the same period. Next:
per-path recovery and packet-number state, then packet processing and network
validation. Multipath advertisement remains disabled and the full goal active.

## 2026-09-20 multipath CID table

Added a serialized pathCIDs table combining per-path CID managers. Base CID
frames map to path zero; multipath frames select their explicit path. The table
checks local receive limits, both issuance limits, and connection-wide peer CID
bindings, including retired history. CID and reset-token changes for an existing
binding are rejected. Local lookup resolves only sent, active CIDs to their path.
MAX_PATH_ID checks preserve the initial peer floor and tolerate reordered limits.

The table caps retained path entries at a configured value up to 64 and peer CID
history at 4096; large wire IDs never drive indexed allocations. Capacity errors
are local resource signals for the future engine integration. Invalid frames do
not commit a new path slot or change CID history. Tests exercise reordered limits,
retired CID reuse across paths, base/path-zero equivalence, scoped retirement,
routing visibility, history/active limits, and failure atomicity. Root race/vet
and default interop race checks pass; seven known interop skips remain. A bounded
fuzz campaign checks table limits and cross-path binding consistency.

This table is not yet connected to live packet processing. Abandonment retention
and slot reclamation, status handling, per-path packet numbers/recovery, and
network validation still need implementation before advertisement can be enabled.
The complete project goal remains active.

## 2026-09-20 per-path CID manager preparation

Refactored the existing CID managers to support additional paths starting
without a handshake CID. Local issuance starts at sequence zero on each new
path; initial-path issuance still starts at one. Local paths share a lifetime
CID uniqueness registry and its 4096-entry bound. Retired CIDs remain in that
registry. Peer CID reception supports an empty initial set and reordered first
frames, preserving independent active limits and retirement floors. Multipath
errors retain PATH_NEW_CONNECTION_ID/PATH_RETIRE_CONNECTION_ID frame types.

Tests cover sequence-zero issuance/reception, duplicates, conflicting data,
limits, reordered delivery, independent rotation, unsent/unissued retirement,
packet-destination retirement, ownership, and the shared history bound. Root
race/vet and default interop race checks pass; seven known interop skips remain.
A bounded two-path CID fuzz test checks floors, active limits, current CID
selection, and isolation.

This prepares reusable per-path state; it does not enable live multipath.
The future path table must select the matching path, enforce negotiated limits,
check peer CID uniqueness across paths, and retain abandonment tombstones.
Additional-path routing, validation, recovery, and scheduling remain unfinished.
The complete goal remains active.

## 2026-09-20 multipath transport parameter

Added draft-21 initial_max_path_id (0x3e), preserving absent versus explicit
zero and enforcing the maximum 2^32-1 with TRANSPORT_PARAMETER_ERROR. Parsing
owns its value, accepts legal nonminimal varints, and rejects malformed lengths
and duplicates. This parameter must not be remembered for later connections;
session resumption remains disabled.

The engine rejects local advertisement until live multipath can be honored.
Peer advertisement is accepted with nonzero handshake CIDs without enabling
multipath; zero-length peer or local CIDs produce PROTOCOL_VIOLATION. All
currently supported cipher suites have the required 12-byte nonce. Tests cover
both roles, boundary values, exact zero encoding, and invalid inputs. TLS-backed
handshakes verify unilateral advertisement still completes base QUIC for both
roles and versions. The TLS-only test advertiser does not enable multipath.

Full root race/vet and default interop race checks pass with the seven known
interop skips unchanged. A bounded transport-parameter fuzz campaign checks
input ownership and round trips. Next: bounded path/CID lifecycle state,
per-path packet-number/recovery state, and live negotiation integration. The
full goal remains active, and multipath is not yet usable over the network.

## 2026-09-20 multipath frame codecs

Added all ten frame types from draft-ietf-quic-multipath-21 section 4:
PATH_ACK (with/without ECN), PATH_ABANDON, both PATH_STATUS variants,
PATH_NEW_CONNECTION_ID, PATH_RETIRE_CONNECTION_ID, MAX_PATH_ID,
PATHS_BLOCKED, and PATH_CIDS_BLOCKED. ACK ranges and CID bodies share the
existing validated codec logic. PATH_ACK does not elicit acknowledgments or
count toward bytes in flight on its own. Frame-context validation requires
both negotiated multipath and 1-RTT; the current engine never enables that flag.

Exact-byte fixtures cover every type. Tests check every truncated prefix,
following-frame boundaries, nonminimal type encodings, malformed ACK/CID
bodies, rejection without negotiation or in other packet types, and unchanged
output buffers on serialization errors. Path IDs remain full wire varints so
future path-limit checks can return PROTOCOL_VIOLATION rather than misclassify
valid syntax as FRAME_ENCODING_ERROR. Full root race/vet and default interop
race checks pass; the seven documented interop skips remain unchanged.

A bounded frame fuzz campaign also checks input immutability, error consumption,
and semantic round trips. This does not establish sustained fuzz coverage.
Next: initial_max_path_id parameter handling and negotiation safeguards, then
path-bound CID/lifecycle and recovery state. Live multipath remains unfinished;
the full implementation and performance goal remains active.

## 2026-09-20 multipath packet protection primitive

Added explicit SealPath/OpenPath methods for draft-ietf-quic-multipath-21
section 2.4. The 32-bit path ID and 62-bit packet number are combined into the
96-bit AEAD nonce input; header protection remains shared. Existing Seal/Open
retain path-zero behavior. Multipath methods reject long headers and retain
packet-number bounds and authenticated reserved-bit validation.

Tests match the draft's published nonce example and exercise all three TLS
suites, both QUIC versions, three key generations, path/packet-number boundary
values, path-zero compatibility, input ownership, and wrong-path authentication
failure. Full root race/vet and default interop race checks pass, with the seven
previously documented interop skips unchanged.

This is an internal primitive, not negotiated multipath support. Callers remain
responsible for nonce uniqueness, never reusing path IDs, per-path packet-number
spaces, and connection-wide key updates/AEAD limits. No live engine calls these
methods yet. Next: multipath transport parameters/frame codecs and path-bound
CID state, followed by recovery, scheduling, migration, and independent-peer
validation. The complete project goal remains active.

## 2026-09-20 bounded frame serialization scratch

NextDatagram now appends frame payloads into a per-connection 1200-byte scratch
array. Sealed packets and retransmission records continue to own independent
data, and failure/disposal clears scratch. This removes repeated frame encoding
allocations, including on attempts blocked by congestion or pacing, at the cost
of 1200 bytes of connection storage.

The same short stream-echo comparison recorded a 16.43 ms median and 7.98 MB
allocated per exchange, versus 19.23 ms and 9.32 MB with receive pooling alone.
The reference recorded 5.51 ms and 0.75 MB, so a substantial gap remains.
A fresh allocation profile no longer lists STREAM serialization among the
leading sampled sites. See [raw data, profiles, and limitations](benchmarks/stream-echo.md).

Ownership tests overwrite scratch after multiple packets are serialized and
verify protected output, retained reliable payloads, and close-time clearing for
both versions. Full root race/vet, default interop race, and race-instrumented
benchmark payload checks pass. Seven known interop skips remain. These measured
allocation improvements do not establish overall performance parity or complete
transport behavior; the full migration/multipath/API/HTTP3/conformance goal remains.

## 2026-09-20 public throughput baseline and UDP buffer reuse

Added a reproducible public-server stream-echo benchmark comparing independent
and upstream servers against the same upstream client, with matched receive
windows and incoming-stream limits. Each operation verifies a concurrent 1 MiB
request and identical response. Recorded three samples of three operations per
variant, plus a separate allocation profile. The baseline exposed a 64 KiB
allocation per UDP receive; runtime buffers now transfer through the bounded
reader queue and return to a sync.Pool after synchronous engine processing.

Short-run median allocated bytes fell from 82.9 MB to 9.3 MB per exchange and time
from 21.2 ms to 19.2 ms. The reference remains substantially better: 5.62 ms and
0.77 MB in the pooled comparison run. This is evidence of an improvement in one
workload and substantial remaining work, not of overall performance parity.
Full root race/vet and default interop race tests pass; benchmark payload checks
also pass under race instrumentation. Seven known interoperability skips remain.

See [method, limitations, raw data, and profile](benchmarks/stream-echo.md).
The next allocation candidates are payload retention/serialization and packet
protection buffers. Migration, multipath, HTTP/3, compatible version switching,
shared transports and broader conformance/performance coverage remain required.

## 2026-09-20 transactional stream allocation

Stream opening and acceptance now inspect the next ID, allocate storage, and
only then consume the counter. Previously, a getStream allocation failure could
advance the opening/acceptance counter even though no handle was returned.
Repeated failed operations now preserve the same candidate ID. Blocking public
opening/acceptance treats local object capacity as a wait condition, while
nonblocking OpenStream/OpenUniStream still report the capacity error.

Fault-injection tests fill the engine's object map and verify repeated failed
opens/implicit accepts do not consume IDs in either direction. A real upstream
peer permits enough streams to fill the 128-local-object limit; the public test
verifies a blocked open can time out, then resumes at the expected next ID after
another stream's FIN is acknowledged and its object reclaimed. Root race/vet and
default interop race suites pass with the same seven documented skips.

Migration, multipath, HTTP/3, compatible version switching, shared transports,
and performance/comprehensive conformance verification remain open goal work.

## 2026-09-20 receive-side cancellation errors

Local CancelRead and peer RESET_STREAM now produce the same public StreamError
shape as send-side cancellation, including stream ID, application code, and
local/remote origin. Engine reads and automatic-release callbacks retain terminal
results. CancelRead no longer overrides a prior peer reset or observed EOF; later
peer resets still undergo protocol validation without replacing an already
reported application result.

Both-version engine tests control EOF/reset/local-cancel ordering, and public
UDP tests wake blocked reads and verify errors after engine storage reclamation.
Upstream public-client tests verify reset codes and origin before and after
cleanup in both versions. Root race/vet and default interop race suites pass;
seven existing documented skips remain. This completes the basic stream
cancellation error shape, not full API parity or broader RFC verification.

## 2026-09-20 send-side stream contexts

Added SendStream.Context (also promoted on bidirectional Stream), canceled when
FIN is queued, local CancelWrite succeeds, peer STOP_SENDING is processed, or the
connection terminates. Normal FIN uses context.Canceled; send cancellation now
has a public StreamError carrying stream ID, error code, and local/remote origin.
Writes retain this typed cancellation after engine storage has been reclaimed.
Contexts inherit connection termination, and terminal engine callbacks are
removed immediately; no per-stream polling goroutine or registry is added.

Root tests verify both versions, read-side independence after Close, local and
remote cancellation, preservation of the first cancellation code, and connection
shutdown causes. Engine tests distinguish application-side close notification
from ACK-based delivery/reclamation and cover late watch registration. Upstream
public-client tests verify STOP_SENDING cancels our send context in both versions.
Root race/vet and default interop race checks pass; seven known skips remain.
Receive-side error mapping and the rest of upstream API parity are still open,
alongside migration, multipath, HTTP/3 and performance verification.

Semantics reference: [upstream SendStream.Context](https://pkg.go.dev/github.com/quic-go/quic-go#SendStream.Context).

## 2026-09-20 fallback and downgrade checks

DialAddr now selects an offered configured version after a valid VN response and
makes at most one fresh connection attempt. Default preference is v1 then v2;
a single configured version still forbids fallback. The original attempt is
fully disposed before retry, with fresh TLS state, CIDs, and a dedicated socket.
The engine owns a copy of the original version and selection preference, ignores
all VN on the second attempt, requires server metadata (with the legacy v1
exception), rejects empty available lists, and replays the selection policy
against authenticated server versions plus the negotiated version.

Tests cover missing/empty metadata, legacy v1, a version being withdrawn from
service, changed preference, and repeated VN. Both public fallback directions
pass between our endpoints. A real UDP proxy injects forged VN against a server
supporting both versions; the client rejects the downgrade with transport error
0x11 after reading contradictory handshake metadata.

Upstream fallback to v1 passes. Fallback to upstream v2 reproduced a new failure:
pinned v0.62.0 omits version_information, so our mandatory second-attempt check
rejects it. That case is explicitly skipped by default, reproducible with
QUIC_INTEROP_VN_FAILURES=1. There are now seven documented default skips.
Root race/vet and default interop race checks pass. Compatible switching,
deployment-specific offered/fully-deployed version policy, address fallback,
and the broader transport/multipath/performance work remain open.

Reference: [RFC 9368 sections 4 and 8](https://www.rfc-editor.org/rfc/rfc9368.html#section-4).

## 2026-09-20 authenticated version information

Added RFC 9368 version_information (0x11) transport parameter encoding and owning
parsing. Validation rejects malformed lengths, zero versions, and a client list
that omits its chosen version; server lists may be empty or omit that version.
Duplicate parameter detection remains shared with the transport parameter parser.
New connections advertise their chosen version. Clients currently offer only
that compatible first flight; listeners advertise the versions enabled for their
deployment. Configurations cannot claim client switching the engine cannot perform.

Handshake processing checks the peer's chosen version against the packet version
and closes with VERSION_NEGOTIATION_ERROR (0x11) on mismatch. Missing metadata is
accepted for the original connection attempt. Tests cover role distinctions,
malformed and duplicate encodings, owned data, both-version exchanges, and an
injected inconsistent server TLS parameter causing an actual client close.
Root race/vet and default interop race suites pass, keeping the six known skips.
Transport parameter fuzzing passes too.

This implements metadata and current-version binding, not complete RFC 9368.
Fallback must still retain/check the original attempt and VN history, enforce
missing-metadata rules (including the legacy v1 exception), and reject downgrade
attacks. Compatible in-connection switching also remains open. Reference:
[RFC 9368 sections 3–4](https://www.rfc-editor.org/rfc/rfc9368.html#section-3).

## 2026-09-20 Version Negotiation response and validation

Added version-independent long-header parsing and Version Negotiation encoding
with up to 255-byte connection IDs. Unsupported-version datagrams of at least
1200 bytes receive a stateless response with swapped CIDs, configured versions,
and a reserved version. Responses are smaller than their triggers; version-zero
packets never cause replies. Existing CID routes are respected even for unknown
versions, including wrong-address discard, before stateless admission.

Clients now reject valid Version Negotiation with a public typed error, requiring
both original CIDs, an already-sent Initial, no prior authenticated packet/Retry,
and omission of the selected version from the offered list. Rejection silently
disposes the attempt. The error records an unauthenticated assertion, not trusted
server capabilities. Automatic fallback and RFC 9368 downgrade protection remain
unfinished; the dialer still selects only one version per attempt.

Tests cover CID bounds/truncation, configured version lists, undersized triggers,
reply-loop suppression, wrong CIDs, current-version inclusion, pre-send and late
negotiation, and route preservation. Public mismatched-version dials return the
typed error against our listener and upstream. Root race/vet and default interop
race checks pass with the same six existing skips. Header fuzzing also passes.

References: [RFC 9000 section 6](https://www.rfc-editor.org/rfc/rfc9000.html#section-6)
and [section 17.2.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-17.2.1).
Further VN work must implement authenticated version information and fallback;
this milestone does not claim complete version negotiation or RFC conformance.

## 2026-09-20 public client dialing

Added DialAddr with context-bound DNS and handshake, TLS configuration cloning
and inferred ServerName, random local/original connection IDs, v1/v2 selection,
and the existing concurrent stream/datagram wrappers. A dedicated client endpoint
uses the shared router/runtime but cannot admit unknown incoming connections.
Failed dials dispose the runtime; successful clients automatically close their
socket after the connection's closing/draining lifetime ends. Canceling the dial
context after success does not close the connection. Shared configuration defaults
and validation now serve both listener and dialer.

Root tests connect both public endpoints in v1/v2 (including Retry), infer and
verify an IP certificate without mutating caller TLS configuration, transfer a
stream, and verify runtime shutdown. Failure tests reject an untrusted certificate
and cancel both pre-start and waiting handshakes. Internal tests verify a client
endpoint ignores unknown traffic and terminates owner/reader goroutines on removal.
Public client interop tests against upstream pass verified handshakes, streams,
datagrams, and peer close in both versions. Full root race/vet and default interop
race checks pass, retaining the six existing explicit skips.

The dialer currently tries only the first resolved address and configured version
(v1 by default). Address racing/fallback, Version Negotiation, shared transports,
0-RTT, stream contexts/error parity, and full API compatibility remain open.
Migration, multipath, HTTP/3, performance comparisons and broader validation also
remain required by the goal.

## 2026-09-20 automatic public stream cleanup

Public stream handles now opt into engine cleanup once accepted receive data is
consumed/canceled/reset and sent data or reset has been acknowledged. Cleanup
runs from the relevant read, cancellation, incoming-frame, or ACK transition;
there is no stream polling goroutine or connection-wide scan. Owner-only handle
state retains EOF/cancellation/reset read results after engine storage is removed.
Release(ctx) remains an optional completion wait, not a required lifecycle step.
Internal engine users can retain the existing explicit release behavior.

Engine tests run 300 streams with an incoming limit of one in both versions,
checking no release before send acknowledgement, preservation of unread data,
exactly-once callbacks, reclaimed objects, and reset error codes. Public upstream
v1 tests run 40 incoming and 160 outgoing streams without Release, exceeding both
the initial incoming limit and the local object cap, and check EOF on an old handle.
Existing public bidi transfers and cancellation cases exercise cleanup too.
Root race tests/vet and default interop race tests pass; the six documented
interop skips remain. Client dialing and stream contexts/error compatibility are
next API work; migration, multipath, HTTP/3 and broader verification remain open.

## 2026-09-20 public streams and deadlines

Added bidirectional and unidirectional opening/acceptance, blocking reads and
writes, FIN close, STOP_SENDING/RESET_STREAM cancellation, and independently
mutable read/write deadlines. Concurrent reads and writes serialize per direction;
blocked writes return committed byte counts when deadlines expire. Deadline
changes cancel and replace uncommitted waits without replaying committed operations.
The experimental Release(ctx) method waits for both directions to finish and
reclaims engine storage/peer stream credit. Automatic cleanup, stream lifetime
contexts, and upstream stream error types remain open API work.

Root tests verify deadline replacement during a pending operation, expiration,
clearing deadlines, and preserving committed outcomes. Upstream v1 tests transfer
140,000-byte requests and 128,000-byte responses across flow-control windows, exercise a
partial timed write and resumption, local/remote stream opening, uni streams,
FIN, and release. Small v1/v2 public exchanges verify cancellation and reset codes.
The large v2 test reproduced the pinned upstream key-update stall and is an
additional explicit default skip, enabled by QUIC_INTEROP_KNOWN_FAILURES=1.
There are now six documented default interoperability skips; no v2 large-transfer
or full API compatibility claim is made. Root race tests/vet and the default
interop race suite pass.

## 2026-09-20 experimental public server API

Added ListenAddr, cancellable Listener.Accept, connection lifetime contexts,
addresses, TLS/version/datagram state, SendDatagram, ReceiveDatagram, and
application close. Default listeners accept v1 and v2 on the same socket.
Acceptance finishes handle construction after consuming a connection even if
caller cancellation races it. Endpoint terminal notifications cancel public
connection contexts and release their watches. Listener.Close owns and closes
accepted connections; this differs from the target upstream listener semantics.

Public API interop exchanges datagrams with upstream peers in both versions,
checks verified TLS and connection state, cancellation, application-close delivery,
and blocked-receive/connection-context wakeups during shutdown. Configuration and
blocked-accept shutdown tests exercise the root API. Public streams and client
dialing are the next API work; this remains experimental and has no parity claim.

## 2026-09-20 event-driven runtime operations

Added UDPServer.Wait for operations that need data or send credit. A nonblocking
callback runs on the endpoint owner and reports whether it completed; otherwise
the caller waits for endpoint activity, cancellation, or shutdown. Failed readiness
checks do not signal other waiters, avoiding feedback loops between blocked reads.
Submitted callbacks still return their actual outcome before caller buffers can
be released or acceptance results lost to cancellation races.

Tests coordinate sixteen waiters, verify exactly one initial check per waiter,
wake all through an owner mutation, and exercise cancellation and shutdown.
The upstream runtime test now receives application datagrams through Wait before
closing each of two connections, for both versions, under the race detector.
Public stream/datagram wrappers still need to bind this mechanism to their API,
including per-operation deadlines and connection cancellation semantics.

## 2026-09-20 serialized UDP server runtime

Added an internal UDPServer owning a socket, reader goroutine, and serialized
endpoint event loop. It routes input, sends committed output, services protocol
deadlines, bounds queued datagrams and send batches, and exposes cancellable
Accept plus serialized Do commands. Close shuts down the socket and waits for
both goroutines. Submitted commands return their actual outcome rather than
losing an accepted connection to a cancellation race.

Root tests check canceled operations, waiting Accept during shutdown, goroutine
termination, and concurrent command serialization under the race detector.
Independent tests connect two concurrent upstream clients, accept them through
the runtime, and verify application-close delivery in v1 and v2.

Do callbacks must finish promptly, must not reenter the runtime, and must not
expose mutable engine state to concurrent access. Public connection/stream
wrappers will enforce this contract; they do not exist yet. Buffer pooling,
socket error reporting, batching/offload, admission rate limits, and listener
ownership/API parity remain unfinished. No production-readiness claim is made.

## 2026-09-20 endpoint accept queue

Added a nonblocking Accept operation to ServerEndpoint. Connections enter its
FIFO only after handshake confirmation and are returned once with their associated
peer address. Repeated synchronization cannot announce them again. Queue and
announcement state are bounded by endpoint connection capacity and removed with
the connection. Acceptance leaves lifecycle ownership with the endpoint.

Connections that close before acceptance are skipped. Draining removes pending
acceptance without releasing routing identities early; endpoint shutdown empties
all queue state and causes Accept to report ErrEndpointClosed. The future socket
owner can wait on its event loop when ErrAcceptWouldBlock is returned.

Root tests cover pre-handshake emptiness, two confirmed peers, unique acceptance,
closed/unaccepted cleanup, draining retention, and shutdown. The shared-socket
upstream test now consumes this queue for concurrent v1/v2 clients. This remains
an internal serialized API; the public blocking listener and concurrent connection
wrappers are not yet implemented.

## 2026-09-20 measured router synchronization improvement

Added routing microbenchmarks at 1, 256, and 4096 connections. The unchanged Sync
path previously rebuilt maps and scanned connections sharing a peer on every
packet. It now compares immutable CID-owner identity, highest issued sequence,
and active count, retaining full collision checking on actual changes. Closing
and draining retention and closed-connection removal still run correctly.

On this Apple M4 / Go 1.26.2, three-sample medians changed from 177 ns to 5.9 ns
for one connection and 39.5 us to 7.7 ns for 4096 connections sharing a peer.
Allocations dropped from two (24 bytes) to zero per unchanged Sync. These are
local microbenchmark results only, not an end-to-end or upstream performance
claim. Commands, raw samples, baseline source, and limitations are preserved in
docs/benchmarks/router-sync.md. A regression test covers replacing a retired CID
without changing the active count. Migration will need explicit cache invalidation.

## 2026-09-20 multi-connection server endpoint

Added a serialized ServerEndpoint combining CID routing, Retry admission, connection
ownership, timer dispatch, and round-robin output. It synchronizes CID changes
before exposing committed packets to the socket, routes duplicate Initials before
admission, bounds admitted connection count, and retains closing/draining routes.
Per-connection failure or timeout does not terminate unrelated connections.
Explicit removal closes the connection and removes its routing identities.

The socket-independent API returns addressed output, including stateless Retry
responses. Tests complete two simultaneous independent connections in each version,
exercise duplicate admission prevention and output fairness, and verify independent
timeout cleanup. Localhost interoperability tests serve two concurrent upstream
clients through one UDP socket for v1 and v2, including Retry/token admission.

This is still an internal endpoint, not a public listener or concurrent connection
API. Application operations must share its serialized owner. Deadline scans are
linear, token/key rotation and admission-rate policy remain owner responsibilities,
and public socket lifecycle, accept queues, backpressure, and API parity are open.
Migration still requires address-aware path state beyond the fixed-peer router.

## 2026-09-20 bounded connection-ID router

Added a serialized per-UDP-endpoint Router with bounded connection capacity,
fixed short-header CID length, collision-checked registration, and atomic CID
synchronization. It reserves newly issued IDs before transmission and removes
retired IDs after synchronization. Initial routing aliases remain for the whole
connection lifetime, including closing/draining, to prevent duplicate admission.
Expired connections release routes when synchronized or explicitly removed.

The router associates connections with canonicalized peer addresses. A known
CID on another address is reserved but not dispatched or readmitted; migration
handling remains future work. Zero-length CIDs use the peer address and cannot
share that address with another connection. Unknown-CID stateless resets are
matched only against connections associated with the source address, then sent
to the engine's authentication-before-reset receive path.

Tests cover Initial aliases, issued/retired IDs, fixed-address enforcement,
IPv4-mapped addresses, draining retention/expiry, atomic collisions, removal,
zero-CID ambiguity, and stateless-reset dispatch. The owner must call Sync after
connection events and before socket transmission; a collision must prevent the
packet from being sent. A public listener and event loop are not yet implemented.

## 2026-09-20 bounded handshake lifetime

Added configurable HandshakeTimeout with a ten-second default and negative-value
validation. This is an absolute local resource limit, independent of negotiated
idle timeout and its PTO floor. It arms on the first timestamped engine operation,
so adapters must service NextDatagram immediately after Start. Retry, retransmits,
and received traffic do not extend it; handshake confirmation disables it.

The deadline joins RecoveryDeadline and is checked before receive, output, and
timeout processing. Expiry reports sticky ErrHandshakeTimeout and immediately
releases TLS keys, buffers, recovery state, and routing identities without a wire
close. Tests cover v1/v2 Retry without deadline extension, late input/output,
resource release, configurable limits, and confirmed connections. Separate
handshake-idle policy and public context/deadline API parity remain future work.

## 2026-09-20 stateless Retry admission

Added AdmitInitial, a stateless Retry-required admission function for unknown
connections. It checks datagram/header bounds and Initial protection before
issuing an address-bound token and Retry response. Returned tokens are verified
against the observed UDP source and Initial destination CID before allocating
TLS state. Successful admission recovers the original CID, generates a server
CID, supplies Retry transport parameters, validates amplification accounting,
and consumes the received Initial in a started connection.

Tests reject malformed/truncated input and tokens replayed from another port,
then complete independent v1/v2 handshakes. New localhost interoperability tests
exercise upstream v1/v2 clients through our Retry issuer and token validator;
the converse upstream-server Retry tests remain enabled.

The caller must route existing CIDs first and enforce admission/rate limits.
This is not yet a public listener or a multi-connection router. Invalid/expired
Retry tokens are discarded with an error; the RFC-recommended stateless
INVALID_TOKEN close is not implemented. NEW_TOKEN admission, 0-RTT, and version
negotiation remain unfinished. Token replay within its bounded lifetime remains
possible for the same address/CID, so router deduplication is still required.

## 2026-09-20 authenticated Retry tokens

Added an immutable, concurrent-safe Retry token codec using XChaCha20-Poly1305
with random 192-bit nonces. Authenticated context binds the full client address
and port, QUIC version, and received Retry destination CID. The original DCID is
encrypted and returned only after verification. IPv4-mapped IPv6 addresses are
normalized. Token lengths and configured lifetimes are bounded before decryption.

Tokens carry an authenticated issue time and lifetime in whole seconds. Future
and expired tokens fail; validation also applies the current maximum lifetime,
so key rotation cannot extend an issued token's expiry. A current and optional
previous key support explicit rotation; the owner must generate secret keys,
replace the codec, and retire old keys. The codec does not schedule key rotation.
Retry tokens remain replayable within their short lifetime for the bound address
and Retry CID, and are separate from NEW_TOKEN and 0-RTT replay protection.

Independent v1/v2 server-after-Retry tests now issue and validate these tokens
before constructing server routing context. Tests cover every-byte tampering,
address/port/version/CID binding, expiry, rotation/retirement, owned output,
IPv4 normalization, empty Retry CIDs, and concurrent use. A fuzz target exercises
untrusted token validation. Public listener/router integration remains unfinished.

## 2026-09-20 server connection context after Retry

Server Config now accepts an explicit RetrySourceCID from the router's verified
Retry context. Initial keys and routing use that CID, while transport parameters
retain the original destination CID and advertise retry_source_connection_id.
Nil and empty CIDs remain distinct. Client use, overlong CIDs, and a Retry CID
identical to the original DCID are rejected. Supplying this context does not
implicitly mark the address validated or lift amplification limits.

Added a version-aware Retry packet builder using the existing integrity-tag
implementation. It validates CID/token bounds and owns its output bytes; token
authentication, path MTU, and amplification policy remain caller responsibilities.

Tests complete independent client/server post-Retry handshakes in v1/v2 with
both nonempty and empty Retry CIDs and verify parameter binding. Packet-builder
tests cover both versions, tag verification, ownership, and invalid inputs.
The listener/router and authenticated token issuance/validation still need
implementation; this is connection setup after externally verified Retry context.

## 2026-09-20 client Retry handling

Clients now accept at most one integrity-verified Retry before processing a
server Initial. Retry changes the destination CID and Initial keys, preserves
the original destination CID and TLS transcript, copies the token into subsequent
Initials, and binds retry_source_connection_id during parameter validation.
Loss/congestion/pacing state resets without resetting packet-number continuity.
Outstanding ClientHello CRYPTO data is repacketized to leave space for the token.

Tests verify v1/v2 integrity rejection, one-Retry enforcement, late Retry rejection,
unchanged ClientHello bytes with a 700-byte token, new keys/CIDs, packet-number
continuity, and flight-state reset. New independent interoperability cases complete
verified v1/v2 handshakes against an upstream server configured to require Retry.
Existing path-validation and v2 key-update interoperability skips remain separate.

Server Retry generation, authenticated token lifecycle/address binding, listener
integration, and 0-RTT replay after Retry remain unfinished. The engine retains
its fixed 1200-byte packet budget; a token leaving no CRYPTO payload room causes
an explicit local resource failure. Supporting larger permitted datagrams and
fully auditing Retry edge cases remain requirements, not claimed completion.

## 2026-09-20 path-validation interoperability

Added upstream path-validation checks to all six role/version/CID scenarios.
Four pass: our server validates against the upstream client with zero-length or
shared-socket CIDs, in both versions. Our client expires against the pinned
upstream server in v1 and v2. Source inspection identifies its current-address
early return before server PATH_RESPONSE generation. The two affected subtests
are explicitly skipped by default, with QUIC_INTEROP_PATH_FAILURES=1 reproducing
them. They are documented in interop/KNOWN_FAILURES.md; no upstream code is changed.

Added protected-packet tests proving that an ACK for a transmitted challenge
neither validates the address nor postpones the validation timer. A wrong token
also fails to validate, while the matching PATH_RESPONSE completes the check.
These tests pass for both versions. Existing v2 stream/key-update skips remain.

## 2026-09-20 locally initiated fixed-path validation

Added ValidatePath and explicit validation state to the serialized connection
engine. Challenges use crypto/rand tokens and become matchable only after actual
packet transmission. Validation runs at most three probes with fresh tokens,
base-PTO spacing, and a three-base-PTO deadline beginning at the first send.
Probe output uses existing congestion, pacing, and amplification gates; ordinary
loss recovery never retransmits a challenge token. Expiry removes queued probes
without closing the existing connection, and late responses are ignored.

A matching response to a short probe validates the address and lifts its
amplification limit, but starts a fresh, expanded probe for minimum-MTU validation.
Only a response matching a sent 1200-byte challenge completes the check. Close
and draining discard validation state. Its deadlines join the engine's timer API.

Tests cover v1/v2 challenge/response exchange, idempotent initiation, unsent and
expired token rejection, fresh retry tokens, nonfatal expiry, and the two-step
address/MTU distinction. Address-aware input/output routing, new-path connection
IDs, independent path recovery, NAT rebinding, and active migration remain open;
this API validates only the adapter-associated fixed path.

## 2026-09-20 fixed-path validation responses

The connection engine now echoes authenticated PATH_CHALLENGE data in a queued
PATH_RESPONSE, ahead of reliable application data. Responses are padded to
1200 bytes when amplification credit permits and otherwise sent unexpanded.
Congestion control, pacing, and flight accounting still apply. Response payloads
are released after sending and never retransmitted on loss or PTO. Packet replay
suppression prevents a duplicated packet from creating duplicate responses.

Unmatched PATH_RESPONSE frames are ignored, as RFC 9000 permits, and do not
validate an address. Tests exercise v1/v2 echo bytes, packet-size requirements,
amplification constraints, duplicate input, non-retransmission, and unsolicited
responses. The current adapter contract restricts input/output to one fixed path;
this ensures responses use the incoming path but does not implement migration.
Locally initiated unpredictable probes, path identity/routing, validation timers,
NAT rebinding, and active migration remain required next steps.

## 2026-09-20 handshake failure classification

Automatic receive-error closing now maps transport-parameter validation failures
to TRANSPORT_PARAMETER_ERROR (0x08), CRYPTO reassembly capacity failures to
CRYPTO_BUFFER_EXCEEDED (0x0d), and conflicting buffered CRYPTO bytes to
PROTOCOL_VIOLATION (0x0a). CRYPTO errors identify frame type 0x06; parameter
errors use an unspecified triggering frame. Fixed wire reasons avoid exposing
unnecessary internal diagnostics while preserving the original local error.

Tests inject oversized CRYPTO offsets and conflicting out-of-order data through
protected v1/v2 Initial packets. A TLS handshake with mismatched advertised and
packet-header connection IDs exercises parameter binding failure through the
normal receive path. Tests decrypt each generated close and check its code and
triggering frame. Other unclassified internal/resource failures remain audit work.

## 2026-09-20 receive-error close mapping

HandleDatagram now prepares CONNECTION_CLOSE after typed TransportError failures
and wrapped Go TLS alerts. Transport errors retain their code, triggering frame,
and reason. TLS alerts use CRYPTO_ERROR + alert value and an empty wire reason;
local certificate diagnostics remain in the returned error. The original failure
stays sticky even if encryption limits prevent generating the close.

Adapters must continue servicing closing output and deadlines after the error,
rather than immediately disposing the engine. Peer closes and stateless resets
remain silent draining transitions. Untyped internal/resource errors and remaining
parameter/stream error classification still require a comprehensive mapping audit;
this does not claim every failure path now has a standards-correct wire error.

Tests inject illegal Initial frames and malformed TLS messages for both versions,
decrypt the automatic response, verify exact error codes, and deliver it to the
peer. A separate exhaustion check verifies no AEAD use beyond its limit.

## 2026-09-20 closing-state coalesced input

Closing-state receive now walks all parseable packets in a coalesced datagram,
with connection-ID/version checks and per-packet authentication. An undecryptable
first packet no longer hides a later valid peer close. Receive/amplification
credit is counted once per datagram, and a peer close preserves the original
termination deadline when transitioning to draining.

Stateless reset matching now follows attempted authentication, matching the
normal receive path's authenticated-packet precedence. An authenticated packet
whose suffix coincides with a reset token no longer triggers draining; an
undecryptable reset addressed to an unknown CID still does. Frame legality is
checked before accepting a close, including rejecting application closes in
Initial packets. Tests cover both versions, damaged preceding packets, exact
credit accounting, reset precedence, and forbidden close encoding.

## 2026-09-20 closes at multiple protection levels

Unconfirmed connections now prepare a close at every available protection level,
covering RFC 9000 section 10.2.3's handshake delivery recommendations. Application
closes retain their code and reason only in the 1-RTT copy; Initial and Handshake
copies carry APPLICATION_ERROR with an empty reason. Confirmed closes remain
1-RTT only. Copies are separate UDP datagrams, each no larger than 1200 bytes;
coalescing them is optional and not implemented here.

Each burst checks amplification credit per datagram and resumes a partially
sent burst when credit arrives. Subsequent response bursts are rate-limited,
limited by received-byte credit, and reuse the original protected bytes. Packet
numbers and AEAD usage are consumed only when initially sealing each copy.
The termination deadline is fixed across responses.

New v1/v2 tests decrypt all three levels, verify application/transport error
encoding, exhaust amplification credit mid-burst, resume sending, and check
byte-identical retransmissions with unchanged AEAD counts and close deadlines.
Automatic error-to-close mapping and the broader lifecycle audit remain open.

## 2026-09-20 application close during handshake

CloseWithError now accepts a started but unconfirmed connection. Its existing
handshake-level transport-close path sends APPLICATION_ERROR (0x0c), with no
reason phrase or application error code exposed, as required by RFC 9000
section 10.2.3. Confirmed connections retain application-close encoding.
Invalid application error codes are rejected before modifying connection state.

Tests exercise v1/v2 Initial protection and Handshake protection after the
client retires Initial keys but before confirmation. They decrypt the close,
check its exact type/code/empty reason, and verify peer draining. Sending at
multiple available protection levels remains outstanding; the engine still
selects a single handshake level for an unconfirmed close.

## 2026-09-20 persistent-congestion recovery audit

Corrected the persistent-congestion transition to clear the recovery epoch,
matching RFC 9002 Appendix B.8. ACKs can now resume window growth immediately;
application-limited ACKs still do not increase the window. The prior checkpoint's
claim that the exposing ACK must remain suppressed was incorrect.

Added minimum-RTT refresh on the next valid RTT sample after persistent
congestion (section 5.2), retaining smoothed estimates and sample eligibility.
Invalid negative samples do not consume the pending refresh. Tests cover
resumed slow start, application-limited suppression, one-shot RTT refresh,
subsequent minimum decreases, and the engine's loss-to-RTT integration.

## 2026-09-20 persistent congestion

Added RFC 9002 section 7.6 loss-interval detection across packet-number spaces
and loss callbacks. The duration uses three times the base PTO including peer
max_ack_delay regardless of encryption level, without exponential backoff.
Endpoints must be ack-eliciting, declared lost, and sent after an RTT sample.
ACKs, unresolved packets, discarded keys, and regressing timestamps break runs.
Late ACKs can split retained loss history. Reported intervals are consumed so
unrelated subsequent loss cannot repeatedly collapse the window.

Both ACK-triggered and time-threshold loss paths now collapse NewReno to two
maximum datagrams when persistent congestion is established. The original implementation retained the recovery epoch here; the audit below
corrects that behavior. PTO expiration by itself never declares persistent congestion.

History is bounded to six times the per-space recovery capacity (24576 records
in the engine). Eviction discards evidence conservatively; extreme history
pressure can therefore omit a persistent-congestion interval. Unresolved packets
also conservatively defer detection. History allocation/scan costs and a more
compact representation require performance evaluation. This remains an internal
single-path implementation; migration and multipath need per-path recovery.

Tests cover cross-space loss and ACK barriers, separate callbacks, late ACKs,
key discard, threshold equality, ACK-only endpoints, pre-sample packets,
bounded history, and ACK-to-loss-to-window integration. Root race/vet and the
default upstream race suite pass; the existing three v2 stream/key-update skips
remain unchanged.

## 2026-09-20 unreliable DATAGRAM engine support

Integrated RFC 9221 send/receive into the connection engine. Peer receive
limits include frame overhead and are independent of local receive support.
SendDatagram copies data, reserves conservative 1200-byte packet space across
CID rotation, and returns recoverable backpressure at 64 pending messages.
ReceiveDatagram transfers ownership from a queue bounded by 64 messages and
64 KiB; overflow drops messages without consuming stream flow credit.

Sent DATAGRAM payloads are released immediately, while packet metadata remains
in recovery for ACK, congestion, and PTO accounting. Loss never requeues the
payload. PTO sends a probe even when the only outstanding packet was a datagram.
Closing and draining release receive buffers. The current single-frame packetizer
uses the length-omitting encoding and limits outgoing payloads to 1158 bytes.

Engine tests cover both QUIC versions, empty messages, input ownership,
duplicate suppression, maximum packet size, frame-overhead limits, invalid
protected frames, queue bounds, and a dropped packet followed by a PTO probe.
Upstream interoperability now exchanges empty, small, and 1158-byte datagrams
in both directions in all six version/role/CID cases, before the existing v2
stream/key-update skips. No upstream implementation code is used by the engine.

Application priorities, delayed ACK scheduling, 0-RTT datagrams, and public
network APIs remain unfinished. This does not establish full RFC 9221 conformance
or performance parity. The broader implementation goal remains active.

## 2026-09-20 packet pacing

Added a single-path token-bucket pacer at 1.25 times cwnd/smoothed RTT. Credit
is capped at ten 1200-byte datagrams and never more than the congestion window.
In-flight sends debit the bucket; pure ACKs bypass it. PTO probes bypass the
readiness check but still debit pacing credit. A timer-resolution floor prevents
zero-RTT estimates from producing invalid arithmetic.

The engine exposes pacing wakeups through its combined deadline API, while
congestion and amplification remain separate gates. Oversized frames are rejected
before pacing. Tests cover burst caps, refill timing, no unbounded idle credit,
probe debt, regressing timestamps, exact driver wakeups, and unpaced ACK output.
Root race/vet and default upstream race checks pass with existing v2 skips.

This is a baseline pacing policy, not a performance improvement claim. Burst
size, timer granularity, allocations, and packet batching require measured
workloads and platform integration. Persistent congestion, ECN, public transport
APIs, migration, multipath, and other remaining gates stay in the active goal.

## 2026-09-20 explicit transport-error close generation

Added CloseTransportError with explicit transport code, triggering frame, and
reason. It can serialize a previously reported failure while keys remain
available, preserving the original application-visible error. Confirmed
connections use 1-RTT; the current handshake policy uses Initial while available,
otherwise Handshake. Initial closes are padded to 1200 bytes. Close generation
refuses exhausted encryption keys or an exhausted connection integrity budget.

Closing-time peer-close recognition now accepts available long-header levels as
well as 1-RTT. Cached close output continues to respect amplification credit;
a server with no credit waits until a routed peer datagram supplies it.
Tests verify v1/v2 Initial protection/padding, transport frame fields, closing
after a reported failure, amplification gating, and AEAD exhaustion. Root
race/vet and default upstream race checks pass with known v2 skips unchanged.

This is an explicit engine API. Automatic mapping of all protocol/TLS failures,
multi-level handshake close bundles, and application-close conversion before
confirmation remain unfinished. The public socket/API adapter and the broader
recovery, migration, multipath, interoperability, and performance work remain
part of the active full goal.

## 2026-09-20 confirmed application closing

Added CloseWithError for confirmed connections. It protects an application
CONNECTION_CLOSE once, terminates stream/recovery work, and retains a copy for
closing responses. Cached ciphertext is copied to the adapter rather than
resealed or exposed for mutation. Responses are paced at one base PTO and
bounded by additional receive credit; unknown routing IDs receive no response.
The closing deadline is fixed at three PTOs.

Application operations return a local-close error while NextDatagram and the
termination deadline remain serviceable by the socket owner. A peer close or
stateless reset transitions to draining without extending the deadline.
Closing-time authentication still observes the connection integrity limit.

Tests cover v1/v2 close contents, bounded cached responses, mutation isolation,
application-operation termination, deadline expiry, and closing-to-draining.
The upstream v1 matrix also verifies receipt of application error code 42 after
stream exchange and a local key update. Root race/vet and default upstream race
checks pass, with the existing v2 stream skips unchanged.

The new API rejects use before confirmation. Handshake-level close packet
selection, transport-error close generation, and the public socket/API adapter
remain unfinished. The full goal stays active alongside recovery, migration,
multipath, extension, and performance work.

## 2026-09-20 peer-close/reset draining lifecycle

Authenticated CONNECTION_CLOSE and stateless-reset detection now enter draining
for three base PTOs. Draining suppresses all output, returns the terminal error
to application operations, and exposes a fixed routing-retirement deadline.
Late input does not extend that deadline. Stream buffers, deferred packets,
packet keys, and recovery flight records are released immediately; routing CID
state is retained until expiry. Timeout processing then releases routing state.

Explicit local disposal now releases buffered transport resources, and idle
expiry disposes silently without entering a peer-close exchange. The future
socket router must service draining deadlines even after application operations
have returned their terminal error.

Tests cover v1/v2 application and transport peer closes, initial-handshake
closure, early/exact deadline handling, no output under repeated late traffic,
immediate buffer/key release, and routing-state retention/retirement. Root
race/vet and the default upstream race suite pass with known v2 stream skips.

Local CONNECTION_CLOSE generation, bounded retransmission during the closing
state, and the shared socket/API adapter remain next lifecycle work. The full
goal remains active; this does not claim complete connection lifecycle support.

## 2026-09-20 AEAD usage limits

Added RFC 9001 section 6.6 cipher usage bounds. The engine counts successful
seals per encryption level/key generation and resets application encryption
counts only when keys change. Before exceeding an application's confidentiality
bound it initiates a key update; if updating is unavailable, or a non-updatable
level exhausts its bound, it stops with AEAD_LIMIT_REACHED rather than reusing
exhausted keys.

Authentication failures use a connection-wide count that survives key updates.
The effective integrity bound follows the strictest cipher encountered on the
connection. Packet protection distinguishes an actual AEAD authentication failure
from malformed input. Trials with current/previous/next receive keys count one
failed packet, while a packet authenticated with any valid generation is not a
forgery. Reaching the integrity bound stops further processing/output.

Tests cover all supported cipher bounds, the last allowed seal and automatic
key change, exhaustion when no update is possible, one-count-per-forged-packet,
and valid new-phase traffic preserving the failure count. Root race/vet and the
default upstream race suite pass with the known v2 stream skips unchanged.

Proactive close notification before exhaustion belongs to the pending closing
lifecycle; the current enforcement stops silently if no safe update is possible.
The complete timing/lifecycle audit, public APIs, recovery policy gaps, migration,
multipath, and full interop/performance gates remain unfinished. Goal active.

## 2026-09-20 negotiated idle timeout

Added negotiated idle-timeout calculation using the minimum nonzero local/peer
max_idle_timeout and a three-base-PTO floor. Activity starts at the first packet
event supplied to the socket-independent engine. Authenticated receive activity
restarts the timer; only the first ack-eliciting send after a receive can restart
it again. ACK-only sends and subsequent probes do not continually extend it.
Duration conversion and multiplication saturate for extreme transport values.

The combined driver deadline now includes idle expiration. Expiry produces a
sticky idle-timeout error and no close packet or further output. Tests cover
zero/nonzero negotiation, the PTO floor, duration bounds, the send-reset rule,
unauthenticated traffic, authenticated traffic, and silent expiry after probes.
Root race/vet and the default upstream race suite pass, with the known v2 stream
skips unchanged.

A pre-packet handshake timeout is still the future socket/API adapter's
responsibility, since this engine's Start does not take a clock value. Complete
closing/draining state, resource routing, public APIs, remaining recovery work,
migration, multipath, and broader validation remain unfinished. The full goal
remains active.

## 2026-09-20 stateless-reset detection

Added reset detection for datagrams that do not authenticate as connection
traffic, including datagrams with unknown/unparseable routing fields. Trailing
tokens are compared in constant time against used, active peer connection IDs.
Actual packet handoff marks destination CIDs used; rotation alone does not.
Duplicate NEW_CONNECTION_ID frames preserve usage tracking, and retired IDs
remove their reset tokens from consideration.

An authenticated packet is not mistaken for a reset even if its trailing bytes
match a token. A detected reset records a terminal error, suppresses further
packets, and cancels the exposed output deadline. The single-path adapter must
deliver otherwise-unroutable datagrams from the configured peer address too.

Tests cover v1/v2 reset detection, unused/retired token rejection, unknown CID
routing, authenticated suffix collisions, minimum length, full-token matching,
duplicate CID state, and output suppression. Root race/vet and the default
upstream race suite pass with the recorded v2 stream skips unchanged.

This adds recognition and output suppression, not the complete draining/resource
lifecycle or reset generation after connection state is lost. Those require the
connection lifecycle and shared socket router. The full goal remains active.

## 2026-09-20 local application-key updates

Added InitiateKeyUpdate to the internal engine. The first update requires
handshake confirmation; subsequent updates require acknowledgment of the current
sending phase. The operation advances read/write secrets, toggles the key phase,
and queues an ack-eliciting probe. A second local update is rejected while the
peer response is outstanding.

Old receive keys are retained without a retirement deadline until a packet in
the new phase authenticates. During that interval, old-phase packets can still
arrive with increasing packet numbers. The three-PTO retirement interval starts
when the first new-phase response arrives. Simultaneous updates converge without
advancing secrets twice. Validated ACK ranges prove acknowledgment of current
sending keys, including when packet metadata was already retired.

Tests cover repeated v1/v2 local updates with packet loss/reordering, simultaneous
updates, pre-confirmation rejection, rejection before peer response, and delayed
old-phase traffic before the new-phase response. The upstream v1 matrix now also
checks a locally initiated update after the multi-window stream exchange.
Root race/vet and the default upstream race suite pass, retaining explicit known
v2 stream skips. AEAD usage limits, automatic update policy, and the complete
key-update timing/lifecycle audit remain outstanding. The full goal stays active.

## 2026-09-20 bounded pure-ACK send history

Recovery now retires oldest non-flight packet metadata when its per-space
history reaches capacity. An ordered index permits constant-time selection;
ACK/loss/discard processing also removes index entries. In-flight records,
including padded ACK packets, are never evicted this way. A full flight ledger
still allows a pure ACK to be sent without storing its timestamp. The monotonic
sent-PN counter continues to reject ACKs for unsent packets exactly.

Engine capacity pressure now applies backpressure to in-flight output instead
of failing the connection solely because ACK history filled the ledger. Tests
send 10,000 pure ACKs through the engine and then successfully send new data.
Recovery tests cover mixed history, full-flight pure feedback, padding
accounting, forgotten-history ACKs, unsent ACK rejection, and index cleanup.
Root race/vet and the default upstream race suite pass with the known v2 stream
skips still explicit.

Retiring ACK-only timestamps can omit an RTT sample when the largest newly
acknowledged packet's metadata has already been forgotten. This resource policy
needs inclusion in the final RFC/performance audit. Retained reliable-frame loss
history and full-flight-ledger PTO behavior remain separate resource-policy
work; this does not claim all long-lived recovery behavior is finished.
The full goal remains active, including the public transport API, connection
lifecycle, migration, multipath, and complete interoperability/performance gates.

## 2026-09-20 stream release and count replenishment

Added explicit ReleaseStream after both directions terminate and the application
has accepted/drained the receive side. This frees stream objects and reliably
replenishes peer MAX_STREAMS limits. The local active-stream cap is now tracked
separately from peer objects. Repeated release is idempotent. Canceling an already
complete receive side discards its unread bytes and returns connection credit,
without waiting for an unnecessary reset.

Closed IDs are tracked as merged ordinal ranges per stream type, bounded by
active holes rather than lifetime stream count. Late frames for released IDs
are ignored after direction validation: they cannot recreate receive buffers or
charge connection credit again. Final-size checks are intentionally retained
only while the stream object exists, as allowed by RFC 9000 section 4.5.
Superseded MAX_STREAMS updates share the existing bounded reliable-control path.

Tests cover 300 sequential streams per version under a one-stream limit,
rejection of release before acceptance/read completion, duplicate release,
late-frame suppression, and cancellation of unread completed data. A separate
closed-ID test compresses almost 100,000 completed IDs while preserving an active
hole. Root race/vet and the default upstream race suite pass, retaining the three
explicit known v2 stream skips.

Next: broader upstream lifecycle tests and the public connection/stream adapter,
while addressing remaining recovery resource policies and connection lifecycle.
The full goal remains active; migration, multipath, remaining extensions, and
performance validation are unfinished.

## 2026-09-20 stream cancellation and send completion

Added engine CancelRead/CancelWrite operations and STOP_SENDING dispatch. Local
read cancellation returns a stable application error and reliably sends STOP
until the receive side completes. Incoming stream data still undergoes flow and
final-size validation. A peer STOP validates the stream direction and causes a
reliable RESET with the requested error code and the committed final size.
Repeated cancellation does not change the first code or enqueue duplicate resets.

Write cancellation retires queued/retransmittable STREAM items while preserving
packet flight accounting. ACK processing tracks unique reliable data items;
a send side completes only after every data item and FIN are ACKed, or its reset
is ACKed. A completed send side ignores later STOP requests. Receive state now
tracks buffered byte count to recognize complete data before application reads,
allowing unnecessary STOP retransmissions to be retired.

Tests cover v1/v2 cancellation under loss/reordering, stable cancellation errors,
reset final-size accounting for queued and lost data, unchanged congestion flight
on cancellation, duplicate STOP behavior, completed sends, and out-of-order FIN
ACKs. Root race/vet and the default upstream race suite pass; the previously
recorded three v2 stream skips remain explicit.

Next: stream retirement/MAX_STREAMS replenishment and broader independent-peer
stream lifecycle tests, plus the outstanding recovery and public API work.
The full goal remains active; the known v2 peer mismatch and broader lifecycle,
migration, multipath, and performance requirements remain unresolved.

## 2026-09-20 upstream streams and peer key updates

Extended the UDP interop driver with serialized application progress. Requests
of 40 KiB and responses of 36 KiB exceed both endpoints' 4 KiB stream and 8 KiB
connection receive windows. The initial runs exposed unsupported key updates
once upstream reached its first packet threshold, rather than a flow-control
failure.

Added authenticated peer key-update handling: current/previous/next read keys,
send-key advancement before acknowledging an update, phase/packet-number
ordering checks, consecutive-update validation, and previous-key expiry after
three PTOs. Retirement is included in the driver deadline so idle connections
also discard old keys. Tests cover v1/v2 updates, phase wrap, reordered old
packets, old-key packet-number violations, forgery, premature updates, and expiry.

All v1 stream cases now pass against unmodified quic-go v0.62.0: our client,
our server with a zero-CID upstream client, and our server with a shared-socket
upstream client. The v2 stream cases reproduce a pinned upstream derivation
mismatch: its key-update function uses the v1 label for v2, contrary to RFC9369.
Our version-specific derivation passes the RFC v2 updated-secret fixture. See
`interop/KNOWN_FAILURES.md` for evidence and the opt-in reproducer. Default v2
stream subtests are explicitly skipped; v2 handshake/CID tests still run. This
is not a completed v2 stream interoperability gate.

Root race/vet and the default upstream race suite pass with those three explicit
skips. Remaining key-update work includes local initiation, AEAD usage limits,
and the complete lifecycle/timing audit. Stream cancellation, retirement, public
APIs, recovery policy gaps, migration, multipath, and full performance validation
remain unfinished. The full goal remains active.

## 2026-09-20 receive-window replenishment

Application reads now replenish stream and connection receive credit at half-
window thresholds. Absolute limits saturate at the QUIC maximum, and stream
credit stays within the fixed receive buffer. Streams with a known final size
receive no further stream credit; their consumption still replenishes connection
credit. RESET_STREAM accounts for and replenishes discarded/missing bytes once.

MAX_DATA and MAX_STREAM_DATA updates use reliable output. A newer limit retires
superseded frame items and compacts their queue entries without removing packet
transmissions from congestion accounting. This avoids accumulating one obsolete
control item per application read. Lost current updates are retransmitted by
existing recovery machinery.

Tests transfer 64 KiB through 2 KiB stream and 3 KiB connection windows in both
v1 and v2, with repeated loss/reordering in both directions. Tests also check
bounded queued credit updates and reset-driven connection replenishment without
duplicate credit or updates to terminal streams. Root race/vet and existing
upstream handshake/CID race tests pass.

Next: cancellation/STOP_SENDING and upstream stream interoperability, followed by
stream delivery completion/retirement and the outstanding recovery policies.
The full goal remains active; no complete transport/parity claim is made.

## 2026-09-20 engine stream integration

Connected stream state to the engine's transport parameters, packet dispatch,
and reliable output. Added internal OpenStream, AcceptStream, WriteStream, and
ReadStream operations. STREAM/RESET_STREAM/stream-blocked frames validate IDs,
directions, stream counts, receive credit, and final size. MAX_DATA,
MAX_STREAM_DATA, and MAX_STREAMS update send limits. Stream violations map to
transport error codes. Writes reserve bounded queue capacity before committing
credit, and existing ACK/loss handling retransmits owned stream frames.

Engine tests now exchange bidirectional and unidirectional streams in v1/v2,
including packet loss, reordering, duplication, and reverse-direction responses.
Additional tests cover exhausted send credit, empty FIN at the limit, illegal
unidirectional writes, and flow-control errors. Root race/vet checks and the
existing upstream handshake/CID race tests pass.

This initial integration is limited to advertised initial receive credit.
Automatic credit updates, STOP_SENDING/cancellation, delivery completion and
stream retirement, MAX_STREAMS replenishment, public APIs, and upstream stream
interop remain outstanding. Configuration currently caps receive stream counts
at 64 per direction and stream windows at 64 KiB; the engine retains up to 256
stream objects. These limits bound resources during integration, not a parity
claim. Next: receive-window updates and cancellation, then upstream stream tests.
The full goal remains active.

## 2026-09-20 stream send state and ID tracking

Added per-stream and shared connection send credit, partial writes, owned STREAM
frame payloads, FIN at the accepted end of input, zero-credit empty FIN, reset
final-size/code retention, and explicit delivery completion transitions. Stale
MAX_DATA/MAX_STREAM_DATA limits are ignored. Retransmission serializes retained
frames without charging new credit. The future engine adapter must reserve
reliable queue capacity before committing a write, and only mark data complete
after all bytes and FIN are acknowledged.

Added constant-memory stream-ID state for local opening, peer implicit opening,
ordered acceptance, MAX_STREAMS updates, endpoint-role/direction validation,
and rejection of references to unopened local streams. A reference to the
largest valid peer stream ID represents preceding implicit streams with counters
rather than allocating per-stream objects. Stream object capacity and cleanup
are separate responsibilities for the upcoming manager.

Tests cover partial writes at both credit boundaries, shared connection credit,
stale limits, buffer ownership, reset accounting and completion, all endpoint
roles/directions, implicit acceptance order, count limits, and extreme stream
IDs. Root race tests and vet pass. These pieces remain standalone; next is stream
object management and integration with engine dispatch/reliable output, followed
by bidirectional upstream stream interoperability. The full goal remains active.

## 2026-09-20 bounded stream receive state

Added `internal/stream` receive state: copied stream data, bounded circular
reassembly, duplicate/overlap handling, FIN and RESET_STREAM final-size checks,
nonblocking reads, and a shared connection receive-flow ledger. Credit is
charged by each stream's highest offset, including gaps and reset final sizes;
duplicates and reads never reduce previously charged credit. Reset consumption
releases credit for all discarded/missing bytes exactly once.

Receive-window increases are constrained by consumed bytes and fixed buffer
capacity. Peer offsets cannot force large allocations. Validation precedes
mutation, including overlap conflicts, final-size mismatches, and stream or
connection flow-control violations. Tests cover reordered FIN, duplicate data,
ring wrap, buffer ownership, terminal EOF/reset behavior, shared limits,
extreme offsets, and mutation-free rejection. A fuzz target exercises these
operations and checks byte order and credit invariants.
Root race tests and vet passed. A five-second fuzz campaign passed with
500,543 executions.

This component is not wired into the packet engine yet. Stream ID/direction
validation, implicit opening and count limits, send state, cancellation,
MAX_DATA/MAX_STREAM_DATA updates, and stream frame dispatch remain next steps.
No stream interoperability or public stream API is claimed. The full goal
remains active; recovery policy gaps and the broader lifecycle, migration,
multipath, and performance requirements remain unfinished.

## 2026-09-20 NewReno congestion-window enforcement

Added a per-path baseline NewReno controller with RFC 9002 initial/minimum
windows, slow start, byte-counted congestion avoidance, and one loss reduction
per recovery epoch. ACKed bytes from the recovery epoch and application-limited
ACK processing do not grow the window. Byte-counting preserves eventual growth
for very small packets instead of truncating every increase to zero.

The engine now gates in-flight output against the window. Pure ACK packets
bypass the gate; padded Initial ACKs count toward flight. A due PTO permits one
ack-eliciting packet beyond the window, still subject to amplification credit
and included in flight accounting. The allowance is consumed by the probe and
cleared on ACK progress or space retirement. Probe selection takes precedence
over a congestion-blocked padded ACK to avoid a handshake deadlock.

Tests cover initial-window sizing, slow start, recovery epochs, minimum window,
tiny-packet avoidance growth, ACK-only exclusion, exact engine flight limits,
one-packet PTO exceptions in v1/v2, and application ACK output at a full window.
Root and isolated upstream interoperability race tests and vet checks pass.

This is baseline congestion control, not completed RFC 9002 support. Pacing,
persistent-congestion detection, ECN validation, and sustained sent-history
retirement remain outstanding. Stream/flow-control state and public network APIs
are still missing. Next: finish recovery resource/timing policies and implement
stream state for bidirectional upstream interoperability. The full goal remains
active, including migration, multipath, lifecycle extensions, and performance.

## 2026-09-20 recovery timers and deadline-driven probes

Implemented RFC 9002 loss/PTO timer selection and saturated exponential backoff.
Loss deadlines take precedence over PTO. Application PTO waits for handshake
confirmation and includes max_ack_delay. PTO uses the latest ack-eliciting send
time in each space, including when that transmission has already been ACKed.
PTO expiry requests a probe without declaring the outstanding flight lost.

The client arms anti-deadlock probes until a Handshake ACK or confirmation proves
peer address validation, even with no ack-eliciting flight. Unchanged timer
re-evaluation cannot postpone those probes. Amplification-blocked servers do not
arm PTO; restoring credit permits an overdue timer to fire. ACK progress and
key discard reset backoff at the appropriate milestones.

The engine exposes RecoveryDeadline/HandleTimeout and selects queued data,
outstanding reliable data, or PING for a probe, respecting amplification credit.
The isolated upstream UDP driver now services these deadlines instead of its
previous fixed-interval manual probes. The main in-memory handshake loss matrix
also advances its simulated clock to actual recovery deadlines. QueueProbe
remains available only as an explicit low-level test hook.

Unit tests cover timer selection, backoff/reset, exact expiry, anti-deadlock,
amplification gating, loss precedence, and v1/v2 padded Initial probes after a
lost flight. Root and upstream interop race/vet checks pass. This is not complete
RFC 9002 support: congestion control, pacing, persistent congestion, ECN, and
sustained sent-history retirement remain outstanding. Next: congestion control
and stream/flow-control state, then bidirectional upstream stream tests.
The full goal remains active.

## 2026-09-20 RTT estimation and ACK-based loss recovery

Added `internal/recovery` with shared path RTT estimates, independent packet
number spaces, sent-packet accounting, exact unsent-ACK rejection for contiguous
packet numbers, packet threshold 3, time threshold 9/8, and loss deadlines.
RTT samples follow the largest-newly-acknowledged/ack-eliciting conditions;
ACK delay is ignored outside application space and capped after confirmation.
Duration arithmetic saturates on extreme peer input and PTO backoff values.

The engine feeds sent packets and ACKs into recovery, automatically queues
reliable data from lost transmissions, and exposes time-threshold deadlines and
timer processing. Frame delivery remains independent from packet accounting:
late ACKs of lost packets retire shared frame data and cancel queued copies.
Discarding encryption levels also discards their recovery accounting. ACK range
membership uses binary search rather than iterating packet-number values.

Tests cover RTT calculations, delay bounds, ACK-only/PADDING distinctions,
invalid/duplicate/late ACKs, byte accounting, packet/time thresholds, tail-loss
exclusion, exact timer boundaries, and duration overflow. Engine tests verify
automatic CRYPTO requeue and late-ACK retirement in v1 and v2. Root and isolated
upstream interop race tests and vet checks pass after integration.

This is partial RFC 9002 support. PTO duration calculation exists, but timer
selection/backoff, probe scheduling, congestion control, persistent congestion,
pacing, and ECN do not. The test UDP driver still uses manual probes. Sent
history is bounded to 4096 records per space; ACK-only history and retained
lost-transmission mappings require sustained-traffic retirement policies.

Next: implement PTO timer state and congestion control, replace test-driver
manual probing, and add stream state/flow control for bidirectional upstream
stream interoperability. The full goal remains active; migration, multipath,
the remaining lifecycle/extensions, and performance comparisons are unfinished.

## 2026-09-20 connection IDs and upstream UDP interoperability

Added local and peer CID managers. Peer updates enforce active limits and
sequence/CID/token consistency, apply monotonically increasing retirement
thresholds, and cannot reactivate retired IDs. Local issuance uses random IDs
and reset tokens, replenishes retired spares, routes active sent IDs, and rejects
retirement of unsent IDs or the packet's destination CID. Initial handshake CID
identity remains separate from the current rotating destination CID.

The engine now handles NEW_CONNECTION_ID and RETIRE_CONNECTION_ID, safely
ignores NEW_TOKEN while resumption is disabled, and supports same-path peer CID
rotation after confirmation. Local uniqueness history is bounded to 4096 issued
IDs (256 for 1-byte IDs); when full, or when the bounded random search finds no
unused value, issuance stops while active IDs remain valid. This is an explicit
resource policy requiring long-lived connection evaluation, not a parity claim.

Added a separate `interop` module pinned to quic-go v0.62.0. Six real loopback
UDP cases pass: each of v1/v2 with our client, our server against upstream's
default zero-CID client, and our server against upstream's shared-socket client.
Nonzero-CID cases include rotation and ACKed retirement/replenishment. Both
clients validate the certificate chain; no verification bypass is used.
Upstream remains absent from the root module dependencies.

The test-only UDP driver uses explicit 20ms probes, not RFC 9002 PTO. This gate
proves handshake/selected CID interoperability only, not stream transport,
congestion behavior, migration, or production readiness. Unit tests also cover
repeated two-sided CID rotations and unsent-ID retirement. A five-second peer
CID fuzz campaign passed with 84,945 inputs. Root and interop race/vet checks
passed, including the final sent-ID and lifetime-bound changes. Root module
dependency inspection confirms that upstream quic-go is not linked into the
implementation.

Next: RFC 9002 recovery/congestion and stream state/flow control, then expand the
independent interop tests to bidirectional stream data and adverse networks.
Retry, version negotiation, stateless reset, key updates, closing/draining,
migration, multipath, HTTP/3, and performance comparison remain outstanding.
The full goal remains active.

## 2026-09-20 socket-independent handshake engine

Added `internal/transport/connection.go`, connecting wire, packet protection,
TLS, transport parameters, ACK tracking, and anti-amplification accounting.
It supports both client and server roles for v1/v2, learns the peer's Initial
SCID, validates authenticated transport-parameter CIDs, generates padded Initial
datagrams, processes coalesced packets independently, and buffers undecryptable
packets within fixed count/byte limits. Invalid unauthenticated input is dropped;
authenticated malformed or prohibited frames produce a sticky failure.

The engine uses independent packet-number spaces, checks ACKs for unsent packet
numbers, avoids ACK loops, confirms with HANDSHAKE_DONE, and discards Initial
and Handshake keys at their RFC 9001 milestones. CRYPTO/HANDSHAKE_DONE items
remain tracked until acknowledged or their space is discarded. QueueProbe is
an explicit loss-test hook that resends outstanding frames with fresh packet
numbers, not an implementation of RFC 9002 timers or congestion control.

NextDatagram commits a send when it hands bytes to its caller. A future UDP
adapter must transmit immediately or close on write failure; it must not roll
back packet numbers or edit/reseal the bytes. This contract keeps nonce use and
path accounting explicit. Engine Close handles cancellation of unfinished TLS
without treating local close-notify as a shutdown failure.

Tests cover v1/v2 authenticated handshakes in both roles, reordering, duplicates,
dropped packet types, lost client Handshake flights, coalescing (including an
invalid packet followed by valid packets), ACK/CRYPTO retirement, no nonce reuse
on probes, old-key retirement, CID mismatch, unsent ACKs, packet-type violations,
anti-amplification gating, buffer bounds, and packet number exhaustion.
`go test -race ./...` and `go vet ./...` passed after fixing local-close handling.
A five-second engine fuzz smoke campaign passed with 17,582 inputs, including
arbitrary authenticated Initial plaintext to exercise dispatch past AEAD.

Still no UDP, public connection/stream API, RFC 9002 recovery/congestion, Retry,
version negotiation, CID rotation, stateless-reset handling, key updates,
closing/draining timers, migration, multipath, HTTP/3, or independent-peer
interoperability. Unsupported application/control processing is explicit;
the handshake engine does not advertise stream or datagram receive capability.

Next: implement peer/local CID management and the remaining established-handshake
control handling, then a local UDP driver and an isolated interop module pinned
to upstream quic-go. Add RFC 9002 recovery/congestion and stream state/flow
control to reach bidirectional stream interoperability. Keep the full goal active.

## 2026-09-20 complete core frame codecs and transport state primitives

Implemented all remaining RFC 9000 core control frame encoders/decoders and a
shared typed frame parser. Added DATAGRAM framing, actual encoded receive-size
checks, packet-type restrictions, server-only NEW_TOKEN/HANDSHAKE_DONE checks,
and ACK-eliciting/probing/bytes-in-flight classification. Short headers now
report OneRTT rather than the zero-value Initial packet type.

Tests cover independent literal wire encodings, truncation at every boundary,
nonminimal and unknown frame types, invalid limits/CIDs, encoder validation
before destination mutation, all packet-type classifications, and DATAGRAM
size accounting when length integers use nonminimal encodings. RFC 9000 section
12.5 prohibits RETIRE_CONNECTION_ID in 0-RTT even though table 3 suggests
otherwise; validation follows the explicit text.

Added `internal/transport` primitives: bounded descending receive/ACK ranges,
duplicate detection, a monotonic discard floor after history eviction, ACK
delay based on arrival of the largest packet, immutable ACK snapshots, and
per-path three-times anti-amplification credit with overflow-safe accounting.
Packets immediately evicted from receive history are discarded without frame
processing. These components are not yet wired into a connection engine.

Validation: `go test ./...`, `go test -race ./...`, and `go vet ./...` passed.
Five-second fuzz smoke campaigns passed for shared frame parsing (250,897
inputs) and bounded receive tracking (59,168 inputs). No network interoperability
or performance comparison has been run.

Next: implement a deterministic connection engine using the wire/TLS/state
components. It needs per-space packet numbers, authenticated packet dispatch,
CID binding, ACK scheduling, output packetization and Initial padding,
handshake confirmation/key retirement, CRYPTO recovery, and path send budgets.
Then add UDP and independent-peer stream tests. The full goal remains active.

## 2026-09-20 transport parameters and ACK/STREAM frames

Implemented all RFC 9000 transport-parameter codecs and defaults, optional
presence versus zero-length CIDs, preferred addresses, endpoint role checks,
range checks, unknown parameter handling, duplicate detection, and owned
decoded values. Added explicit validation against observed Initial/Retry CIDs.
Parameter duration values remain wire integers to avoid time.Duration overflow.
DATAGRAM-size and QUIC-bit-greasing parameters are understood, but those
transport features remain unimplemented.

The TLS adapter validates both its local parameters and incoming peer
parameter encoding/role/ranges. Tests inject malformed extensions into real
TLS flights and verify rejection in both directions. CID binding remains the
future connection engine's responsibility; TLS alone cannot observe packet IDs.

Implemented ACK/ACK_ECN and all STREAM flag combinations. ACK range decoding
checks subtraction underflow and bounds allocation by actual input length.
STREAM frames enforce the maximum final offset and support both explicit
length and remainder-of-packet data. These codecs do not implement stream
state, ACK scheduling, flow control, loss recovery, or congestion control.

Validation: `go test ./...`, `go test -race ./...`, and `go vet ./...` passed.
Three additional 5-second fuzz campaigns passed: transport parameters
(696,272 inputs), ACK frames (532,707), STREAM frames (689,422).

Next: finish the core control frame codecs, frame dispatch, and packet-type
frame restrictions. Then implement a deterministic connection engine with
packet number spaces, ACK tracking, CRYPTO recovery, handshake confirmation,
CID binding, and anti-amplification before UDP and independent-peer stream tests.
The full goal remains active; no usable network transport exists yet.

## 2026-09-20: wire, protection, and TLS foundation

Implemented from scratch: varints, packet-number decoding, v1/v2 header
mapping/parsing/encoding, coalesced packet boundaries, three TLS cipher suites,
Initial keys, packet/header protection, Retry integrity, key-update derivation,
CRYPTO frames, bounded reassembly, and serialized Go TLS integration.

RFC 9001 and RFC 9369 Appendix A fixtures verify complete client/server Initial
packets, Retry packets, ChaCha20 short-header packets, and updated secrets.
Tests reject altered and truncated ciphertext, enforce authenticated reserved
bit checks, and exercise malformed bounds and offset exhaustion. TLS tests use
trusted generated certificates (no verification bypass) and move fragmented,
reordered, duplicated CRYPTO data through our protected packet pipeline.

No sockets, public connection/stream API, transport parameter validation,
ACK/recovery, flow control, migration, multipath, HTTP/3, or independent-peer
interop exist yet. The TLS adapter does not confirm QUIC handshakes or manage
key lifetimes. Low-level packet keys do not enforce nonce uniqueness or AEAD
usage limits; those responsibilities belong to the future connection engine.

Next: implement typed transport parameters and their role/range/CID checks,
then the remaining core frame codecs (particularly ACK and STREAM). Build a
deterministic connection engine before adding UDP and independent-peer tests.
Keep quic-go out of the implementation's dependencies; an isolated integration
test module may depend on a pinned upstream release.

See README for checks and docs/PLAN.md for the complete goal. Do not mark the
goal complete at this checkpoint.

Validation on Go 1.26.2, macOS arm64:

- `go test ./...`, `go test -race ./...`, and `go vet ./...` passed.
- Coverage: handshake 81.2%, packet 86.5%, wire 92.3% of statements.
- Five bounded fuzz runs (5 seconds each, two workers) passed: headers,
  protected packet opening, CRYPTO reassembly, CRYPTO frames, and varints.
  They executed 3,455,800 inputs in total. These are smoke campaigns, not
  a substitute for sustained fuzzing and independent review.
- Varint and Initial sealing microbenchmarks execute successfully. No upstream
  comparison or performance claim has been made. Benchmark timings from this
  checkpoint are unsuitable for comparison because fuzzers ran concurrently.
- Git repository initialized on `main`; initial files are uncommitted.
