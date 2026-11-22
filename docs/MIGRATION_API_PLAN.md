# Separate probing and switching for migration

Status: public Conn.AddPath / Path.Probe / Path.Switch and path errors are
implemented as of 2026-09-30. A handle associates a caller-owned Transport with
the connection's primary QUIC path. Probe serializes concurrent callers with
context cancellation, keeps ordinary traffic on the old socket, and retains
successful proof. Switch waits for pending write completion before committing.
Close cancels an unused association, is idempotent and rejects the active path.
Closing a handle does not close the Transport or borrowed socket. Atomic address
snapshots avoid reading engine state outside its owner.

Own-peer public tests cover v1/v2 with and without multipath, six successive
switches, stream exchanges on both sides of switching, canceled-probe retry,
waiting caller cancellation, path/source/owner closure and primary traffic after
failed probes. Default independent tests cover v1/v2 two-switch sequences, with
and without a validated unused probe abandoned between switches.

Longer sequences against upstream v0.63.0 stop receiving proof when its probe CID
supply is exhausted. An upstream-only control reproduces the same diagnostic.
Both longer sequences remain opt-in known-failure reproductions, not passing
coverage. The default shorter fixture does not establish arbitrary repeated
migration. See interop/KNOWN_FAILURES.md.

The controller also rejects a reservation retired after successful proof, even
before endpoint servicing detaches it. Both service and output mark revoked
candidates failed under reliable-retirement queue pressure, clear pending
responses and retry cleanup once capacity returns. A focused v1/v2 regression
covers the handle, service and output boundaries. The pre-fix behavior fails all
six cases. Packet construction and commit already rejected the revoked CID;
the correction makes public proof state and retained cancellation consistent.

TestPublicAddPathSharedSource exercises a Transport already carrying another
connection. Both v1/v2 connections exchange bidirectional streams through its
single socket, and the resident still transfers after the migrated connection
closes. This is own-peer coverage; arbitrary sharing/collision/lifetime races
and independent shared-source validation remain unfinished.

Established foreign sockets now receive stateless resets with unknown random
CID bytes through immutable owner-published tuple/token hints. Only used active
tokens are published; first-use publication precedes source write submission.
The source scans bounded registrations with constant-time token comparisons and
preserves normal CID routing. Ambiguous hints do not select an owner. The final
owner rechecks current tuple and token state and attempts authentication before
accepting a reset, so stale hints alone cannot close a connection.

TestForeignResetRouting covers malformed/mismatched packets, wrong tuple, unused
and retired tokens, stale socket hints and ambiguity/revocation. First-use
publication ordering has a separate test. The independent restart test uses
public AddPath/Probe/Switch then a fresh upstream Transport on the same server
socket/reset secret; v1/v2 terminate with the typed reset error. Removing only
source reset forwarding makes both cases time out. This does not establish
candidate-time reset behavior, every multipath combination, or reset suffixes
that collide with an existing normal CID route.

Reset tuple eligibility now also includes locally initiated probes after they
use their reserved peer CID. The attempt must retain that reservation, be
validating or validated, and belong to an active QUIC path. Receive-time expiry
rejects unfinished stale attempts. Foreign hints include these eligible tuples;
server-side remote probing does not grant reset authority. Owner-level v1/v2
checks cover unsent, unchecked, failed, detached, retired, abandoned, expired and
wrong-peer cases. The independent probing reset fixture disables both directions
on the original socket, restarts upstream before Probe and requires the typed
reset result through the alternative Transport. Removing candidate eligibility
makes both versions time out. Independent preferred-address and combined
multipath cases still require validation.

Owned-socket CID collision dispatch now tries a unique local reset target after
normal authentication fails; authenticated packets retain precedence. The live
independent test rewrites an upstream reset prefix to another connection's CID
while preserving the token and verifies that the other connection stays usable.
Initial unknown-CID ambiguity remains a separate audit item. Cross-owner collision
handling is described below.

Local-CID-first collisions can now fall through to a foreign reset owner after
local authentication fails. The original packet buffer transfers once through
bounded handoff; metadata records the failed source dispatch. Final registration,
tuple and token checks remain on the destination. Valid authenticated input,
including closing-state packets and post-decryption protocol violations, suppresses
handoff. The independent migrated-target fixture disables its old socket and
verifies reset termination through the shared socket without closing the resident.
Foreign-CID-first collisions now use a bounded authentication-result return. The
source reserves a ticket only for a unique distinct reset target, records its exact
identity, and counts pending plus queued returns against a shared 64-slot limit.
Authentication or accepted reset at the first owner suppresses return. Otherwise
the source rechecks the recorded target without normal CID redispatch. Local
receipt or one final foreign handoff follows; final handoffs have no return ticket.
Destination shutdown cancels queued tickets and source shutdown rejects late
completion without taking buffer ownership. Race tests cover stale/replaced and
ambiguous targets, queue pressure, shutdown and authenticated suffix collisions.
Independent v1/v2 tests cover local and foreign reset targets with original sockets
muted; removing return reservation makes all four cases fail.

Reset recognition now also accepts long headers across local fallback and foreign
handoffs. Long-header CID routing precedes token selection to preserve coalesced
1-RTT authentication. Tests include malformed long prefixes, authenticated trailing
packets and closing state; independent fixtures preserve upstream reset tokens and
substitute long headers for all v1/v2 local/foreign owner and target combinations.
Unknown-CID duplicate-token ambiguity remains a separate audit item.

Reset-hint publication now compares per-path peer and CID identities, preventing
both allocation churn for shared peers and stale hints when distinct values
collapse to one value. A lazy source-owned HMAC token index now narrows alternate local
reset selection before live tuple/token checks. Router.Sync publishes first-use
and retirement changes; removal releases identities. Warmed selection no longer
scans every local connection. Foreign-registration and initial unknown-CID scans,
cold bootstrap, memory at scale and end-to-end costs remain work. See
[index measurements](benchmarks/foreign-reset-index.md) and the historical
[publication measurements](benchmarks/foreign-reset-publication.md).

Remaining restrictions: one uncommitted candidate per QUIC path, concrete source
addresses of the current peer's address family, no cross-family migration,
and no established foreign-write failover. Stateless resets on an established
foreign binding or eligible local probe now have token-hint forwarding; broader
probe/reset combinations and ambiguous CID/token dispatch remain incomplete.
OpenPath continues to create a negotiated multipath path with its existing
ownership rules; Probe/Switch apply only to AddPath migration handles. Complete
failure ordering, concurrent alternatives, source sharing and combined multipath
behavior still require validation. The historical sections below may describe
limitations superseded by the current public API.

The endpoint now connects its authorized registry to scoped route reservation and
submits selected foreign packets through the source writer's bounded queue.
Ordinary packets retain an owner-side pending completion; cached close packets
use copied one-shot submissions with closing expiry bounding their lifetime.
Borrowed descriptors are cleared after submission. Candidate servicing recognizes
registered foreign sockets without making them listener-local addresses.

A path whose established foreign output cannot reserve capacity suppresses its
output-dependent recovery/pacing/MTU deadlines until another selection attempt;
path validation expiry and connection lifetime deadlines remain eligible. Candidate
probes retain their existing absolute validation deadline. Capacity and publication
wakeups resume selection. Regular pending writes still hold further construction
until completion. Failed writes on captured, uncommitted local alternatives now
cancel that candidate and its matching registration while preserving established
output. Zero-byte failure removes the latest probe from recovery and rejects later
ACKs for that packet number; partial results retain recovery state. Cancellation
under retirement-queue pressure remains retryable. A late result cannot remove a
replacement candidate/registration, and switching waits for pending completion.
Established foreign-path failures retain connection-removal fallback; broader
failure/cancellation combinations still require validation.

TestForwardOutputRuntimeProbeAndSwitch drives real v1/v2 UDP sockets with separately
owned source and destination loops. It receives an encrypted staged challenge from
the foreign source address, returns proof through CID forwarding, checks validation
without automatic switching, explicitly commits, and receives STREAM data and
application close on the foreign socket. The peer uses this implementation's keys;
this internal fixture is complemented by the public and independent tests above.
Broader failure/cancellation races and multiple alternatives remain unfinished. The sections below retain development
history, including limitations superseded by this current runtime checkpoint.

Reference contract: [quic-go migration documentation](https://quic-go.net/docs/quic/connection-migration/),
checked 2026-09-30. AddPath associates a Transport without sending a probe;
Probe validates reachability; Switch later selects the path for application data.
[RFC 9000 section 9](https://www.rfc-editor.org/rfc/rfc9000.html#section-9)
permits validation from a new local address before migrating.

The existing Conn.Migrate opens an owned socket, validates and switches as one
operation. It includes PING in candidate challenge packets to nominate migration
at the peer and holds ordinary output on the old tuple while that nomination is
pending. That policy must not be reused unchanged by a staged Probe call.

Internal prepareLocalPathProbe now reserves a fresh CID and candidate under the
existing admission gates, then marks it for explicit switching. Candidate output
omits PING, old-path STREAM/ACK output remains eligible, and endpoint servicing
retains a validated candidate without committing it. Existing explicit commit
checks still require current ownership, a sent reserved CID and validation.
Socket failure and revoked reservation cleanup still apply while a validated
candidate waits. Existing immediate and preferred-address migration modes retain
their previous behavior. At most one local candidate per path is supported here.

TestStagedLocalProbePreservesBinding uses real handshakes and packet encryption,
v1/v2 and base-only/multipath modes. It decrypts the outgoing candidate packet,
checks that it contains a challenge and no non-probing frames, constructs an
authenticated response using the peer's keys, and checks old-tuple STREAM output
before and after proof. Repeated endpoint service cannot switch the binding.
Explicit commit succeeds; cancellation, missing candidate socket and revoked CID
make a later stale commit fail. This is an own-engine test with synthetic peer
proof, not an independent end-to-end staged path exchange. It does not yet test
an unreachable old path, multiple concurrent candidates, or delayed switching
under sustained loss and CID rotation.

The next peer-receive audit found that ordinary probing-only packets on a new
client tuple were dropped before PATH_RESPONSE queuing. The server now admits a
bounded probe candidate on authenticated PATH_CHALLENGE, retains a replay watermark
across validation expiry, and separates proof from non-probing nomination. Traffic
on the established tuple retains proof while clearing any older nomination.
A newer non-probing packet on the candidate is still required to switch.

TestStagedProbeRealPeerExchange now exercises mutual validation using both engines
without synthetic responses, preserves old-path STREAM output, and completes an
explicit switch followed by server nomination. TestProbingCandidateNominationAndReplay
covers expiry without extension, replay after expiry and reordered nominations.
TestUpstreamStagedProbeIndependentServer uses pinned upstream AddPath/Probe/Switch
against the independent server in v1/v2, including a successful old-path stream
between Probe and Switch. These tests do not supply the missing local public API.

Repeated independent probing exposed a second lifecycle gap: an unused validated
candidate could occupy the sole slot indefinitely after the peer abandoned it.
A newer authenticated ordinary probe can now replace an unselected ordinary
candidate. Preferred-address and non-probing-nominated candidates are preserved;
stale/replayed probes cannot displace current state. Existing reservation cleanup
retires CIDs spent on displaced candidates before further probe output.

The upstream fixture now performs six successive switches per version. Its
abandonment variant additionally probes and closes five unused paths between
switches, checking old-path streams before each switch. Both variants pass in
v1/v2. The abandonment fixture timed out on the first replacement probe in both
versions before this fix. Single-candidate replacement is still not simultaneous
multi-candidate retention or proof of all path-churn/loss combinations.

FuzzMigrationSelectionAuthorization adds an event-history safety oracle for
ordinary probing, reordered non-probing packets, reachability proof, expiry and
commit attempts across three tuples. It records candidate birth, independent
proof events and the latest external nomination, then requires every successful
commit to agree with them. Non-commit operations and failed commits must preserve
the established binding. Inputs are bounded to 768 bytes (256 events). It assumes
authenticated inputs and valid proof at the selection boundary; it does not
verify cryptography, token matching, preferred-address authorization or CID
retirement. A two-worker 10-second campaign passed 12,259 executions (11.399s
including termination). This is bounded safety evidence, not exhaustive model
equivalence or sustained fuzzing.

The runtime now has an on-demand packetHandoff inbox as a cross-owner transport
primitive. It holds at most 64 receivedUDP records and transfers the pooled buffer
only when offer succeeds. Full/closed inboxes leave ownership with the caller;
reader errors and missing buffers are rejected. A mutex orders offers against
shutdown, which rejects later producers and drains retained buffers. Its channel
stays open to avoid send/close races. No queue allocation occurs until installed
on the receiver owner.

UDPServer consumes handoffs through the same serialized receive helper as socket
input. The receiving owner still performs classification, authentication and
connection mutation. Tests cover FIFO metadata, overflow ownership, concurrent
producers/shutdown and delivery through a real runtime using a raw marker packet.
The inbox fixture alone does not prove CID-based cross-Transport QUIC delivery.
The subsequent registration work below adds dispatch; foreign-local authorization
and output routing remain missing.
No second reader or cross-owner synchronous call is introduced by this primitive.
The default receive loop now has an additional disabled select case; its potential
performance cost has not been measured.

CID-based receive forwarding is now implemented internally on the source owner.
A handoffRoute atomically replaces a complete set of 1–20-byte CID reservations,
bounded to 4096 foreign CIDs per router. Prefix collisions are rejected against
both local and other foreign identities; local CID publication also checks foreign
reservations. This supports distinct CID lengths without ambiguous short-header
matching. Zero-CID local routing cannot coexist with foreign reservations because
foreign peer/address constraints are not yet registered.

Short-header input (including greased QUIC bits) moves its buffer to the registered
inbox before raw classification. Full/closed inboxes cause a drop with no local
admission/reset fallback. An internal forwarded flag limits packets to one owner
handoff, preventing cycles. Long headers are not forwarded by this path facility.
Tests cover copied CID ownership, atomic rejection/replacement/removal, prefix
ambiguity, local-first/foreign-first and zero-CID collisions, actual UDP-to-inbox
dispatch, queue-pressure/closed-target drops and loop prevention. The socket test
uses a short-header-shaped marker, not an authenticated cross-owner QUIC exchange.

Registration and binding still require explicit internal calls outside owner
callbacks. Once bound, CID snapshots now follow issuance/retirement before
advertisement. Stateless-reset token forwarding and creation/binding of routes
from the public path APIs remain to be implemented.
Registration preflight scans existing reservations and has not been benchmarked
at its capacity limit. Receive-side foreign-local authorization is described below; cross-Transport
output and public path integration remain missing, so this does not enable AddPath.

Runtime-level forwarding registration cleanup is now implemented. The destination
inbox tracks subscriptions under its mutex; closing it atomically revokes them
and posts a coalesced wakeup to each source. Sources prune their own routing maps,
including before CID collision checks, without waiting on another owner. Source
shutdown unsubscribes its outgoing routes. Explicit removal is idempotent and
revokes the handle; replacing a CID set with an empty set preserves the handle
until removal or runtime closure. Registration count is bounded to 4096 even for
empty CID sets. Updates check revocation after pruning to prevent resurrection
when pruning overlaps destination shutdown.

Race tests cover both shutdown directions, explicit/repeated removal, empty
registrations, reuse of released CIDs, stale updates, concurrent update/shutdown,
and reciprocal runtime closure. This cleanup is scoped to the socket runtimes;
automatic CID publication still needs to create and bind the registrations.

An internal destination-owner binding now associates an existing registration
with a routed connection. It rejects foreign runtimes, unregistered/closed
connections, revoked handles and reassignment to another connection; repeated
binding to the same connection is harmless. Router.Remove revokes all bound
registrations and wakes their source owners. Closing/draining connections retain
their routes until final removal. Source-side explicit removal unsubscribes and
releases the destination's connection reference. Unrelated registrations survive
connection removal. Connection pointers are used as identities under the inbox
mutex; source owners do not inspect their state.

Tests exercise v1/v2 closing and draining expiry with real engine handshakes,
unrelated-subscription retention, explicit unsubscribe, binding validation and
live runtime wakeup after connection removal. Registration and binding remain
explicit internal operations; subsequent CID rotation uses the publisher below.

Forwarding routes now support asynchronous complete-set CID updates from another
owner callback. Each request owns copied IDs and a buffered, single-result
acknowledgement, with an optional coalesced notification. The source runtime
applies updates on its existing route wakeup and reports successful installation
or the collision/capacity error. Rejected updates retain the previous CID set.
One request per route and 4096 aggregate pending CID entries per source bound
queued state, including duplicate entries. Revocation completes pending requests
with ErrEndpointClosed and returns pending capacity exactly once; processing
never waits for the requester to consume its result.

Tests cover immutable request bytes, dispatch before/after acknowledgement,
collision atomicity, empty replacement, full notifications, invalid IDs, pending
capacity/reuse, source/destination/connection revocation and simultaneous updates
submitted from reciprocal owner callbacks. A successful acknowledgement records
installation and does not guarantee the route has not subsequently closed. The
snapshot and scheduling integration below consumes these acknowledgements.

Connection-owner snapshot publication is now available internally through
Router.publishForwardCIDs. Committed local routing changes advance a revision;
unchanged Sync calls retain it. Every bound registration tracks its requested
and acknowledged revisions. An acknowledgement for an older snapshot cannot
authorize the current CID set: the owner submits the latest snapshot and remains
unready until all bound foreign sources confirm it. Repeated binding to the same
connection preserves publication state. Publication errors remain visible and
do not themselves remove the primary connection. Closing the connection/inbox
still revokes the routes and pending requests.

Tests cover v1/v2 issuance while an older snapshot is pending, independently
delayed acknowledgements from two sources, stable repeated polling, same-count
CID retirement/replacement, collision rejection and additional multipath path
CID issuance. This helper must exclusively manage updates for its bound routes;
manual updates must not be mixed with publication.

ServerEndpoint now refreshes publication after network activation/local migration
servicing and before packet construction. Pending or rejected reservations hold
NEW_CONNECTION_ID and PATH_NEW_CONNECTION_ID frames, including retransmissions;
other queued traffic remains eligible. Selection preserves held items without
marking them sent or advancing packet numbers. Both legacy and path packet
builders skip held advertisements, preserve datagram size-drop handling, and
allow PTO PINGs when eligible retransmissions are unavailable. Removing a failed
alternative releases the hold without closing the primary connection.

Acknowledgements and binding/removal events wake the receiving runtime through
an on-demand channel. Source processing also wakes remaining destinations when
shared pending-ID capacity becomes available, including cancellation before
application. A publisher rejected before it could enqueue a request can retry.
Tests exercise v1/v2 legacy/base/multipath output, activation before snapshots,
held send accounting, ordinary STREAM progress, PTO, datagram shrink, failed
alternative removal, cross-destination capacity wakeups, and actual UDP output
resuming from an acknowledgement without another application command.

These gates control CID advertisements, not permission to use a foreign local
address. The future public path controller must await initial route readiness,
observe publication errors/revocation and dispose of failed alternatives before
probing or selecting a foreign socket. Receive-side authorization is implemented
below. Foreign candidate/socket
lifetime integration and output routing remain missing; no complete authenticated
cross-Transport exchange is claimed.

Receive forwarding now carries the registration identity that selected the
packet. Registrations capture the source runtime's primary local address;
packets received by another socket on that runtime are not forwarded under that
registration. A reader failure revokes registrations for the failed local socket.
The destination checks live subscription/binding, exact source local address,
concrete tuple, confirmed address-aware connection and CID selection for that
same connection before invoking its normal authenticated path receive logic.
Foreign addresses are not inserted into the endpoint-wide local-address set.
Missing/revoked/mismatched provenance, long headers and unknown traffic have no
listener, raw-packet or stateless-response fallback.

Tests exercise v1/v2 base-only and multipath path-zero candidate responses using
real handshake keys: only the matching registration admits encrypted proof, and
proof does not switch the established binding. Wrong connection/local/peer,
missing/unbound/revoked provenance, bad ciphertext and closed endpoints cannot
validate the candidate. Source socket scope, source failure cleanup and real UDP
handoff provenance are also checked. The encrypted test injects a prepared
candidate and peer proof directly at the endpoint; it is not a complete runtime
cross-Transport probe exchange. Candidate servicing still assumes owned sockets,
and cross-Transport output remains absent. These must be integrated before public
path creation/probing can use this receive authority.

A foreign socket writer is now available as an internal primitive. The socket
owner lazily installs a 64-request inbox; offers copy up to 65535 wire bytes and
return a buffered result plus an optional wakeup. Overflow leaves responsibility
with the caller, while closure fails queued requests and rejects later offers.
The owner rechecks live registration, connection binding and concrete local
socket identity immediately before writing. Requests contain no connection or
recovery pointers beyond the opaque registration's existing identity links.

The writer performs exactly one socket write and returns its exact byte count
and error. It neither retries nor changes ECN/recovery state. Generic sockets
reject marked writes before sending bytes; the requesting owner must decide
whether it can roll back ECN accounting and retry without marking. Partial writes
and EMSGSIZE are preserved for owner-side handling. Tests cover copying/FIFO,
queue capacity, completion/notification pressure, concurrent close, queued-route
revocation, unbound rejection, actual UDP source/payload, and no retry on success,
partial writes or local errors. The live fixture sends a marker, not a QUIC flight.

This writer is not yet connected to Outbound scheduling. That integration must
preserve borrowed output bytes and baseLarge metadata, handle queue pressure
before committing an unsendable packet, and apply completion only on the owning
connection runtime. Existing HandleWriteError assumes synchronous completion:
its latest-packet checks and idle snapshot restoration cannot simply be invoked
after arbitrary intervening sends/receives. Pending-write ordering, ECN retry,
PMTU handling and closure need explicit completion semantics before foreign path
probing or application output can use this primitive.

The writer now offers explicit pre-construction capacity reservations. Queued
requests plus outstanding tickets share the same 64-slot limit, and ordinary
offers cannot take a reserved slot. A ticket is consumed by one submit or cancel;
submit copies wire bytes and cannot fail from competing queue pressure. Invalid
submissions consume and release their ticket. Closure/revocation remain possible
lifetime failures and are rechecked before submission/execution.

Cancellation, dequeue, invalid submission, route revocation and writer shutdown
wake blocked registration owners through their existing publication channels.
Waiter state is bounded by the router registration limit and revoked entries are
pruned. The source owner prunes invalid tickets on route-change wakeups, so a dead
destination does not require its old requester to return and cancel reservations.
Tests cover competing offers, exactly-once submission, post-submit cancellation,
invalid/revoked/closed tickets, capacity reuse/wakeup, concurrent submit/cancel,
and runtime destination shutdown with another registration waiting for capacity.
The scheduler does not yet use these tickets; packet commitment/completion and
PMTU/ECN integration remain outstanding.

Outbound.retain now copies wire bytes and all optional PMTU/large-packet metadata,
including the reusable baseLarge storage. Connection/path/send-item identity
pointers deliberately stay shared with the connection owner; the retained
Outbound must never be sent to the foreign socket owner. Tests build v1/v2 MTU
probes, large STREAM/DATAGRAM and base large packets, clear the borrowed buffers
and descriptors, and successfully apply the retained packet's write rejection.

Idle rollback is now guarded by a connection activity revision captured in write
metadata. Every engine send/receive activity event advances the revision, even
with equal timestamps. PMTU rejection restores the pre-send idle state only if
no newer activity occurred; restoration itself advances the revision. All five
write-metadata cases preserve later authenticated peer PING activity in v1/v2.
A temporary overlay restoring the old unconditional rollback failed all ten
regression cases; the fixed implementation passes. Equal-clock event replay has
an additional focused test, while existing synchronous rollback tests still pass.

The connection-owner completion primitive now retains the outbound descriptor,
consumes a pre-construction write ticket and polls the exact socket result once.
It retries unchanged ciphertext only after a zero-byte unsupported-ECN error and
successful recovery-accounting rollback. If another producer occupies the freed
queue slot, retry waits for capacity and its existing owner wakeup without
repeating rollback or packet construction. Retry errors cannot trigger another
ECN retry. Closed connections discard results without recovery mutation.
Submission failures synthesize a completion and notify the owner, so they cannot
leave a caller waiting for a socket result that will never arrive.

Short successful writes become io.ErrShortWrite; positive byte counts and errors
are preserved and must not be treated as atomic unsent-packet rejections. The
completion helper deliberately leaves alternative-path failure policy to the
future controller. Tests cover queue pressure/wakeup, closure/revocation,
submission failure, partial results, absent ECN commit and repeated polling.
V1/v2 encrypted-packet fixtures verify retained metadata through completion and
PMTU rejection for all five write-metadata forms.

The endpoint scheduler now consumes pending foreign completions before selecting
the next packet for that connection. Pending output blocks both endpoint and
base/bound-multipath packet construction. Another connection remains eligible.
Successful completion releases the hold; atomic zero-byte failure enters retained
PMTU handling, while a positive-count failure removes the connection without
unsent-packet rollback. Duplicate starts reject and release the extra ticket.
Close, closing, draining and endpoint removal release the retained descriptor;
late results cannot mutate disposed recovery state.

While a foreign write is pending, deadline selection and timeout processing defer
loss/PTO, pacing, keepalive and path-validation work requiring output. Handshake,
idle, old-key retirement and closing/draining deadlines continue normally. This
prevents repeated expired send-deadline wakeups and lets idle expiry terminate a
stalled connection. Tests cover both legacy and multipath scheduling, both QUIC
versions, full versus partial PMTU failure, unrelated-connection progress,
key/idle expiry and terminal transitions.

This integrates completion consumption and gating, but does not yet originate
foreign writes from live path selection. The live scheduler still needs to select
a foreign route and reserve capacity before packet commitment, then initiate the
write with the endpoint wake channel. Foreign socket/candidate lifetime and
alternative-path failure handling are incomplete: the current completion error
policy retains the ordinary connection-removal fallback. Delayed completion also
needs broader validation against ACK/loss/path-retirement interactions and queue
latency before claiming complete asynchronous send semantics.

The subsequent probe-output audit found two gaps in the shared migration sealer:
it could be called directly while a foreign write was outstanding, and it did not
include emitted probes in connection send statistics. It now respects the pending
hold and publishes packet/byte totals after a successful emission, matching normal
path output. Regression tests first reproduced both failures in v1/v2 peer/local
challenges, staged probes and responses; fixed tests also cover retries and
credit-blocked attempts. This correction does not add foreign-route selection.

Packet selection now has a scoped route-capacity gate. The controller supplies
owned local addresses and authorized, publication-ready foreign writer/route
pairs. Candidate probes reserve before key preparation and sealing; ordinary
path packets reserve after their pacing/congestion gates and before sealing.
Cached close output reserves before emission. Packet numbers, flight/credit and
emission counters cannot be committed by regular/probing output when the selected
foreign writer has no capacity. The gate returns no packet for that route, so a
staged candidate's blocked queue does not prevent established-path STREAM output.

The selector returns the selected foreign ticket with the immutable output tuple.
Reselection, construction errors, empty output and mismatched transfer cancel
unused tickets. A successful transfer leaves submission/cancellation to the caller.
V1/v2 tests fill a writer, verify pre-commit invariants, send on the original path,
free one slot, reserve and decrypt the candidate packet, and submit/complete it
while competing offers cannot steal the ticket. Further tests cover established
foreign output, revoked routes, construction failure, and cached-close attempts
that produce no output and must release their ticket.

This selector is not yet invoked by the live endpoint route controller. That
controller must populate the authorized map after CID-publication readiness,
maintain foreign candidate/socket lifetime, distinguish route-capacity waits from
expired send deadlines, and connect tickets to runtime submission. Cached closing
output still needs its own asynchronous completion policy because ordinary pending
write setup rejects terminal connections. Alternative-path failure isolation and
public migration APIs remain unfinished.

The endpoint now owns a bounded foreign-writer registry, separate from its owned
local sockets. Binding requires the endpoint's live confirmed connection, matching
source-writer/router identity, the exact handoff registration bound to that
connection, and a concrete non-owned local address. Duplicate identical binding
is idempotent; another registration for the same local address is rejected.
Removing or pruning a closed/revoked writer unsubscribes its registration without
closing the primary connection. Endpoint connection removal clears these entries.

The registry builds scoped route snapshots only after publication polling. Each
eligible registration must acknowledge the current local CID revision itself;
pending, failed, stale or revoked publications cannot authorize output. A CID
snapshot-sync error yields no eligible foreign routes, rather than authorizing an
older cached snapshot. Separate source acknowledgements remain separate: one can
be ready while another waits, with CID advertisement gating still observing all
bound sources. Closure after snapshot creation is rejected again at reservation
and execution. Tests cover v1/v2 snapshot changes, invalid identities/addresses,
registration collision, disposal, per-source readiness and concurrent writer close.

The registry and selector still require connection in the live output loop.
Candidate lifetime must recognize registered foreign sockets without adding them
to listener-local addresses, and all pre-publication/capacity waits need bounded
wake/deadline behavior. Runtime submission, terminal-output policy and public
migration operations remain unfinished.

Next implementation requirements:

- Connect a candidate to an existing Transport without creating a second reader
  for its PacketConn or transferring socket ownership implicitly.
- Route connection CIDs received by that Transport to the original serialized
  connection owner. Keep registrations current through issuance, retirement,
  reset-token updates, key updates and connection shutdown.
  Publication must acknowledge foreign CID reservations before advertising them.
  Use an asynchronous owner protocol: a pending update cannot be returned as a
  Router.Sync error because ServerEndpoint.sync removes the connection on that
  error. Packet construction/send accounting must not commit advertising output
  while the corresponding foreign reservation is still pending.
  In particular, networkOwner.activate advertises initial multipath path CIDs
  during output preparation; snapshot/gating must follow this activation and
  still precede sealing and send-accounting commits.
- Route output through the nominated Transport while preserving ECN, PMTU/error
  handling and per-packet send accounting. Avoid synchronous owner-to-owner
  calls that can deadlock when migrations occur in both directions.
- Give public Path handles cancellation, repeat-call, close and error semantics;
  serialize Probe/Switch/Close with transport closure and connection failure.
  Preserve the independent OpenPath multipath API alongside migration paths.
- Extend beyond the current one-candidate-per-path restriction to support the
  reference's multiple probed alternatives. Account for each CID reservation,
  validation timer and retained response queue.
- Validate both independent peer roles, socket wrappers, primary Transport
  closure, CID retirement, loss/reordering, repeated switches, and simultaneous
  connections on both Transports before claiming API parity.
