# Normative evidence audit

This is an incremental requirement-to-code/test map, not a conformance certificate.
Reviewed incrementally against the current worktree through 2026-09-30. A row records inspected
implementation and test evidence; it does not certify unlisted requirements,
combinations, errata, security properties or independent interoperability.

## RFC 9000 section 10.3.1: indexed alternate local reset candidates (2026-09-30)

A source-owner index transforms used active tokens with keyed HMAC-SHA256 for
lookup, preserving duplicate candidates and final live tuple/constant-time token
checks. Router.Sync updates membership before endpoint output is exposed; removal
releases owner references. Bootstrap is lazy, and foreign connection state is not
read. Unit tests exercise first use, retirement, duplicates, stale hits and owner
lifetime. Omitting incremental publication fails both independent v1/v2 reset
cases that create a local target after foreign traffic has enabled the index.
Source: [RFC 9000 reset detection](https://www.rfc-editor.org/rfc/rfc9000.html#section-10.3.1).

## RFC 9000 section 10.3: reset tokens with long headers (2026-09-30)

Receivers must recognize a valid reset token regardless of header form. Local
fallback and foreign token-hint selection now accept long-header datagrams, with
live tuple/token checks at the destination. Foreign CID routing parses long DCIDs
first, so a coalesced datagram's authenticated 1-RTT tail retains precedence over
another connection's matching reset token. Malformed long headers can still use
otherwise-unroutable token hints.

TestForeignLongHeaderResetHint, TestEndpointResetCIDCollision and
TestForeignCIDResetReturn cover malformed prefixes, local collisions, coalesced
authentication and closing-state precedence in v1/v2. Disabling long-header CID
selection fails all four coalesced authentication cases. The independent
TestStatelessResetLongHeaderCollisionIndependent combines an upstream-generated
reset token with a fixture-generated Handshake prefix and observed live CID;
all eight owner/target/version combinations retain the unrelated connection.
This tests mandatory receiver tolerance, not upstream generation of long resets.
Source: [RFC 9000 section 10.3](https://www.rfc-editor.org/rfc/rfc9000.html#section-10.3).

## RFC 9000 section 10.3.1: foreign CID to another reset owner (2026-09-30)

The socket owner now reserves a bounded return only for a distinct, uniquely
matching reset target across local connections and foreign token hints. The first
foreign owner reports authentication/reset acceptance before fallback. An
unauthenticated packet returns with its original buffer and recorded target;
the source rechecks current identity/tuple/token eligibility before local receipt
or one final foreign handoff. The final owner rechecks its live state, including
stale published hints. Authenticated closing and invalid-key-phase input suppress
return. Runtime shutdown cancels or rejects outstanding reservations.

TestForeignCIDResetReturn covers v1/v2, both target locations, stale/replaced and
ambiguous targets, bounded queues, lifetime and buffer provenance. Independent
TestStatelessResetForeignFirstLocalTargetIndependent and
TestStatelessResetForeignFirstForeignTargetIndependent force observed-CID collisions
with upstream restart resets while original sockets are disabled. Both require the
typed reset and continued bidirectional transfer on the unrelated connection.
Removing return reservation fails all four cases. This does not establish global
reset ambiguity handling for other dispatch paths or all multipath combinations.
Source: [RFC 9000 reset detection](https://www.rfc-editor.org/rfc/rfc9000.html#section-10.3.1).

## RFC 9000 section 10.3.1: local CID to foreign reset fallback (2026-09-30)

Source-side authentication now precedes forwarding a CID-colliding reset to a
foreign connection owner. A bounded one-hop transfer carries reset-fallback
provenance; the destination rechecks registration, tuple and token state and
retains authentication-before-reset handling. Revocation or retirement after
publication cannot authorize receipt. The independent restarted-upstream fixture
migrates the reset target onto a resident connection's socket, deliberately
collides with its observed live CID, disables the old socket and requires the
typed reset while resident traffic remains usable. Removing forwarding fails both
versions. Reverse foreign-CID-first evidence is recorded above.

Authentication progress includes closing-state decryption and authenticated
reserved-bit/key-phase violations. A next-key reserved-bit error is no longer
masked by failed earlier key attempts. The handoff fixture verifies closing and
key-phase error precedence against matching foreign reset suffixes. Separate local
protected fixtures exercise reserved-bit errors under next keys. These are not
independent-peer malformed-header tests; full closing/key-update combinations
remain an audit gap. Source: [RFC 9000 reset detection](https://www.rfc-editor.org/rfc/rfc9000.html#section-10.3.1).

## RFC 9000 section 10.3.1: locally owned CID/reset collisions (2026-09-30)

The owned-socket endpoint now attempts reset fallback after CID-selected input
fails to authenticate, including a reserved CID rejected by fixed-peer policy.
It selects only a unique matching reset target among the other locally owned
connections, with current used-token and tuple eligibility. Successful packet
authentication suppresses fallback even if the datagram suffix matches another
connection's reset token. Source: [RFC 9000 section 10.3.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-10.3.1).

TestEndpointResetCIDCollision exercises both QUIC versions, fixed-peer/network
owners, correct authentication, damaged ciphertext, wrong peer/socket, token
retirement and fallback ambiguity. The independent restart fixture preserves an
upstream-generated reset's token while deliberately rewriting its random prefix
to a CID observed on another live connection's short-header traffic, then verifies
that only the reset target closes. The corrected fixture fails in both versions
when fallback is disabled.
Cross-owner/forwarded-packet collisions and ambiguity during the router's initial
unknown-CID reset selection are not covered by this change.

## RFC 9000 section 10.3.1: migrated stateless resets (2026-09-30)

[RFC 9000 section 10.3.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-10.3.1)
requires reset detection when packet association or decryption fails, excludes
unused/retired connection-ID tokens, and requires token comparisons that do not
leak token values. Foreign sockets now receive immutable hints containing used
active tokens and established peer tuples. First-use output publishes its token
before the foreign writer can expose the packet. Source matching uses constant-
time comparisons; it only routes. Destination processing rechecks live tokens
and bound tuples and retains authentication-before-reset handling.

TestForeignResetRouting checks eligibility and stale-hint rejection;
TestForeignFirstUseResetPublication checks submission ordering. The independent
TestStatelessResetMigratedIndependentRestart receives a real reset from a restarted
upstream v0.63.0 server after public staged migration, requiring typed errors in
context and stream APIs. Removing forwarding makes both version cases expire.
Locally initiated probes now gain tuple eligibility only after using a still-owned
peer CID with a reset token. Unfinished expired attempts are rejected at receive
time; unused, unchecked, failed, detached, retired and abandoned attempts are
excluded. The independent probing restart fixture blocks both directions on the
original socket and verifies reset errors through the alternative Transport before
switching. Normal CID routing still has precedence; suffix/CID collision dispatch,
independent preferred-address and full combined-multipath coverage remain unproven.

## RFC 9000 section 5.1.2: retired migration reservations (2026-09-30)

[RFC 9000 section 5.1.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-5.1.2)
requires an endpoint to stop using connection IDs covered by an increased Retire
Prior To value and retire them. Existing peer-ID processing, candidate output
and migration commit checks reject revoked reservations. The public migration
handle now also checks reservation ownership before reporting usable proof.
This closes an API-state gap; the existing send and commit checks already
prevented the revoked CID from being used for a new committed migration.

A revoked candidate is marked failed even when unrelated spent reservations
make reliable retirement exceed queue capacity. Pending responses are cleared,
the candidate slot is retained for cleanup, and endpoint servicing retries when
capacity returns. TestMigrationHandleRejectsRetiredProof covers v1/v2 admission,
endpoint service and direct output under that pressure. Removing the new checks
and failure transitions fails all six cases. This is an owner-level state test,
not an independent encrypted NEW_CONNECTION_ID injection test or a complete
CID lifecycle audit.

## RFC 9000 sections 8.2.2 and 9.1: probing before migration (2026-09-30)

Sources: [validation responses](https://www.rfc-editor.org/rfc/rfc9000.html#section-8.2.2)
and [new-path probing](https://www.rfc-editor.org/rfc/rfc9000.html#section-9.1).
An authenticated challenge on a permitted new client tuple now reaches the
candidate response queue even without a non-probing frame. Probe proof alone
cannot authorize a server binding switch. The selection retains proof during
old-path traffic and requires a newer non-probing nomination before commit.
A packet watermark outlives validation expiry to reject replayed probe admission.

TestStagedProbeRealPeerExchange covers encrypted v1/v2 base/multipath mutual
validation, old-path data and subsequent switching. The isolated upstream
TestUpstreamStagedProbeIndependentServer covers real AddPath/Probe/Switch with
old-path stream delivery between probing and switching. Selection tests cover
expiry, replay and reordered nomination. Concurrent candidate tuples remain
limited to one per path. Newer ordinary probes can replace unselected ordinary
candidates, preventing abandoned proof from blocking future probes. Reordered
probes cannot replace newer state; nominated/preferred candidates are retained.
Independent repeated-switch and abandoned-probe cases pass in v1/v2. This is not
full multi-candidate response conformance.

## RFC 9000 section 13.1: acknowledgments of unsent packets (2026-09-29)

Source: [RFC 9000 §13.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-13.1).
Detectable ACKs of packets not sent SHOULD cause PROTOCOL_VIOLATION. ACKs apply
to the packet-number space carrying them (§12.5); optional ECN feedback does not
exempt a frame from this validation.

| Inspected invariant | Implementation | Evidence and limits |
| --- | --- | --- |
| Reject packet numbers at or beyond the next send number. | `internal/recovery/sent.go`: OnSent requires contiguous numbers; OnACK checks ranges and largest against each space's next value before ACK/ECN accounting. | `TestACKValidationAndCapacity`; the new protected-close matrix uses exactly nextPN, including zero when nothing has been sent. Intentional number skipping is not supported by this ledger. |
| Forgetting expendable packet metadata must not permit ACKs of future numbers. | Next send number outlives bounded pure-ACK timestamp storage. | `TestACKOnlyHistoryCannotExhaustRecovery` sends 10,000 packets against capacity four, accepts ACKs of sent packets and rejects packet 10,000. |
| Reject numbers consumed by locally failed sends. | OnACK checks the rejected-number ledger before accounting. | `TestRejectMTUProbe`, `TestRejectLargePacket` include later ACK rejection and migration; this is recovery-level evidence, not part of the new protected-close matrix. |
| Send the correct protected close after detecting an invalid ACK. | `connection.go:handleAck` selects 0x0a and the ACK frame type; `error_close.go` initiates the close. | `TestUnsentACKProtectedClose`: v1/v2, Initial/Handshake/Application, both endpoint roles, ACK/ACK_ECN (24 cases). The opposite engine decrypts and processes the close, verifies remote 0x0a plus trigger 0x02/0x03, and enters draining. |

The Handshake fixture exchanges real TLS flights but withholds client Finished
to retain keys on both sides. Application cases complete TLS first. Initial
cases use Initial protection before sending packet zero. These are own-engine
packet tests, not an independent-peer oracle. They do not cover every malformed
range, stale early-data ACK, multipath/path-retirement combination or optimistic
ACK defense. Lower-level tests and code inspection do not replace those gaps.

## RFC 9000 sections 13.1 and 13.2.1: authentication and ACK loops (2026-09-29)

Sources: [packet processing](https://www.rfc-editor.org/rfc/rfc9000.html#section-13.1)
and [sending ACK frames](https://www.rfc-editor.org/rfc/rfc9000.html#section-13.2.1).
Packets require successful authentication and processing before acknowledgment.
Non-ack-eliciting input must not provoke non-ack-eliciting output, even when packet
numbers have gaps. Earlier silent packets can be included in a later ACK.

`connection.go` removes protection before recording receipt. Frame parsing finds
ack-eliciting content; only that content sets ackPending. Duplicate non-eliciting
packets do not arm acknowledgment output. `NextDatagram` clears the pending flag
when emitting an ACK. Receipt bookkeeping precedes semantic frame handlers;
therefore bookkeeping alone is not proof that failed processing cannot emit ACKs.
Fatal semantic failures use the separate connection-close path.

`TestNonElicitingPacketGapsDoNotLoop` completes real TLS and tests v1/v2, both
roles, and ACK/ACK_ECN/PADDING (12 cases). It delivers packets 1000 and 2000 twice,
requiring an idle output queue and no pending ACK after each. A corrupted PING at
4000 cannot advance receive history or produce output. A valid PING at 3000 then
elicits exactly one ACK; the other endpoint's keys decrypt it and the test checks
coverage of 1000/2000/3000 but not 4000. These fixtures have no outstanding work
that would independently justify sending data.

This supplements the handshake-driven `TestEngineACKDoesNotCauseACKLoop` and
receive ECN tests. It does not establish semantic-failure atomicity, multipath
ACK scheduling, delayed-ACK policy, all range-eviction patterns, or independent
peer conformance. No production change was needed for these cases.

`TestSemanticFailureNeverEmitsACK` adds wire-output evidence for three semantic
failures after successful packet authentication and parsing: stream receive-credit
exhaustion, excess stream count, and data beyond a previously established final
size. All packets start with a valid PING; the final-size case also processes a
valid FIN before the contradictory frame. Across v1/v2 and both roles (12 cases),
the emitted protected packet must contain the expected transport close code and
offending STREAM type, no ACK or other application frames, and no pending ACK
output. The fixture decrypts and scans the entire close payload. `NextDatagram`
prioritizes closing output; closing setup clears pending ACK flags.

This proves no acknowledgment escapes for those failures despite receipt
bookkeeping preceding semantic handlers. It does not require or prove rollback
of earlier frame effects, cover every frame-error path, or establish multipath
closing behavior. Those remain separate audit work.

## RFC 9000 section 7.4.1: remembered transport state

Source: [RFC 9000 §7.4.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-7.4.1).

| Requirement or policy | Inspected implementation | Evidence and limits |
| --- | --- | --- |
| Exclude connection-specific IDs, addresses, reset tokens and ACK policy from remembered state. | `internal/wire/early_transport.go`: scalar snapshot embeds `EarlyLimits`; excluded fields cannot enter this record. | `TestEarlyLimitsExcludeConnectionSpecificState`, `TestEarlyTransport`; ticket codecs separately bind version. Needs full extension inventory. |
| Remember other understood parameters for early sending. | `EarlyTransport` retains credit, idle timeout, UDP size, migration flag and partial-reset support; DATAGRAM credit is included. | Codec and ticket-state tests cover storage; extension behavior is not certified by this core schema. Multipath plus early data remains unsupported. |
| Reject early data if mandatory remembered limits cannot be supported. | `internal/handshake/tls.go`: server resume event compares `EarlyTransport.Supports`; client completion checks accepted limits and remembered partial-reset support. | `TestEarlyLimitsCreditComparison` lowers each of eight fields; `TestTLSEarlyDataPolicy` exercises real TLS acceptance/rejection. |
| Use remembered credit for early packets, then promote stream limits for established sending. | `internal/transport/early.go`: `syncClientEarly`; `connection.go`: client promotion at TLS completion; `stream_limits.go`: updates existing send streams. | `TestStreamInitialCreditPromotion`, `TestResumedStreamCreditHandshakePromotion`, and the protected partial-handshake `TestEarlyCIDLimitPromotion` cover promotion. Frame-driven credit changes still need a comprehensive audit. |
| Optional stricter rejection for idle timeout, UDP size or migration policy changes. | Values are stored; server acceptance compares mandatory limits and partial-reset support through `EarlyTransport.Supports`, not these optional fields. | This stricter policy is not implemented. It is not a mandatory-limit failure. |

Implementation-specific observations:

- A follow-up scheduler audit found `issueLocalIDs` using fresh peer CID credit
  while early write keys could still be active. It now selects the remembered
  `ActiveConnectionIDLimit` while `sendingEarly()` is true. The new
  `TestEarlyCIDLimitPromotion` failed before this fix in both versions (four IDs
  issued against remembered credit of two) and passes afterward. The strengthened fixture splits the real server TLS flight between
  EncryptedExtensions and Finished and delivers protected packets until fresh
  parameters are visible with early write keys still active. It observes actual
  0-RTT output and two peer CIDs, then delivers Finished and verifies promotion
  to four. The same fixture now exhausts remembered stream/connection credit
  and stream count while Finished is withheld, and refuses newly advertised
  DATAGRAM support. After completion it verifies new credit without resetting
  consumed bytes and delivers a DATAGRAM using the new capability. No server
  reads can mask parameter promotion with MAX_DATA/MAX_STREAM_DATA updates.
  This is an engine-pair packet test, not independent-peer evidence.

- Invalid/missing private ticket records disable early capability without requiring
  ordinary PSK resumption to fail. `TestTicketLimitRecord`,
  `TestTLSRestoresTicketLimits` and `TestTicketVersionBinding` cover malformed
  state, ownership and version binding. Read `internal/handshake/ticket_state.go`
  together with the resume/store event handling, rather than treating codec
  round trips as proof of the TLS policy.
- Accepted-limit failure is converted by `internal/transport/error_close.go` to
  the implementation's protected protocol-error close.
  `TestAcceptedEarlyLimitFailureCloses` covers that mapping. A lower-level error
  assertion alone would not establish the emitted close behavior.
- Independent peer fixtures include `interop/early_accept_test.go` and
  `interop/early_datagram_test.go`. Their prior passing results support actual
  early packet interoperability in both roles, not exhaustive parameter coverage.
  They were not rerun during this documentation audit.

Validation in this audit: wire and handshake selected race tests passed (1.209s,
1.705s). Promotion and accepted-limit close tests were run separately with exact
names and passed (1.318s). The broad selection matched no transport tests, so its successful exit
is not counted as transport coverage.

Reproduce the relevant selections from the root:

```sh
go test -race ./internal/wire ./internal/handshake -run 'TestEarly(Limits|Transport)|TestTLSEarlyDataPolicy|Test.*Ticket'
go test -race ./internal/transport -run 'TestStreamInitialCreditPromotion|TestResumedStreamCreditHandshakePromotion|TestAcceptedEarlyLimitFailureCloses'
```

## Partial reset draft 11: released receive state (2026-09-29)

Sources: [draft-ietf-quic-reliable-stream-reset-11 §§4–5.2](https://www.ietf.org/archive/id/draft-ietf-quic-reliable-stream-reset-11.txt)
and [RFC 9000 §4.5](https://www.rfc-editor.org/rfc/rfc9000.html#section-4.5).
This review concerns the named draft, not a claim of final RFC conformance.

| Requirement or policy | Inspected implementation | Evidence and limits |
| --- | --- | --- |
| Charge final size to both flow controllers, including abandoned suffix bytes (§4). | `internal/stream/receive.go`: `resetAt` validates before committing final size; prefix completion consumes the discarded suffix. | `TestReceivePartialResetValidationAtomic`, `TestPartialResetReceiveLifecycle`, `TestPartialResetCanceledReceive`. Protected eviction test also checks total consumed credit after 1,025 releases. |
| Use the smallest reliable size; ignore a reordered increase (§5.2). | `Receive.resetAt` retains the minimum and trims buffered data; `Push` validates final size before discarding unneeded bytes. | `TestReceivePartialResetReductionAndOrdinaryReset`, lifecycle tests. Ignoring a larger reliable size must not bypass final/error consistency checks. |
| Detect changed error codes or final sizes and close with the specified error (§5.2). | Active receive state checks both; `partialResetHistory.validate` checks released streams while their records remain cached. Transport maps errors to connection transport errors with offending frame type. | `TestPartialResetReleasedValidation` uses TLS-negotiated, authenticated v1/v2 packets, both receiver roles, both reset forms, changed final/error, data beyond final and a contradictory FIN (24 cases). It verifies local closing, then delivers the protected close to the sender and checks remote code, offending frame type and draining. |
| Permit reset after FIN, including reordered arrival (§5.1). | Receive state preserves EOF already delivered to the application and validates later partial resets. | `TestReceivePartialResetFINAndWrap`; sender late full-data/FIN ACK optimization remains incomplete. |
| Keep historical metadata bounded (implementation policy). | `partial_reset_history.go` retains 1,024 FIFO entries; closed-ID ranges outlive metadata. | `TestPartialResetHistoryProtectedEviction` completes and releases 1,025 actual unidirectional streams via protected packets, in both TLS roles and v1/v2. Late contradictory frames for the evicted ID neither recreate objects nor change connection or stream-count credit; a changed code for the next retained ID still fails. No claim of lifetime contradiction detection. |

The 2026-09-29 follow-up rechecked
[draft 11 §5.2](https://datatracker.ietf.org/doc/html/draft-ietf-quic-reliable-stream-reset-11#section-5.2)
and expanded this test from server receivers only to both endpoint roles. Earlier
descriptions of both-role coverage for this particular test were overstated.
The new close checks establish peer-visible output rather than only a local error.
They use our own engine as receiver of that output, not an independent peer, and
do not inspect every other frame in the closing packet or cover multipath closing.

Sender-side follow-up: `TestPartialResetProtectedACKOrders` verifies that
authenticated ACKs traverse recovery and still preserve missing-prefix delivery
obligations. Its eight cases cover both versions, both sender roles and reset/data
ACK order, with repeated tail/reset ACKs. It decrypts emitted STREAM and reset
packets before constructing selective ACKs, then checks no release across a hole
and exact-once release after completion. This strengthens the direct-item ACK
tests; it is not independent-peer evidence. Late full-data/FIN completion without
the reset ACK remains an unimplemented optimization, not established by this test.

RFC 9000 §4.5 explicitly makes closed-stream final-size error detection optional
because retaining all final sizes can impose significant state. Draft §5.2 makes
closure mandatory when a receiver detects a contradiction, without specifying a
history retention duration. Our interpretation permits discarding old records:
known contradictions are fatal; unknown historical values are not reconstructed.
This is an implementation interpretation of the two texts, not an explicit draft
endorsement of a 1,024-entry cache. The cache bound does not prove all connection
metadata is bounded: closed-ID ranges and other resource limits require their own
audits. Bidi release, long-running adversarial sequences and combined-extension
history eviction remain separate coverage gaps.

The sender currently requires the current reset and its reliable prefix to be
acknowledged, as §5.2 specifies. It conservatively continues waiting for reset ACK
even if late ACKs of original full data plus FIN could prove an alternative
terminal state. Retired/trimmed item completion is not sufficient proof of full
FIN delivery. That alternative needs explicit retained coverage evidence before
being enabled; this audit does not claim it is implemented.

Validation: protected retained/evicted history regressions passed under the race
detector (1.535s); full stream/transport race suites passed (1.332s/40.059s),
and scoped vet passed. These are own-engine packet tests, separate from the independent
public API interoperability evidence in [partial reset status](PARTIAL_STREAM_RESET.md).

## Keepalive timing policy (2026-09-29)

[RFC 9000 §§10.1–10.1.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-10.1)
permits ack-eliciting traffic to defer peer idle expiry and requires a three-PTO
minimum for the idle timeout. It does not mandate a particular keepalive period.
The additional 1.5-base-PTO keepalive floor is a compatibility policy from pinned
[quic-go v0.63.0](https://github.com/quic-go/quic-go/blob/v0.63.0/connection.go),
not an additional RFC requirement.

Single-path `keepAliveDeadline` now takes the larger of that floor and the
configured period capped at half the minimum nonzero negotiated idle timeout.
PTO includes peer ACK delay after confirmation and excludes exponential probe
backoff. The multiplier saturates instead of wrapping. Disabled keepalive remains
disabled. Existing suppression after an ack-eliciting send leaves silent-peer
probing to recovery; queue attempts do not extend idle life.

`TestKeepAlivePTOFloor` failed on the prior implementation in v1/v2: it scheduled
a 1 ms configured period where the selected RTT/variance/ACK delay required
637.5 ms. It now checks no early queue, due protected PING output, authenticated
ACK rearming, changed RTT and duration saturation. Existing tests cover silent
expiry, backpressure and surviving multipath paths. A subsequent multipath
selection audit, below, extends the floor to the chosen eligible path. Full
reference scheduling parity is not claimed.

Validation: full root race suite passed (root 7.220s, transport 40.047s), strengthened
ACK-rearming regression passed separately (1.263s), and independent keepalive plus
migration selections passed (26.348s). The independent matrix includes disabled,
1 ms and 10 s configuration, both TLS roles and v1/v2. It tests liveness/expiry;
exact PTO-derived scheduling is asserted in the deterministic engine test. Both
module vet checks passed. The entire independent interop suite was not rerun.

## Multipath connection keepalive selection (2026-09-29)

Target rechecked: datatracker still identifies
[draft-ietf-quic-multipath-21](https://datatracker.ietf.org/doc/draft-ietf-quic-multipath/).
Its [section 5.9](https://www.ietf.org/archive/id/draft-ietf-quic-multipath-21.txt)
distinguishes connection idle expiry from optional per-path keepalives. It does
not prescribe the path-selection or interval policy implemented here.

`keepAlivePath` now supplies both timer calculation and PING enqueue. It chooses
the smallest base PTO among eligible idle application paths, breaking ties by
path ID. Eligibility excludes abandoned paths, missing peer CIDs, local migration,
pending MTU validation, unvalidated nonzero paths and unacknowledged queued work.
The selected path's PTO provides the saturating 1.5-PTO floor. Retained slow or
abandoned paths do not raise another usable path's interval. With no eligible
path, attempts remain spaced by the configured/capped interval and enqueue
nothing; attempts never count as idle activity. Congestion and pacing gates still
apply at packet output. This does not promise immediate transmission through a
blocked congestion window, keep every path's middlebox mapping alive, or replace
recovery's probes after an ack-eliciting send.

`TestMultipathKeepAlivePTOSelection` covers both versions and either path being
faster, all listed exclusions, deterministic ties, no early queue and authenticated
output on the selected path. Normal and tie cases also pass through the actual
scheduler, checking that a slow lower-ID path cannot borrow the PING. Tests with
unrelated blocked work inspect selected-path output directly. These fixtures
construct multipath state after ordinary TLS; public negotiated keepalive tests
separately cover both roles, both versions and primary-path retirement. Neither
fixture is independent multipath-peer evidence. Full root race tests passed
(root 7.157s, transport 39.993s) and root vet passed; independent interop was
not rerun for this change.

## Stream-count blocked signaling (2026-09-29)

[RFC 9000 §§4.6, 13.3 and 19.14](https://www.rfc-editor.org/rfc/rfc9000.html#section-4.6)
recommend signaling stream-count exhaustion and require current limits in
retransmitted blocked signals. Increasing MAX_STREAMS grants unblock opening;
nonincreasing grants have no effect. Receivers must not depend on a blocked signal
to grant further credit.

OpenStream previously returned ErrWouldBlock without producing STREAMS_BLOCKED.
It now queues one reliable notification per direction and limit, with fixed
32-byte queue charge and no stream object allocation. Queue pressure leaves the
notification retryable and does not fail the connection. Valid increases retire
older signals across queued/sent mappings; equal or lower values leave them
unchanged. Initial/accepted-early limit promotion uses the same retirement path.
Early rejection rebuilds the stream set and discards application output, including
notification state. Existing release-driven MAX_STREAMS advertisement remains
independent of incoming blocked signals.

`TestStreamsBlockedLimitLifecycle` covers both versions, roles and directions,
zero initial limits, queue pressure, duplicate application attempts, transmission,
simulated recovery loss, authenticated equal/increased/decreased MAX_STREAMS,
new stream admission and the new blocking limit. The independent
`TestStreamsBlockedUpstream` observes actual frames in the pinned peer's trace
while public synchronous opens remain blocked, then checks cancellation. These
are independent 1-RTT checks. Additional own-engine tests now exercise accepted
and rejected 0-RTT, loss of the isolated early notification, and larger limits at
accepted resumption. They verify true TLS resumption, no implicit peer stream
opening, retirement, reset of deduplication state after rejection and notification
at a new limit. The promotion case drops the old early signal so its retirement
cannot be credited to an ACK of that early packet.

A synthetic multipath fixture transmits the signal on path zero, fails that path,
and checks evacuation of the same logical item to path one. A protected
MAX_STREAMS on the survivor must retire both old sent mappings and transferred
queue mappings exactly once; a new blocking limit then queues on the survivor.
These extension checks cover both versions and stream directions. They are not
independent early/multipath peer evidence. The focused race selection passed
(1.545s), and scoped vet passed; production was unchanged. Byte-level blocked signaling implementations are mapped below; extension-specific
coverage remains incomplete. No universal flow-control
conformance claim is made.

## Stream-byte blocked signaling (2026-09-29)

[RFC 9000 §§13.3 and 19.13](https://www.rfc-editor.org/rfc/rfc9000.html#section-19.13)
recommend STREAM_DATA_BLOCKED when stream credit prevents sending. Recovery must
use the current blocking limit and stop retransmitting obsolete blocked signals.

WriteStream now reports stream-credit exhaustion before payload allocation, with
one reliable notification per stream/limit. Nonempty writes still return
ErrWouldBlock; empty FIN remains permitted. Local queue pressure leaves signaling
retryable without closing the connection. Connection-only exhaustion and empty
non-FIN writes do not generate this stream-scoped signal. Connection-wide
DATA_BLOCKED emission is handled separately, as mapped below.

MAX_STREAM_DATA increases and accepted-handshake limit promotion retire stale
stream signals; equal/lower grants do not. FIN and ordinary/partial reset retire
pending signals so recovery cannot keep advertising a terminal sender as blocked.
Streams that never reported blocking skip the retirement scan entirely.

`TestStreamDataBlockedLifecycle` exercises both versions, roles and uni/bidi
streams, queue pressure, deduplication, transmitted notification retirement,
protected grants, reblocking and all three terminal paths. A negative regression
distinguishes connection credit, queue pressure and empty writes. Independent
`TestStreamDataBlockedUpstream` uses a one-byte peer window, a public two-byte
write with deadline, and received-frame traces to verify the stream ID and limit
in both roles and versions. Own-engine early-data and multipath retirement tests
are mapped below; independent coverage of those transitions remains open.

The added packets exposed an existing pinned-peer v2 key-update defect in the
one-byte HTTP/3 setup fixture. Peer phase tracing and a diagnostic copy changing
only the peer's v2 key-update label isolated the cause. Default v2 setup coverage
uses two bytes; the strict one-byte failure remains opt-in. See
[known failures](../interop/KNOWN_FAILURES.md#http3-one-byte-setup-window-2026-09-29).
This is a remaining interoperability gap, not a production crypto workaround.

## Connection-byte blocked signaling (2026-09-29)

[RFC 9000 §§13.3 and 19.12](https://www.rfc-editor.org/rfc/rfc9000.html#section-19.12)
recommend DATA_BLOCKED when connection credit prevents sending. It reports the
connection limit, separately from stream-specific credit.

Nonempty writes now detect both constraints before payload allocation. Each
stream records whether it attempted a connection-blocked write; a connection
counter and notification flag deduplicate the reliable DATA_BLOCKED frame. Local
queue pressure preserves a later signaling retry. A stream's FIN or ordinary/
partial reset removes its pending interest, retaining the shared signal until
the last recorded blocked sender ends. Increasing MAX_DATA or accepted-handshake
credit retires the signal and clears recorded interest; nonincreasing grants do
not. None of these transitions returns already-used connection credit. Early
rejection rebuilds the stream set, including these fields.

`TestDataBlockedSharedLifecycle` covers both versions, roles, and uni/bidi streams,
queue pressure, duplicate/multiple waiters, transmitted notification loss,
nonincreasing/increased protected grants, three terminal paths and a new waiter
at the same limit. A separate test exhausts both credit types and grants each
independently. Independent `TestDataBlockedUpstream` observes the reported limit
in peer packet traces while public writes block after one byte, in both TLS roles
and QUIC versions. `TestDataBlockedSharedRecoveryUpstream` extends independent
coverage to two streams sharing a two-byte connection window, both TLS roles,
versions and stream directions. Public writes exhaust the grant, both streams
report deadline exhaustion, and peer traces observe DATA_BLOCKED at limit two.
Peer application reads replenish connection credit; both streams write another
byte, block again and produce a limit-four notification. Both then send FIN
without new connection credit, with exact payload and EOF checked at the peer.
The trace check allows retransmissions; it does not claim packet-level
deduplication or independent early-data/multipath loss/retirement coverage.

`TestByteBlockedEarlyGeneration` covers both byte-blocked frame types together,
v1/v2 and uni/bidi streams with real TLS resumption. Two streams exhaust both
credit types, producing two stream notifications and one shared notification.
Each notification must leave in the 0-RTT flight. Acceptance, dropped early
packets, rejection and increased handshake limits check retirement, queue charges,
shared waiter counts, deduplication and fresh blocking generations. The promotion
case drops the early packets so their ACK cannot explain stale-signal retirement;
application reads are deferred until after state checks to avoid fresh grants
masking failures.

`TestByteBlockedMultipathRetirement` uses a synthetic multipath fixture after a
real TLS handshake. It sends all three notifications on path zero, abandons that
path and requires the same logical items to transfer to path one. Protected
MAX_DATA/MAX_STREAM_DATA grants on the survivor remove old sent references and
transferred queue entries, refund exactly 96 charged bytes and permit fresh writes
and blocking notifications on the surviving path. These are own-engine lifecycle
checks, not independent multipath interoperability or a broad impairment matrix.

## Unmapped scope

The remainder of RFC 9000, RFC 9001 and RFC 9002 still needs clause-level review.
Extension-specific remembered state, v2/compatible negotiation, migration and
multipath require separate maps. Existing test volume does not substitute for
that review. See [remaining requirements](REMAINING.md) for the broader objective
and [interop exceptions](../interop/KNOWN_FAILURES.md) for excluded failures.
