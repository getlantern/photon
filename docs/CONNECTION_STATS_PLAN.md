# Connection statistics implementation audit — 2026-09-29

ConnectionStats now has a public snapshot API backed by the accounting layers
below. This document retains implementation decisions and remaining validation
gaps; matching declarations do not establish full behavioral parity.
The reference inspected is the locally pinned upstream v0.63.0 module, principally
connection.go, internal/ackhandler/sent_packet_handler.go and
internal/congestion/cubic_sender.go. No upstream implementation code is copied.

The reference API has ten fields: four RTT durations and six byte/packet counters
for sent, received and lost traffic. Its comments include retransmissions, exclude
UDP framing, describe RTT for the active path, and describe loss counts as
potentially decreasing after late acknowledgment. Those comments are not enough
to establish its behavior: inspected loss-counter writes only increment, and
ReceivedPacket calls are in decrypted-packet processing. A future differential
fixture must distinguish documented behavior from measured reference behavior.

## Existing engine state is insufficient

| Existing state | Why it cannot serve directly as the public statistic |
| --- | --- |
| Conn.authenticatedPackets | Internal stateless-reset detection counter; excludes unauthenticated packets and is not a routed-input packet count. |
| Conn.budget.Received | Anti-amplification accounting occurs after routing/header checks and must not be repurposed for observability; deferred replay must not charge a datagram again. |
| recovery.Sent packet maps | Track currently retained transmissions, with bounded pure-ACK metadata; removal is not equivalent to a lifetime counter. |
| recovery.Sent.BytesInFlight | Outstanding congestion-controlled bytes, not total bytes sent. ACKs, loss and key discard all reduce it. |
| congestionHistory | Bounded persistent-congestion evidence, now retaining validated packet byte sizes for correction events; eviction and interval retirement limit late-ACK reconstruction. It must not grow without bound merely to serve statistics. |
| Conn.RTTStats | Reads the recovery instance's path-local estimator; multipath needs an explicit selection policy and connection totals must survive path disposal. |

## Proposed implementation sequence

1. Introduce an owner-managed connection accumulator, distinct from recovery maps
   and anti-amplification credit. Record packet and byte events at named commit
   points. Define how locally rejected socket sends, Retry, version negotiation,
   coalesced packets, duplicates and undecryptable input contribute before wiring
   the public API. A datagram count cannot substitute for a QUIC packet count.
2. Add loss and late-ACK events with sizes before removing recovery metadata.
   Separate declared network loss, key-space discard, rejected local sends and
   path abandonment. Choose and document a bounded late-ACK horizon; eviction
   must not erase accumulated totals or allow repeated decrements. Public loss
   semantics must be explicit if they differ from the reference comments.
3. Aggregate totals across path creation/disposal and snapshot the selected
   primary path's RTT. Preserve the final snapshot after shutdown. Use existing
   owner serialization for mutation and a concurrency-safe publication mechanism
   for callers, including callers after the UDP owner has stopped. Do not read
   mutable recovery maps directly from the public method.
4. Expose all ten fields and the public method once backed by real accounting.
   Regenerate the root API inventory only after implementation and validation.

Required deterministic checks include exact packet-length accounting, coalescing,
duplicate and failed-authentication input, deferred handshake replay, loss followed
by a late ACK and duplicate ACK, retransmission, local send rejection, key discard,
version restart, migration and path retirement. Public checks must race snapshots
with active traffic and shutdown, retain final counters after closure, and exercise
both versions and endpoint roles. A separate independent-peer fixture should
compare the chosen semantics under controlled traffic; matching declarations or
nonzero counters is insufficient.

## Implemented foundation: late-loss correction events

Recovery Events now includes RecoveredPackets and RecoveredBytes. A validated
ACK moves retained lost history to acknowledged and reports its packet/byte
contribution once. It does not add those packets to Events.Acked, reintroduce
flight bytes, undo congestion responses, or expose a public statistics method.
Packet sizes use a uint16 after OnSent's existing 65,535-byte bound validation.

Corrections reuse the existing history horizon: eviction, key-space discard and
persistent-congestion interval retirement can remove evidence. A future loss
estimate can therefore retain false-positive loss outside this horizon. This
limitation must be documented if that estimate becomes public. No separate
unbounded ledger has been added.

Tests cover all three packet-number spaces, exact byte sizes, invalid ACK
atomicity, duplicate ACKs, discard, eviction, identical numbers in distinct spaces,
and a cumulative ACK extending beyond retained history. The latter verifies that
ordinary outstanding ACKs and loss corrections remain separate, and covers the
bounded map-scan path as well as the single-number lookup path.

## Implemented foundation: connection loss aggregation

Conn now owns lossAccounting with lifetime declared and recovered packet/byte
totals. Ordinary ACK-driven loss, explicit DetectLoss and loss-timer expiry feed
declarations; validated late ACK events feed corrections. Multipath ACK dispatch
and per-path loss timers update the same connection accumulator. Path abandonment,
Retry requeue and key discard are not declared network loss. MTU-probe and pure-ACK
packets are included if recovery actually declares them lost; congestion-response
eligibility does not filter these counters.

The accumulator is separate from path state and is not reset when a path is
abandoned. It retains gross declaration and correction totals separately; the public
loss estimate subtracts the latter without losing diagnostic history.
Direct accumulator access requires the serialized owner; the published snapshot
below provides concurrent public access.

TestConnectionLossAccounting covers v1/v2, ordinary/multipath ACK dispatch,
packet-threshold/time-threshold loss, exact byte totals, duplicate late ACKs and
key discard/path abandonment. Fixtures install controlled recovery metadata and
dispatch ACK frames through owner methods; they are not wire/independent-peer
statistics evidence. The later layers below add sent/received accounting and
shutdown publication; broader lifecycle tests remain open.

## Implemented foundation: committed sends

Conn now owns sentTotals, updated at successful emission in NextDatagram,
nextPacketOnPathSelection and nextClosingDatagram. Each output is one QUIC
packet, so packet count increases once and bytes increase by the emitted length,
including padding/protection and excluding UDP framing. Ordinary retransmissions,
probes and repeated cached close packets pass through these boundaries. Empty
output, failed preparation and merely sealing a cached close are not emissions.

The accounting boundary is engine commitment, before socket I/O, as in the
reference's packet-sent accounting. Locally rejected writes remain attempted
sends; these totals are not proof of delivery to the network or peer. Endpoint
stateless Retry/version-negotiation responses are outside a Conn and are excluded.
This policy does not modify packet-number, flight or anti-amplification accounting.

TestCommittedSendAccounting covers v1/v2 and ordinary/multipath output, exact
lengths, close preparation, close emission and suppressed close output.
TestCloseResponseRateAndCachedPacket checks two identical protected close sends
are counted twice while rate-limited attempts are excluded. The shared drain test
helper now checks each returned packet against counter deltas across handshake,
data and retransmission fixtures. Dedicated local-write-failure and MTU statistics
assertions remain open. Concurrent public access and receive accounting are
covered by the subsequent layers below.

## Implemented foundation: routed receive arrivals

Conn.receivedTotals is updated once at ordinary or bound-multipath datagram
ingress, after validating the supplied ECN metadata. Byte totals include the
entire routed datagram, irrespective of authentication or processing success.
Identifiable coalesced packet boundaries each count once. A short-header packet
consumes the remaining datagram; an unparseable remainder counts as one packet
attempt, without scanning its bytes for possible hidden packets. Empty input
counts neither bytes nor packets. Unknown traffic that never reaches a Conn is
outside these totals.

Accounting is separate from anti-amplification and authentication counters.
Deferred decryption/frame replay bypasses ingress, so it cannot add arrivals.
The short-header hot path requires no extra parse; coalesced long-header input
has an accounting parse pass before normal processing. No performance improvement
is claimed. This explicitly defined attempt-count policy still needs comparison
with independent peers; the reference's public comments alone do not prove parity.

Tests cover v1/v2 and ordinary/multipath duplicate, corrupt, malformed and empty
input. A reversed real TLS server flight requires both actual deferral and later
replay while checking exact arrival deltas. The coalesced-handshake fixture checks
multiple packets in one datagram. Public concurrent snapshots, closing/admission
edge cases, and differential accounting remain open.

This work does not establish full behavioral parity or a performance improvement. The
root missing-symbol count is now 60 of 246. Remaining work includes independent-peer
accounting comparisons and broader lifecycle checks.

## Public snapshot publication

The public ConnectionStats type and Conn.ConnectionStats method expose all ten
reference fields. The owner builds a value snapshot after packet emission,
receive processing and timeout handling. A short RWMutex-protected assignment
publishes it without allocating a new snapshot. Readers return a value copy and
never enqueue owner commands, making the method usable from owner-side callbacks.
Callbacks observe the last completed event, not the event currently executing.

RTT comes from the lowest-numbered surviving prepared path, normally path zero;
no RTT sample yields zero values instead of the initial PTO estimator. Totals
aggregate all paths. Close publishes before disposing path state and subsequent
reads retain that snapshot. Loss estimates subtract retained late corrections
from gross loss, floored at zero. This snapshot policy is explicit and is not a
claim that the upstream implementation offers an identical atomic view.

Public v1/v2 tests read concurrently with a real unidirectional transfer, call the
getter inside an owner callback, close both UDP owners, and verify retained final
values. Engine tests verify all ten fields, path-zero retirement and fallback RTT,
unsampled RTT, and final RTT after path disposal. Independent malformed-input and
loss-counter comparisons, wider migration lifecycle combinations, and sustained
performance/concurrency evaluation remain open.

## Independent-peer traffic validation

TestConnectionStatisticsIndependentTraffic in interop uses pinned upstream
v0.63.0 in both TLS roles and both QUIC versions (four cases). After a handshake
snapshot, it transfers 30,000 verified bytes in each direction on separate
unidirectional streams, including FIN. Sent and received packet counts must grow;
byte-count deltas must cover the complete payload in each direction. Observed RTT
must be positive and internally ordered. A peer application close must cancel
the connection context without decreasing retained sent/received totals.

All four cases passed under the race detector (1.400s), and interop vet passed.
This is independent-peer evidence that ordinary traffic updates the public API.
It does not compare exact wire byte counts or loss estimates with upstream,
exercise impairment/multipath/malformed traffic, or wait for final UDP-owner
disposal. The separate root test covers owner disposal and stable final snapshots.

## Protected-packet loss correction validation

TestStatisticsProtectedLossCorrection covers v1/v2 and both sender roles. It
completes TLS, clears prior application-space feedback with authenticated ACKs,
then emits four PING packets. The peer's keys decrypt each packet and verify its
packet number and frame; observed lengths provide the expected sent/lost bytes.
An authenticated ACK of the fourth packet triggers packet-threshold loss of the
first. The published estimate must increase by one packet and that packet's exact
length. A late ACK of the first packet must remove precisely that contribution.

Replaying the protected ACK packet and sending the same ACK evidence under a new
packet number cannot cause another correction or change send totals. Closing the
engine retains the corrected snapshot. The new test passed race (1.462s).
This is own-engine protected-packet evidence, not independent-peer loss behavior;
timer/multipath loss remains covered at the controlled owner-dispatch level.

## Independent late-ACK loss correction

TestStatisticsIndependentLateACK runs both endpoint roles against the pinned
upstream implementation in v1 and v2 (four cases). After an acknowledged warm-up, a UDP relay withholds one
full-sized short-header datagram while forwarding subsequent traffic. PMTU
discovery is disabled on both peers. Our sender's WaitAcknowledged completes
through retransmission while the original remains withheld; the public loss
estimate must still include at least that datagram's byte length and one packet.

The relay then forwards the exact original bytes. An actual upstream ACK must
reduce the published estimate by exactly one packet and the withheld datagram's
length. The receiver finally consumes and verifies the complete 30,000-byte
payload and FIN. The initial client-role matrix passed race (1.401s), including
ten repetitions (2.045s). The expanded role/version matrix passed race (1.541s)
and ten repetitions (2.744s, forty leaf cases); interop vet also passed.

The fixture deliberately delays application consumption of EOF until after the
correction. Earlier versions that read EOF first consistently lacked a late
correction. Failure-only peer qlog showed the delayed packet was received (for
example packet 9), but the subsequent ACK covered only a newer packet (36).
Consuming EOF permits stream-credit/ACK feedback that can retire the peer's old
receive history. The revised ordering keeps that exchange out of the experiment;
it does not weaken the exact correction assertion or change production behavior.
Bounded failure traces retain packet headers and ACK ranges, not stream contents.

This establishes a real independent ACK correction within retained histories for
both sender roles. It does not establish every peer's old-packet ACK policy,
upstream statistics parity, multipath loss, or WAN behavior.

## Protected multipath statistics

TestStatisticsMultipathCrossPathACK adds v1/v2 and both sender roles using the
prepared-path engine fixture. Four packets emitted on path one provide exact
packet/byte deltas. A protected PATH_ACK arriving on bound path zero acknowledges
the fourth path-one transmission, producing exactly one lost-packet contribution.
Path zero's flight accounting and RTT remain unchanged, and the published RTT
continues to select path zero.

A late path-one ACK delivered over path zero removes exactly that loss. Repeating
the same ACK evidence over path one, using a packet number already used on path
zero, adds an arrival but no duplicate correction. Abandoning path one and closing
the engine preserve aggregate counters. The initial four cases passed race
(1.380s); the final statistics/path-isolation/recovery selection passed (1.378s),
and transport vet passed.

These are authenticated own-engine packet tests on synthetically prepared paths,
not independent multipath negotiation or network scheduling evidence. Independent
multipath statistics, impairment combinations and WAN behavior remain open.
