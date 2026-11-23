# Datagram size and fragmentation status

Concrete-address Linux/macOS UDP runtimes now start discovery automatically
after handshake confirmation, and on additional paths after validation. The
public original-path payload limit defaults to 1280 bytes. Config.InitialPacketSize
clips nonzero values to 1200–1452. Authenticated probe ACKs can raise the usable
limit to the smaller of 1452 bytes and the peer's advertised limit. Public
Config.DisablePathMTUDiscovery disables upward probes, not the configured starting
size. Wildcard sockets use the configured size through the base packet builder,
but lack address-aware discovery. Oversized STREAM/DATAGRAM local EMSGSIZE now
uses the base builder's 1200-byte validation and fallback path as well. New or reset paths use
1200. The standalone engine retains a zero-value default of 1200. Full reference
migration-policy parity and constrained-path validation remain unfinished.

Larger STREAM writes reserve metadata for splitting onto smaller paths after
loss, migration or local EMSGSIZE feedback. Baseline writes use up to 1140 data
bytes, reserving worst-case packet/frame overhead. See the preliminary
[local transfer measurements](benchmarks/portable-stream-payload.md); those
measurements predate automatic discovery and are not its performance evidence.

[RFC 9000 section 14](https://www.rfc-editor.org/rfc/rfc9000.html#section-14)
requires avoiding IP fragmentation. The UDP runtime now configures sockets before
starting readers or transferring dynamically added sockets into its registry:

- Linux: IP_MTU_DISCOVER / IPV6_MTU_DISCOVER with IP_PMTUDISC_PROBE, including
  both protocol settings for dual-stack sockets. This keeps DF behavior while
  allowing future userspace probes to operate independently of cached route MTU.
- macOS: IP_DONTFRAG for IPv4, IPV6_DONTFRAG for IPv6 and dual-stack sockets.
  Apple's [IPv6 option handling](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/netinet6/ip6_output.c)
  sets INP2_DONTFRAG for mapped IPv4 output, consumed by the
  [IPv4 UDP output path](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/netinet/udp_usrreq.c).
  Applying IP_DONTFRAG directly to an AF_INET6 socket fails on the tested macOS host.
- Other platforms: explicit nonfragmentation setup is not implemented. The build
  fallback leaves kernel settings unchanged; platform conformance is unverified.

Setup errors on supported platforms reject registration without taking socket
ownership. A multi-socket constructor can have configured earlier sockets before
another socket fails; the caller still owns all of them. x/sys is a direct
production dependency for the socket options.

Tests inspect kernel option values for IPv4, IPv6 and dual-stack sockets and
exercise initial runtime ownership, dynamic registration and closed-socket
rejection. Runtime evidence currently comes from macOS arm64. Linux amd64 is
cross-compiled, not runtime-validated here. No packet-capture or low-MTU network
namespace evidence is claimed.

`mtu_search.go` now supplies a standalone bounded size-search component. After
an owner validates the 1200-byte baseline, it accepts a configured peer/local
ceiling and proposes binary-search sizes. Only committed probe packet numbers
can confirm a size. It permits one pending probe, retries a size up to three
times, accepts delayed ACKs for current-size attempts, and ignores abandoned
attempt evidence. A reset clears path-size evidence while preserving the packet
number high-water mark. The path initializer connects this component to
per-path packet construction and recovery; it is not the complete RFC 8899 state
machine. Eligible socket runtimes invoke it lazily during output selection.

Recovery now carries dedicated MTU-probe metadata through ACK/loss events.
Probes consume ordinary flight and congestion-window capacity. Their losses do
not reduce NewReno's window and cannot anchor persistent-congestion intervals;
ordinary losses still can, and an acknowledged probe still interrupts a loss
interval. This requires a sender to label only dedicated padded probes, never
ordinary application packets. The staged sender labels dedicated encrypted
PING-plus-padding packets separately from ordinary data.
Probes obey amplification, congestion and pacing gates and are not queued as
reliable frame retransmissions. ACK/loss feedback drives the search, including
a late ACK after recovery discarded the original probe metadata. Local and
remote migration commits reset size evidence.

Tests cover encrypted v1/v2 probe/ACK convergence, validation and peer ceilings,
blocked-send atomicity, packet-threshold loss, and late ACK confirmation.
Dedicated probe commits now also impose a minimum send spacing of one second
or the path's then-current smoothed RTT, whichever is larger. Rapid ACKs and
local EMSGSIZE feedback do not bypass it. Ordinary output is unaffected. When
discovery is otherwise ready, the sender exposes the remaining delay through the
existing path pacing deadline, allowing the owner to wake without new traffic.
The spacing survives migration to avoid a burst across tuple changes. This is
an additional rate policy, distinct from the discovery ACK-wait timer below.
Tests cover short/long RTTs, early ACKs, deadline visibility, ordinary output
and resumption at expiry.

A separate discovery ACK-wait deadline now expires after max(16 seconds, three
base path PTOs), excluding PTO backoff. The owner observes and services it with
other path deadlines. Expiry releases the pending search attempt but leaves the
packet in QUIC recovery: flight bytes, congestion evidence and packet numbers
are unchanged. A new attempt uses fresh packet protection and restarts the wait.
ACK/loss feedback, local write rejection and migration reset invalidate obsolete
deadlines. Late ACKs remain useful while the candidate is retained; three failed
attempts narrow the search. Tests cover timer expiry, bounded retries, retained
flight, late ACKs, slow RTTs and reset cancellation. This is still an incomplete
DPLPMTUD state machine; real network validation remains incomplete.

When search settles below the configured ceiling, a ten-minute upward-recheck
deadline is armed once. Duplicate feedback cannot postpone it. Expiry reopens
the upper interval while retaining the confirmed size and packet-number history.
Fresh probe ACKs are still required to increase that size. Searches at their
configured ceiling do not schedule fruitless upward rechecks. Migration clears
old maintenance timers while preserving the separate probe rate limit. Tests
cover v1/v2 completion after local EMSGSIZE, timer visibility and expiry, old ACK
isolation, fresh confirmation, ceiling suppression and reset cancellation.

A staged black-hole heuristic now consumes ordinary packet ACK/loss metadata.
Three large-packet losses plus an acknowledged later small packet conservatively
return the estimate to 1200 bytes and defer upward discovery for ten minutes.
Dedicated probe losses do not contribute. A newer acknowledged large data packet
or a successful probe at least as large as the estimate clears older failures.
Packet-number boundaries isolate migration and fallback episodes. Ordinary
congestion/loss accounting remains unchanged. This is an implementation policy,
not proof of an MTU failure or a mandated RFC threshold. Tests use real recovery
records for hypothetical larger ordinary output. An additional encrypted v1/v2
engine test now confirms a 3000-byte size, lowers a deterministic link's limit to
1200, and drops three large data packets. Normal PTO, small-packet ACK and loss
processing trigger fallback without injected loss metadata or a manual MTU reset.
Reserved splitting then completes a 7.5 KiB stream with correct payload/FIN and
full charge release. This proves the engine's simulated-link recovery sequence;
real kernel MTU/ICMP behavior remains unverified.

Loss-driven fallback now requests fresh baseline path validation. The next
path send initiates the existing padded 1200-byte PATH_CHALLENGE exchange and
holds STREAM/DATAGRAM traffic, including on path zero, until an expanded
challenge receives its matching PATH_RESPONSE. Packet ACKs alone do not clear
this gate. Shared borrowing cannot send application traffic on the gated path;
other healthy paths remain usable. Existing validation deadlines and failure
handling abandon a failing path and evacuate reliable data. Migration commits
clear obsolete revalidation requests. Tests cover path zero and secondary paths,
ACK-only evidence, matching responses, failure evacuation, and the encrypted
black-hole transfer now including successful baseline revalidation.

Baseline challenges now use one dedicated per-path control slot rather than
application queue capacity. Initial requests, PTO requests and validation retries
share it. Blocked sends retain the token without recording a sent probe or
consuming a packet number; response/expiry/reset clears it. A saturated-queue
test validates start, amplification blocking, retry and encrypted completion
without changing the application memory charge. Established-path responders now
have an eight-entry per-path FIFO outside application charges for PATH_RESPONSE
frames that would otherwise fail queue admission. The reserve preserves tokens,
prioritizes pending responses, obeys send credit and clears only after a committed
send. PTO reuses pending responses instead of allocating PING. Migration clears
old-tuple reserved responses. Exhausting both application capacity and all eight
reserved entries retains the existing resource-error policy; pending tokens are
never overwritten. Tests cover v1/v2 FIFO output, duplicate packet suppression,
blocked send state, reserve bounds and mutual full-queue baseline completion.

The multipath scheduler tries ordinary output and shared application data across
available and backup paths before discovery probes. Tests cover both path roles
and verify discovery resumes when the shared queue drains. Rebinding tests emit
real v1/v2 probes, commit the new peer port, and show that delayed old-tuple ACKs
cannot confirm the new tuple's search; fresh probe ACKs can.
Local migration tests also run encrypted mutual validation for v1/v2 in base and
multipath modes. They confirm a larger size before moving, leave a larger probe
outstanding, verify failed commits preserve its evidence, and verify successful
commits reset evidence and flight state without reusing packet numbers. A delayed
old-probe ACK is injected at the recovery boundary; a fresh probe and encrypted
ACK confirm the conservative starting proposal on the new socket tuple.

The endpoint now tags dedicated probe output and handles synchronous EMSGSIZE
without removing the connection. It narrows the candidate bound, drops flight
accounting without congestion loss, restores idle activity and rearms recovery.
Packet numbers remain consumed; a bounded rejection ledger rejects ACKs for
locally unsent probes even after a path reset. Exhausting that ledger falls back
to connection removal. Pacing and amplification credit remain conservatively
consumed. Other socket errors retain removal; oversized STREAM rejection is
handled as described below.
Tests inject wrapped EMSGSIZE at the endpoint boundary for v1/v2; actual low-MTU
kernel/network behavior has not yet been exercised.

`stream_delivery.go` supplies a bounded byte/FIN ledger for one original STREAM
frame and a zero-copy split helper. Overlapping fragment ACKs and a late ACK of
the original transmission can complete that logical item only once; a failing
final completion callback leaves its last ACK unapplied. The ledger bounds its
coverage bitmap to one maximum UDP payload, and splitting keeps FIN on the tail.
Unit tests and a byte-oracle fuzz target exercise ordering, overlap, FIN,
completion failure, offset bounds and size limits.

A staged owner operation now replaces a queued STREAM item with two children
sharing its ledger and original completion callback. Original packet mappings
remain valid for late ACKs. Memory charges include the ledger, bounded member
table and retained fragment nodes; failed admission leaves queue ownership
unchanged. Reliable ACK handling and cancellation release the logical item once,
including the stream completion barrier and FIN. The group is capped at 128
retained items. A bounded split tree now directs loss, PTO, explicit probes,
detachment and path evacuation to unacknowledged leaves while retaining parent
packet mappings for late ACKs. An ACK of an intermediate parent retires its
entire subtree. Tests cover duplicate loss callbacks, cross-path queue ownership,
PTO fragment choice and evacuation followed by a late original ACK.

Per-path packet construction now invokes splitting when a STREAM frame exceeds
the confirmed path limit. Splits use portable 1140-byte prefixes to avoid repeated
subdivision as MTU changes. Shared-path borrowing splits at the source, so a
blocked destination cannot discard the tail. Encrypted v1/v2 tests confirm a
3000-byte path, send a 2500-byte STREAM frame, simulate loss/fallback, and verify
small retransmissions, reassembly and exactly-once completion with both fragment
ACKs and a late original ACK. These tests now use WriteStream admission, including
its actual stream completion barrier.

WriteStream can select a larger payload using the chosen path's confirmed size.
Before consuming stream/connection credit it reserves all portable-split nodes,
coverage storage and branch bookkeeping. Splitting consumes that reservation
without requiring spare send-buffer capacity. Completion or cancellation refunds
the full logical charge. If larger admission cannot fit, a portable partial write
can still progress. Tests cover saturated-buffer fallback splitting through the
maximum UDP payload size, cancellation refunds, blocked credit/FIN atomicity and
portable admission without a ledger. Public operation starts with portable
frames and may increase after discovery confirmation. Low-level manually queued
large frames without a reservation can still fail on split-allocation exhaustion.

Oversized ordinary STREAM output now carries synchronous write-result metadata.
Wrapped EMSGSIZE releases its flight accounting without congestion loss, keeps
its packet number consumed, restores idle activity, and requeues reliable leaves.
The rejected packet mapping is removed and future ACKs for it fail validation.
The path resets to 1200, requires expanded baseline validation, and delays upward
search for the existing cooldown. Reserved split capacity remains available at a
full application queue. Tests cover v1/v2 endpoint rejection, baseline gating,
reserved splitting, accounting, and rejected ACKs. A complete deterministic
endpoint transfer also confirms 3000 bytes via encrypted probing, injects a
rejected 2500-byte STREAM write, exchanges baseline validation and smaller
retransmissions through endpoint send/receive routing, and verifies exact
reassembly, FIN, acknowledgment completion, and zero retained send charges or
flight bytes. Both v1/v2 run with and without a dropped first baseline challenge;
the dropped case retries through the endpoint deadline and timeout API. This is
injected socket-error coverage, not a real kernel MTU test.

Unequal-path failover now has deterministic encrypted v1/v2 transfer coverage.
After discovery confirms a 3000-byte path, three 2500-byte writes leave one
transmitted and two queued items when that path is abandoned. The 1200-byte
survivor carries the abandonment exchange and smaller STREAM fragments. Tests
verify exact 7500-byte reassembly, FIN, logical completion, released send charges
and survivor flight accounting. A second variant delays the first original ACK
until fragments are being sent on the survivor. The failed path is injected via
the owner failure API; this does not demonstrate real socket failure detection.

The ordinary multipath scheduler also has deterministic simultaneous unequal-MTU
coverage for v1/v2. Three large writes are admitted onto a confirmed 3000-byte
path, then both paths become equally preferred. The scheduler borrows and splits
onto the 1200-byte path while continuing larger sends on the original path. The
first small-path packet is delayed to exercise cross-path reordering. Exact
7500-byte reassembly, FIN, completion and released flight/send charges are checked
with both paths still active. This uses synthetic prepared path contexts and an
in-memory encrypted link, not a public multipath socket integration test.

Automatic activation has real loopback UDP tests for v1/v2, confirming larger
sizes on both endpoints without invoking the staging hook, and checking the
opt-out. Admission tests cover unbound/unvalidated paths, peer caps and preserving
an existing search. Root race tests and vet pass. These loopback checks do not
establish behavior on actual constrained routes or complete RFC 8899 compliance.
Remaining work includes constrained-network validation and discovery on wildcard
and additional-platform sockets. Small-packet, DATAGRAM and unrelated write
failures still follow existing runtime failure handling.

`TestUDPDiscoveryConstrainedRelay` now runs both versions through a real UDP
userspace relay that discards datagrams above 1388 bytes in both directions.
Ordinary small PING traffic provides later ACKs for normal loss detection; the
test does not edit discovery/recovery state or advance a simulated clock. Both
peers automatically converge to 1388, then deliver a 5000-byte STREAM with exact
payload, FIN and sender acknowledgment completion. V1/v2 cases run concurrently
and the full race suite passes. This verifies a userspace size filter, not route
MTU, ICMP handling, kernel EMSGSIZE or network-namespace behavior.

`TestUDPDiscoveredMTUShrinks` uses the same live relay, then atomically lowers its
limit from 1388 to 1200 after both peers finish discovery. Three full-size STREAM
writes (3984 data bytes) are dropped; no small trailing STREAM supplies an ACK.
Production PTO/recovery and baseline validation recover the transfer without
further driver PINGs or internal state edits. Tests verify exact bytes/FIN,
sender acknowledgment, at least three new oversized drops, confirmed fallback
to 1200, completed baseline validation, the upward-search cooldown and released
send-buffer charges for v1/v2. Full root race tests and vet pass. This remains
userspace relay evidence rather than kernel route-MTU validation.

Ordinary queued PATH_RESPONSE frames now retain arrival order and precede newly
queued local PATH_CHALLENGE frames as well as application data. Previously both
frame types were pushed onto the queue head, reversing responses and allowing a
new challenge to overtake them. V1/v2 encrypted tests verify three response tokens
in order, 1200-byte output, retained challenge/STREAM work and released response
charges. This addresses control-queue ordering motivated by the prompt-response
requirement in [RFC 9000 §8.2.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-8.2.2);
it is not a complete scheduler audit. The scheduler now checks pending responses
before ordinary ACK/data selection; same-path response selection precedes its
ACK, and PTO reuses the response instead of displacing it. If congestion,
amplification, recovery capacity or pacing blocks the response, a pending ACK
can still be sent. The response remains queued and pacing deadlines survive the
ACK fallback. Tests cover pending ACKs on both paths, PTO reuse, congestion and
pacing blockage, and ACK-only failure to prove baseline reachability. Candidate
migration senders remain separate audit work.

Local migration candidates now retain up to eight pending response tokens in
arrival order instead of replacing a single slot. Committed sends pop one token;
blocked sends preserve it. Overflow returns the existing resource error without
overwriting queued work. Expiry clears the queue. Local migration commit carries
the first response and remaining FIFO into the established path without charging
application capacity. Encrypted v1/v2 tests cover the send queue, bound and expiry;
commit tests cover full application buffers and token preservation. Peer-initiated candidates now use the same queue and transfer pending responses
when rebinding commits. Challenges in the committing packet are skipped by normal
frame dispatch because candidate handling already queued them; other frames,
including STREAM, still dispatch normally. An encrypted v1/v2 test retains two
earlier tokens plus a third in the commit packet, verifies exactly three ordered
replies, and checks admission of that packet's stream data. Candidate challenge packets now have separate bounded replay history per path.
Both local and peer-initiated handlers suppress repeated challenges without
recording normal packet receipt or ACKs. The history survives migration commit,
so a replay can admit formerly withheld stream data while suppressing its already
handled challenge. A 64-range bound rejects forgotten old challenge packets;
it is not a second ACK history. Tests replay a peer candidate packet sixteen
times before commit and again after commit, verify one response per challenge
and subsequent admission of its stream data, and exercise history eviction.
This does not claim completion of every migration control-frame replay case.

Candidate CID controls now have separate bounded per-path replay history too.
An obsolete NEW_CONNECTION_ID can legitimately produce a retirement response,
so simply re-running otherwise idempotent CID state handlers for every candidate
packet replay could exhaust the send buffer. The candidate control helper now
skips already-received or previously processed packet numbers; normal dispatch
also skips their already-applied CID controls after commit while still admitting
withheld STREAM frames. V1/v2 dispatch-level regression tests cover repeated
obsolete-CID input, unchanged queues/receive admission, later stream delivery,
a 64-range cap and eviction. These tests operate at the decoded control boundary;
they are not independent-peer wire tests.

The peer migration encrypted v1/v2 regression now also carries CID replacement
and an obsolete CID advertisement through the actual bound receive path. Sixteen
pre-commit replays preserve retirement queue count/charges; post-commit replay
admits withheld stream data without repeating either CID controls or challenges.
The local migration mutual-validation test likewise replays a candidate challenge
sixteen times and verifies pending response identity, tail, charges and normal
receive/ACK state. These are encrypted in-memory packet tests, not independent-
peer migration interoperability evidence.

Path packet-number reconstruction now uses the larger of normal receive history
and a separate authenticated-packet high-water mark. Authentication advances the
latter even when candidate admission withholds the packet, but never queues an
ACK or records ordinary receipt. This follows
[RFC 9000 §17.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-17.1).
Receiver-boundary v1/v2 tests cover short encodings across a 16-bit boundary,
reordering, forged high packet numbers and isolation between path spaces. The
test drives authentication directly; it does not claim a complete sender-side
packet-number encoding/ACK-policy interoperability scenario.

The authenticated reconstruction reference now belongs to `numberSpace`, shared
by the legacy receiver and the adopted path-zero engine. Both advance it only
after successful authentication. Normal pre-activation retention records receipt,
but a saturated retention buffer deliberately withholds receipt/ACK admission;
the reference now advances in that case too. A v1/v2 test saturates that buffer,
authenticates across a truncated-number boundary, restores capacity, hands off
to the path owner and checks subsequent decoding/admission. The candidate test
also verifies that resetting migration recovery preserves this reference.

`FuzzCandidateChallengeReplayHistory` compares range-based replay tracking with
an independent bounded packet-membership oracle. It mixes candidate challenges,
normal admitted packets and unrelated frames over a 512-number domain, up to 512
operations per input. Checks cover freshness, retained membership, eviction floor,
64-range candidate capacity, and separation from receive/ACK admission. Explicit
seeds force eviction and reordering. A local 10-second fuzz run with four workers
completed 190226 executions successfully; focused race checks and vet also pass.
This bounded campaign is additional evidence, not exhaustive replay-state proof.

Larger write admission now reserves fallback capacity without eagerly allocating
the byte-coverage ledger, branch map or fragment table. The first split builds
those structures and consumes the precharged reservation; blocked splitting does
not publish partially initialized state. Unsplit ACK/cancellation releases the
whole logical charge directly. Full-buffer and encrypted fallback tests pass.
The [short-transfer benchmark follow-up](benchmarks/mtu-startup-transfer.md)
records lower allocation cost with remaining overhead and measurement limits.

Deferred-reservation regression coverage now explicitly combines completion
failure, rejected split requests, full-buffer materialization and a late original
ACK for 1141-, 2500- and 65527-byte STREAM items. The failed callback and invalid
splits retain the descriptor, queue identity and logical charge. Portable splits
then consume only reserved capacity; the original ACK completes every fragment
and refunds once. Focused race tests and vet pass. This tests returned-error
atomicity, not recovery from Go runtime out-of-memory termination.
