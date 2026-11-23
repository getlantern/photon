# Partial stream reset implementation

Target checked 2026-09-28:
[draft-ietf-quic-reliable-stream-reset-11](https://www.ietf.org/ietf-ftp/internet-drafts/draft-ietf-quic-reliable-stream-reset-11.html),
published 2026-09-06. Public configuration can now advertise support; broad
validation remains incomplete. The v0.63.0 reference Config comment still names draft-09; full behavior
and legacy-codepoint interoperability need separate verification.

Implemented wire foundation:

- Empty reset_stream_at transport parameter 0x1d, both roles. Nonempty values
  and duplicates are transport-parameter errors; nonminimal TP encodings remain
  accepted under the normal QUIC parameter rules.
- RESET_STREAM_AT frame 0x24 with stream ID, application error, final size and
  reliable size. Reliable size greater than final size is a frame encoding error.
  Encoders validate before modifying destination storage.
- Reception context requires locally advertised support and application-data
  packet space (client 0-RTT or either endpoint's 1-RTT). The frame is
  ack-eliciting, contributes to in-flight accounting, and is not probing.
- Config.EnableStreamResetPartialDelivery enables local receive advertisement.
  Sending depends independently on peer support. ConnectionState exposes separate
  Local and Remote capability flags. Selected server configuration controls the
  admitted connection's advertisement, including overriding listener defaults.

Receive-state foundation (2026-09-28): internal stream.Receive.ResetAt stores a
pending delivery obligation, discards buffered/input bytes beyond it, waits for
gaps, and surfaces reset after delivering the prefix. Decreasing reliable sizes
win over reordered larger values. Flow accounting charges final size at reset
receipt and consumes the discarded suffix once the prefix is delivered or its
obligation is removed. FIN already delivered as EOF stays EOF. Partial-reset
error-code changes and inconsistent final sizes fail before state mutation.
Ordinary resets keep their previous first-code behavior until partial reset is
involved. Bounded bitmap iteration handles discarded tail data and ring wrap.

Transport receive integration (2026-09-28) now dispatches RESET_STREAM_AT to
this state and supplies the local capability to packet validators. A nonconsuming
terminal query reports the remote reset once the prefix is complete, including
when it remains unread. Cancellation preserves its local cause, retires
STOP_SENDING after completion, discards the completed prefix and restores
connection/stream-count credit. Accepted complete prefixes survive connection
close through the existing retained-read path.

Released partial-reset streams keep final-size/error-code metadata in a bounded
FIFO history of 1,024 streams. Within that history, inconsistent late STREAM,
RESET_STREAM and RESET_STREAM_AT frames raise final-size/stream-state errors.
Eviction drops these consistency checks; merged closed-ID ranges still prevent
stream recreation or duplicate credit. This is deliberately bounded metadata,
not lifetime validation of every closed stream. Direct transport tests cover
both versions, gap notification, unread retention, cancellation, retirement,
FIFO bounds and shutdown. Negotiated protected-packet regressions now cover late
violations while metadata is retained, plus 1,025 real unidirectional releases
and eviction in both TLS roles and v1/v2. Evicted IDs cannot regain objects or
flow/stream credit; retained contradictions remain fatal. The normative retention
interpretation and remaining gaps are recorded in [the audit](CONFORMANCE.md).
These own-engine tests do not establish independent interoperability by themselves;
public negotiated tests are described below.

Send-state foundation (2026-09-28): internal Send.ResetAt commits a boundary
within already-written data and permits later reductions while retaining the
first application error and final size. Retransmission filtering borrows only
the needed prefix of owned STREAM frames and clears FIN; it never returns flow
credit. Completion requires both the current reset information and the contiguous
prefix to be acknowledged. Older larger-reset ACKs cannot acknowledge a newer
smaller reset. An ordinary reset removes the prefix obligation but needs its own
ACK. Fully acknowledged data plus FIN remains an alternative completion path.
The connection must supply validated contiguous-prefix ACK progress. The existing
per-item delivery ledger now supports reducing its required byte prefix and
removing its FIN obligation while preserving original bounds for late ACKs.
Suffix ACKs cannot satisfy missing prefix bytes, overlapping ACKs count once,
and failed completion callbacks leave the reduction unapplied. The
per-item retainStreamItemPrefix operation now connects this ledger to shared
send-item ownership, queued frame replacement across packet-number spaces and
loss traversal. It preserves original wire images, trims replacement leaves,
removes abandoned tails/FIN, handles further MTU splitting and charges bounded
metadata against existing reservations or connection capacity. Reduction and
completion errors leave that item's state unchanged. Node-history exhaustion
returns ErrSendBuffer; zero-prefix retirement can still release it. Unit/model
fuzz coverage includes late ACKs, cross-path loss, reductions and exact-once
charge release. These are owner-level simulated packet mappings, not yet a
negotiated protected-packet interop test.

Stream-wide staging now collects unique logical roots across queued and sent
mappings on every application path. It prepares all reductions without changing
live state, checks net metadata charges plus reset-frame budget, and can fund
retained prefix metadata by retiring abandoned tails. Whole data-only items
before the boundary remain on their existing ACK path without added ledgers.
Commit retires satisfied obligations before adding charges, rewrites queues
once, and prunes retired packet mappings while retaining live late-ACK evidence.
It returns reduced obligations for subsequent stream accounting; it does not
label them fully acknowledged data. Preparation and commit require one serialized
owner operation with no intervening ACK, write, or path event.

Engine-level CancelWriteAt now validates application readiness, the peer's advertised
receive capability, stream bounds, output-path viability and total queue budget
before committing Send.ResetAt plus the stream transaction. Old reset controls
are deduplicated and retired in the same budget transaction; reductions retain
the first application error/final size and reliably queue the new boundary.
Identical repeated requests do not add another control frame.

Logical item completion now reports required-prefix delivery for a partial reset,
instead of treating the original queued FIN flag as evidence of full delivery.
Only when every retained logical item is covered is the current reliable prefix
reported acknowledged; a higher-offset ACK cannot skip gaps. Current-reset ACK
and prefix completion may arrive in either order. STOP_SENDING or ordinary
CancelWrite removes the prefix obligation, preserves the original reset code,
retires partial controls and waits for the ordinary reset ACK. Path send validators
use the peer's receive capability.

`TestPartialResetProtectedACKOrders` supplements direct send-item acknowledgment
tests with eight protected-packet cases: v1/v2, both sender roles, and reset ACK
before or after data ACKs. It emits two separate STREAM packets (the second with
FIN), commits a partial reset, and emits its control packet. Each packet is
decrypted and checked before the fixture injects selective authenticated ACKs.
A tail ACK and repeated tail ACK cannot cover the missing first write, even if
the reset has already been acknowledged. After all required ACKs, stream release
occurs exactly once and queued storage reaches zero; repeated reset ACKs remain
harmless. These fixtures test recovery-to-stream wiring using our own packet
protection, not an independent peer's ACK generation. They do not implement the
remaining late-full-data/FIN completion optimization or cover multipath ACKs.

The byte-model `FuzzStreamDeliveryPartial` now also rejects selected completion
callbacks, requiring exact ledger preservation before retrying ACKs or boundary
reductions. It includes FIN-only items. On 2026-09-29, seed/unit race checks and
a 1,000,000-execution fixed-count campaign passed. A preceding timed campaign
ended with a harness deadline error and is not counted as successful validation.
Reproduce the successful campaign from the repository root:

```sh
env GOCACHE=/Volumes/ExtremeSSD/macdev/quic-go-build-cache go test ./internal/transport -run '^$' -fuzz '^FuzzStreamDeliveryPartial$' -fuzztime=1000000x -parallel=4
```

Protected engine tests exercise v1/v2 with loss/reordering using actual TLS
negotiation. The surviving-multipath-path fixture still injects capability after
TLS, so that combined-extension case is not negotiation evidence. A separate
public UDP test now negotiates both extensions, validates path one with real
traffic, and retires the client's primary path. It verifies 32 KiB reliable
prefixes, reset identity, stream release and retained delivery errors in v1/v2,
with uni/bidirectional streams and optional secondary-path packet loss. The bidi
case also resets a reliable prefix in the reverse direction. Relay counters
confirm traffic and loss on the remaining path. This is own-peer coverage, not
independent multipath interoperability; it does not replace the synthetic
post-garbage-collection test's distinct lifecycle scope. Independent interop now covers the
engine server sending to unmodified quic-go v0.63.0 clients that really advertise
support during TLS, with zero, 256-byte and 1,500-byte prefixes from 5,000-byte
streams. Those tests confirm asymmetric capability flags, exact delivered bytes,
remote reset identity and eventual sender retirement. They do not cover reverse
roles, independent impaired paths or early data.

Remembered transport schema 3 now records the server's reset capability in both
its authenticated ticket and the client-local cached state. Schema 2 remains
readable with reset support false. Server acceptance checks reject 0-RTT when
support is withdrawn, while allowing ordinary TLS resumption; client completion
rejects a peer that accepted early data while withdrawing support. TLS-only v1/v2
tests cover preserved, newly enabled and withdrawn support, invalid acceptance,
and genuine schema-2 ticket resumption. These are capability-memory tests, not
proof of RESET_STREAM_AT transport delivery in early packets.

Engine CancelWriteAt now permits client 0-RTT using the remembered server
capability; the early packet validator enables the frame on that basis. Rejection
retires whole logical send groups, including trim/split metadata, exactly once.
Engine v1/v2 tests cover bidirectional and unidirectional streams, accepted early
prefixes, reordering/replay, dropped early flights recovered in 1-RTT, boundary
reduction, absent capability, policy rejection and capability withdrawal. Queued
and transmitted partial resets are discarded on rejection, and rejected early
ciphertext cannot reset the fresh stream with the same numeric ID. The fixture now configures server reception before construction and negotiates
support through the regular TLS/ticket path; the old test-only TLS replacement
has been removed.

Live interop with unmodified quic-go v0.63.0 now covers an engine client sending a
zero-prefix reset in 0-RTT for v1/v2. The reference's TLS callback is held until an
early packet arrives; with all STREAM bytes discarded, that packet is the reset.
Acceptance validates the remote reset and sender retirement; policy rejection and
capability withdrawal validate TLS resumption, early rejection and a fresh stream.
Public client early-boundary tests now cover a nonzero prefix against v0.63.0
in v1/v2, including one dropped early prefix packet. The server's TLS callback is
held until early traffic arrives, and cancellation occurs before it is released.
Accepted streams deliver the complete prefix then the original remote reset;
sender release and WaitAcknowledged preserve the local reset. Rejected early
streams (policy rejection or withdrawn support) require explicit NextConnection;
stale boundary/cancel/write/close/release operations cannot affect the new stream
with the same ID. TLS resumption and post-handshake capability state are checked.
This is a single early-packet loss case, not broad independent impairment coverage.

Public SendStream.SetReliableBoundary now snapshots committed bytes through the
connection owner. CancelWrite selects partial reset for a supported, nonzero
boundary and ordinary reset otherwise. Repeated cancellation preserves the first
reset, and later boundary calls do not alter it. STOP_SENDING continues to abandon
the prefix via ordinary reset. Both TLS roles have public uni-stream sender
interop against v0.63.0 for v1/v2, supported and unsupported peers, advancing the
boundary twice, reset identity, release and retained WaitAcknowledged errors.
These tests permit optional suffix bytes already delivered before reset.

Write admission reserves 32 bytes once per capable send stream (also for early
writes, since support may be newly enabled at resumption). All queue admission
uses the budget after reservations. The reservation is not pending wire output;
it is consumed by reset enqueue or released on FIN acknowledgement, ordinary
cancellation, stream release, early rejection or connection teardown. A public
boundary lands between committed STREAM items, so arbitrary-offset trim metadata
is not needed for its original cancellation. Direct engine CancelWriteAt and later
boundary reductions can still require bounded trim metadata and fail admission.
Full-queue engine tests cover retained whole prefixes and newly enabled support
after early writes. Normal write/close paths remain within the configured budget.

Local receive advertisement is enabled by public configuration, including
GetConfigForClient selection. Live receiver interop against v0.63.0 covers v1/v2,
both TLS roles and both stream directions, enabled/disabled support, asymmetric
capability flags, prefix/reset delivery and stream release. Server cases select
the opposite policy from the listener default to verify both overrides. Capability
snapshots remain stable after shutdown.

Public early receiver tests against v0.63.0 cover v1/v2, both stream directions,
and selected accept/reject/withdrawn-support policies. In the no-loss accepted
case the complete prefix and reset are consumed before client Finished. Rejected
resumptions deliver a clean fresh stream with the reused numeric ID. Both sides'
resumption, early acceptance and selected capability flags are checked. A loss
case drops the first 0-RTT packet using the first accepted Retry's CID, preserving
coalesced Initial data; handshake progress is allowed so the reference can recover
application data. The fixture asserts that the targeted packet was dropped.
This is single-packet loss coverage, not a full impairment/state-machine audit.
The transport currently
waits for current-reset ACK even when a late full-data/FIN ACK could provide an
alternative completion proof; it does not falsely claim full FIN delivery from a
trimmed item's completion.

Required next integration:

1. Extend protected receive/history validation to bidirectional release,
   combined-extension/state-model coverage and adversarial longevity. The bounded
   retired-metadata interpretation has an initial clause-level audit; it still
   needs review in the full conformance/security assessment.
2. Extend sender coverage to independent loss/reordering, reverse TLS role, all
   stream directions, path/MTU changes and full-data/FIN alternative completion.
   Extend reservation coverage to independent full-queue and path-change cases.
3. Extend public receive configuration/state validation to concurrent selected
   connections, early resumption and combined extension/path behavior. Basic
   asymmetric negotiation, public configuration/state and selection are integrated.
4. Extend public early coverage to multiple losses/reordering and application
   concurrency. Both independent roles, selected server policy, receiver-side
   bidirectional streams and basic stale-client-handle tests are present.
5. Validate against independent peers for v1/v2, both roles and stream directions,
   including loss, reordering, duplicate resets, zero/full prefixes, flow-control
   blocking, FIN/reset races, STOP_SENDING and resource retirement. Audit latest
   versus legacy draft behavior explicitly.

Tests cover syntax, varint boundaries, output ownership, packet context,
parameter validation, receive/send transitions and accounting, negotiated
receive support, asymmetric sender/receiver interop and TLS capability memory. Full
public integration, broad independent early-reset delivery and feature parity
remain unproven.

Delivery-result preservation: a FIN may close the send context before a reset
is requested or received. StreamAcknowledged now checks the separately retained
reset error, and automatic release provides that delivery failure to public
handles independently of the receive result. WaitAcknowledged therefore retains
the reset after engine storage is removed. The original FIN context notification
remains single-shot. Regression coverage includes local and remote ordinary
resets after FIN and the public released-handle path.
