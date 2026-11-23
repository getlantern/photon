# HTTP/3 implementation status

HTTP/3 is under construction. Experimental public APIs now include a net/http
handler server, listener serving and graceful shutdown, an established-connection
client, authenticated dialing and an origin-based pooled RoundTripper. See the
[endpoint API guide](../http3/README.md) and [latest checkpoint](PROGRESS.md) for
current behavior and limitations. Full quic-go parity is not yet established.

The notes below record successive internal implementation stages; their older
statements about unavailable public APIs are superseded by the current guide.

Raw request streams and connections (`Stream`, `RequestStream`, `HTTPStreamer`,
`RawClientConn`, `RawServerConn`) sit on the same session, message and datagram
components described below; see [HTTP3_RAW_STREAMS.md](HTTP3_RAW_STREAMS.md).

## QPACK decoder feedback

This section is current; it supersedes what the stage notes below say about
feedback being written synchronously.

`FieldDecoder` serves every request on a connection behind one lock. Its
feedback (section acknowledgments, stream cancellations, insert count
increments) goes to the QPACK decoder stream, whose flow-control credit the
peer controls. No `FieldDecoder` method writes to that stream or waits for a
write: `Submit`, `Cancel`, `EncoderInstruction`, `Detach` and `Close` only take
the lock for in-memory work. So a peer that withholds decoder-stream credit
cannot hold a header read, a cancellation, request cleanup or the release of a
request's concurrency slot.

- Queue. Feedback is appended to a queue under the lock, in the order the
  decoder produced it and before the result that caused it becomes visible. One
  goroutine, started when the queue becomes non-empty and gone when it is
  empty, writes batches outside the lock.
- What RFC 9204 requires. Decoder instructions only inform the encoder. A
  Section Acknowledgment must be emitted "after the decoder finishes decoding"
  a section that used the dynamic table
  ([section 2.2.2.1](https://www.rfc-editor.org/rfc/rfc9204.html#section-2.2.2.1)),
  with no bound on how long after;
  [section 2.2.2.3](https://www.rfc-editor.org/rfc/rfc9204.html#section-2.2.2.3)
  lets the decoder choose when to send Insert Count Increments and notes that
  delay only costs compression; a Stream Cancellation only lets the encoder
  drop references
  ([section 2.2.2.2](https://www.rfc-editor.org/rfc/rfc9204.html#section-2.2.2.2)).
  Until feedback arrives the encoder assumes less: the stream may be blocked
  ([section 2.1.2](https://www.rfc-editor.org/rfc/rfc9204.html#section-2.1.2))
  and its entries stay referenced. So late feedback is always safe, and nothing
  requires a decoded section to wait for its acknowledgment. What must hold is
  order and exactly-once delivery. By
  [section 2.1.4](https://www.rfc-editor.org/rfc/rfc9204.html#section-2.1.4)
  an acknowledgment raises the Known Received Count to the section's Required
  Insert Count and an increment adds to it, so an increment counts from what
  the instructions before it acknowledged: moved past an acknowledgment it
  would over-report (a connection error at the encoder) or under-report for
  good. The queue is never reordered, and increments are merged only when
  adjacent, which the encoder cannot tell from receiving them separately. Each
  acknowledgment stands for the earliest unacknowledged section on its stream,
  so none is dropped or duplicated. An acknowledgment followed by a
  cancellation of the same stream is sent in that order; QUIC never reuses a
  stream ID, so a cancellation cannot meet a later stream.
- Bound. Unwritten feedback is limited to 8192 instructions (16 bytes each). A
  request owes one acknowledgment per field section that used the dynamic table
  and at most one cancellation, so only a peer that has stopped reading its
  decoder stream gets there. Past the bound the decoder fails with
  H3_EXCESSIVE_LOAD and the connection is closed with that code.
- Overflow and state. Queueing changes nothing but the queue. A call whose
  feedback overflows the bound first finishes its own work on the pending
  sections it started with, then ends the decoder as its last step, which
  releases whatever is still waiting.
- Failure. A failed write is reported through the session's hook as before: the
  session is canceled and the connection aborted, and the decoder fails, which
  releases blocked sections and makes later calls return the error. The call
  that produced the feedback has already returned successfully. After GOAWAY
  and a graceful H3_NO_ERROR close the hook reports the feedback as merely
  undeliverable, and the decoder detaches instead: table state stays so
  buffered responses still decode, and further feedback is dropped.
- Shutdown. The session interrupts the decoder stream with a write deadline,
  then closes or detaches the decoder, then waits for the writer (`Flushed`).
  `Close` and `Detach` themselves never wait. An interrupted write does not
  become the decoder's error, which would look like a retryable read deadline
  to request readers; the session's `Close` supplies the cause. Feedback still
  queued at that point is discarded with the connection.

The initial `internal/http3wire` layer follows
[RFC 9114 sections 7 and 8](https://www.rfc-editor.org/rfc/rfc9114.html#section-7):

- Streaming type/length parsing, using the existing QUIC variable integers.
  Nonminimal encodings are accepted. No payload allocation uses a declared
  frame length. Callers must consume the current payload before the next header.
- Unknown frame types remain available for endpoint extension handling or
  skipping. The four prohibited HTTP/2-only frame types are rejected.
- Clean stream termination within a frame yields H3_FRAME_ERROR. Abrupt source
  errors remain available to the endpoint instead of being mislabeled as FIN.
- Exact single-ID payload parsing for CANCEL_PUSH, GOAWAY, and MAX_PUSH_ID.
  Endpoint role and permitted-ID checks are still required.
- SETTINGS parsing with an explicit entry limit, duplicate detection (including
  unknown identifiers), rejection of HTTP/2-only settings, and transactional
  encoding. Unknown settings are retained; the endpoint must ignore identifiers
  it does not implement. QPACK settings identifiers are named; compression
  itself remains unfinished.
- Control-stream parsing after the stream-type prefix: SETTINGS must come first
  and occur once, with separate payload-byte and entry limits. Unknown frames
  are discarded with constant scratch space. Clean EOF is a critical-stream
  error; malformed framing retains its more specific frame error. Underlying
  non-EOF errors are preserved for endpoint reset/cancellation handling.
- Frame placement on control/request/push streams, including sender restrictions
  for MAX_PUSH_ID and PUSH_PROMISE. Control GOAWAY identifiers cannot increase;
  server GOAWAY IDs must identify client bidirectional streams. MAX_PUSH_ID
  cannot decrease. CANCEL_PUSH still needs connection-wide push-state checks.
- Unidirectional stream-type parsing and a concurrent-safe, fixed-size registry
  for incoming control and QPACK encoder/decoder streams. Duplicate critical
  types and client-initiated push streams are rejected. Critical identities
  survive closure, preventing replacement; confirmed FIN/reset is a critical
  stream error. Unknown types are not registered and must be discarded or
  aborted by the future endpoint. Closure before a complete stream-type prefix
  is tolerated. QPACK stream ownership follows
  [RFC 9204 section 4.2](https://www.rfc-editor.org/rfc/rfc9204.html#section-4.2).

Tests cover one-byte fragmentation, clean and truncated EOF, abrupt stream
errors, frame boundaries, maximum integer lengths, reserved values, duplicate
settings, resource limits, and settings round trips. A fuzz target exercises
stream framing, settings re-encoding and control sequences. These are codec tests, not independent
HTTP/3 interoperability evidence.

Remaining work includes live control/critical stream creation and dispatch,
request/response sequencing, QPACK encoder table policy and remaining
instructions, field validation, push, GOAWAY lifecycle, `net/http` adapters,
HTTP datagrams/capsules, and independent peer tests. Allocation limits for
buffered field sections must be enforced by the endpoint; control SETTINGS
buffering already uses explicit caller-provided limits. In contrast,
the streaming frame reader deliberately imposes no small DATA-frame size cap.

`internal/qpack` now provides prefixed integer encoding/decoding for prefix
widths 1–8 and values through 62 bits, plus the three decoder-instruction wire
formats from [RFC 9204 section 4.4](https://www.rfc-editor.org/rfc/rfc9204.html#section-4.4).
Incomplete inputs consume no bytes; failed encodes leave the destination intact.
Redundant integer encodings are accepted up to an explicit 16-byte limit.
Zero Insert Count Increment is rejected. Instruction validation against table
state, complete field sections and blocked-stream handling are
not yet implemented. Tests include inherited RFC 7541 integer vectors,
boundaries, overflow, fragmented instructions and fuzz round trips.

QPACK field-section prefixes now reconstruct Required Insert Count with the
negotiated maximum table capacity and decode signed Delta Base. Invalid modulo
values, negative bases, and results exceeding 62 bits are rejected. Future
insert counts remain valid prefixes for the eventual blocked-decoder machinery.
Encoding does not assert that referenced entries are available; table reference
validation and blocked-stream limits still need implementation.

The decoder-side dynamic table now supports capacity updates, oldest-entry
eviction, insertion, duplication, and absolute/encoder-relative lookup.
It starts at zero capacity, preserves insertion counts across table clearing,
and rejects oversized entries or capacity increases beyond the local maximum.
Field bytes are cloned so small stored fields cannot retain large source
buffers. Evicted string references are cleared and index storage is compacted.
This table does not implement the encoder's outstanding-reference eviction
policy. Encoder instruction parsing and atomic table updates are now connected
through `ConsumeEncoderUpdate`; live stream dispatch remains unfinished.

The complete 99-entry static table is generated from RFC 9204 Appendix A XML
with `internal/qpack/testdata/generate_static.py`. Lookup returns field copies;
reverse lookup prefers exact values, then the lowest matching name index.
Names and values are byte-exact. Sensitive-field indexing remains encoder policy.
Tests cover every entry, bounds, concurrent reads and reverse lookup. The isolated
interop module additionally decodes every indexed static reference using pinned
`github.com/quic-go/qpack` v0.6.0 and compares names and values, including rejection
of index 99. This checks table compatibility, not full QPACK interoperability.

QPACK string literals now support raw and Huffman forms with variable length
prefixes and explicit decoded-byte limits. The independent Huffman codebook is
generated from RFC 7541 Appendix B; the generator verifies binary and hexadecimal
columns against each other. The codec rejects EOS symbols, non-EOS padding and
padding longer than seven bits. String errors consume no input and expose no
partial output. Tests include RFC examples, every octet, fragmented framing,
size limits, fuzzing and an independent HPACK codec comparison in the isolated
interop module. The initial decoder uses a bitwise trie; throughput optimization
awaits measurement. Live stream dispatch remains unfinished.

All four encoder instructions now have independent wire codecs: capacity updates,
insertions with static/dynamic name references, literal-name insertions, and
duplicates. Decoded literal bytes share a per-instruction limit. Parsing and table
application are separate operations; `ConsumeEncoderUpdate` combines them without
changing the table or consuming input on an error. Name references resolve before
eviction. RFC 9204 Appendix B sequences, fragmented input, invalid references,
capacity failures, self-eviction, Huffman combinations and fuzzed instruction
sequences are tested. A future bounded stream reader must buffer incomplete
instructions, map errors to QPACK_ENCODER_STREAM_ERROR, and notify blocked
sections after insertions. These codecs do not yet provide that stream lifecycle.

All five field-line representation forms now have wire codecs, including static,
relative and post-base references, literal names, Huffman strings and preserved
never-index flags. Per-line reference resolution validates static indices,
relative underflow, post-base overflow, Required Insert Count bounds and evicted
entries. It reports the highest referenced count contribution for the future
whole-section validator. Parsing enforces a combined literal-byte cap; a section
owner must additionally count resolved fields, including indexed fields, against
its total field-section budget. Never-indexed indexed encodings are rejected.
The isolated upstream decoder checks 503 static/literal combinations; it does
not support dynamic references, which currently have local boundary tests only.
HTTP validation and live stream integration remain unfinished.

Whole-section decoding now enforces exact Required Insert Count equality with
the largest referenced dynamic index plus one, total decoded size (including
32 bytes per occurrence), and an independent field-count limit. Field order,
duplicates and never-index flags survive decoding; errors return no partial
fields and do not mutate the table. A blocked section returns its reconstructed
prefix for later resumption without reinterpreting modulo counts after table
updates. The connection owner must serialize table access, dispatch returned instructions
and map errors to HTTP/3.
Independent upstream-encoded sections exercise all static entries, custom
values, literal names and duplicates; dynamic section coverage uses RFC fixtures.

The receive-side `qpack.Decoder` now owns dynamic table updates and copied blocked
section bodies. Configured limits bound blocked stream count, total buffered
encoded bytes, individual encoded/decoded sections, field count and instruction
literal bytes. One section per stream can be pending: the owner pauses further
section delivery on that stream until completion or cancellation. A second
submission returns local backpressure without changing state. Completed sections
are removed immediately; no lifetime stream history is retained.

Each insertion resumes newly ready sections before another instruction can evict
entries. Results are ordered by stream ID when several become ready together.
Dynamic sections produce acknowledgments; remaining speculative inserts produce
nonzero Insert Count Increments. Cancellation releases pending bytes and emits a
Stream Cancellation even if the section has not arrived. The owner prevents
submission after cancellation and calls cancellation at most once. Returned
instructions must be transmitted reliably in call order and are not internally
queued. Incomplete encoder instructions are retryable; other decoder failures
are sticky and discard blocked payloads. Live instruction-stream buffering,
HTTP/3 error mapping and critical stream dispatch are still unfinished.

`EncoderInstructionReader` now frames encoder instructions from an `io.Reader`
after the stream-type prefix. String lengths are checked before allocation;
encoded buffer capacity stays within an explicit per-instruction cap. String
bodies are read once without repeated parsing as fragments arrive. Each call
returns exactly one borrowed encoded instruction, valid until the next call,
for semantic processing with `Decoder.ConsumeEncoder`. It does not overread the
next instruction. Boundary EOF, truncated EOF and other source errors remain
distinguishable and sticky; partial storage is released on error. The endpoint
must still map critical-stream closure and source errors, and serialize decoded
updates with section submissions. A reader-to-decoder test verifies unblocking,
acknowledgment and speculative insertion feedback under one-byte fragmentation.

The decoder feedback stream now has fixed-storage instruction reading and ordered
writing. `DecoderInstructionReader` consumes one instruction without read-ahead,
validates its integer and opcode, and preserves boundary EOF, truncation and
source failures. `FeedbackWriter` validates an entire batch before sending bytes,
uses fixed scratch storage, and counts complete instructions accepted by the
writer. Any write failure becomes sticky, including short writes without errors;
feedback is never retried after possible partial delivery. Local validation errors
send nothing and leave the writer usable. The endpoint must serialize batches in
decoder-result order, supply stream type/deadlines, and handle critical-stream
failure. A local decoder-to-writer-to-reader test verifies acknowledgments,
increments and cancellation ordering; this is not live peer interoperability.

`EncoderReferences` now tracks FIFO outstanding dynamic sections per stream and
reference multiplicities under explicit section/reference limits. It derives
Required Insert Count, bounds potentially blocked streams, and handles ACKs,
cancellations and Insert Count Increments. Confirmed receipt alone never releases
an outstanding field reference. Cancellation releases references without advancing
Known Received Count. Eviction requires confirmed receipt, no outstanding
references, and oldest-first ordering. Invalid operations preserve state.

The tracker owns no field contents or wire scheduler. The future encoder must
atomically coordinate actual insertion/eviction with tracking, record sections
before committing them to the wire, and enforce stream cancellation lifecycle.
It keeps no lifetime stream tombstones. Current blocked-stream accounting scans
bounded outstanding queues; performance has not been compared with a peer.
Tests include feedback generated by our decoder, FIFO/duplicate ACKs, repeated
references, copied input, resource limits, and fuzzed accounting invariants.

`EncoderTable` now owns table contents and reference bookkeeping together. It
preflights every eviction required by capacity changes or insertions, then
encodes the instruction before committing either state. Name references and
duplicates resolve before eviction. Failed operations return no wire bytes and
preserve both states, including when an early victim is evictable but a later
victim is protected. Successful instructions must be reliably scheduled in order;
uncertain write failures require connection termination. This does not yet select
fields or automatically construct/track field sections. Those remain the next
encoder integration steps. Tests compare emitted updates with our decoder table;
this comparison is internal consistency evidence, not independent interoperability.

`EncoderTable.EncodeSection` now builds complete sections from explicitly selected
representations. It resolves references, calculates exact decoded and encoded
sizes before output allocation, derives Required Insert Count, and encodes the
prefix and fields. It commits reference tracking only after encoding succeeds;
all failures return no wire bytes and leave tracking unchanged. Limits cover
field count, decoded section bytes and encoded section bytes. The caller still
chooses representations and insertion policy, and commits successful output in
stream order. RFC dynamic-section bytes and the local blocked/ACK cycle are
tested. Existing independent static/literal cases now use this whole-section
encoder; upstream dynamic-section interoperability remains untested.

`EncodeFields` now selects representations for ordered name/value pairs with
never-index flags. It compares per-field encoded sizes across literals, static
references and permitted dynamic references, using Huffman only when smaller.
Static references win ties against dynamic references; newer dynamic entries win
dynamic ties.
Dynamic references respect peer blocked-stream limits and local tracking budgets;
when unavailable, literals remain usable. Sensitive values are never emitted as
indexed fields. Insertion policy remains explicit via `Apply`, and HTTP semantic
validation belongs to the endpoint. The selector uses a bounded linear dynamic
table scan and does not claim globally optimal section size or measured speed.
An independent decoder validates 298 selected static/literal fields; dynamic
selection and never-index preservation have local tests and fuzz coverage.

`InsertField` provides an explicit opt-in eager admission policy. It skips
never-index fields, exact static/dynamic matches, entries too large for current
capacity, protected/unreceived eviction victims and instructions over a wire
budget. It compares literal-name and available static/dynamic name-reference
instruction sizes, using Huffman only when smaller. Capacity configuration is
separate. This policy has no reuse predictor and no measured optimality claim.
Each returned update commits table state independently of section encoding; it
must be reliably sent even if a later section fails. The local insertion-to-
selection-to-blocked-decoding-to-ACK cycle is tested; independent dynamic peer
coverage remains outstanding. Live HTTP/3 stream ownership and error handling
are the next integration work.

A first live integration gate now exists in the isolated interop module:
`TestHTTP3LiveIndependentServer`. Our public QUIC connection, framing/SETTINGS,
critical-stream registry and QPACK codecs interoperate with upstream's HTTP/3
server over v1 and v2. It verifies POST body delivery and request trailers, then
response headers, exact echoed bytes and response trailers. The fixture retains
critical streams and parses initial server SETTINGS. This test-only assembly is
not a public endpoint: it does not dispatch later critical-stream events or
implement complete HTTP validation/lifecycle. QPACK capacity is zero and the
dynamic compression still needs independent live coverage.

The reverse live gate, `TestHTTP3LiveIndependentClient`, now uses upstream's
HTTP/3 Transport against our server components over v1 and v2. It validates
request bodies/trailers and response bodies/trailers through upstream's net/http
interface. Together the two test assemblies establish basic bidirectional live
HTTP/3 interoperability with static/literal QPACK. They do not yet constitute a
production connection owner, complete HTTP semantics, asynchronous critical-stream
dispatch, push, datagrams or independent dynamic-QPACK evidence.

`internal/http3conn.OpenLocal` is the first reusable connection lifecycle component.
It creates control, QPACK encoder and QPACK decoder streams, writes their type
prefixes and control SETTINGS, then transfers writer ownership without sending
FIN. It validates settings before network mutation. Once setup starts, failures
abort the connection rather than reset/replace critical streams. Context
cancellation interrupts blocked writes using temporary deadlines; running
cancellation callbacks are joined before those deadlines are cleared. A transport
adapter uses our public QUIC connection. The future connection owner must enforce
exactly-once initialization and assign sole writers. Both live interop roles now
use this component; ongoing incoming dispatch and a public endpoint are unfinished.

`http3conn.Incoming` now performs cancellable unidirectional type classification
and critical identity registration. Unknown types are stopped with STOP_SENDING;
FIN/reset before a complete prefix is tolerated. Duplicate critical types and
client push streams return connection errors. Distinct streams can be classified
concurrently, with atomic registry claims. Confirmed FIN/reset on a registered
critical stream maps to H3_CLOSED_CRITICAL_STREAM; local deadlines/cancellations
retain their original cause. Setup deadlines are cleared after joining any
cancellation callback. The owner must bound concurrent prefix readers, close the
connection on returned protocol errors, and dispatch known streams. Both live
roles now use this classifier. It is not yet a complete accept/dispatch loop.

`RunIncoming` now owns a bounded accept/classify/dispatch loop. The configured
worker budget includes incomplete stream prefixes and must allow at least three
critical streams. Exhaustion aborts with H3_EXCESSIVE_LOAD. The first handler
failure cancels other workers and selects the connection error; confirmed critical
closure becomes H3_CLOSED_CRITICAL_STREAM. Parent cancellation does not choose a
protocol error. Workers are joined before return, and temporary deadlines unblock
stream reads during cancellation. Handlers must cooperate with context when not
reading and cannot change deadlines or launch unjoined work. Push validation,
control/QPACK state coordination and public endpoint ownership still need wiring;
this loop has deterministic race tests but is not yet used by the live fixtures.

`http3conn.Handlers` now connects the dispatcher to streaming control and QPACK
instruction readers. It delivers validated wire units to context-aware callbacks;
those callbacks must serialize semantic compression/settings state. QPACK codec
failures map to encoder/decoder stream error codes, encoded instruction resource
limits map to H3_EXCESSIVE_LOAD, and transport read errors retain their causes.
Both live HTTP/3 roles use ongoing dispatch rather than a one-shot control read.
Their zero-capacity callbacks remain test policy, not a complete connection owner.
Live lifecycle tests additionally verify duplicate control streams and control
FIN against an independent QUIC peer, including the application error code
observed on the wire, for v1 and v2. Dynamic QPACK ownership, full HTTP semantics
and public APIs remain unfinished.

`ValidateFieldStructure` now validates lowercase token field names, field-value
characters, pseudo-header context/order/uniqueness and prohibited connection-
specific fields. TE is permitted only as trailers in request headers. The
:protocol pseudo-header requires explicit extension permission. Validation leaves
ordered fields and ordinary duplicates unchanged. Failures are `MessageError`
values for H3_MESSAGE_ERROR stream resets, not connection errors. This is only
the structural layer: required pseudo-headers, method/status/URI semantics,
content-length checks and trailer eligibility remain unfinished. Both live roles
apply it to incoming header and trailer sections.

`ValidateControlData` now layers required pseudo-header and basic value validation
on the structural checks and returns parsed control metadata. It checks method
tokens, scheme syntax, basic path rules, HTTP authority/Host presence and agreement,
ordinary CONNECT host/port and forbidden scheme/path, negotiated extended CONNECT
form, and response status (100-599 except 101). The permission argument must come
from actual extension negotiation; connection ownership of that setting is still
unfinished. Both live roles use this layer. Full URI grammar, scheme-specific
policy for non-HTTP targets, WebSocket handshakes, body lengths, trailer eligibility
and message order are not covered. This is not a complete HTTP message validator.

Sources: [RFC 9114 sections 4.3-4.5](https://www.rfc-editor.org/rfc/rfc9114.html#section-4.3),
[RFC 9220](https://www.rfc-editor.org/rfc/rfc9220.html#section-3), and
[RFC 8441 section 4](https://www.rfc-editor.org/rfc/rfc8441.html#section-4).

`ParseContentLength` now validates decimal syntax and equal repeated/list values,
returning canonical decimal text without integer overflow. A forwarding endpoint
must replace repeated fields with that single value. `BodyLength` checks received
DATA totals, rejects overruns before delivery and underruns at message completion,
and retains protocol errors. HEAD/304 metadata can describe arbitrarily large
representations without requiring a body; informational/204/205 responses have
no content. Successful CONNECT responses ignore Content-Length on receipt and
both CONNECT directions treat DATA as tunnel bytes. This receive-side helper is
not an outbound-header policy or a frame-order validator. Fields/control data
must already be validated. Trailer eligibility and full message lifecycle remain
unfinished. Live fixtures now use the helper and explicit content lengths.

Sources: [RFC 9110 section 8.6](https://www.rfc-editor.org/rfc/rfc9110.html#section-8.6),
[CONNECT semantics](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.3.6), and
[RFC 9114 malformed messages](https://www.rfc-editor.org/rfc/rfc9114.html#section-4.1.2).

`Message` now combines field validation and byte accounting into a serialized
receive-side message state machine. It distinguishes informational/final/trailer
sections, enforces DATA placement and one trailer section, checks lengths at
trailers or FIN, and retains terminal protocol errors. Ordering failures use
connection-scoped H3_FRAME_UNEXPECTED; malformed fields and lengths use stream
H3_MESSAGE_ERROR. Unknown frames leave state unchanged; PUSH_PROMISE placement
is checked, while payload/push-ID validation remains external. Successful CONNECT
responses enter tunnel mode; the request owner signals establishment when sending
the corresponding successful response. Trailer checks currently cover pseudo-
headers and framing/routing fields (content-length, host, trailer), not all
field-specific or status-specific trailer rules. Complete application policy,
resource limits and public endpoint ownership remain unfinished.

Both live roles use Message. Their exchanges now include 103 Early Hints and
unknown frames around the request/response, alongside body lengths and trailers.
Sources: [RFC 9114 message framing](https://www.rfc-editor.org/rfc/rfc9114.html#section-4.1)
and [RFC 9110 trailer limitations](https://www.rfc-editor.org/rfc/rfc9110.html#section-6.5.1).

Status-specific receive checks now reject even empty trailer sections on 204 and
304 responses with a sticky H3_MESSAGE_ERROR. Headers-only completion remains
valid. Established CONNECT retains its separate tunnel rules. General sending
eligibility is enforced by `ValidateOutgoingTrailers`: callers must supply a
field-definition permission policy, and unknown fields fail unless that policy
explicitly permits them. A Trailer declaration does not grant permission. Core
structural/framing/routing checks cannot be bypassed by that callback. The helper
does not itself authorize trailers for a status or stream state. Incoming unknown
trailers remain separate; no header merging is performed. The live fixtures now
validate their defined custom trailer fields before encoding them.

Sources: [204](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.3.5),
[304](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.4.5), and
[trailer field eligibility](https://www.rfc-editor.org/rfc/rfc9110.html#section-6.5.1).
Public HTTP APIs still need to provide a field-definition policy and coordinate
outgoing status/stream validation; this helper is not that endpoint integration.

SETTINGS_ENABLE_CONNECT_PROTOCOL (0x08, RFC 9220) is now recognized by the wire
codec; values other than 0 or 1 fail with H3_SETTINGS_ERROR, including during
transactional local encoding. `SettingsState` publishes validated immutable
snapshots of understood settings to concurrent request workers, applies defaults,
rejects repeated peer settings and retains terminal errors. It records QPACK
capacity/blocked-stream and field-section limits without retaining unknown IDs.
Client sending permission depends on the received server advertisement; server
receiving permission depends on its own successfully sent advertisement, never
on a client setting. Live dispatcher callbacks apply peer settings to this state,
and the independent server obtains request validation permission from it.

The constructor must receive the exact local settings after OpenLocal succeeds.
The connection owner still needs to apply QPACK/field limits and integrate public
request sending. This state covers 1-RTT only; remembered settings and 0-RTT
validation remain unfinished. These changes do not implement a WebSocket API.
Sources: [RFC 9220](https://www.rfc-editor.org/rfc/rfc9220.html#section-3) and
[RFC 8441 settings semantics](https://www.rfc-editor.org/rfc/rfc8441.html#section-3).

`FieldEncoder` now owns a serialized QPACK encoder, decoder feedback and exclusive
encoder-stream writes. Construction through SettingsState requires received peer
SETTINGS and applies the smaller of local/peer capacity, blocked-stream and field-
section limits. Required Insert Count encoding retains the peer's full advertised
maximum even when using a smaller local table. Insertion remains explicit and
NeverIndex is preserved. Successful inserts write before subsequent references;
short/failed writes permanently stop the owner, and invalid feedback maps to
QPACK_DECODER_STREAM_ERROR. The connection owner must terminate on these failures.
Per-request owners still serialize section encoding/transmission; writer deadlines
or connection cancellation must interrupt blocked writes. This owner is tested
in-process, including dynamic table wraparound, but has not yet replaced the live
fixture's separate encoder/feedback tables. Live dynamic QPACK and the receive-side
compression owner remain unfinished. Source: [RFC 9204](https://datatracker.ietf.org/doc/html/rfc9204).

Both live roles now construct FieldEncoder from the received settings and the
actual local encoder stream. The same owner handles all encoded sections and
incoming decoder feedback; the former separate zero-capacity feedback table has
been removed. Publication between control/decoder callbacks is atomic, with
request work starting only after initialization. Encoder writes use the test's
connection deadline. The peer's settings still determine whether dynamic use is
possible; these exchanges do not demonstrate live dynamic compression.

Live lifecycle coverage now also injects impossible Insert Count Increment
feedback and invalid ENABLE_CONNECT_PROTOCOL values. It verifies both the local
protocol error and the independent peer's observed application close code for
QUIC v1/v2 (QPACK_DECODER_STREAM_ERROR and H3_SETTINGS_ERROR respectively).
Receive-side QPACK ownership and live dynamic sections remain unfinished.

`FieldDecoder` now owns receive-side QPACK state, blocked section results and
exclusive decoder-stream feedback writes. SettingsState construction derives wire
limits from the local advertisement and rejects budgets that cannot honor its
table capacity or blocked-stream allowance. Submit returns a buffered one-result
channel, with no worker goroutine per section. Encoder updates resume waiting
sections, and feedback is queued before results become visible (see QPACK
decoder feedback above; it was originally written at that point). Cancel releases
a pending section; Close releases all waiters and table state. Callers must close
on connection termination, cancel each stream at most once, and prohibit later
submissions on canceled streams. No lifetime stream tombstone map is retained.

Malformed updates map to QPACK_ENCODER_STREAM_ERROR, malformed field sections
(including those decoded after unblocking) to QPACK_DECOMPRESSION_FAILED, and
local section resource exhaustion to H3_EXCESSIVE_LOAD. Writes fail permanently
without retry. Feedback I/O must be interruptible by a deadline or transport
closure; Close does not interrupt a writer blocked while holding the owner lock.
Per-stream section ordering and connection termination remain external ownership
responsibilities. Unit/race coverage exercises blocking, resumption, ACK order,
cancellation races and failure fanout. Live fixture integration is still pending.

The live fixtures now share FieldDecoder between request HEADERS decoding and
peer encoder-stream callbacks, using the actual local decoder stream for feedback.
They advertise a 4096-byte table and eight blocked streams, wait for each section's
result in request order, and close the owner when dispatch ends. Cancellation
interrupts critical-stream writes before closing the decoder, and the cancellation
callback is joined. The former isolated per-request DecoderTable instances are
removed. Both live roles still interoperate with the upstream HTTP/3 endpoints.

A separate v1/v2 live test sends handcrafted dynamic QPACK over upstream QUIC:
HEADERS referencing insertion 1 arrives first and remains blocked, then an encoder
stream capacity/insertion update releases it. The test checks x:y and the exact
Section Acknowledgment byte seen by the peer. This proves transport-integrated
blocking/resumption/feedback, not dynamic QPACK interoperability with an independent
HTTP/3 implementation. Full endpoint APIs and independent dynamic-peer testing
remain unfinished.

`FieldStream` now coordinates each request's ordered section decoding and QPACK
cancellation. It rejects concurrent Decode calls, makes Cancel idempotent, prevents
submissions after cancellation, and cancels the stream's QPACK work when a Decode
context ends. It does not reset QUIC itself; request transport lifecycle remains
with the stream owner. Cancellation feedback is queued and no longer waits for
the decoder stream. Both live HTTP/3 roles use this wrapper for section reads.

Blocked-section live tests now cover cancellation in addition to insertion-driven
resumption. The canceled waiter receives ErrFieldSectionCanceled, later submission
is rejected, and the peer sees a Stream Cancellation instruction. Unit tests check
one instruction under repeated/concurrent cancellation and context cancellation
after a section is observably blocked. These tests retain handcrafted QPACK peers;
independent dynamic-QPACK interoperability remains unfinished.

(Superseded: feedback is now queued, so a canceled stream reports its own
cancellation, and a failed feedback write fails the decoder for every other
stream and the session.) A cancellation error-propagation bug in FieldStream is fixed: after a failed
feedback write, subsequent Decode calls return that terminal failure instead of
masking it as an ordinary request cancellation. Regression coverage cancels an
observably blocked decode with a short-writing feedback sink, checks failure
fanout to another blocked request, and verifies that later cancellation/decoding
never retries the write or loses its error. A transport deadline or close is still
required to interrupt a writer that has not returned; this change addresses
returned failures, not asynchronous interruption of arbitrary io.Writer values.

Independent offline dynamic-QPACK interoperability is now covered using pinned,
unmodified ls-qpack 2.7.0 CLI tools. Both encoding directions compare 80 sections
with required dynamic references, table eviction and count wraparound. ls-qpack's
encoder produced 108 insertions; its decoder accepts our 128-byte chosen capacity
against a 256-byte maximum with one-byte reads and header/update reordering.
The peer build is reproducible via interop/lsqpack/build.sh and optional tests run
when LSQPACK_BIN is set. No peer implementation dependency is added to production.

This is codec-level evidence: offline tools model acknowledgments and do not
exchange live feedback. The peer encoder CLI omits initial capacity signaling;
the test explicitly mirrors its out-of-band capacity contract. Independent live
dynamic-QPACK/HTTP3 testing, cancellation feedback interoperability, malformed
inputs and public endpoint APIs remain unfinished. See
[peer test instructions](../interop/lsqpack/README.md) for exact scope and commands.

Independent encoder output is now also exercised through FieldDecoder with delayed
updates. The pinned ls-qpack fixture produces 80 blocked sections; all resume with
correct fields and acknowledgments. A second pass cancels every third section,
checking 26 cancellations and 54 successful acknowledgments with no duplicates or
missing feedback. This adds independent-input blocked/cancellation coverage but
still does not establish interactive feedback interoperability with the peer.


Interactive codec feedback is now tested against the pinned ls-qpack library via
a test-only C API driver. Real decoder acknowledgments, cancellation and insert
increments return through pipes, with whole and one-byte writes. With duplication
disabled all 80 sections block, and cancellation mode cancels 27. The duplication-
enabled peer falls back to literals after three blocked sections; all remaining
fields still decode correctly. This behavior is retained in the tests and exposed
as a strict known-failure gate rather than treated as full dynamic interoperability.
See the peer instructions and known-failures document. Public HTTP3 endpoints and
independent live dynamic HTTP3 interoperability remain unfinished.


The ls-qpack fallback is now isolated to its increment baseline for this fixture.
A two-section standalone probe (no independent decoder involved) shows ACK raising
Known Received Count to 2, followed by increment 1 leaving it unchanged. A separate
experimental peer build using the current Known Received Count for addition and
bounds passes that probe and all duplicate-enabled interactive cases with 80
blocked sections. The checked-in patch is diagnostic only; canonical unmodified-
peer tests retain the failure gate. Production QPACK behavior is unchanged.


Live independent dynamic QPACK now has a v1/v2 fixture: ls-qpack encodes responses
on an upstream QUIC server, while our client receives HEADERS before encoder
updates and verifies blocking/resumption, decoded fields, response semantics and
FIN. Real feedback travels on our QPACK decoder stream, is read by the independent
QUIC peer and is passed unchanged to ls-qpack. Eight sequential responses and ten
insertions complete per connection. The advertised decoder maximum is 256, matching
the independent encoder configuration; the shared dispatch helper now accepts the
exact local SETTINGS rather than requiring its default fixture values.

This uses unmodified ls-qpack with duplication disabled and test-owned server HTTP3
framing. It does not claim a complete independent HTTP3 endpoint, live QPACK
cancellation, concurrent request coverage or public endpoint availability.


Live ls-qpack coverage now cancels three of eight blocked response sections on
both QUIC versions. It verifies idempotent FieldStream cancellation, rejection of
later decoding on canceled owners, exact H3_REQUEST_CANCELLED observed by the
upstream transport, and actual QPACK cancellation accepted by the independent
encoder. Later encoder updates still generate insert-count feedback; surviving
responses continue on the same connection. This is explicit cancellation after
an observably blocked Submit, not a concurrent Decode/context-cancellation test.


`MessageReader` now combines frame reading, bounded encoded HEADERS, FieldStream
QPACK decode and Message semantics. Next consumes HEADERS and returns decoded
fields plus initial/informational/trailer classification. DATA stays streamed
through Read, with actual byte counts checked before delivery. Clean FIN validates
message completion; truncated framing, body-length failures and protocol errors
are sticky. Payload-not-consumed is retryable local backpressure. Unknown frames
and PUSH_PROMISE remain visible for endpoint policy and payload validation.

The reader requires serialized calls, a per-stream FieldStream and explicit
encoded-header limit. Its context controls QPACK waits; transport deadlines must
interrupt source I/O. The owner still cancels abandoned streams, maps errors to
stream/connection closure, validates pushes/extensions and coordinates CONNECT.
The live independent-server fixture now uses this reader for informational
responses, body and trailers. This is reusable internal receive-path code, not
a public HTTP3 client/server endpoint yet.


MessageReader's asynchronous QPACK path is now tested after observing a pending
section under the decoder lock. Three cases cover insertion-driven resumption
followed by DATA/FIN, context cancellation and connection decoder failure. Error
cases release the pending request and retain the same error on later Next/Read.
Cancellation emits exactly one instruction under repeated owner cancellation;
late inserts produce only insertion feedback, never a section acknowledgment or
message revival. This does not test interruption of arbitrary blocking source or
feedback writers, which still requires the transport owner's deadlines/closure.


`MessageWriter` now provides the corresponding internal send path: it validates
HEADERS/message state, encodes through the shared FieldEncoder, writes DATA frames
with preflight body-length checks, requires an explicit outgoing trailer policy,
and sends FIN only after successful completion validation. Writes and close
failures are sticky; uncertain bytes/FIN are never retried. Partial DATA progress
excludes frame header bytes. Successful Close is idempotent. Calls and external
extension writes on the same stream must be serialized by the endpoint.

The upstream-client fixture now receives responses through MessageWriter, retaining
informational headers, body, trailers and unknown-frame coverage. Unit tests cover
writer/reader round trips, pre-transmission rejection, short header/payload writes,
partial progress, failed FIN and no retries. Transport cancellation/reset, QPACK
reference lifecycle, interruptible writes and connection policy remain with the
endpoint owner; this is not a public HTTP3 endpoint yet.


Both live interoperability roles now use MessageReader/MessageWriter for both
request and response directions. The independent client sends POST/body/trailers
through MessageWriter to upstream HTTP3; the independent server receives upstream
POST/body/trailers through MessageReader. Existing informational responses,
unknown frames, content checks and v1/v2 coverage remain. The fixtures no longer
carry separate manual message/QPACK receive loops or request frame construction.
Connection dispatch, settings policy and net/http adaptation are still test-owned;
public endpoint ownership remains unfinished.


`Session` now owns SETTINGS application, encoder publication, QPACK instruction
routing and incoming critical-stream dispatch after OpenLocal. Configuration
supplies exact local settings, bounded codec/worker budgets and explicit endpoint
control/push policy. WaitEncoder waits for peer SETTINGS; canceling that wait does
not stop the session. Parent cancellation or Stop interrupts critical writes,
joins dispatcher work, and closes the decoder to release blocked requests.

A transport wrapper cancels the session before dispatcher abort/join, covering
workers blocked writing QPACK feedback or encoder capacity. Tests force encoder
initialization to block and verify both parent cancellation and duplicate critical
streams terminate the session. Live fixtures now use Session, replacing their
manual settings/codec/lifecycle orchestration. Endpoint request ownership,
connection shutdown, GOAWAY/push policy and net/http adaptation remain external.


Session now accepts QPACK Stream Cancellation before encoder publication/peer
SETTINGS. No sections can have been encoded through this session yet, so there
are no references to release. Rejecting every early feedback instruction was
unnecessarily strict for an abandoned request. Early Section Acknowledgment or
Insert Count Increment remains QPACK_DECODER_STREAM_ERROR. This follows the
cancellation behavior in [RFC 9204 section 4.4.2](https://datatracker.ietf.org/doc/html/rfc9204#section-4.4.2).
Tests keep the decoder stream open, observe cancellation processing before
sending SETTINGS, then verify encoder publication and use. A canceled waiter
must not terminate the session. Early ACK/increment cases verify connection error.


Session now publishes a PeerGoaway snapshot before invoking the endpoint's control
callback. The existing ControlReader validates role/range/non-increasing limits;
the snapshot distinguishes server request IDs from client push IDs. Any received
GOAWAY prohibits initiating new requests/promises, regardless of its limit. For
already initiated work, Rejects identifies IDs at or above the limit; lower IDs
might have been processed and are not automatically safe to retry. See
[RFC 9114 section 5.2](https://datatracker.ietf.org/doc/html/rfc9114#section-5.2).
The endpoint must serialize admission decisions, process every lower limit, cancel
rejected streams and decide retries. Snapshot publication alone does not implement
graceful shutdown. Tests cover publication-before-callback, both roles, maximum,
repeated/decreasing/zero limits and immutable returned copies.


Session now owns outgoing control writes through SendGoaway. It validates local
role/range and non-increasing limits, serializes writes and permits repeated
limits. Invalid local IDs return ErrGoawayID without writing or poisoning the
session. Write failures abort with H3_INTERNAL_ERROR and cancel the session;
uncertain bytes are never retried. Stop/parent cancellation also interrupts the
control writer, and session completion joins an in-flight control write.

Tests check both roles' actual frames through ControlReader, maximum/repeated/
decreasing/zero limits, invalid IDs, short writes and a blocked write interrupted
by Stop. Graceful admission/draining/retry policy and live GOAWAY interoperability
remain incomplete. Control/push callbacks must not synchronously call Stop, which
joins them; canceling the parent context initiates shutdown from a callback.


GOAWAY now has live QUIC v1/v2 coverage with an upstream QUIC client and our
server Session. The peer receives emitted maximum/repeated/decreasing/zero
request limits. It sends client push limits 5, 5, 0 and verifies publication via
control callbacks/snapshots; the non-multiple-of-four ID tests the client role.
A subsequent increase to 1 must close the connection with remote H3_ID_ERROR,
even after our server has sent its final zero limit. Session shutdown is joined.
The peer's HTTP3 control framing is fixture-owned; independent HTTP3 endpoint
draining, admission and retry semantics remain unverified and unfinished.


Session now offers bounded client request leases via MaxRequests/BeginRequest.
Admission is serialized with received GOAWAY. Any GOAWAY blocks new admission;
server limits cancel existing leases at/above the boundary with ErrRequestRejected
before the control callback. Lowered limits re-evaluate retained leases. Canceled
leases stay charged until idempotent Finish, so stalled cleanup remains bounded.
Session failure cancels leases with its cause and clears registry references.
Zero MaxRequests disables managed admission. The endpoint must register each
allocated request ID exactly once, never reuse IDs, and handle failed admission,
stream/QPACK cancellation and retries; no lifetime tombstone map is retained.

Tests cover budgets, invalid/duplicate IDs, release, rejection boundaries, lower
limits, canceled waiters, connection failure and concurrent GOAWAY/admission.
A broader run also found an accept/abort race that could mask protocol failure
with context.Canceled. The session transport now propagates context.Cause from
the dispatcher, with a deterministic regression test. Request leases are internal
infrastructure; public endpoint integration and live leased-request draining are
still unfinished.


Managed request leases now have live v1/v2 coverage against upstream QUIC with
fixture-owned HTTP3 control/response framing. Two GET streams are admitted; server
GOAWAY(4) rejects stream 4 while stream 0 remains active, and new admission fails.
The fixture cancels rejected QPACK work and sends STOP_SENDING; the peer must see
remote H3_REQUEST_CANCELLED. Stream 0 then completes a valid response through
MessageReader. Leases are released explicitly. This establishes partial draining
behavior over transport, not public endpoint cleanup automation or retry policy.


`MessageBody` now adapts a MessageReader positioned after final/initial HEADERS to
io.ReadCloser. Reads stream DATA across frame boundaries, skip unknown frames,
deliver validated trailers separately and require explicit PUSH_PROMISE policy.
Nil trailer policy discards incoming trailers; nil push policy rejects promises.
Close is concurrent-safe and invokes endpoint cleanup once, reporting completion
only after clean semantically valid FIN. It cancels QPACK waits; the mandatory
cleanup callback/deadlines must interrupt source I/O and release request resources.
Callers must Close after EOF/errors as well as abandonment. No reset/retry policy
or connection closure is selected by the adapter.

Tests cover fragmented DATA/empty frames, unknown frames, trailers, short content,
push rejection, early close, retained cleanup failure and concurrent Close while
source I/O is blocked. The live leased-request GOAWAY fixture uses MessageBody for
the accepted response and releases its lease through successful body cleanup.
Full public net/http adaptation and generalized request cleanup remain unfinished.


MessageBody now preserves context.Cause when a QPACK/body wait returns the matching
context cancellation error. Previously both pre-canceled reads and blocked trailer
reads reduced GOAWAY rejection to context.Canceled. Tests reproduce both cases,
check sticky ErrRequestRejected and exactly one cancellation on cleanup. Separate
source-injected protocol/transport failures concurrent with rejection retain their
original errors, avoiding a false guarantee that the request was unprocessed.
Cleanup and retry decisions still belong to the endpoint.


Before net/http adaptation, a sender/receiver distinction was corrected in
MessageWriter: successful CONNECT responses now reject Content-Length before any
QPACK/header bytes are sent. The receive-side Message continues to ignore that
field on successful CONNECT as required. Outgoing equivalent repeated/list length
values are normalized to one decimal field; caller slices are not mutated and
NeverIndex from any occurrence is retained. These sender rules follow
[RFC 9110 section 8.6](https://datatracker.ietf.org/doc/html/rfc9110#section-8.6).
Tests cover 200/201/299 CONNECT statuses with zero, nonzero and invalid length
values, receiver tolerance and an encoded/decoded normalization round trip.


`ReadHTTPResponse` now adapts the receive path to net/http.Response. It bounds
informational responses, supplies independent interim header maps, canonicalizes
lengths, preserves repeated ordinary headers, removes pseudo-fields and exposes
MessageBody. Declared trailer keys are present before body reads; actual trailers
populate Response.Trailer without merging into Header. Oversized numeric metadata
stays intact in Header and uses -1 in the int64 ContentLength slot; exact wire
body validation stays in MessageReader. Successful CONNECT ignores Content-Length.
Bodyless responses without declared lengths report zero. Cleanup runs on failure
before final headers or transfers to the returned body's Close on success.

The live client fixture now uses this adapter against upstream HTTP3 on v1/v2.
Tests cover informational limits/callback errors, duplicate normalization,
Set-Cookie preservation, announced/unannounced trailers, large HEAD metadata,
CONNECT, bodyless statuses and failure cleanup. TLS state, decompression, tracing,
redirects, retries, pooling and the public HTTP3 transport/server remain unfinished.


`ReadHTTPRequest` now builds server net/http.Request values from MessageReader.
It preserves escaped path/query and RequestURI, validates target/authority mapping,
places authority in Request.Host, combines split Cookie fields, canonicalizes
lengths and keeps trailers separate. It supports ordinary CONNECT authority-form
and negotiated extended CONNECT, returning ControlData for the protocol value.
Body uses MessageBody with failure/close cleanup ownership. Unknown request lengths
use -1; very large values remain intact in Header and are checked by the message
layer. TLS state, RemoteAddr and handler lifetime remain server responsibilities.

Tests cover paths/queries, cookies, body/trailers, ordinary/extended CONNECT,
malformed escapes/fragments/authorities, OPTIONS asterisk-form and Host fallback.
The live server fixture now consumes this adapter against upstream HTTP3 clients
on v1/v2. Public handler dispatch and full client/server APIs remain unfinished.
