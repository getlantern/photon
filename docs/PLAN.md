# Implementation and verification plan

The user authorized an independent Go implementation with QUIC v1/v2,
connection migration, current multipath, quic-go feature compatibility,
and measured performance improvements. The ongoing goal remains active until
the implementation and validation are finished. Work in bounded, tested
increments to preserve session availability. Do not substitute upstream
quic-go for the implementation or claim complete conformance from local tests.

## Architecture

- `internal/wire`: encodings and bounded parsers. No sockets or connection state.
- `internal/packet`: standard-library/x-crypto packet protection. Connection
  state must enforce packet number uniqueness, replay handling, AEAD usage
  limits, key retirement, and coordinated key updates.
- `internal/handshake`: serialized TLS events and independent CRYPTO offset
  spaces, with bounded reassembly and explicit output ownership.
- `internal/transport`: bounded received-packet tracking, per-path
  anti-amplification accounting, connection-ID management, and a socket-independent
  handshake engine.
- Connection engine: serialized state transitions, explicit timestamps and
  packet I/O, independent from the UDP adapter. Keep local connection IDs,
  packet number spaces, encryption levels, and path identity distinct.
- Path manager: validation, address binding, anti-amplification budgets,
  RTT, congestion state, pacing, PMTU, and lifecycle per path. Base QUIC still
  has three connection packet-number spaces; multipath adds the specification's
  path-specific application spaces and nonce construction. Do not accidentally
  introduce per-path packet numbers under the current single-path nonce scheme.
- Public API: context cancellation, explicit ownership, bounded queues,
  stream deadlines, transport reuse, and documented upstream compatibility.

## Standards targets

This is a starting inventory, not a claim that every related RFC is applicable
or already implemented. Add section-level requirements and tests as each
subsystem is implemented. Pin moving drafts and upstream comparison versions.

| Standard | Scope | Current status |
| --- | --- | --- |
| [RFC 9000](https://www.rfc-editor.org/rfc/rfc9000.html) | Core transport, streams, connection IDs, migration | Public client/server streams, flow control, CID lifecycle, migration, close/drain and idle handling implemented and tested; exhaustive section-level conformance audit incomplete |
| [RFC 9001](https://www.rfc-editor.org/rfc/rfc9001.html) | TLS, packet protection, key lifecycle | Protection vectors, key retirement/updates/usage limits and version-bound 1-RTT resumption implemented; opt-in public early dial/listener and STREAM/DATAGRAM transport with independent accepted STREAM gates exist; early API and combined-extension coverage remain incomplete |
| [RFC 9002](https://www.rfc-editor.org/rfc/rfc9002.html) | Loss detection, congestion control | RTT/loss/PTO, NewReno, pacing, persistent congestion and ECN implemented with unit/interop gates; full conformance and adverse-network validation incomplete |
| [RFC 8999](https://www.rfc-editor.org/rfc/rfc8999.html) | Version-independent invariants | Basic header parsing |
| [RFC 9369](https://www.rfc-editor.org/rfc/rfc9369.html) | QUIC v2 | Public v2 handshakes/streams/migration and protection vectors; pinned upstream v2 key-update failures explicitly tracked |
| [RFC 9368](https://www.rfc-editor.org/rfc/rfc9368.html) | Compatible version negotiation | Authenticated version information and incompatible fallback; experimental public opt-in switching and ticket/token binding tested; combined-negotiation downgrade checks tested; independent v1-to-v2 switching in both roles passes; reverse-direction independent switching remains unverified (see COMPATIBLE_VERSION.md) |
| [RFC 9221](https://www.rfc-editor.org/rfc/rfc9221.html) | QUIC DATAGRAM | 1-RTT engine send/receive, negotiation, bounded queues, congestion/loss accounting; priority API and delayed ACK policy pending |
| [RFC 9287](https://www.rfc-editor.org/rfc/rfc9287.html) | QUIC bit greasing | Public opt-in, authenticated bit handling and randomized sending; own-peer and independent Picoquic migration/multipath gates exist; broader PMTU/reordering audit pending (see QUIC_BIT.md) |
| [RFC 8899](https://www.rfc-editor.org/rfc/rfc8899.html) | Datagram packetization layer PMTU discovery | Automatic concrete-address Linux/macOS probing, loss/fallback/migration handling and local EMSGSIZE integration; wildcard/other-platform discovery and full audit incomplete (see MTU.md) |
| [RFC 9114](https://www.rfc-editor.org/rfc/rfc9114.html) | HTTP/3 | Experimental public server, client and pooled RoundTripper, request semantics and independent-peer gates; full API/semantic parity incomplete (see http3/README.md and HTTP3.md) |
| [RFC 9204](https://www.rfc-editor.org/rfc/rfc9204.html) | QPACK | Encoder/decoder tables, instructions, field sections, bounded blocking and feedback integrated with HTTP/3; independent ls-qpack/peer gates exist; full audit and compression/performance parity incomplete |
| [RFC 9297](https://www.rfc-editor.org/rfc/rfc9297.html) | HTTP datagrams and capsules | Negotiated HTTP datagram dispatch, bounded capsules, Extended CONNECT and capsule-protocol validation implemented with independent gates; full ecosystem/API interoperability incomplete |
| [Multipath draft-21](https://datatracker.ietf.org/doc/draft-ietf-quic-multipath/21/) | Multipath negotiation, frames, protection, lifecycle | Experimental live v1/v2 negotiation, per-path protection/recovery, CID/status lifecycle, validation, abandonment and migration; independent Picoquic fixtures; full audit incomplete—see [multipath evidence](MULTIPATH.md) |

Multipath status was rechecked on 2026-09-25: the
[Datatracker](https://datatracker.ietf.org/doc/draft-ietf-quic-multipath/) still lists
draft-21 in the RFC Editor queue, with first edit in progress. Recheck status and final
assigned code points before release. Scheduling and address discovery are not specified by that draft;
they need separate policies and tests. Survey additional extensions, errata,
and upstream capabilities before declaring the inventory complete.

## Milestones and acceptance criteria

1. **Wire and cryptographic foundation.** RFC vectors, malformed-input tests,
   bounded parsers, fuzz targets, and TLS integration. Partial implementation
   exists; transport parameters, core frame codecs, shared dispatch, and
   packet-type restrictions are implemented and wired into a handshake engine.
2. **Core interoperable transport.** UDP client/server, parameter validation,
   packet number spaces, ACKs, CRYPTO recovery, handshake confirmation, packet
   coalescing, address validation and anti-amplification. Then bidirectional
   streams, stream and connection flow control, final sizes, cancellation,
   close/drain, and idle timers. Gate: certificate-verified v1/v2 stream exchange
   with a pinned upstream quic-go release in both client/server roles.
   Current evidence includes public v1/v2 UDP handshakes, stream transfers,
   cancellation/retirement, flow control and CID lifecycle against quic-go
   v0.63.0 (peer refreshed 2026-09-28), plus engine loss/reordering tests. Higher incoming stream windows
   and default 100/100 credit now work without the old local/global stream caps.
   Long v2 transfers against the pinned peer can fail at its known key-update
   boundary; keep those explicit failures separate from passing shorter gates.
3. **Recovery and resilience.** RFC 9002 RTT/loss/PTO/congestion algorithms,
   pacing, persistent congestion, ECN, PMTU, reordered/duplicate packets,
   packet loss, and controlled network simulations. Ensure untrusted input
   cannot allocate memory proportional to offsets or unbounded stream counts.
   RTT estimation, packet/time-threshold loss detection, and saturated PTO
   duration calculation, timer selection/backoff, and baseline NewReno send
   gating, packet pacing, and bounded cross-space persistent-congestion detection
   are implemented, including ECN validation and history retirement. Broader
   adverse-network, fairness, platform and sustained-load evaluation remains
   required; existing tests are not proof of exhaustive RFC coverage.
4. **Complete base lifecycle.** Retry and token validation, version negotiation,
   stateless reset, CID rotation/retirement, key usage limits and updates,
   0-RTT/resumption with parameter binding, transport errors, and exhaustive
   RFC 9000/9001/9002 requirement audit.
5. **Migration.** Probing, NAT rebinding, active migration, preferred address,
   disable_active_migration semantics, CID privacy, anti-amplification,
   simultaneous paths during validation, failed-path recovery. Test movement
   during active streams and loss, including both IP families.
6. **Multipath.** Pin current specification and negotiate opt-in support.
   Implement path IDs, per-path packet numbers and ACKs, nonce construction,
   coordinated key updates, path CIDs/status/closure/limits, per-path recovery
   and congestion. Gate: independent multipath interoperability and fallback
   to ordinary QUIC. Add schedulers and evaluate heterogeneous path behavior.
7. **Feature/API parity.** DATAGRAM, HTTP/3, QPACK, HTTP datagrams, observability,
   qlog and platform I/O optimizations. Inventory quic-go's public API and
   dependencies such as WebTransport separately; define and test supported
   integration boundaries rather than claiming all ecosystem packages are
   part of one transport RFC.
8. **Performance and release gates.** Reproducible independent benchmarks,
   loss/migration/multipath workloads, fuzz campaigns, race and platform tests,
   interoperability matrix, documentation, and external security review.

## Current unfinished work

See [remaining requirements](REMAINING.md) for the current evidence-backed gap
map and 0-RTT implementation sequence. This plan is a live summary; chronological
entries in PROGRESS.md and subsystem notes can describe superseded limitations.
No milestone above is complete solely because a named feature has a passing test.

## Benchmark rules

Pin this implementation and the upstream baseline by revision and record Go
version, OS, architecture, CPU, network topology, cipher, payload/stream sizes,
logging, and offload settings. Use repeated runs and statistical comparisons.
Measure goodput, throughput, p50/p95/p99 latency, CPU, allocations, peak and
steady-state memory, handshake cost, fairness, migration outage, and multipath
reordering overhead. Include IPv4/IPv6, LAN/WAN, loss/reordering, asymmetric
paths, many connections, and both bulk and small-message workloads. Local
microbenchmarks are diagnostic only, never evidence of universal superiority.
