# Independent QUIC implementation in Go

This repository is a new implementation, not a fork of upstream quic-go.
The module path is `github.com/getlantern/photon`. Upstream quic-go will be
used as an external interoperability and performance reference, not as the
transport implementation.

**Status: experimental client dialing, server listener, stream, and datagram APIs,
backed by an independent connection engine.** Full API compatibility remains
unfinished. Local interoperability tests pass with the documented exceptions;
this does not establish complete RFC conformance or a performance advantage.

`Transport{Conn: udpSocket}` now supports `Listen`, `ListenEarly`, `Dial`, `DialEarly`, and `Close`.
Closing its listener preserves accepted connections and permits a replacement
listener. Closing the transport stops QUIC and returns the still-open socket to
its caller. Dialed and accepted connections share that socket. This adapter
supports `net.PacketConn` wrappers that expose UDP addresses and honor deadlines.
Plain wrappers use generic reads/writes without ECN or kernel-dependent MTU probing;
`*net.UDPConn` and `OOBCapablePacketConn` wrappers retain socket capabilities
through their control-message methods. Preferred-address sockets and other
Transport options remain unfinished. `ListenAddr` continues to own and close its socket and connections.

`Transport.WriteTo` and `ReadNonQUICPacket` share the socket with raw datagrams.
Raw reception starts on first read and uses a bounded queue. Packets with both
leading bits clear are eligible, but known QUIC routes take precedence so greased
QUIC packets stay with their connection. Use these APIs instead of reading or
writing the socket directly while the transport owns it.

Requires Go 1.26 or newer. Cryptography uses Go's standard library and
`golang.org/x/crypto`; no cryptographic primitives are implemented from scratch.

Experimental multipath is opt-in through `Config.EnableMultipath`, following
[RFC 10069](https://www.rfc-editor.org/rfc/rfc10069.html). Both endpoints must enable it. Bind listeners to a
concrete IP and set the client's `Config.LocalAddr` to a concrete IP (port zero
is allowed). `ConnectionState.SupportsMultipath` reports negotiation.
`Conn.OpenPath(ctx, id, local, remote)` opens and validates additional client
paths with IDs 1–3 in increasing initiation order; `Path.Close` abandons the path and closes its socket.
`Conn.AbandonPath(ctx, id)` retires a path (including primary ID zero) while
preserving its socket, provided another validated path remains. This permits a
planned handoff without closing a shared listener socket.
`Conn.SetPathBackup(ctx, id, backup)` advertises a receive preference to the
peer. Updates are reliably queued with per-path ordering; the call does not
wait for acknowledgment or force the peer's scheduling policy.
New stream/datagram frames are distributed round robin across validated
available paths, with backup paths used when no available path qualifies.
Idle validated paths can carry queued stream/datagram frames from another
path. Blocked attempts leave queue ownership unchanged. This initial scheduling
policy is not RTT-aware and has no measured throughput advantage yet.
Cancellation closes an attempted socket; IDs used for probes cannot be reused.
The connection owns these sockets. Closing the last viable path or last registered socket through
a path handle is rejected; close the connection instead. Servers support client
NAT address rebinding on negotiated multipath paths within the existing socket
family, including fresh destination CIDs when an unused peer CID is available
for validation probes. The server validates the new address before
admitting its application data; Conn.RemoteAddr reports primary-path changes.
`Conn.PathAddresses(id)` reports a consistent local/remote binding pair on either
endpoint, including additional paths. Address snapshots survive abandonment and
shutdown; their presence does not imply that a path remains usable.
IP changes reset RTT and congestion estimates; port-only changes retain them.
Independent v1/v2 multipath interoperability has been exercised in both roles
against pinned Picoquic, including selected loss cases; see [interop](interop/picoquic/README.md).
Address-family changes, wildcard packet-info, and comparative multipath performance
evaluation remain open.

HTTP/3 now has an experimental [HTTP/3 server](http3/README.md),
with independent QPACK, standard Go handlers, bounded request scheduling and
graceful shutdown, listener serving, and a client RoundTripper for established
connections plus dialing, an origin-based connection pool, gzip decoding and bounded
rejection retries. General retry policy, push, extended CONNECT
and 0-RTT remain unfinished.
See [HTTP/3 status](docs/HTTP3.md).

Linux/macOS UDP sockets explicitly disable IP fragmentation. Concrete-address runtimes
automatically probe UDP payload sizes up to 1452 bytes, bounded by the peer limit.
`InitialPacketSize` defaults to 1280 bytes; nonzero values are clipped to
1200–1452. `DisablePathMTUDiscovery` disables upward probes while retaining that
original-path starting size. Migration or size failures can reset a path to 1200.
Constrained-network
validation and broader platform support remain unfinished; see
[datagram size status](docs/MTU.md).

Concrete-address listeners also support ordinary QUIC client rebinding without
multipath negotiation when both endpoints use nonzero CIDs. This uses RFC 9000
frames and the same validation/recovery policy. Wildcard listeners, zero-length
peer CIDs, and connections whose peer CID history was retired before handoff
retain the fixed-address engine. Clients with a concrete `Config.LocalAddr` also
activate ordinary path ownership after confirmation and expose path-zero address
snapshots without negotiating multipath. `Conn.Migrate(ctx, local)` opens a new
concrete client socket and validates/commits primary-path migration, waiting for
available peer CIDs. Port zero selects a free port. Cancellation cancels an
uncommitted attempt; a committed migration wins the cancellation race. An unused
previous socket is closed. The original dial must use a concrete `LocalAddr`,
nonzero CIDs and a peer permitting active migration. `Path.Migrate(ctx, local)` migrates an established additional path while retaining
its path ID; `Path.Close` releases its latest socket. Cross-family migration
remains unfinished.

Implemented and tested:

- Fixed-path PATH_CHALLENGE responses with padding, amplification checks, and
  send-once semantics, plus locally initiated validation with fresh tokens,
  bounded retries, expiry, and separate address/MTU validation. Migration remains open.

- Handshake close packets at all available protection levels, with application
  details stripped from Initial/Handshake copies and cached response bursts.

- Persistent-congestion loss intervals across packet-number spaces, with a bounded
  evidence history and NewReno minimum-window response.

- QUIC variable-length integers and packet-number reconstruction.
- v1/v2 long headers, short-header routing fields, Version Negotiation parsing,
  and coalesced packet boundaries. Stateless Version Negotiation responses and
  client rejection validation and one fallback attempt with authenticated
  downgrade checks are implemented.
- RFC 9368 version-information parameter encoding, role/range validation,
  handshake advertisement, and chosen-version binding to the packet version.
  Experimental Config.EnableCompatibleVersionNegotiation enables v1/v2 switching
  using Versions as preference. InitialVersion can select a different supported
  first-flight version without changing that preference. Public IPv4/IPv6 gates
  cover both roles, Retry, ticket/token provenance and forged combined-negotiation
  rejection. Broader independent switching verification remains unfinished; see docs/COMPATIBLE_VERSION.md.
- Initial secret derivation, AES-128-GCM, AES-256-GCM, ChaCha20-Poly1305,
  header protection, key-update derivation, and Retry integrity tags.
- CRYPTO frame encoding and decoding, bounded out-of-order reassembly.
- All RFC 9000 core frame codecs and shared dispatch, including ACK ranges,
  STREAM flags, flow-control frames, connection IDs, path probes, and closes.
- Packet-type and endpoint-role frame restrictions, plus ACK-eliciting,
  probing, and bytes-in-flight classification.
- All RFC 9000 transport parameters, preferred-address encoding, sender-role
  and range checks, and Initial/Retry connection-ID binding validation.
- RFC 9221 DATAGRAM negotiation, bounded send/receive queues, size validation,
  and unreliable delivery integrated with congestion control and pacing.
  QUIC-bit-greasing remains a parameter codec only.
- Bounded receive/ACK history with replay suppression and a per-path
  anti-amplification accounting component.
- A socket-independent v1/v2 handshake engine with packet-number spaces,
  coalesced input, bounded deferred decryption, Initial padding, authenticated
  frame dispatch, CID binding, ACK generation, confirmation, and key retirement.
- Explicit transport-error closing, including padded Initial closes and
  preservation of a previously reported failure.
- Confirmed application closing with cached protected close packets, bounded
  responses, and transition to draining.
- Peer-close/reset draining with a fixed three-PTO routing lifetime and
  immediate release of stream buffers and packet keys.
- Stateless-reset detection using constant-time token comparisons for used,
  active peer CIDs, with immediate output suppression.
- Local/peer connection-ID issuance, consistency checks, active limits,
  retirement, routing updates, and same-path rotation.
- RFC 9002 RTT estimation and ACK-based packet/time-threshold loss detection,
  connected to reliable-frame retransmission; bounded ACK-delay/PTO arithmetic.
- Per-key confidentiality and connection-wide integrity usage-limit enforcement,
  with automatic application-key updates at the encryption bound.
- Locally initiated and authenticated peer application-key updates, send-key changes,
  reordered old-key reception, and three-PTO old-key retirement.
- Negotiated idle timeout with authenticated activity tracking, the three-PTO
  minimum, and silent expiration.
- Recovery timer selection, PTO backoff, handshake anti-deadlock probes,
  address-validation gating, and encryption-level timer retirement.
- Bounded token-bucket packet pacing with timer wakeups, ACK bypass, and
  accounted PTO exceptions.
- BBRv3 congestion control (draft-ietf-ccwg-bbr-07), the default, driven by
  delivery-rate sampling and paced at its bandwidth estimate.
- NewReno congestion-window gating, slow start, byte-counted congestion
  avoidance, recovery-epoch loss reduction, and bounded PTO send exceptions,
  selected with `Config.CongestionControl`.
- A standalone bounded stream receiver with reassembly, FIN/RESET final-size
  validation, and shared connection receive-flow accounting.
- Stream send-credit/FIN/reset state and constant-memory stream-ID
  opening/acceptance tracking, including direction and count-limit checks.
- Explicit terminal stream release, reliable MAX_STREAMS replenishment, and
  merged closed-ID ranges that suppress late-frame resurrection.
- Reliable stream cancellation via STOP_SENDING/RESET_STREAM and send completion
  after all data and FIN are acknowledged.
- Engine stream opening/acceptance, reliable writes, reads, receive-window
  replenishment, superseded-credit retirement, and frame dispatch;
  v1/v2 bidirectional and unidirectional in-memory transfers under loss/reordering.
- A TLS 1.3 adapter with per-level keys, offsets, and copied TLS event data.
- Byte-for-byte packet protection tests from RFC 9001 and RFC 9369 Appendix A.
- In-memory certificate-verified TLS handshakes over protected packets in both
  versions, with fragmented/reordered/duplicated CRYPTO data and a check that
  both endpoints derive matching application traffic keys.
- Full engine-to-engine handshake tests under packet loss, reordering,
  duplication, and coalescing. Probe retransmissions use fresh packet numbers.
- Local UDP interoperability tests against upstream quic-go v0.65.0 in both
  roles and versions, including CID rotation and zero-length peer CIDs.
- Upstream v1 bidirectional stream requests/responses exceeding both peers’
  receive windows; v2 equivalents run since the upstream v0.65.0 pin.

The lower-level TLS tests use two instances of this implementation. The separate
`interop` module uses a real upstream peer over loopback UDP; it does not yet
establish complete transport behavior. Its v1 and v2 stream cases pass; the large v2 cases were skipped until the
upstream pin moved to v0.65.0 (see interop/KNOWN_FAILURES.md). Two current-path
validation cases against the upstream server are also explicitly skipped because
it ignores those challenges; validation against its client passes in both versions.
Upstream fallback to v2 is also skipped by default because the pinned server
omits mandatory authenticated version information; v1 fallback passes. The handshake engine
checks packet CIDs against transport parameters and distinguishes TLS completion
from QUIC confirmation. The lower-level TLS adapter cannot perform these packet
checks on its own. ACK-driven loss detection queues retransmissions automatically;
the caller services loss/PTO deadlines through the engine's timer API. The
interop driver uses these deadlines. The congestion controller (BBR, or NewReno)
and pacing gate in-flight output;
persistent-congestion detection uses bounded cross-space history. Linux/macOS
UDP readers acquire ECN markings for receive feedback in ACKs, preserving counters
per packet-number space and multipath path. IPv4/IPv6 reception has runtime
coverage on macOS and Docker Linux arm64; see docs/LINUX_VALIDATION.md. These
UDP runtimes also test ECN with up to ten ECT(0) packets per network path or
three base PTO durations, validate peer feedback, and apply validated congestion
signals to the congestion controller: NewReno halves its window, and BBR answers
CE marks as it answers loss. Unsupported ancillary writes
fall back locally to Not-ECT with corrected recovery accounting. Migration
preserves cumulative feedback and starts fresh path testing; failed ECN contexts
remain disabled. Independent quic-go tests verify continued ECT(0) marking
beyond the probe budget and fallback to Not-ECT when the peer cannot report
ECN, for both roles, v1/v2, and IPv4/IPv6 on macOS.
Broader platform testing and ECN-specific independent-peer
validation of the full sender state machine remain incomplete.
Recovery retains at most 4096 packet records per space. Old pure-ACK metadata
is evicted first, preserving in-flight records and exact unsent-ACK checks.
Evicted ACK-only timestamps cannot supply RTT samples. Long-lived reliable-loss
history and full-ledger probe behavior still need production evaluation.
Client Retry is implemented, including integrity checks, new Initial keys, token
copying, ClientHello repacketization, and parameter binding. Servers can initialize from verified Retry routing context and construct Retry
packets. Authenticated Retry tokens bind addresses, versions, CIDs, and expiry, with
explicit key rotation support. Authenticated but expired Retry tokens receive a
stateless INVALID_TOKEN close; unreadable tokens carry no validation evidence.
Transport.VerifySourceAddress controls Retry for unvalidated sources: nil/false
skips Retry while retaining amplification limits, and true requests Retry.
ListenAddr retains its legacy mandatory-Retry policy. Public listeners also issue one NEW_TOKEN after
confirmation, protected by a separate token key. Transport.MaxTokenAge sets the lifetime
(default 24 hours), with subsecond precision; legacy tokens remain readable.
Transport.TokenGeneratorKey can preserve tokens across transport replacement;
shared Transport listener restarts retain their key even when randomly generated. Valid tokens bind the client IP and QUIC version and permit a subsequent
connection from a different port to avoid Retry. Legacy ListenAddr restart replaces its key, so old recognized tokens no longer
validate the address. DialAddr can opt into a shared
Config.TokenStore (NewLRUTokenStore) to consume cached tokens scoped to server
name, port and QUIC version. Client storage caps tokens at 512 bytes and 24 hours,
with bounded duplicate suppression. Independent upstream-client and upstream-server
reuse tests cover v1/v2. A stateless token-validated admission function
connects these pieces, and an internal multi-connection endpoint combines routing,
admission, timers, and output. An internal UDP runtime provides serialized socket
ownership and cancellable acceptance. The public ListenAddr API accepts v1/v2
connections and exposes contexts, datagrams, and application close. Listener.Close
also closes accepted connections. DialAddr uses a dedicated UDP socket, certificate
verification through the supplied TLS configuration, cancellable DNS/handshake, and
automatic socket shutdown after closing/draining. It tries the first resolved address
and configured versions in preference order (v1 then v2 by default), with one
fallback after valid Version Negotiation. Address racing, shared client sockets,
listener ownership parity, full compatible version negotiation,
and migration remain unfinished. Retry tokens that
cannot fit the configured Initial budget produce a local resource failure.
`Config.KeepAlivePeriod` opts into periodic PINGs after handshake confirmation;
zero disables it and negative values are rejected. The interval is capped at
half the negotiated nonzero idle timeout. Authenticated traffic postpones the
next keepalive, and an ack-eliciting send leaves further probes to loss recovery
until a peer packet arrives. Keepalive uses ordinary congestion/pacing and path
validation gates, and cannot keep a silent peer alive past idle expiry. It is a
connection-level facility, not a promise to preserve every multipath NAT binding.
The behavior follows [RFC 9000 §10.1.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-10.1.2).

Stream concurrency follows negotiated per-direction limits rather than fixed
local or global object caps. `Config.MaxIncomingStreams` and
`Config.MaxIncomingUniStreams` set each peer-initiated concurrency limit: zero
selects 100, negative values disable that direction, and values above 2^60
are clipped to the protocol maximum. `Config.RequireStreamCreditRelease` keeps a
finished peer-initiated stream counted against those limits until the
application calls `ReleaseStreamCredit` on it. Locally opened streams follow the peer's
advertised credit. Receive windows default to 512 KiB per stream with growth
enabled and can be configured up to 16 MiB; receive storage is allocated on the
first retained payload byte, grows with the buffered span, and is released on
reset or terminal consumption. Idle and empty streams reserve no payload arrays.
A far-ahead fragment can still allocate the full window, so large concurrency
settings can consume substantial memory; see the receive-credit paragraph below.
Public streams automatically release terminal engine storage and replenish
stream counts; released IDs retain compact
closed ranges instead of final-size records. The public API exposes bidirectional
and unidirectional streams, blocking reads/writes, cancellation, and mutable
deadlines. Cleanup waits for consumed/canceled receive data and acknowledged
send data or reset. Terminal read results remain available on public handles
after cleanup. Release(ctx) is an optional completion wait. Send-side Context cancellation
tracks FIN, local/remote cancellation, and connection shutdown; cancellation
causes identify the stream, code, and endpoint. Reads also return StreamError
for local cancellation and peer reset, preserving the first terminal result
through cleanup. Broader upstream API compatibility remains unfinished.
CID issuance retains bounded lifetime uniqueness history (4096 IDs, or the
smaller ID space); after exhaustion, existing IDs remain valid but no more
spares are issued. This policy still needs long-lived connection evaluation.
Clients can resume a 1-RTT TLS session using the caller's
`tls.Config.ClientSessionCache`. Ordinary `DialAddr` disables early data;
`DialAddrEarly` preserves valid early-data tickets and may return before TLS
completion. Servers opt in with `Config.Allow0RTT`; applications must tolerate
replay. `Listener.Accept` waits for confirmation. `ListenAddrEarly` returns an
`EarlyListener` whose Accept can return after ClientHello processing, before client
authentication and handshake completion. Applications requiring those guarantees
must wait for `HandshakeComplete` or connection failure. Early listeners can send
1-RTT responses immediately; accepting replayable requests still requires Allow0RTT.
Clients only offer cached tickets whose
recorded originating QUIC version matches the attempted version; missing or
ambiguous version metadata forces a full handshake. Cached state is privately decoded before
use so per-connection changes do not mutate a shared cache entry. Setting
`SessionTicketsDisabled` still forces a full handshake. New transport parameters
are negotiated for each connection. Servers issue one TLS session ticket after handshake completion, with early
data enabled only by `Allow0RTT`, unless the effective TLS configuration disables tickets. Default
wrapping uses the shared configuration's ticket keys across connection clones;
custom WrapSession/UnwrapSession callbacks are retained. The pinned upstream gates
verify both client and server v1/v2 resumption, verified identity, application
bytes/FIN and the client explicit-disable control. Accepted and rejected 0-RTT STREAMs and DATAGRAMs
now have independent v1/v2 coverage in both roles; full early API parity remains unfinished;
explicit key rotation across listener restarts and custom ticket callbacks have
independent-peer coverage, including GetConfigForClient selection with conflicting
base policy. Live `SetSessionTicketKeys` rotation on the original listener TLS
configuration now affects subsequent handshakes; retiring a ticket key preserves
established connections. Ticket wrapping/unwrapping failure isolation has independent-peer coverage;
automatic clock-driven rotation and broader callback policies need further coverage.
Server tickets now include a namespaced record of the QUIC version, eight
remembered credit limits, idle timeout, UDP payload limit and migration-disable flag. Resumption restores valid, version-matched records;
missing, duplicate or mismatched version metadata forces a full handshake.
Malformed transport payloads with a proven matching version remain usable for
ordinary 1-RTT resumption. Server checks run before TLS selects a resumption key,
including configurations returned by GetConfigForClient. Clients also attach their own server-limit snapshot to cached TLS
sessions, restoring it from a private copy on resumption. Refreshed tickets use
the issuing connection's current parameters. Extension-specific remembered state, application
bindings and combined-extension coverage remain unfinished.

Early streams are permanently invalidated on rejection with `Err0RTTRejected`;
no application request is replayed automatically. Stream opens and accepts also
return `Err0RTTRejected` until `NextConnection(ctx)` completes. This method waits
for successful TLS completion and re-enables fresh streams on the same connection;
it never revives rejected stream handles. Calls are concurrent-safe and idempotent.
Canceling a pending transition leaves the rejection gate in place.
Early data supports compatible v1/v2 switching in either direction: early packets
retain the original version and protection keys, while Handshake and 1-RTT use the
negotiated version. Local engine and UDP tests cover this combination; independent
early-plus-compatible negotiation coverage remains pending. Multipath with early
data still returns a configuration error.
Version Negotiation received after an early dial has returned fails that connection;
automatic fallback across an already-exposed early handle is not implemented.

Run checks:

```sh
go test ./...
go test -race ./...
go vet ./...
go test ./internal/wire -run '^$' -fuzz '^FuzzHeader$' -fuzztime=10s
go test ./internal/packet -run '^$' -fuzz '^FuzzOpen$' -fuzztime=10s
go test ./internal/handshake -run '^$' -fuzz '^FuzzReassembly$' -fuzztime=10s
go test ./internal/wire -run '^$' -fuzz '^FuzzTransportParameters$' -fuzztime=10s
go test ./internal/wire -run '^$' -fuzz '^FuzzAckFrame$' -fuzztime=10s
go test ./internal/wire -run '^$' -fuzz '^FuzzStreamFrame$' -fuzztime=10s
go test ./internal/wire -run '^$' -fuzz '^FuzzFrameParser$' -fuzztime=10s
go test ./internal/transport -run '^$' -fuzz '^FuzzReceived$' -fuzztime=10s
go test ./internal/transport -run '^$' -fuzz '^FuzzEngineInitialInput$' -fuzztime=10s
go test ./internal/transport -run '^$' -fuzz '^FuzzPeerIDs$' -fuzztime=10s
go test ./internal/wire ./internal/packet -run '^$' -bench . -benchmem
```

Independent interoperability is a separate module, deliberately keeping upstream
out of the implementation's dependencies:

```sh
cd interop
go test -race ./...
```

See [the implementation plan](docs/PLAN.md) for remaining work and
[the progress record](docs/PROGRESS.md) for the next concrete step.

An initial [public stream performance comparison](docs/benchmarks/stream-echo.md)
records both a UDP buffer-allocation improvement and the substantial remaining
gap versus upstream on one loopback workload. No overall performance parity is claimed.

A [public multipath transfer baseline](docs/benchmarks/path-transfer.md) records
current one/two-path overhead and the routing allocation hotspot.

Preferred server addresses can be configured with
`Config.PreferredAddresses []netip.AddrPort` (one concrete address per family).
`ListenAddr` owns those sockets; port zero selects an ephemeral port, available
through `Listener.PreferredAddresses()`. Clients validate before switching.
See [preferred-address status](docs/PREFERRED_ADDRESS.md) for examples, tests,
and remaining independent interoperability and failure-coverage gaps.


Receive credit can be configured with `Config.InitialStreamReceiveWindow` and
`Config.InitialConnectionReceiveWindow`. Zero selects 512 KiB per stream and
768 KiB per connection. Zero `MaxStreamReceiveWindow` and
`MaxConnectionReceiveWindow` select growth ceilings of 6 MiB and 15 MiB,
respectively, raised to the initial window if necessary. Set each maximum equal
to its initial value for fixed windows. A local consumption policy proposes
doubling when at least half a window is read within two measured RTTs.
Explicit maxima below initial values are rejected. Stream windows above 16 MiB
and connection windows above the QUIC varint maximum are also rejected.
Storage grows with the buffered span: an initial in-order byte uses a 4 KiB
payload ring plus a 512-byte presence bitmap, excluding object and allocator
overhead. A far-ahead fragment can still allocate the full window, and allocated
storage is retained until terminal cleanup or empty-window growth. Size windows
together with incoming stream limits. Default
values matched quic-go v0.63.0 when checked, but memory efficiency, growth tuning and
large-window configuration behavior do not yet have full parity.

`AllowConnectionWindowIncrease(conn, delta)` can veto each proposed connection
window increase; nil permits growth up to the configured maximum. The callback
receives the public connection and additional credit before the increase is
committed. It runs on the connection owner and must not block or call connection
or stream methods. Denying growth preserves the current window; later consumption
can trigger another proposal. This controls connection credit, not total memory
or independent stream-buffer growth.


`Config.HandshakeIdleTimeout` limits inactivity before TLS completion (default
5 seconds). Authenticated receives and the first ack-eliciting send after a
receive restart it; repeated sends cannot sustain an unresponsive peer.
`HandshakeTimeout` separately limits total time to TLS completion;
zero selects twice the idle timeout, with overflow saturation. It does not bound
the wait for handshake confirmation that follows, so give `DialAddr` a context
deadline to limit the whole dial. Both limits
are local and separate from the negotiated idle timeout. See the
[API parity inventory](docs/API_PARITY.md) for remaining contract differences.


Transport inactivity and absolute handshake expiry return `*IdleTimeoutError`
and `*HandshakeTimeoutError`, respectively. Both implement `net.Error` with
`Timeout() == true` and `Temporary() == false`, and match `net.ErrClosed` through
`errors.Is`. Use `errors.As` to distinguish them. Stream send operations, stream
opening/acceptance and DATAGRAM operations retain the connection's terminal cause
after the client socket owner exits; receive operations retain their existing
buffer-drain behavior.

## License

Licensed under the [Apache License, Version 2.0](LICENSE). RFC-derived test
vectors and tables keep their IETF Trust notices in
[internal/packet/testdata/LICENSE](internal/packet/testdata/LICENSE) and
[internal/qpack/LICENSE-RFC](internal/qpack/LICENSE-RFC).
