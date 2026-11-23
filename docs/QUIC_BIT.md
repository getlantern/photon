# QUIC-bit greasing

Target: RFC 9287, checked 2026-09-25 at
https://www.rfc-editor.org/rfc/rfc9287.html . This is incomplete extension support.

The existing transport-parameter codec handles the empty grease_quic_bit parameter
and rejects nonempty values. ParseHeader retains strict fixed-bit validation.
ParseHeaderWithQUICBit adds an explicit allowZero receive policy, without rewriting
bytes or relaxing CID/length/version constraints. Both v1 and v2 packet type
mappings are tested. Version Negotiation and unknown-version invariant behavior
remain unchanged.

Receive permission comes from the endpoint's own advertised support. Peer support
instead grants permission to vary the bit in outgoing packets. These directions
must not be confused. The packet's original first byte remains AEAD associated
data. Tests seal/open zero-bit long and short packets across v1/v2 and AES-128-GCM,
AES-256-GCM and ChaCha20-Poly1305; changing the bit after sealing fails authentication.
The metamorphic header fuzzer compares permissive zero-bit parsing with strict
set-bit parsing, including malformed packets, without input mutation.

Engine header parsing now centralizes local receive policy across normal/coalesced
input, buffered replay, closing, path routing and path probes. CID routers tolerate
either bit to identify the connection; the engine enforces local policy before
processing. Unknown Initial admission and version selection apply template policy.
Internal engine configuration can advertise GreaseQUICBit. Public Config exposes
EnableQUICBitGreasing (off by default) to advertise receive support. Sending varies
the bit after processed peer parameters advertise support, regardless of our own
receive setting. The connection consumes cached cryptographic random bits and
checks peer permission on every choice. Ordinary/closing long and short headers,
multipath/PMTU sends, path closes and migration probes choose the bit before sealing.
Stateless Retry/rejection/reset behavior stays set-bit without peer permission.

Engine tests cover both roles/versions and all local/peer support combinations,
including tampering. Routing retains wrong-address identity protection. Admission
tests prove zero-bit Initials receive Retry only under local support, and Retry
itself keeps the bit set without authenticated peer permission.

Remaining integration:

- Dedicated PMTU greasing observations and broader independent-peer coverage.
  Picoquic evidence for both roles and address families is recorded below;
  upstream quic-go provides the negative/fallback gate.
- Optional pre-handshake client greasing requires a sufficiently recent NEW_TOKEN
  tied to remembered server support. It must not be inferred from session resumption
  alone; servers must not reuse remembered client permission.
- Reordered-traffic and broader independent-peer tests remain.

Live public endpoint stream tests pass for every receive-support combination in
both versions. Unit tests verify no random-bit consumption without peer permission,
bit selection without changing other flags, cache refill and immediate permission
removal. No pre-handshake remembered-token greasing is enabled.

## Wire observations

A transparent IPv4/IPv6 UDP relay now counts short-header first-byte values without
changing or decrypting datagrams. For both versions, 48 serial stream echoes cover
all four client/server receive-advertisement combinations. The full matrix runs
both with no loss and with every seventeenth short-header datagram dropped in each
direction. Tests require actual drops in both directions while all echo bytes
arrive intact. Drops depend on ordinal position, not on the QUIC bit. Every permitted direction
must exhibit both values; every unpermitted direction must have zero cleared bits.
One run observed 107 short-header datagrams in each direction, with 42–65 cleared
bits in permitted directions and none otherwise. These counts demonstrate exercised
behavior, not statistical proof of cryptographic randomness.

An independent upstream v0.62.0 server, which does not advertise this extension,
accepts our advertised client and echoes data with no cleared bits in either
direction, over both IPv4 and IPv6. The v2 fallback workload stays below the
separately documented peer key-update gate. Reordered-traffic observations remain outstanding. Relay and echo workers are
joined during cleanup, including failed tests.

## Migration and multipath

The existing public lifecycle tests now run with greasing both disabled and enabled
on both endpoints. For v1/v2, this covers repeated primary-address migration with
base QUIC and multipath, canceled-migration isolation, additional-path migration,
base-path abandonment, surviving path closure, and released-socket rebinding.

The 256 KiB two-path transfer matrix also runs with greasing off/on, v1/v2 and
clean/lossy secondary paths. It requires substantial payload traffic on both paths,
byte-for-byte delivery and actual drops in loss cases. Independent per-path relays
count cleared short-header bits in client traffic: both paths must exhibit them
when enabled, and neither may do so when disabled. These are own-to-own IPv4 tests,
not independent multipath greasing interoperability or PMTU-specific evidence.

## Independent greased multipath/migration

TestPicoquicGreasedMultipath passes against the existing external Picoquic server
fixture for v1/v2, clean and lost-probe paths. It downloads exact files, abandons
path 0, migrates path 1 repeatedly, and migrates once during a 1 MiB download.
Every large transfer must show both short-header bit values in both directions.
Four new NAT bindings each lose their first request/response in loss scenarios,
with exact drop counts verified. This provides positive independent client-role
coverage across multipath, migration and limited loss, using IPv4 and IPv6.

TestPicoquicClientGreasedMultipath adds the reverse role: the external client
advertises support through public transport-parameter APIs before starting TLS.
Both versions deliver an exact 1 MiB response after path-zero abandonment, under
clean, request-probe loss, response-probe loss, combined probe loss, and three
large-response packet losses. Both bit values must occur in each direction.
Both address families run the complete reverse-role matrix.
Client ACK frequency determines packet count, so no fixed count is required for
that direction. The library itself is unmodified.

Both roles now pass against a fresh build of the pinned peer and TLS sources,
whose checkouts were verified clean. See interop/picoquic/README.md for revisions,
binary hashes and reproduction commands. Broader reordering remains open.
