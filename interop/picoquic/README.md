# Picoquic multipath interoperability

These optional gates use an unmodified Picoquic server and a small test client
linked to the same unmodified Picoquic library. Picoquic is
not a production dependency and its source is not copied into this repository.

Install Git, a C/C++ compiler, CMake, pkg-config (or pkgconf), and OpenSSL
development headers/libraries. The build downloads Picoquic and PicoTLS:

```sh
# Run from the repository root; choose a directory outside the repository.
./interop/picoquic/build.sh /tmp/quic-external-peer
cd interop
PICOQUIC_DEMO=/tmp/quic-external-peer/interop-build/picoquic/picoquicdemo \
PICOQUIC_CLIENT=/tmp/quic-external-peer/interop-build/picoquic-client \
  go test -race -run 'TestPicoquic(Multipath|ClientMultipath)$' -count=3 -v
```

The build pins [Picoquic](https://github.com/private-octopus/picoquic) revision
`8c6bbf82b6b6d98a413a69e6bf2265e7f6a10161` and PicoTLS revision
`bfa67875982afc4c24f21e146cef4747fa189c2f`. Existing checkouts at a different
revision are rejected. The test accepts an explicitly supplied binary; it does
not independently attest the binary's source revision. Each test skips when its
corresponding binary variable is absent. Supply absolute paths to the binaries.

For both QUIC v1 and v2 over IPv4 and IPv6, our client verifies the server certificate, negotiates
multipath, opens path 1 on a separate local UDP socket, abandons path 0, and
downloads an exact 832-byte file and a 1 MiB binary file over bidirectional
streams. It then migrates path 1 twice, checking that its local address changes
while its path ID remains unchanged, and downloads the same 1 MiB file after
each migration. A final 1 MiB download reads and verifies a 1 KiB prefix,
migrates path 1 a third time, and verifies the remaining response. The current
64 KiB per-stream receive window prevents the peer from finishing that response
while application consumption is paused for migration. Each transfer checks
exact bytes and stream EOF. A UDP relay
provides a separate server-facing NAT binding for every client socket. Both
clean cases and cases dropping the first packet in each direction on all four
new bindings (path opening plus three migrations) run, with exact drop counts.
The application protocol is HTTP/0.9 (`hq-interop`), not HTTP/3. Each case launches a fresh server
with temporary credentials and content; the process is stopped during cleanup.
Peer output is attached to failures.

This establishes independent multipath negotiation, additional-path validation,
abandonment, established-path migration, and stream delivery with limited probe
loss in this direction. It does not establish complete multipath draft
conformance, arbitrary loss/reordering recovery,
or a performance advantage. The first
verified build used macOS arm64, CMake 4.3.4, pkgconf 3.0.7, OpenSSL 3.6.3,
and the system C compiler.

## Reverse-role library client

`TestPicoquicClientMultipath` normally uses `PICOQUIC_CLIENT`. Our test-only C
application driver configures certificate verification with the temporary trusted
root and localhost SNI, requests a file, and waits for a path-1 CID before opening
its second socket path. The server sends a 1 KiB prefix, validates path 1,
abandons path zero, and sends the rest of a 1 MiB binary response. The test
checks distinct client socket bindings, exact downloaded bytes, and completion.
For each version and address family, the relay runs clean, first-request-loss, first-response-loss,
and combined probe-loss cases on the additional socket. A fifth case drops the
next three server datagrams of at least 512 bytes on path one's NAT binding,
armed after path-zero abandonment and before writing the remaining response.
Exact probe and bulk drop counters, two distinct NAT bindings, and the complete
download are required. Payloads stay encrypted in the relay, so its size filter
does not classify individual QUIC frames.

Both v1 and v2 pass. QUIC framing, packet protection, TLS, ACKs, recovery and
congestion control all remain in the unmodified external library.

The driver reads two internal Picoquic CID queries to avoid the demo issue below;
that API dependency is why its source revision is pinned. It does not modify the
library's transport state directly. Reverse-role established-path migration and
arbitrary loss/reordering remain unverified.

## Original demo diagnostic (currently failing)

With `PICOQUIC_REVERSE_DIAGNOSTIC=1`, `TestPicoquicClientMultipath` runs the pinned demo as a certificate-verifying
client against our server, requesting another socket with `-A 0.0.0.0/0`.
It intends to abandon path zero and verify a 1 MiB downloaded file on path one.
It currently fails before additional-path validation in both versions:

```sh
PICOQUIC_REVERSE_DIAGNOSTIC=1 \
PICOQUIC_DEMO=/tmp/quic-external-peer/interop-build/picoquic/picoquicdemo \
  go test -race -run TestPicoquicClientMultipath -count=1 -v
```

This diagnostic is explicitly opt-in and overrides the library client driver.
It is not counted as a passing compatibility result. Failure output includes peer
stdout and qlog, and the harness joins its process waiter during cleanup.

Observed qlog: TLS completes with certificate verification and multipath
transport parameters, then the demo reports `Probe new path failed with exit
code -1` and exits before validation. Its process exit status is nevertheless
zero; the test requires path validation and exact file contents rather than
trusting that status. The source suggests a peer-side CID
readiness mismatch: `picoquic_check_new_path_allowed` checks
`unique_path_id_next` (still zero); `picoquic_find_avalaible_unique_path_id`
skips existing path zero and selects one; `picoquic_assign_peer_cnxid_to_tuple`
returns -1 because no CID for path one has arrived yet. A preceding path-zero
ACK triggers this before the additional-path advertisements arrive. Sending a
small response prefix did not change the result.

Our runtime already advertises unused-path CIDs during multipath activation,
consistent with [draft section 3.2.1](https://www.ietf.org/archive/id/draft-ietf-quic-multipath-21.html#section-3.2.1).
The pinned library and demo have not been patched. The separate client driver
above waits for the correct path's CID. This diagnosis of the demo failure is
based on the trace and source, not a confirmed upstream bug report.

## Preferred server address

The same pinned `picoquicdemo` can exercise preferred-address migration against
our public listener, without enabling the multipath extension:

```sh
cd interop
PICOQUIC_DEMO=/absolute/path/to/picoquicdemo \
  go test -race -run '^TestPicoquicPreferredAddress$' -count=3 -v
```

For v1 and v2 over IPv4 and IPv6, our listener advertises a second bound socket. Picoquic
verifies the generated certificate, requests a file, and receives a 1 KiB prefix.
The server waits for its preferred local address to become committed before
sending the remaining bytes and FIN. The test verifies the exact 64 KiB output
file and successful peer process exit. This proves an independent client can
migrate to our preferred server address; impaired networks and simultaneous client migration remain unverified.

The reverse role uses the same unmodified binary:

```sh
PICOQUIC_DEMO=/absolute/path/to/picoquicdemo \
  go test -race -run '^TestPicoquicPreferredServer$' -count=3 -v
```

The server opens distinct original and preferred sockets using `-p original:preferred`.
Its `-4` and `-6` options accept bare IP addresses at this pin, despite help text
showing an optional port. The fixture advertises both loopback families on the
preferred port. Our client connects to the original socket, waits for the exact
preferred RemoteAddr, then verifies a 64 KiB file and FIN over the migrated path.
Both v1/v2 and IPv4/IPv6 are covered. Each runs directly and through an
original-address relay that drops the first datagram in each direction. The
loss fixture asserts both drops occurred. Migration bypasses the relay, changing
the client source tuple seen by the server. It covers recovery from original-path
loss followed by preferred migration; it does not impair preferred-path validation
probes or migrated file data.
The pinned encoder uses the IPv4 port for the IPv6 field; strict mode advertises
only the tested family and retains the IPv6 failure (see ../KNOWN_FAILURES.md).

## QUIC-bit greasing with multipath

`TestPicoquicGreasedMultipath` reuses the server fixture with our public
`EnableQUICBitGreasing` enabled. It requires both bit values in each direction
for every 1 MiB download, including after path migration and during the download
that changes paths mid-response. Both versions and address families run with clean paths and first-
probe loss. Relay counters observe only short-header first bytes; no packet
payload or key material is inspected.

```sh
PICOQUIC_DEMO=/absolute/path/to/picoquicdemo \
  go test -race -run '^TestPicoquicGreasedMultipath$' -count=1 -v
```

The 2026-09-25 run used the surviving fixture binary at
`/tmp/quic-pinned-driver-repro/interop-build/picoquic/picoquicdemo`, SHA-256
`b6103d609b0497f026720f1333b8103c2bb565ef6886f18760b8ed63cc47f7ca`.
Its source checkout had been removed, so this run records binary identity but
does not freshly attest its source revision. The repository build script still
pins the revisions above. The pinned source's server transport-parameter handling
was separately checked for greasing negotiation. At that checkpoint, reverse-role greasing and IPv6
independent migration coverage remained outstanding; see the newer run below.


### Fresh build and reverse-role greasing (2026-09-25)

`TestPicoquicClientGreasedMultipath` enables the driver's optional
`--grease-quic-bit` argument. It copies the client's local transport parameters,
sets `do_grease_quic_bit`, and applies them through Picoquic's public API before
starting the connection. No peer library source is changed. The reverse-role
matrix above checks both short-header bit values in both directions in all ten
version/impairment cases, with exact payload, binding and loss checks retained.
The diagnostic demo mode skips this gate because it does not configure greasing.

```sh
PICOQUIC_CLIENT=/absolute/path/to/picoquic-client \
  go test -race -run '^TestPicoquicClientGreasedMultipath$' -count=3 -v
```

A fresh build at `/Volumes/ExtremeSSD/macdev/quic-grease-peer` verified both
source HEADs against the pins above and both checkouts had clean `git status`.
The forward greased, reverse ordinary, and reverse greased race gates passed
in 29.855s (24 version/impairment cases). Interop vet passed. SHA-256:

- picoquicdemo: `e1534ef4a9c82fe33674fdad922bba9526f46ac2f8f8c41d38aa2114d962014c`
- picoquic-client: `e57aa25b512580838e85fe5726a42ff72ec7365d8e66d559a320f62e0323716e`

This supersedes the source-provenance limitation of the earlier surviving binary
for these new runs. Positive greasing now has independent IPv4 evidence in both
roles; IPv6 independent path changes remain outstanding.


### IPv6 multipath and greasing (2026-09-25)

All four multipath gates now run on both `127.0.0.1` and `::1`. The relay binds
front and per-client backend sockets in the server's address family. The external
client driver accepts `--ipv6`, uses IPv6 loopback and sockets, and preserves the
complete local sockaddr when probing the additional path. Its rebuilt SHA-256 is
`90a91b4b08bc5dfc432c100d3b3f79518388b246d012ca5ac1f11d38b4bb27e9`;
peer library sources and the demo executable are unchanged.

The new IPv6 greased matrix passed under the race detector (14 cases, 27.155s).
This covers both versions, both roles, exact payloads, required bit variation,
additional-path validation, base-path abandonment, and the existing probe/bulk
loss cases. With our client, all three established-path migrations also run over
IPv6, including migration during a response. Mixed-family migration and arbitrary
reordering are not established by this same-family loopback matrix.

IPv4 greased regression also passed (14 cases, 26.830s). Ordinary Picoquic
multipath in both roles/families plus upstream migration and keepalive tests
passed together (40 cases, 88.938s), covering the relay's existing consumers.
All runs used the race detector; interop vet passed.

### Compatible version switching

`TestPicoquicClientCompatibleMultipath` configures our server to prefer the other
version and uses the driver's `--desired-version=HEX` argument to call the peer's
public compatible-negotiation API. Both endpoints must report the selected
version; the peer driver checks its final library version before returning success.
The existing exact 1 MiB response, additional-path validation, abandonment,
greasing and five impairment assertions remain active, over IPv4 and IPv6.

```sh
PICOQUIC_CLIENT=/absolute/path/to/picoquic-client \
  go test -race -run '^TestPicoquicClientCompatibleMultipath$' -count=1 -v
```

The pinned library supports v1-to-v2 switching, but lacks the v2-to-v1 transition.
That branch is skipped unless QUIC_INTEROP_KNOWN_FAILURES=1; see
[the preserved diagnostic](../KNOWN_FAILURES.md). The rebuilt driver's SHA-256 is
`79d362e4bef18a5a8765693f089169b65ea502c877d3ad2cf010296b990b1ae5`.
The external peer source checkout remains clean at the documented pin.

The dedicated v1-to-v2 IPv4/IPv6 gate passed ten cases under the race detector
(4.181s); the final combined reverse-role regression passed 50 cases (14.350s)
with explicit v2-origin skips. Interop vet passed. See KNOWN_FAILURES.md for the
separate intermittent ordinary-transfer timeout observed before that rerun.


### Our client switching against the Picoquic server

`TestPicoquicCompatibleMultipath` starts our public client in v1 using
`InitialVersion`, advertises preference `[v2,v1]`, and requires v2 connection state.
The transparent relay must observe v1 in the first client long header and zero
Version Negotiation packets. The pinned
Picoquic server selects the first supported client preference. Both IPv4 and IPv6
run clean and first-probe-loss cases, retaining greasing observations for every
large transfer, exact file bytes, abandonment, repeated migration, and migration
during a response. No peer binary or library source changes are needed.

```sh
PICOQUIC_DEMO=/absolute/path/to/picoquicdemo \
  go test -race -run '^TestPicoquicCompatibleMultipath$' -count=1 -v
```

The v2-origin branch uses the existing known-failure opt-in because the pinned
library lacks v2-to-v1 switching. It is not counted as independent passing evidence.

The final combined independent switching gates passed 14 cases under the race
detector (26.913s), including first-flight and no-VN observations. Both v2-origin
branches retain explicit skips for the pinned peer limitation. Interop vet passed.

## Early data and compatible switching

With PICOQUIC_DEMO set, TestPicoquicEarlyClientControl validates an accepted v1
0-RTT HTTP/0.9 request against the unmodified peer. A relay observes early wire
packets and discards client 1-RTT output until the complete response is verified.
TestPicoquicEarlyCompatibleRecovery seeds a v1 ticket, starts in v1, negotiates v2,
checks the pinned peer's early rejection, and verifies an explicit fresh request
after NextConnection. Three repeated race runs pass. Strict acceptance across the
switch is an opt-in known failure (TestPicoquicEarlyCompatibleAcceptance), not a
completed compatibility claim; see ../KNOWN_FAILURES.md. These cases do not enable
multipath. Peer files and credentials are confined to each test's temporary directory.
