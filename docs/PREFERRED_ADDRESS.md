# Preferred-address implementation status

RFC 9000 sections [5.1.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-5.1.1),
[9.6](https://www.rfc-editor.org/rfc/rfc9000.html#section-9.6), and
[18.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-18.2) define this feature.

## Advertisement and CID ownership

The internal transport Config accepts PreferredAddresses, at most one concrete
address per family. The server socket owner must register all advertised sockets
before connection admission. Unregistered advertisement and client-role use are
rejected. New admissions omit preferred sockets that have since been removed.
The public Config.PreferredAddresses exposes this configuration as a slice of
netip.AddrPort. ListenAddr binds these sockets and advertises their actual ports;
port zero requests an ephemeral port. The primary address must be concrete.
Listener.PreferredAddresses returns a caller-owned copy of the bound addresses.
DialAddr ignores this server-only option. Partial construction failures close
all sockets already opened, and Listener.Close closes the preferred sockets too.

Each admitted connection generates a fresh fixed-length CID and reset token;
the listener template does not share identities across connections. The local
ledger reserves this CID as sequence 1 before TLS output and ordinary issuance.
It counts toward the peer's active limit, owns its bytes, and retains uniqueness
history through retirement. Subsequent NEW_CONNECTION_ID issuance starts at 2.
The existing router reservation must succeed before output is handed to a socket.
Direct caller-supplied wire PreferredAddress parameters remain rejected: the
owner supplies addresses and the connection generates its own CID/token.

The client registers sequence 1 while processing authenticated TLS parameters.
This includes active-limit checks, duplicate consistency, retirement and path-zero
history. Reset recognition is enabled only after actual CID use and ends at
retirement.

## Client validation

After confirmation and path-zero activation, the socket owner selects a compatible
registered socket and advertised destination. It prefers the established socket's
family and socket, otherwise uses deterministic address order. No new socket is
allocated. Admission defers if no compatible socket or unused CID is available.
A started attempt is remembered so expiry does not repeatedly restart probing.

Preferred probes contain probing frames only. Ordinary traffic continues on the
original path until validation permits owner commit. Expiry preserves that path.
Admission and commit check the exact authenticated advertised address. Changing
local addresses toward the handshake address remains prohibited when active
migration is disabled, even if that same address is advertised as preferred.

## Server validation

Authenticated path-zero packets arriving at the exact advertised preferred local
address can start bounded validation without a non-probing nomination. The server
answers challenges and probes from that address using an unused peer CID.
Original-address traffic preserves preferred reachability proof but revokes an
older nomination. Commit requires validation and a newer preferred non-probing
packet, checks authorization again, and resets recovery for the local change.
A packet watermark survives candidate expiry to prevent replay from renewing it.
Unknown local addresses remain rejected; the endpoint also requires registered
sockets. Ordinary local/client-address migration keeps its existing rules.

## Evidence and remaining work

Tests cover CID limits, issuance/retirement, duplicate handling, reset-token
lifecycle, selection ordering, forged input, expiry and replay. Encrypted engine
tests exercise server admission and commit. Real UDP fixture tests verify original
traffic during probing, successful switching and unanswered-probe fallback.

TestPreferredAdvertisementUDPHandshake now exercises the real TLS handshake over
UDP, automatic client migration, and server commit for v1/v2 over IPv4/IPv6. It
also removes the preferred socket before admission and verifies that the server
omits the parameter and stays on its original address. Separate tests check
per-connection identity generation and rejection without registered sockets.
Public listener tests additionally exchange an exact bidirectional stream payload
after migration across both versions and families, check LocalAddr/RemoteAddr,
verify returned address ownership, and rebind sockets after close or partial
construction failure to detect leaks. The public test found a missing RemoteAddr
snapshot update on client preferred commit, which is now fixed.
Those public tests use this implementation in both roles. Independent coverage
now adds two distinct cases:

- Upstream quic-go v0.62.0 authenticates the preferred parameter and exchanges
  exact stream payloads while remaining on the original address, for v1/v2 and
  IPv4/IPv6. Its comment claiming immediate CID retirement is stale: the called
  AddFromPreferredAddress method queues the CID. The test does not require
  immediate retirement or claim migration support from this peer.
- The pinned Picoquic demo client migrates to our preferred server socket and
  downloads an exact 64 KiB file over IPv4/IPv6 for v1/v2. Our server sends a 1 KiB
  prefix, waits for committed preferred-local-address state, then sends the rest
  and FIN. The test verifies both the downloaded bytes and process exit. This
  optional gate requires PICOQUIC_DEMO; see interop/picoquic/README.md.

The reverse optional gate, TestPicoquicPreferredServer, uses the unmodified
Picoquic demo as server with distinct original and preferred sockets. Our client
waits for RemoteAddr to commit the advertised address, then downloads an exact
64 KiB payload with FIN. Both versions and address families are exercised.
The fixture advertises both families because the pinned peer encodes its IPv4
port in the IPv6 field; IPv6-only advertisement remains a strict diagnostic.
See interop/KNOWN_FAILURES.md for the limitation and reproduction.

Still required:

- Extend the independent Picoquic gates in both roles to impaired networks.
- Broader stream/data delivery, loss/reordering, cancellation, socket failure
  after advertisement, and recovery/ECN/MTU transition coverage.
- Simultaneous local/preferred migration policy and tests.
- Alternate-family socket allocation policy; current selection uses only
  sockets already registered with the runtime.

The complete preferred-address feature remains an open part of the project goal.

Example (with a configured server TLS certificate and ALPN):

```go
listener, err := quic.ListenAddr("127.0.0.1:4433", tlsConfig, &quic.Config{
    PreferredAddresses: []netip.AddrPort{
        netip.MustParseAddrPort("127.0.0.1:4434"),
    },
})
```

Deterministic encrypted-packet recovery coverage (TestPreferredValidationRecovery)
now withholds the first two challenges, delivers a delayed first-probe response
after three transmissions, and delivers a proof after expiry. Both v1 and v2
verify original-path packet acceptance during validation, commit on a retained
valid proof, and rejection of expired or reordered proofs that could revive a
candidate. This is engine-level simulated impairment, not independent-peer or
real-network loss evidence.

A server candidate's preferred local socket can fail while the established path
still uses a healthy socket. Socket failure now cancels that candidate before
further scheduling, allowing normal output to retire its used peer CID and keep
the original connection alive. TestPreferredCandidateSocketFailure reproduces
the previously retained candidate in both versions and checks subsequent output,
CID retirement and connection retention. Actual UDP reader failure during this
transition is now covered by TestPreferredCandidateUDPReaderFailure: after an
authenticated fixture handshake, real UDP probes create a candidate; closing the
preferred socket triggers its reader failure. Both v1/v2 and IPv4/IPv6 verify
candidate cancellation, continued original-path packet acceptance and a decrypted
server PING received from the original socket. Failure after preferred commit
and independent-peer socket-failure coverage remain open.

Local/preferred admission policy is now regression-tested in both versions by
TestPreferredAndLocalMigrationSerialization. An existing application local move
defers automatic preferred migration without consuming its one attempt; a pending
preferred move causes a new local request to return an error. Neither steals the
other's CID reservation or changes the established binding. Cancellation before
and after probe transmission is covered. A sent CID is retired; preferred
migration remains deferred during CID exhaustion and resumes after an authenticated
NEW_CONNECTION_ID supplies a replacement. Public Migrate documents this busy
behavior. These engine tests do not cover simultaneous remote rebinding, competing
commits or public runtime request races.

The independent-server fixture now also runs through an original-address relay
that drops the first datagram in each direction and asserts both drops occurred.
After handshake recovery, the client migrates directly to the advertised socket
and verifies the exact 64 KiB file. This runs for v1/v2 over IPv4/IPv6. Bypassing
the relay also changes the client source tuple observed by Picoquic. The preferred
probes and migrated transfer are not impaired; that loss gate remains open.
