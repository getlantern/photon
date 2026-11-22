# Compatible version negotiation

Experimental public opt-in; complete RFC 9368 conformance remains unproven.

Requirements checked 2026-09-25 against
[RFC 9368](https://www.rfc-editor.org/rfc/rfc9368.html) and
[RFC 9369 sections 4–5](https://www.rfc-editor.org/rfc/rfc9369.html#section-4).
Both v1 and v2 are mutually compatible. Their packet protection labels and Initial
salts differ. A compatible transition remains one connection: packet numbers,
CRYPTO offsets, recovery and TLS transcript cannot restart.

The TLS adapter now provides NewNegotiatingServer. It withholds local transport
parameters from crypto/tls until client parameters have been parsed and a selector
returns the negotiated version and encoded server parameters. Version selection
runs before traffic-secret events are converted to packet keys. Server parameters
are supplied at QUICTransportParametersRequired, before EncryptedExtensions.
The client has SetNegotiatedVersion, valid only before keys or peer parameters
exist, and only for an advertised supported version. The transport must first
authenticate the candidate Initial and prevent subsequent version changes.

Protected TLS tests complete v1-to-v2 and v2-to-v1 handshakes, including certificate
verification and negotiated server parameters. Negative tests reject unoffered or
unknown versions, inconsistent client/server chosen versions, malformed server
parameters and selector errors, without installing keys or exposing flights.
Original-version ticket acceptance is preserved; new ticket metadata uses the
negotiated version. TLS-level ticket tests now cover both switching directions:
issue on the original version, resume while switching, resume a newly issued ticket
on the negotiated version, and decline to offer it on the old version. Changed
receive limits distinguish restored ticket state from newly issued state.

These tests exposed a server event-order issue: version selection precedes the
resume event, so reading old ticket metadata using the current version silently
lost its limits. TLS now keeps the original version separately for ticket restore.
A further test alters only client-local ticket provenance, leaving the opaque
server ticket unchanged; the server rejects the wrong-version ticket and completes
a full handshake while still negotiating the new version. Early-data keys remain
absent throughout. Public switched-connection resumption still needs integration.

The connection engine now has an internal CompatibleVersions opt-in. The server
validates client version/CID parameters before selecting by preference and uses
the negotiating TLS adapter. Switching updates Initial keys and packet version
without rebuilding the connection's packet-number, recovery or CRYPTO state.
Original server Initial receive keys remain available until normal Initial-space
retirement following negotiated Handshake processing (and are released on close).
The client opens an offered candidate-version Initial before committing; malformed
or tampered packets cannot select a version. Original-version server CRYPTO locks
the original choice. Server-side authenticated client version information remains
bound to the original first flight, while client-side checks use the negotiated
version.

Engine tests confirm both switch directions under clean/lost-first-packet and
normal/reversed-and-duplicated delivery. Dedicated gates check original Initial
retransmission authentication, tamper non-selection, key retirement and refusal
to switch after original server CRYPTO. Existing handshake/transport race suites
pass. Combined incompatible-then-compatible configuration preserves the fallback
attempt version separately from the negotiated version for downgrade checks.

Retry coverage now includes both switching directions, normal and empty Retry
CIDs, clean and lost/reversed/duplicated handshake delivery, and token-validated
admission. Clients ignore correctly tagged alternate-version Retry packets and
keep post-Retry Initials in the original version. Token validation rejects the
alternate version. Completed handshakes preserve authenticated original and Retry
CID parameters. Direct construction retains amplification limits; admission lifts
them only after address-bound token validation.

Address-token tests also run after each switch direction. Newly issued NEW_TOKEN
values validate for the negotiated version and fail for the original version.
Dropping the first token packet exercises reliable retransmission of the same
opaque token, acknowledgment, distinct fresh issuance and queue-full handling.

Public Config.EnableCompatibleVersionNegotiation now enables initial-attempt
switching (off by default). Versions supplies server preference and client offers;
clients normally initiate with their first configured version. InitialVersion can
select another supported first-flight version without changing preference order.
The default list is v1/v2. An initial version outside that list is rejected.
A client reacting to a Version Negotiation packet retains its compatible offers.
It replays its original selection policy against the authenticated server list
plus the final negotiated version, then compares the result with the fallback
connection's initial version. Comparing only with the final version would accept
a forged VN when a subsequent compatible switch returned to the first version.

Public IPv4/IPv6 tests cover both directions and all client/server enablement
combinations, requiring exact request/response data and negotiated connection
state on both sides. Public ticket/token sequences switch, reconnect directly on
the negotiated version with resumption and token reuse, then initiate in the
original version without either reuse. The token callback now stores tokens using
the connection's negotiated version, rather than the attempted version captured
at dial time. Fallback and forged-downgrade tests also run with the option enabled.

A public relay test first reproduced acceptance of forged VN followed by switching
back to the original version when validation compared only the final version. The
corrected gate rejects both directions with VERSION_NEGOTIATION_ERROR (0x11).
The relay requires an Initial carrying the switched-back server version, so a
rejection without exercising compatible switching does not satisfy the gate.
Engine tables also cover retiring versions, absent/empty lists, and ordinary valid
fallback. With only v1/v2 supported, a switch back exposes that the original
version was available and therefore cannot justify the incompatible fallback.

Independent server-role evidence now uses the pinned Picoquic client driver.
V1-to-v2 switching passes over IPv4 and IPv6 with multipath, greasing, exact 1 MiB
responses, path-zero abandonment and all five probe/bulk loss scenarios (ten
cases, race gate 4.181s). Both endpoints check their negotiated version. The peer's
v2-to-v1 transition times out and its pinned source has no compatibility entry for
that direction; the strict diagnostic is preserved in interop/KNOWN_FAILURES.md.
Our-client v1-to-v2 evidence now uses TestPicoquicCompatibleMultipath against the
unmodified pinned Picoquic demo server. Config.InitialVersion selects v1 while
Versions advertises preference [v2,v1]. The relay records the first client long
header's v1 version and connection state must report v2. Both independent role
fixtures require zero observed Version Negotiation packets to distinguish
compatible switching from a separate fallback connection. Both IPv4/IPv6, clean
and lost-probe paths, greasing, exact downloads, abandonment, three migrations
and migration during a 1 MiB response are covered. Independent v2-to-v1 evidence
remains absent; that peer direction stays an opt-in known-failure diagnostic.

Coalescing tests now split ClientHello before version parameters can be parsed,
requiring an original-version ACK before selection. They check that server Initial
packet numbers continue increasing, receive/recovery objects survive the switch,
and ACK-only input does not lock the client version. Both switching directions
exercise old-first, old-last (before any short header), separate-old and late-old
prefix layouts, plus duplicate datagrams and handshake completion.

The late-old-prefix cases initially failed: the receive path rejected the entire
datagram based on its first packet's version. Version filtering now happens per
packet after framing, also in confirmed-path and closing receive paths. This
preserves valid negotiated packets following parseable retired-version packets,
consistent with [RFC 9000 section 12.2](https://www.rfc-editor.org/rfc/rfc9000.html#section-12.2).
Path tests retain unknown-version, truncation, different-CID and tampered-tail
rejections; closing tests require a valid trailing close to enter draining.

Post-switch key-update tests now cover both switching directions, initial
application keys and four subsequent generations. They alternate the initiating
endpoint, drive loss/reordering/duplication recovery, and exchange acknowledged
PINGs before allowing the other endpoint to initiate. Test-only TLS traffic
secrets are captured in memory and used to derive reference keys with the expected
negotiated version; original-version references must fail to open the same packets.
This checks version binding against the packet primitive's separately tested RFC
vectors, not an independent implementation of cryptography. Synthetic verification
packets are not injected into live packet-number spaces and secrets are not emitted
to test output. Negotiated-suite coverage here is the suite chosen by crypto/tls;
packet tests separately cover all three supported TLS suites.

Remaining integration:

- Broaden fragmentation and original/negotiated loss-recovery assertions beyond
  the split-ClientHello and coalesced transition gates.
- Add public negative authenticated CID-parameter cases and broader loss tests.
- Broaden combined-negotiation deployment/retiring-version scenarios beyond the
  currently implemented v1/v2 pair.
- Test fragmentation, loss, duplication, reordering, Retry, resumption, both roles,
  public connection state, independent peers and wire observations.

0-RTT remains disabled. Internal engine switching does not establish complete
RFC 9368 conformance.
