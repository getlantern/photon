# Early-data implementation status

0-RTT remains disabled in both TLS roles. The implementation supports ordinary
1-RTT resumption. This checklist describes unfinished acceptance work, not an
assertion of 0-RTT compliance.

| State | Current handling | Remaining work |
| --- | --- | --- |
| Seven RFC 9000 credit limits and DATAGRAM size | Stored in server tickets and client-local cache metadata; credit comparison implemented | Apply limits to early stream/connection state and validate accepted parameters |
| Idle timeout, UDP payload size, migration-disable flag | Stored in schema 2, restored with credit limits | Apply remembered client policy; decide optional server rejection policy |
| Connection IDs, reset token, preferred address, ACK settings | Excluded from remembered state | Continue using current connection parameters |
| QUIC version | Client cache selection and server unwrap require a unique matching originating version, including GetConfigForClient | Implement compatible negotiation interactions |
| QUIC-bit greasing | Not authorized by ticket snapshot | Keep NEW_TOKEN issuance time and extension association; enforce seven-day rule |
| Version Information extension | Not included in early snapshot | Audit remembered-state requirements and negotiation behavior |
| Multipath | InitialMaxPathID excluded from snapshot | Audit early processing against the selected draft and test with an independent peer |
| Application settings and replay policy | No early acceptance | Define API policy and HTTP/3 settings binding |
| Early packets | Not enabled | Sending, receiving, rejection rollback, Retry, loss recovery and independent interoperability |

The schema-2 transport snapshot is bounded to 84 bytes. Its enclosing record
includes the QUIC version and a namespace in SessionState.Extra. Schema-1
credit-only records cannot restore the new state; ordinary TLS resumption can
still proceed. Server callbacks must protect the ticket state. Client cache
entries are privately decoded before session-event mutations.

[RFC 9000 section 7.4.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-7.4.1)
requires clients attempting early data to remember applicable understood transport
parameters and excludes connection-specific parameters. The optional server
policy for idle timeout, UDP size and migration differs from mandatory credit
compatibility. Storage alone does not implement either policy.

[RFC 9287 section 3.1](https://www.rfc-editor.org/rfc/rfc9287.html#section-3.1)
requires a recent NEW_TOKEN associated with negotiated greasing before a client
can clear the QUIC bit prior to receiving current transport parameters.
[RFC 9368 sections 7.2–7.3](https://www.rfc-editor.org/rfc/rfc9368.html#section-7.2)
discuss version-specific ticket scoping and require compatibility definitions to
address early packets. An exact-version metadata check alone does not complete
that audit.

Client cache selection now enforces the originating-version requirement of
[RFC 9369 section 5](https://www.rfc-editor.org/rfc/rfc9369.html#section-5).
Entries without provable origin are cache misses. A version-matched older
transport schema may still resume without early data. Server unwrap now applies the corresponding rejection before TLS selects the PSK,
including dynamically selected TLS configurations. The later QUICResumeSession
transport-state check alone is insufficient for this requirement.
The NEW_TOKEN version audit also remains outstanding.
