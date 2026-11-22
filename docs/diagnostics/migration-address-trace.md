# Migration address timeout diagnostics

Updated 2026-10-09. The v1 response-loss and both-loss address assertion has
occasionally failed in broad race runs. The one-second deadline is unchanged;
the cause is not established.

The opt-in `QUIC_INTEROP_MIGRATION_TRACE` fixture records a mutex-protected ring
of 512 formatted events: upstream 1-RTT packet numbers and frame kinds,
PATH_CHALLENGE/PATH_RESPONSE values, loss events, relay tuples/checksums and
first-flight loss decisions, plus migration commit/stream/address milestones.
It copies descriptions immediately rather than retaining qlog frame storage.
No payloads or cryptographic keys are retained. With tracing off, packet checksums
and milestone address formatting are skipped. This is independent-test tooling,
not implementation qlog API support.

From the interop directory, with the project's configured Go cache:

```
QUIC_INTEROP_MIGRATION_TRACE=1 go test -race -v -run '^TestIndependentClientMigrationUpstreamServer$/^1$/^(response|both)$' -count=10 .
QUIC_INTEROP_MIGRATION_TRACE=all go test -race -v -run '^TestIndependentClientMigrationUpstreamServer$/^1$/^response$' -count=1 .
```

The [ten-repeat log](migration-address-traced-repeat.txt) passes both selected
loss modes (61.911s). The [successful sample](migration-address-trace-sample.txt)
passes (3.432s including race shutdown). In that sample:

- Round 1: incoming PN 10 carries a client challenge; the first server response
  is dropped. About one second later PN 11 carries the retry; a fresh server
  challenge is answered in client PN 12. Stream delivery and address confirmation
  follow the local commit.
- Round 2: incoming PN 21 starts another migration and the first server response
  is dropped. PN 22 retries; PN 23 answers the fresh server challenge. Local commit
  is logged just before the upstream response event, and both precede stream
  delivery and the successful address assertion.

These observations demonstrate the diagnostic content, not the failing sequence.
Upstream v0.63.0 processes frames before deciding whether validation and packet-
number conditions permit changing its remote address. Stream delivery therefore
is not itself evidence of address publication. That static control-flow observation
is a hypothesis guide, not a finding that upstream or the test caused the timeout.

Upstream v0.63.0 computes receive datagram checksums only for long-header inputs;
normal 1-RTT qlog events in this sample have checksum zero. Relay checksums still
identify repeated encrypted datagrams, but must not be joined to those zero qlog
fields. The timeline supports time/order correlation only for these packets.
Tracing can perturb scheduling and no reproduced failure is captured yet.

The [full traced independent suite](migration-address-traced-suite.txt) passes
91.305s (race enabled). The untraced migration matrix also passes (17.454s),
and interop vet passes. These successful runs do not clear the earlier intermittent
failures; capturing a failing timeline remains the next diagnostic step.
