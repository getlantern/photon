# Foreign reset selection and publication

Historical measurement before the [source-owned token index](foreign-reset-index.md).
That subsequent change removes the local scan from warmed foreign-CID alternate
selection; the publication results and rebuild tradeoffs below still apply.

Measured 2026-09-30 on Apple M4, darwin/arm64, Go 1.26.2, default
GOMAXPROCS 10. Five samples per case, 200 ms per sample, medians below.
Synthetic connection setup is outside timing. These are local microbenchmarks,
not end-to-end transport results or an upstream quic-go comparison. Short samples
and system noise limit interpretation of small timing differences.

Unchanged publication, with every path sharing one socket/peer and one used token
per path:

| Paths | Before ns/op | After ns/op | Before B/op / allocs | After B/op / allocs |
| --- | ---: | ---: | ---: | ---: |
| 1 | 66.35 | 59.70 | 0 / 0 | 0 / 0 |
| 2 | 328.30 | 97.09 | 608 / 5 | 0 / 0 |
| 8 | 807.30 | 319.90 | 608 / 5 | 0 / 0 |

The old comparison counted raw peer/token visits against deduplicated set sizes.
Multiple references to a peer caused repeated publication even without change.
The inverse transition could also hide removal: two distinct values becoming one
shared value could retain a stale hint. Immutable path and CID sequence records
now distinguish those states. Four regression cases fail under the old source;
retirement, first use, abandonment and empty-state reuse also have tests. Datagram
token matching retains its constant-time comparison; the new identity comparison
runs only on the connection owner against its own state.

Rebuilding publication after changing one token on every operation costs more:

| Paths | Before ns/op | After ns/op | Before B/op / allocs | After B/op / allocs |
| --- | ---: | ---: | ---: | ---: |
| 1 | 241.90 | 362.10 | 608 / 5 | 1624 / 9 |
| 2 | 334.50 | 477.30 | 608 / 5 | 1880 / 11 |
| 8 | 837.00 | 1201.00 | 608 / 5 | 3416 / 23 |

The identity records increase changed-snapshot allocations and retained metadata.
The table measures allocations per rebuild, not resident heap size. Rebuilds
follow path/CID eligibility changes; unchanged publication is invoked during
ordinary forwarding/output. No universal speedup is claimed. Compact identity
storage is a possible follow-up if rebuilds or retained memory prove significant.

Alternate reset selection still scans source-owned connections. All synthetic
connections below have one eligible path and share the received tuple; no token
matches. This isolates `reserveResetReturn`, excluding CID routing, packet
handoff, authentication and socket I/O:

| Connections | Before ns/op | After ns/op | B/op / allocs |
| --- | ---: | ---: | ---: |
| 1 | 156.8 | 162.8 | 0 / 0 |
| 256 | 27568 | 28883 | 0 / 0 |
| 4096 | 533477 | 538190 | 0 / 0 |

The publication fix does not change this scan. About 0.54 ms per foreign packet
at 4096 local connections is an unresolved scaling cost. A future index must
track first-use/retired tokens, current/probing tuples, immutable foreign hints,
registration revocation and ambiguity, without exposing token values through
lookup timing or reading another owner's connection state. Final owner checks
must remain authoritative.

Reproduce from the repository root:

```sh
env GOCACHE=/Volumes/ExtremeSSD/macdev/quic-go-build-cache go test ./internal/transport -run '^$' -bench '^BenchmarkForeignReset(Selection|Publication)$' -benchmem -benchtime=200ms -count=5
env GOCACHE=/Volumes/ExtremeSSD/macdev/quic-go-build-cache go test ./internal/transport -run '^$' -bench '^BenchmarkForeignResetPublicationChange$' -benchmem -benchtime=200ms -count=5
```

Raw results: [before](foreign-reset-before.txt), [after](foreign-reset-after.txt),
[rebuild before](foreign-reset-change-before.txt),
[rebuild after](foreign-reset-change-after.txt). The exact pre-change production
source is retained in [foreign-reset-before.go.txt](foreign-reset-before.go.txt).
Use a Go `-overlay` mapping from the absolute `internal/transport/forward_reset.go`
path to that artifact to reproduce the baseline without modifying the worktree.
The temporary measurement and negative-control overlay was removed after use.
