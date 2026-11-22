# Indexed unknown-CID reset routing

Measured 2026-10-06 on Apple M4, darwin/arm64, Go 1.26.2, GOMAXPROCS 10.
Five 200 ms samples per case; medians below. Before and after ran sequentially
without concurrent test jobs. The reference is the preceding independent code,
not upstream quic-go.

Synthetic fixed-peer connections share one remote address. Both variants already
have an initialized reset-token index; construction and publication are excluded.
The baseline still scans connection state, while the new router uses index buckets
and skips zero-CID scans for multi-connection peer buckets. Each candidate still
passes registration, live state, tuple and constant-time token checks. Duplicate
cases share one token among every connection; a single-connection duplicate is
therefore a unique match. These benchmarks exclude authentication and UDP I/O.

| Connections / outcome | Before ns/op | After ns/op |
| --- | ---: | ---: |
| 1/miss | 130.8 | 172.9 |
| 1/unique | 130.8 | 251.9 |
| 1/duplicate | 130.6 | 247.4 |
| 256/miss | 17435 | 141.5 |
| 256/unique | 17539 | 220.2 |
| 256/duplicate | 2403 | 300.5 |
| 4096/miss | 437401 | 143 |
| 4096/unique | 415951 | 219.4 |
| 4096/duplicate | 47798 | 394.9 |

All measured cases allocate zero bytes and zero objects per lookup. The keyed
lookup adds cost for one connection; larger populations avoid a full scan.
Index activation policy is unchanged: routing uses it only when already active.
Without an index the complete scan remains. This does not establish whole-stack
throughput, latency or superiority to upstream. Bootstrap, maintenance and memory
costs remain those of the existing index; see [index report](foreign-reset-index.md).

Artifacts: [before](router-reset-before.txt), [after](router-reset-after.txt),
[exact baseline source](router-reset-before.go.txt). Benchmark:
`go test ./internal/transport -run '^$' -bench '^BenchmarkRouterUnknownReset$' -benchtime=200ms -count=5`.
To reproduce the baseline against the same benchmark, use a Go overlay replacing
`internal/transport/router.go` with the saved baseline source.

## Single-owner direct lookup, 2026-10-07

Routing now bypasses keyed lookup when at most one owner is registered, while
continuing normal index publication/removal. At larger populations the indexed
path remains. Five isolated 200 ms samples on the same machine, from
[adaptive routing output](router-reset-adaptive.txt), give these medians:

| Connections / outcome | Adaptive ns/op |
| --- | ---: |
| 1/miss | 146.1 |
| 1/unique | 151.1 |
| 1/duplicate | 147.3 |
| 256/miss | 148.9 |
| 256/unique | 230.1 |
| 256/duplicate | 313.8 |
| 4096/miss | 167.1 |
| 4096/unique | 232.9 |
| 4096/duplicate | 436.3 |

All cases still allocate zero bytes/objects. The one-owner measured overhead is
lower than the prior indexed run, but is not demonstrated equal to or faster
than the original approximately 131 ns scan baseline. Measurements are from
separate runs/dates without confidence intervals; variation at other population
sizes must not be attributed to this single-owner branch. No activation,
maintenance or bootstrap cost changes are claimed. Population-transition tests
cover token replacement during the direct path followed by growth, ambiguity,
removal to zero and re-addition.
