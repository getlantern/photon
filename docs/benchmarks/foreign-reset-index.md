# Source-owned reset-token index

Measured 2026-09-30 on Apple M4, darwin/arm64, Go 1.26.2, default
GOMAXPROCS 10. Five samples per case, 200 ms per sample. Medians follow.
This isolates warmed alternate-reset selection; synthetic setup and index bootstrap
are excluded. It does not measure authentication, UDP I/O, full packet throughput,
end-to-end latency, or performance against upstream quic-go.

| Source-owned connections | Matched scan ns/op | Indexed ns/op | B/op / allocations |
| --- | ---: | ---: | ---: |
| 1 | 176.2 | 152.3 | 0 / 0 |
| 256 | 30092 | 158.9 | 0 / 0 |
| 4096 | 606017 | 160.1 | 0 / 0 |

All connections have one eligible token and share the received tuple. The incoming
suffix matches none. One foreign registration has no published tokens; the benchmark
therefore does not characterize a large foreign-registration scan or duplicate-token
buckets. Small timing differences are sensitive to local system noise. The large
connection-count improvement removes the previously measured local scan from this
particular warmed dispatch path.

The matched scan control uses the current implementation with only
`Router.enableResetIndex` changed to return immediately. Both benchmark setup and
runtime calls then retain the complete local scan. Raw outputs:
[indexed](foreign-reset-index.txt), [matched scan](foreign-reset-index-control.txt).
The control overlay was removed after measurement. The prior
[publication/selection report](foreign-reset-publication.md) is historical evidence
before this index and uses a slightly different scan implementation.

The index uses a fresh random 256-bit secret with standard HMAC-SHA256, and stores
only transformed token keys. Lookup does not use raw token values as map keys.
Matched buckets preserve all connection identities rather than overwriting a
duplicate. Live tuple checks and constant-time raw token verification remain final
authority. Foreign connection state is never inspected by this index.

The index is enabled lazily on the first foreign CID dispatch, bootstrapping current
source-owned tokens once. Successful Router.Sync updates one registered connection;
removal drops its entries. Endpoint output synchronizes before exposing packets,
which makes newly used tokens available for immediate reset receipt. Only used,
active tokens from nonabandoned paths are indexed. Stale hits are rejected by live
checks. Router users must continue to Sync after connection events.

Maintenance has a cost. Warmed unchanged synchronization measures:

| Eligible tokens / paths per connection | ns/op | B/op / allocations |
| --- | ---: | ---: |
| 1 | 215.9 | 0 / 0 |
| 8 | 1142 | 0 / 0 |

[Raw synchronization results](foreign-reset-index-sync.txt). These costs are added
to connection synchronization after the index is enabled. Bootstrap and changed
membership allocate; retained maps scale with indexed active tokens and owners.
The follow-up below measures cold bootstrap and a single actual-hit workload.
Retained heap at scale, varied token counts and end-to-end tradeoffs remain open. Foreign hint selection still scans
registrations. Initial unknown-CID routing also retains its existing scan; this
change targets alternate local selection for foreign CID dispatch.

Reproduce warmed selection and maintenance:

```sh
env GOCACHE=/Volumes/ExtremeSSD/macdev/quic-go-build-cache go test ./internal/transport -run '^$' -bench '^BenchmarkForeignResetSelection$' -benchmem -benchtime=200ms -count=5
env GOCACHE=/Volumes/ExtremeSSD/macdev/quic-go-build-cache go test ./internal/transport -run '^$' -bench '^BenchmarkResetTokenIndexSync$' -benchmem -benchtime=200ms -count=5
```

The owner-level tests cover keyed transformation, zero-allocation warmed lookup,
bootstrap, first use through an unchanged-CID Sync, replacement, retirement,
duplicate candidates, wrong peers, stale-token rejection, abandonment and removal.
Existing independent foreign-first/local-target restart tests create the local
connection after foreign traffic has enabled the index. Removing incremental Sync
publication makes both versions time out; the real implementation passes the
focused owner tests and is checked by the full independent suite.

## Cold initialization and validated hits

Follow-up on the same machine/toolchain with five 200 ms samples per case.
Synthetic connection construction remains outside timing. Bootstrap includes a
fresh random secret, HMAC state, all index maps and publication of one used token
per connection. Each operation constructs a new index; old indexes become garbage.

| Connections | Bootstrap median ns/op | Allocated B/op | Allocations/op | Validated hit ns/op |
| --- | ---: | ---: | ---: | ---: |
| 1 | 827.1 | 2288 | 19 | 229.0 |
| 256 | 106534 | 198803 | 1065 | 233.4 |
| 4096 | 1751192 | 3241267 | 16491 | 235.3 |

The hit benchmark calls local candidate selection with one uniquely matching token
and a valid established tuple among otherwise unrelated connections. It includes
live tuple and token checks and reports zero allocations at every size. It excludes
return-ticket reservation, forwarding, packet authentication and connection teardown.
Duplicate-token buckets and probe-specific hits are not characterized by this table.

Bootstrap at 4096 connections takes about 1.75 ms and allocates about 3.24 MB. This
is a real one-time cost on the first foreign-CID dispatch; the earlier warmed query
numbers excluded it. B/op is total allocation traffic, not measured resident heap.
[Raw bootstrap and hit results](foreign-reset-index-cold-hit.txt).

```sh
env GOCACHE=/Volumes/ExtremeSSD/macdev/quic-go-build-cache go test ./internal/transport -run '^$' -bench '^BenchmarkResetTokenIndex(Bootstrap|Hit)$' -benchmem -benchtime=200ms -count=5
```

## Stateful differential fuzzing

`FuzzResetTokenIndexLifecycle` applies bounded synthetic owner-event sequences:
used/token flags, token replacement, retirement, duplicate values, path abandonment
and restoration, local/remote tuple changes, owner removal/re-addition, closed-state
changes and path deletion. After every event it checks exact token-bucket membership
against live connection state and compares indexed selection with the complete scan
across both local and remote tuples. Repeated removal must leave no index entries.
Long/short header bits and too-short inputs are included; this is selection fuzzing,
not cryptographic packet parsing or concurrent mutation of an owner.

Three checked-in seeds pass under the race detector. A two-worker, 30-second
race-enabled fuzz session completed 20,026 executions, discovering 61 additional
coverage inputs (64 including seeds), and passed in 33.354s. Interesting inputs
remain in Go's fuzz cache; no failing input was found. A temporary overlay that
retained removed owners in token buckets makes all three seeds fail (0.389s),
including completeness and final-removal assertions. The overlay was removed.
This is bounded evidence, not an exhaustive lifecycle proof.

```sh
env GOCACHE=/Volumes/ExtremeSSD/macdev/quic-go-build-cache go test -race ./internal/transport -run '^$' -fuzz '^FuzzResetTokenIndexLifecycle$' -fuzztime=30s -parallel=2
```
