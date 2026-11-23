# Public multipath transfer baseline

Measured 2026-09-20 on Apple M4, darwin/arm64, Go 1.26.2, GOMAXPROCS=10.
Both endpoints use this independent implementation. These results are not an
upstream quic-go comparison.

Each operation opens a new unidirectional stream, writes 256 KiB, and waits for
the receiver to verify every byte and FIN. Connection, TLS, path validation,
payload and receive-buffer allocation are outside the timer. Stream lifecycle,
flow-control updates, socket I/O, encryption, and receiver completion signaling
are included. Each operation exceeds the initial 64 KiB stream window.
Residual control/ACK work may overlap the first operation. Allocation figures
include both endpoints and the harness.

Three samples of 25 operations each; medians:

| Version | Mode | ms/op | MB/s | B/op | allocs/op |
|---|---|---:|---:|---:|---:|
| v1 | Ordinary QUIC | 3.304 | 79.35 | 2,134,116 | 21,345 |
| v1 | Multipath, one active path | 5.327 | 49.21 | 4,977,521 | 78,705 |
| v1 | Multipath, two active paths | 8.488 | 30.88 | 4,843,533 | 84,763 |
| v2 | Ordinary QUIC | 3.202 | 81.86 | 2,181,608 | 21,337 |
| v2 | Multipath, one active path | 5.350 | 49.00 | 4,926,733 | 78,763 |
| v2 | Multipath, two active paths | 8.517 | 30.78 | 4,863,941 | 84,846 |

Multipath currently adds substantial overhead here. Two active paths are about
2.6 times slower than ordinary QUIC in these short samples. Both paths use
loopback, so this does not measure aggregation over independent bottleneck links.
There is no injected delay/loss, no concurrent connections, and no claim about
Internet throughput, robust statistical significance, or universal performance.

A separate sampled allocation profile of the v1/two-path case attributes 48.75%
of allocated bytes to Router.Sync. That profile includes benchmark setup and
calibration, unlike the reported timed counters. It points to CID routing
synchronization as the next optimization target; it does not establish that
routing consumes the same fraction of CPU or elapsed time.

Reproduce:

```sh
go test -run '^$' -bench '^BenchmarkPublicPathTransfer$' -benchtime=25x -count=3 -benchmem
go test -race -run '^$' -bench '^BenchmarkPublicPathTransfer$' -benchtime=1x
go test -run '^$' -bench '^BenchmarkPublicPathTransfer/v1/paths=2$' -benchtime=25x -memprofile /tmp/quic-path-transfer.mem -o /tmp/quic-path-transfer.test
go tool pprof -top -alloc_space /tmp/quic-path-transfer.test /tmp/quic-path-transfer.mem
```

The benchmark labels paths=0 for ordinary QUIC, paths=1 for negotiated multipath
with only the primary active, and paths=2 for an additional validated path.

[Raw samples](path-transfer.txt) · [Sampled allocation profile](path-transfer-alloc.txt)

## Routing snapshot optimization

The next run on the same machine caches the successfully synchronized multipath
CID owners, highest issued sequence numbers, and active counts per path.
Unchanged routing no longer rebuilds CID maps/strings. Membership, owner,
issuance and retirement changes still invalidate the snapshot; collision
preflight precedes snapshot publication. No other transport change separates
these runs.

Three samples of 25 operations; new medians:

| Version | Mode | ms/op | MB/s | B/op | allocs/op |
|---|---|---:|---:|---:|---:|
| v1 | Ordinary QUIC | 3.224 | 81.31 | 2,170,564 | 21,364 |
| v1 | Multipath, one active path | 3.589 | 73.03 | 2,277,082 | 22,917 |
| v1 | Multipath, two active paths | 6.398 | 40.97 | 2,290,898 | 26,390 |
| v2 | Ordinary QUIC | 3.203 | 81.85 | 2,211,164 | 21,357 |
| v2 | Multipath, one active path | 3.592 | 72.99 | 2,251,817 | 22,941 |
| v2 | Multipath, two active paths | 6.381 | 41.08 | 2,281,395 | 26,393 |

Two-path elapsed time fell about 25%, allocated bytes about 53%, and allocation
count about 69% in these samples. Two-path operation still takes about twice
ordinary QUIC's time in this loopback workload. The sampling/environment limits
above still apply; this is not evidence of universal improvement.
Router.Sync no longer appears in the top twelve allocation sites in the separate
post-change sampled profile. Packet sealing/opening and stream/socket buffers
now account for prominent allocation sites.

[Post-change raw samples](path-transfer-route-cache.txt) ·
[Post-change sampled profile](path-transfer-route-cache-alloc.txt)

## Scheduler scratch storage

The next change replaces the scheduler's growing ID slice and reflection-based
sort with a local 64-ID array and typed sorting. The existing table capacity
bounds active plus retained path state at 64. Scheduling order is unchanged.
A regression test measures idle scheduling at zero allocations, versus four
before the change.

Three further 25-operation samples gave these two-path medians:

| Version | ms/op | MB/s | B/op | allocs/op |
|---|---:|---:|---:|---:|
| v1 | 6.335 | 41.38 | 2,172,357 | 19,576 |
| v2 | 6.308 | 41.55 | 2,176,965 | 19,566 |

Allocation counts fell about 26% relative to the route-cache run. Elapsed times
are close enough to the preceding short run that these samples do not establish
a meaningful throughput improvement. Ordinary QUIC still takes about half the
two-path elapsed time. Packet sealing was inspected but left unchanged: it
already makes one correctly sized owned output allocation, whose reuse would
require a separate lifetime/ownership design.

[Scheduler-change raw samples](path-transfer-scheduler.txt)

## Base-protocol owner and single-path scheduling

The ordinary server now uses address-aware path-zero ownership on concrete
listeners with nonzero peer CIDs. A base-only scheduler shortcut avoids generic
path collection, sorting and cross-path searches. All packet-level ACK, pacing,
congestion and amplification checks still run.

On the same Apple M4 / Go 1.26.2 host, three isolated 25-operation samples gave
these median completed-transfer times:

| Version | Before shortcut | After shortcut |
|---|---:|---:|
| v1 | 3.374 ms | 3.382 ms |
| v2 | 3.351 ms | 3.317 ms |

These short samples do not establish an end-to-end improvement. A separate
same-state idle scheduler benchmark measured median 59.04 ns for generic path
selection versus 13.34 ns for the base-only shortcut (zero allocations for both).
That measures idle scheduler CPU overhead, not traffic throughput. Both transfer
endpoints remain this implementation; this is not an upstream comparison.
An initial post-change run overlapped a CPU benchmark and was discarded; the
reported post-change and idle results were rerun sequentially.

Raw retained samples: [base-owner-scheduler.txt](base-owner-scheduler.txt).

```sh
go test -run '^$' -bench '^BenchmarkPublicPathTransfer/v(1|6b3343cf)/paths=0$' -benchtime=25x -count=3 -benchmem
go test ./internal/transport -run '^$' -bench '^BenchmarkBasePathIdleScheduler$' -count=3 -benchmem
```

Run these commands sequentially with other benchmark/test jobs stopped.
