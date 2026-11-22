# Typed ACK-only retention queue — 2026-09-29

Recovery's expendable ACK-only timestamp history now uses typed nodes containing
packet number and links. Previously container/list allocated an element and
boxed the packet number. Indexing, FIFO eviction, arbitrary removal, space
discard and packet-number validation retain their existing behavior. Removal
clears both links. No node pool or retained-capacity reservation was introduced.

TestACKOnlyRetentionModel compares recovery's retention path with a slice ledger
for 30,000 sends, removals and discards across capacities 1, 2 and 17. It passed
on both implementations. Existing ACK validation, full-flight-ledger, pure-ACK
capacity and migration tests run in the full recovery race suite.

The benchmark exercises complete OnSent bookkeeping with steady ACK-only history
eviction at capacities 32 and 1024. It includes congestion-history allocations
and map work, not just the new queue. Warmup fills both histories and exceeds
packet number 255 to avoid the runtime's small-integer boxing cache. This is a
microbenchmark, not a throughput, tail-latency or peak-memory comparison.

Raw measurements: [control](ack-only-control.txt), [typed queue](ack-only-final.txt).
The [reverse patch](ack-only-control.patch) restores sent.go's container/list
implementation in an isolated copy of this tree; the then-unused typed queue
file can remain. Keep the same benchmark and Go toolchain in both copies.

```sh
go test ./internal/recovery -run '^$' -bench '^BenchmarkACKOnlyRetention$' -benchtime=100000x -count=3
```

Runs use Go 1.26.2, darwin/arm64, Apple M4. Baseline and final measurements are
sequential, with no tests or other benchmarks running alongside either.

Medians of three 100,000-iteration samples (not confidence intervals):

| Capacity / implementation | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| 32 / list control | 178.4 | 120 | 3 |
| 32 / typed queue | 182.2 | 88 | 2 |
| 1024 / list control | 207.8 | 124 | 3 |
| 1024 / typed queue | 200.0 | 92 | 2 |

The measured allocation reduction is one allocation and 32 bytes per send in
this fixture. Timing is mixed: the smaller history regresses about 2.1%, while
the larger improves about 3.8%. These short samples do not establish throughput
superiority. The subsequent end-to-end comparison below still leaves the echo
performance gap open. B/op is cumulative allocation, not peak memory.

Validation: control model race 1.406s; final full recovery race 1.257s; full root
race passed (root 8.950s, HTTP/3 1.456s, transport 41.741s); full default independent
race 86.156s; both vet checks passed. Default independent coverage retains the
documented exclusions in interop/KNOWN_FAILURES.md.

## Independent-peer stream echo follow-up

The unchanged v1 stream fixture exchanges a 1 MiB request and identical response
per operation, excluding setup/handshake and checking payload and FIN. Both
servers use the same pinned v0.63.0 client. The control overlay changes only
sent.go back to container/list; its diff against the final file was inspected.
Run from interop, sequentially with no concurrent tests or benchmarks:

```sh
go test -overlay=/absolute/path/to/control.json -run '^$' -bench '^BenchmarkServerStreamEcho$' -benchmem -benchtime=100x -count=3 .
go test -run '^$' -bench '^BenchmarkServerStreamEcho$' -benchmem -benchtime=100x -count=3 .
```

The overlay's Replace map points the absolute sent.go path to a control copy
restored using ack-only-control.patch. Raw data:
[control](stream-echo-v063-ack-only-control.txt),
[final](stream-echo-v063-ack-only-final.txt).

| Server / run | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Independent / list control | 11109820 | 1720588 | 38085 |
| Independent / typed queue | 11153798 | 1679040 | 37225 |
| Upstream / control run | 5987550 | 656028 | 19792 |
| Upstream / final run | 6325495 | 657506 | 19800 |

The independent fixture's medians show 2.26% fewer allocations and 2.41% fewer
allocated bytes. Median elapsed time increases 0.40%; there is no demonstrated
throughput improvement. The unchanged upstream server's median time varies
about 5.6% between runs, underscoring timing variability in these short samples.
In the final run, independent exchanges remain about 1.76x slower, with 2.55x
allocated bytes and 1.88x allocation count. These whole-process measurements
include both server and client and do not isolate server CPU or peak memory.
No new production change was made for this follow-up.
