# Exact-sized short headers — 2026-09-29

A current independent-server echo profile identified short-header construction as
an allocation site. The profiled process includes both the independent server and
upstream client; it is not an isolated server CPU measurement. CPU samples were
dominated by syscall/runtime work, while makeHeader and its packet-number append
accounted for about 5.55% of sampled allocation objects cumulatively.

Profile summaries: [run](current-echo-profile-run.txt),
[CPU](current-echo-cpu-top.txt), [allocation objects](current-echo-alloc-top.txt).
The profile used the pre-change header builder, already including typed ACK-only
retention. Profiling timings are not used as performance comparison results.
The [allocation attribution follow-up](echo-allocation-attribution.md) separates
the upstream client's receive stack from our implementation's allocation sites.

The short-header builder now allocates its complete length (first byte, CID,
four-byte packet number), copies the CID, and writes the packet number directly.
The previous sequence could grow storage twice for common CID sizes. Returned
headers remain independently owned; there is no new shared scratch buffer or
pool. Long-header and early-packet construction are unchanged.

TestShortHeaderOwnedStorage covers CID sizes 0, 4, 8, 16 and 20, both key phases,
packet-number truncation, parser agreement, and isolation from subsequent header
construction and mutation of the connection's CID. The same ownership test passed
on the control. Existing packet, key-phase, greasing, early-data and connection
suites supply broader integration checks.

The control is retained as a [reverse patch](short-header-control.patch). Apply it
in an isolated copy, or map the absolute connection.go path to the restored copy
using Go's overlay Replace map. Microbenchmark reproduction:

```sh
go test ./internal/transport -run '^$' -bench '^BenchmarkShortHeaderConstruction$' -benchmem -benchtime=100000x -count=3
```

Go 1.26.2, darwin/arm64, Apple M4; median of three 100,000-iteration samples:

| CID bytes | Control ns/op | Final ns/op | Control B/op / allocs | Final B/op / allocs |
| --- | ---: | ---: | ---: | ---: |
| 0 | 32.91 | 17.24 | 16 / 2 | 5 / 1 |
| 4 | 40.79 | 16.02 | 32 / 3 | 16 / 1 |
| 8 | 24.05 | 14.22 | 17 / 2 | 16 / 1 |
| 16 | 21.94 | 17.33 | 25 / 2 | 24 / 1 |
| 20 | 35.32 | 15.13 | 73 / 3 | 32 / 1 |

Raw results: [control](short-header-control.txt), [final](short-header-final.txt).
The implementation removes one or two allocations per short header in these
cases. Timing differences in a microbenchmark do not establish an application
throughput gain.

## Matched independent-peer echo

The unchanged v1 fixture checks each complete 1 MiB request and identical reply
plus FIN, using the same pinned v0.63.0 client and matched server receive windows.
Setup is excluded from timing. From interop, run the header control overlay and
then the final implementation with:

```sh
go test -run '^$' -bench '^BenchmarkServerStreamEcho$' -benchmem -benchtime=100x -count=3 .
```

Add `-overlay=/absolute/path/to/control.json` for the control. Raw results:
[control](stream-echo-v063-header-control.txt),
[final](stream-echo-v063-header-final.txt).

| Server / run | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Independent / control | 9991449 | 1691431 | 37342 |
| Independent / final | 10004099 | 1662377 | 35568 |
| Upstream / control run | 5306407 | 648289 | 19647 |
| Upstream / final run | 5358312 | 647441 | 19651 |

Independent allocations fall 4.75% and allocated bytes fall 1.72%; median elapsed
time increases 0.13%, so no end-to-end speedup is established. Final independent
exchanges remain about 1.87x slower, with 2.57x allocated bytes and 1.81x allocations
versus upstream. These short whole-process results include client and server;
they are not confidence intervals or isolated server memory/CPU measurements.
All measurements ran sequentially with no concurrent tests or benchmarks.

Full root race passed (root 8.639s, HTTP/3 1.425s, transport 41.271s), full default
independent race passed (85.599s), and both vet checks passed. Default independent
coverage retains the documented exclusions in interop/KNOWN_FAILURES.md.
