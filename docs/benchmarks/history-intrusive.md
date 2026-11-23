# Embedded recovery-history links — 2026-09-29

Congestion records now contain their ordering links, replacing the separate
container/list node. The history index points directly at records. Removal clears
both links; capacity eviction, cross-space ordering, late ACK barriers and local
packet rejection retain their previous behavior. This follows the earlier
[record-boxing reduction](history-boxing.md).

A deterministic slice-ledger model checks 30,000 mixed sends, capacity evictions,
ACK retirements, space discards and arbitrary removals at capacities 1, 2 and 17.
It passes against both implementations. Existing persistent-congestion and local
packet rejection tests also pass. Full recovery race passed (1.359s), full root
race passed (root 8.849s, HTTP/3 1.372s, transport 40.912s), full default pinned
interop race passed (77.958s), and both vet checks passed. Default interop retains
the documented exclusions in interop/KNOWN_FAILURES.md.

## Measurements

Go 1.26.2, darwin/arm64, Apple M4. Benchmarks ran sequentially after correctness
checks completed. Values are medians of three samples, not confidence intervals.
The control is the previous pointer-backed record plus container/list node.

| History send + ACK, 100000 iterations | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Previous list control | 100.10 | 96 | 2 |
| Embedded links | 73.78 | 64 | 1 |

Raw [control](history-intrusive-micro-control.txt) and
[final](history-intrusive-micro-final.txt) outputs. This benchmark keeps only one
record live; it does not measure peak memory under a large outstanding flight.

The unchanged echo fixture uses the upstream v0.63.0 client for both servers,
QUIC v1, a new bidi stream per 1 MiB request plus response, fixed 64 KiB stream /
1 MiB connection windows, and excludes setup from accounting.

| Echo, 100 iterations | ms/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Independent, list control | 10.204 | 1,754,818 | 39,991 |
| Independent, embedded links | 10.100 | 1,729,046 | 38,295 |
| Upstream v0.63.0, final run | 5.394 | 650,589 | 19,713 |

Median allocation count decreased 4.24% and bytes decreased 1.47%. Median time
changed -1.01%, but final times span 10.048–11.148 ms and allocated bytes span
1.698–1.757 MB. These short runs do not establish a throughput improvement or a
stable precise whole-exchange byte reduction. Final independent medians remain
1.87x slower and 2.66x allocated bytes versus upstream. WAN, tail latency, CPU,
peak memory and many-connection results remain open.
Raw [control](stream-echo-v063-intrusive-control.txt) and
[final](stream-echo-v063-intrusive-final.txt) outputs retain all samples.

## Reproduction

Apply the [reverse patch](history-intrusive-control.patch) to copies of
internal/recovery/persistent.go and internal/recovery/ack_lookup_test.go. Create
an overlay JSON with a Replace map from each absolute repository path to its
control copy. The test-file overlay preserves representation-specific assertions;
the new slice-ledger model works unchanged against both versions.

Use an appropriate GOCACHE. From the repository root:

```sh
go test -run '^$' -bench '^BenchmarkCongestionHistoryACK$' -benchmem -benchtime=100000x -count=3 ./internal/recovery
```

From interop:

```sh
go test -run '^$' -bench '^BenchmarkServerStreamEcho$' -benchmem -benchtime=100x -count=3 .
```

Add `-overlay=/absolute/path/to/overlay.json` for control runs. To reproduce the
older boxing experiment from current code, first apply this reverse patch, then
its historical boxing reverse patch to persistent.go.

## Sustained outstanding-history follow-up

A second fixture keeps 32 or 1024 records near capacity, declares each packet
lost after a quarter-history delay, then late-ACKs alternate packets after a
half-history delay. It scans for persistent congestion once every 64 sends.
Unacknowledged packets maintain capacity pressure; an untimed assertion checks
that final occupancy is capacity or capacity minus one. Prefill is outside timing.
One operation is one send with the applicable delayed callbacks, not one ACK.

Sequential three-sample medians, 100000 operations each, same platform and control:

| Capacity | Implementation | ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| 32 | Previous list | 89.97 | 96 | 2 |
| 32 | Embedded links | 103.10 | 64 | 1 |
| 1024 | Previous list | 155.80 | 97 | 2 |
| 1024 | Embedded links | 142.90 | 65 | 1 |

Allocation reduction survives capacity pressure. Timing is mixed: the 32-record
median regresses while the 1024-record median improves; samples have substantial
variation. These data do not support universal speedup. B/op measures allocation
volume, not retained heap or peak process memory. Raw [control](history-outstanding-control.txt)
and [final](history-outstanding-final.txt) outputs are retained. Reproduce with the
same overlay procedure and `-bench '^BenchmarkCongestionHistoryOutstanding$'`.

Added FuzzPersistentCongestionHistory, whose independent slice oracle enumerates
possible lost endpoint pairs and compares both detection and retained history.
Inputs exercise late ACKs, discard, eviction, local removal, missing RTT samples,
clock regressions, non-eliciting packets and MTU probes. Three deterministic seeds
include history overflow and a late ACK splitting a loss run. A 30-second campaign
with four workers passed 5,821,234 executions (31.265s including shutdown). This is
bounded fuzz evidence, not exhaustive RFC conformance or a security audit.

Final recovery race passed (1.241s), the same seed suite passed against the old
list control (1.244s), and recovery vet passed. Only tests and benchmarks changed
in this follow-up; the preceding full root and default interop validations apply
to the unchanged production implementation.
