# ECN ACK history lookup

Measured on Go 1.26.2, macOS arm64, Apple M4, GOMAXPROCS=10. Five sequential
300 ms samples before and after the change, without simultaneous test runs:

```
go test ./internal/recovery -run '^$' \
  -bench '^BenchmarkECNACKRetainedHistory$' -benchtime=300ms -count=5
```

Build cache was placed on the external SSD for both runs. The benchmark retains
4,095 unacknowledged marked transmissions after loss, then repeatedly sends and
acknowledges one fresh marked packet. It checks the resulting ECN counts.

| Sample | Before ns/op | After ns/op |
| --- | ---: | ---: |
| 1 | 72742 | 48534 |
| 2 | 72115 | 52570 |
| 3 | 72044 | 51932 |
| 4 | 72740 | 52594 |
| 5 | 72225 | 52066 |
| Median | 72225 | 52066 |

Median time decreased approximately 27.9%. Both runs report 7 allocations per
operation. Reported bytes/op varied across samples; no allocation improvement
is claimed.

The change directly looks up packet numbers when validated ACK ranges cover
fewer numbers than the retained ECN ledger contains. Otherwise it scans the
bounded ledger, avoiding enumeration of huge peer-provided ranges. It preserves
marking evidence and late-ACK accounting.

This is a synthetic recovery microbenchmark with substantial retained history.
It includes send/recovery overhead beyond the ECN lookup. It neither measures
end-to-end throughput nor compares against quic-go. Other workload distributions
and platforms need separate measurements.

## Follow-up: packet and persistent-congestion ACK lookup

The remaining ACK scans in the packet ledger and persistent-congestion history
now use the same bounded direct-lookup decision. Dense ranges retain map scans;
persistent-congestion history ordering, prefix trimming, and loss detection are
unchanged. The lookup decision allows equal range width and ledger size too.

Adaptive-duration benchmark runs reached many more iterations after this change,
which changed the persistent-history occupancy. Therefore this comparison uses
exactly 10,000 iterations for both versions (`-benchtime=10000x -count=5`). An
isolated external-SSD source copy restored the prior packet/history scans for
the baseline; the working implementation supplied the after measurements.
Other environment settings match the preceding experiment. Runs were sequential.

| Sample | Prior scans ns/op | Bounded lookup ns/op |
| --- | ---: | ---: |
| 1 | 54269 | 816.7 |
| 2 | 53395 | 627.1 |
| 3 | 52999 | 497.4 |
| 4 | 53592 | 444.2 |
| 5 | 52730 | 416.6 |
| Median | 53395 | 497.4 |

Both sides report seven allocations per operation. Bytes/op vary between
samples; no allocation improvement is claimed. After samples are short and
show substantial warm-up variability, so their exact speedup should not be
extrapolated. The result demonstrates removal of history-sized ACK scans in
this synthetic workload, not an end-to-end or upstream-comparative gain.
