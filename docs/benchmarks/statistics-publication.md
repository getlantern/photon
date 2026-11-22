# Connection statistics publication cost — 2026-09-29

The owner now publishes an 80-byte value snapshot behind an RWMutex after network
and timer events. Readers copy it without submitting owner work. This measurement
checks publication cost in the existing stream-echo workload, not correctness or
the full cost of adding statistics accounting.

The control makes publishStatistics a no-op while retaining all underlying
counters and the snapshot storage. It deliberately does not provide working
public snapshots and is only suitable for this benchmark, which does not read
statistics. The [control patch](statistics-publication-control.patch) is a reverse
patch against internal/transport/statistics.go; use an isolated copy or Go overlay.
All other implementation and fixture code is the same.

Go 1.26.2, darwin/arm64, Apple M4. Sequential 100-iteration runs, three samples
each, after tests and vet completed. Medians:

| Server / publication | ns/op | B/op | allocations/op |
| --- | ---: | ---: | ---: |
| Independent / disabled | 10007605 | 1653697 | 35622 |
| Independent / enabled | 10075642 | 1685553 | 35549 |
| Upstream / control run | 5398856 | 651340 | 19688 |
| Upstream / final run | 5394863 | 648504 | 19660 |

Independent median time rises about 0.68%; individual samples overlap and vary
more widely. Allocated-byte results are also variable. These short results do not
establish a significant regression, negligible overhead, or any performance gain.
They do not isolate server CPU or peak memory, or measure frequent concurrent
statistics readers. The final whole-process independent/upstream comparison is
about 1.87x time, 2.60x allocated bytes and 1.81x allocation count.

Raw results: [control](stream-echo-v063-stats-control.txt),
[final](stream-echo-v063-stats-final.txt). From interop:

```sh
env GOCACHE=/Volumes/ExtremeSSD/macdev/quic-go-build-cache go test -run '^$' -bench '^BenchmarkServerStreamEcho$' -benchmem -benchtime=100x -count=3 .
```

Add `-overlay=/absolute/path/to/control.json` for the control. Each iteration
checks a complete 1 MiB request and identical echo plus FIN, using the same pinned
v0.63.0 client and matched server receive windows. Setup is excluded from timing.
