# Recovery history allocation measurement — 2026-09-29

Recovery history now stores pointers to its congestion records. ACK, loss and
space-discard processing update the state in place instead of assigning another
boxed struct to a container/list interface. Ordering, capacity, late-ACK barriers
and persistent-congestion policy are unchanged. Existing history/ACK tests and
full root/default independent race suites passed; both vet checks passed.

The pre-change allocation profile attributed 42 MiB (4.87% of sampled allocated
bytes) to ACK-time record boxing in a 500-operation echo run. Send-buffer storage
and socket handling remain larger contributors. CPU samples were dominated by
system calls and runtime scheduling; this profile includes both the independent
server and the upstream client, and does not isolate server CPU cost.

Artifacts: [API-refresh baseline](stream-echo-v063-api-refresh.txt),
[profile run](stream-echo-v063-api-profile-run.txt),
[allocation top](stream-echo-v063-api-alloc-top.txt),
[allocation sites](stream-echo-v063-api-alloc-sites.txt),
[CPU top](stream-echo-v063-api-cpu-top.txt).

## Matched measurements

Go 1.26.2, darwin/arm64, Apple M4. Runs were sequential after all correctness
checks completed; no tests or fuzzing ran alongside benchmarks. Each value below
is the median of three runs, not a confidence interval. The control replaces
only persistent.go using a Go overlay and the supplied reverse patch.

| History send + ACK (`100000x`) | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Boxed-record control | 121.90 | 144 | 3 |
| Pointer-backed final | 95.83 | 96 | 2 |

This removes exactly one 48-byte allocation in this operation. The simple workload
keeps one record live at a time; it does not measure peak recovery history size.
Raw [control](history-boxing-micro-control.txt) and
[final](history-boxing-micro-final.txt) outputs retain the sample variability.

The unchanged echo fixture uses the same upstream v0.63.0 client for both servers,
QUIC v1, one new bidi stream for each 1 MiB request plus response, fixed 64 KiB
stream / 1 MiB connection windows, and excludes setup from benchmark accounting.

| Echo (`100x`) | ms/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Independent, boxed control | 10.187 | 1,840,997 | 41,726 |
| Independent, pointer final | 10.174 | 1,820,959 | 40,009 |
| Upstream v0.63.0, final run | 5.368 | 649,027 | 19,682 |

Independent allocation count decreased 4.11%; median bytes decreased 1.09%.
Allocated-byte samples vary substantially (the final samples span 1.768–1.826 MB),
so this does not establish a stable whole-exchange byte reduction of a precise
size. Median time changed by only -0.12%; no throughput improvement is established.
The independent server remains approximately 1.90x slower and allocates 2.81x as
many bytes as the reference in the final run. WAN, tails, CPU, peak memory and
multi-connection comparisons remain open.
Raw [control](stream-echo-v063-history-control.txt) and
[final](stream-echo-v063-history-final.txt) outputs are retained.

## Reproduction

Apply [the reverse patch](history-boxing-control.patch) to a copy of
internal/recovery/persistent.go. Create an overlay JSON whose Replace map points
from that file's absolute repository path to the control copy's absolute path.
Use `-overlay=/absolute/path/to/overlay.json` for control runs only. Do not run
correctness tests with the overlay: the current internal test type assertions
intentionally reflect pointer-backed records.

From the root, with an appropriate GOCACHE:

```sh
go test -run '^$' -bench '^BenchmarkCongestionHistoryACK$' -benchmem -benchtime=100000x -count=3 ./internal/recovery
```

From interop:

```sh
go test -run '^$' -bench '^BenchmarkServerStreamEcho$' -benchmem -benchtime=100x -count=3 .
go test -run '^$' -bench '^BenchmarkServerStreamEcho$/^independent$' -benchtime=500x -count=1 -memprofile=/tmp/quic.mem -cpuprofile=/tmp/quic.cpu -o /tmp/quic-profile.test .
go tool pprof -top -alloc_space /tmp/quic-profile.test /tmp/quic.mem
go tool pprof -top /tmp/quic-profile.test /tmp/quic.cpu
```

Profiling adds overhead; its timing is not used in the matched comparison.
Reproducing the original boxing profile requires the control overlay. The report
retains textual profiles; temporary binary profiles and executable are not
repository artifacts.
