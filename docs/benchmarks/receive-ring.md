# Receive storage growth measurements — 2026-09-29

Receive credit capacity now grows independently of allocated storage. Initial
small contiguous input allocates at most a 4 KiB payload ring and a 512-byte
bitmap. The ring grows geometrically to cover the incoming span relative to the
consumed offset, capped by the advertised capacity. Expansion remaps occupied
positions; credit-only growth no longer copies buffered data. Partial-reset
trimming happens before storage admission. Empty/terminal behavior is unchanged.

This is not sparse storage: a far-ahead byte can require the whole window.
Allocated storage normally remains until terminal cleanup; growing an empty
window also releases it. Growth can increase cumulative allocations for sustained
bulk transfers. No general throughput or peak-memory superiority is claimed.

## Measurements

Go 1.26.2, darwin/arm64, Apple M4. Runs were sequential after race tests and fuzz
processes finished. Each cell below is the median of three runs; these short
samples are not confidence intervals. The control uses the previous production
receive implementation through a Go overlay, with current fixtures/configuration.

First-byte allocation (`1000x`, four allocations in every case):

| Advertised stream window | Control B/op | Growing ring B/op |
| --- | ---: | ---: |
| 64 KiB | 73,896 | 4,776 |
| 512 KiB (current default) | 589,995 | 4,776 |
| 6 MiB | 7,078,064 | 4,776 |

The default-window reduction is about 99.19%. This microbenchmark creates a
receiver and retains one byte at offset zero. It does not measure a far-ahead
fragment, full window, concurrent connections, or process RSS.

Existing v1 public-server echo fixture (`100x`, one new bidi stream per 1 MiB
request/response, same upstream client, 64 KiB / 1 MiB fixed server windows):

| Server / implementation | ms/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Independent, control ring | 10.174 | 1,843,467 | 41,756 |
| Independent, growing ring | 10.234 | 1,865,247 | 41,723 |
| Upstream v0.63.0, final run | 5.302 | 644,066 | 19,560 |

Echo time increased about 0.58% and bytes about 1.18%; there is no demonstrated
throughput improvement. The final independent implementation remains about 1.93x
slower and allocates 2.90x as many bytes as the peer in this run. Growth/remapping
cost and workload variability both matter; this comparison does not isolate CPU
cost. These fixed-window results are not measurements of the new public defaults.

Raw output: [allocation control](receive-ring-allocation-control.txt),
[allocation final](receive-ring-allocation-final.txt),
[echo control](stream-echo-v063-ring-control.txt),
[echo final](stream-echo-v063-ring-final.txt).

## Reproduction and validation

Run from the root with an appropriate GOCACHE:

```sh
go test ./internal/stream -run '^$' -bench '^BenchmarkReceiveWindowFirstByte$' -benchmem -benchtime=1000x -count=3
```

Run from `interop`:

```sh
go test -run '^$' -bench '^BenchmarkServerStreamEcho$' -benchmem -benchtime=100x -count=3 .
```

For the control, apply [the reverse patch](receive-ring-control.patch) to a copy
of `internal/stream/receive.go`, and use a Go overlay mapping the production
absolute path to that copy. Add `-overlay=/absolute/path/to/overlay.json` to the
same commands. Do not run correctness tests with that control overlay: the new
storage-allocation regressions intentionally require the new behavior.

Full root race tests and full default independent interop race tests passed
(transport 40.119s, interop 72.544s); root/interop vet passed. Two bounded fuzz
campaigns passed: FuzzReceiveGrowth 1,720,928 executions in 15.315s and FuzzReceive
1,692,827 executions in 16.271s. Deterministic regressions cover wrapped retained
data during expansion, overlap rejection, credit-only growth, partial-reset tail
removal and an odd-sized final allocation. Existing models cover final size,
flow accounting, resets and consumption. This is not an exhaustive security audit.

Validation follow-up (2026-09-29): an absolute-offset byte-map oracle now checks
storage growth independently of Receive's ring representation. The bounded model
fuzzer passed 14,878 executions (31.221s), covering input/reads/growth and final
FIN or partial-reset delivery. A deterministic near-varint-limit test forces
physical expansion twice and checks exact delivery, overlap rejection and final
accounting. These add correctness evidence without changing the measurements above.
