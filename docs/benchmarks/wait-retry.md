# Reusing a waiting operation's reply storage — 2026-09-29

UDPServer.Wait previously built a callback and reply channel for every readiness
check. It now builds one command per Wait call and reuses it after each completed
reply. No storage is pooled or shared between operations. Submission still joins
the result if context cancellation races a committed operation. Endpoint shutdown
terminates the Wait; that operation's channel cannot be reused by another caller.

TestWaitRepeatedWakeAndCommittedCancellation exercises 32 owner notifications,
then cancels the context inside the successful callback. Success must survive
that cancellation, and a subsequent Wait must execute its own callback. This test
passes under the race detector on both the control and final implementations.
Existing notification tests cover multiple blocked callers and prevention of
mutual wakeups; cancellation/shutdown tests cover termination while blocked.

BenchmarkWaitRetries uses a minimal owner goroutine executing the real Wait
submission path, with an already signaled notification channel to force retries.
It deliberately excludes socket, TLS, packet, timer and notification-allocation
costs. Results are per complete Wait, not per individual readiness check.

Go 1.26.2, darwin/arm64, Apple M4; median of three 10,000-iteration samples:

| Readiness checks | Control ns/op | Final ns/op | Control B/op / allocations | Final B/op / allocations |
| --- | ---: | ---: | ---: | ---: |
| 1 | 577.1 | 592.6 | 152 / 3 | 152 / 3 |
| 8 | 3563 | 3047 | 1216 / 24 | 152 / 3 |
| 32 | 14246 | 11658 | 4864 / 96 | 152 / 3 |

The allocation reduction scales with retries; there is no reduction for an
operation ready immediately. These short samples do not establish an end-to-end
throughput improvement. The independent-peer follow-up below checks its effect
on the stream-echo workload.

Raw samples: [control](wait-retry-control.txt), [final](wait-retry-final.txt).
The [reverse control patch](wait-retry-control.patch) restores only the production
change. Apply it in an isolated copy, or use an overlay mapping the absolute
internal/transport/udp_server.go path to its restored contents. Run sequentially:

```sh
env GOCACHE=/Volumes/ExtremeSSD/macdev/quic-go-build-cache go test ./internal/transport -run '^$' -bench '^BenchmarkWaitRetries$' -benchmem -benchtime=10000x -count=3
```

Add `-overlay=/absolute/path/to/control.json` for the control run. Benchmarks ran
without concurrent tests or other benchmarks.

## Independent-peer stream echo

The unchanged v1 echo fixture verifies a 1 MiB request and identical reply plus
FIN, with the same pinned v0.63.0 client and matched server receive windows.
Sequential control and final runs, each 100 iterations × three samples, yielded:

| Server / run | Median ns/op | Median B/op | Median allocations/op |
| --- | ---: | ---: | ---: |
| Independent / control | 10094250 | 1668795 | 35634 |
| Independent / final | 10028845 | 1644719 | 35639 |
| Upstream / control run | 5402014 | 649148 | 19683 |
| Upstream / final run | 5365000 | 650550 | 19734 |

Echo allocation counts are effectively unchanged. The small timing change and
variable allocated-byte results do not establish an end-to-end improvement.
The change is retained for the demonstrated reduction under repeated readiness
checks, without additional allocations for immediately ready operations.
Final independent/upstream ratios remain approximately 1.87x elapsed time,
2.53x allocated bytes, and 1.81x allocations. These measurements include both
endpoints; they are not isolated server CPU or peak-memory measurements.

Raw results: [control](stream-echo-v063-wait-control.txt),
[final](stream-echo-v063-wait-final.txt). From interop, run each implementation:

```sh
env GOCACHE=/Volumes/ExtremeSSD/macdev/quic-go-build-cache go test -run '^$' -bench '^BenchmarkServerStreamEcho$' -benchmem -benchtime=100x -count=3 .
```

Full root race passed (root 8.621s, HTTP/3 1.410s, transport 41.099s), full default
independent race passed (85.327s), and both vet checks passed. The default suite
retains the exclusions documented in interop/KNOWN_FAILURES.md. Echo benchmarks
ran after all checks finished, sequentially with no other test/benchmark load.
