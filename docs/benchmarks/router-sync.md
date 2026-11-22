# Router synchronization microbenchmark

Measured on Apple M4, darwin/arm64, Go 1.26.2. Three samples per case,
100 ms per sample, default GOMAXPROCS (10). All synthetic connections share one
remote address, stressing the old zero-CID ambiguity scan. Setup is untimed.
These measurements are a local before/after comparison, not an upstream quic-go
comparison or an end-to-end throughput/latency claim. Short samples and local
system noise limit statistical conclusions.

| Connections | Before median ns/op | After median ns/op | Before allocations | After allocations |
| --- | ---: | ---: | ---: | ---: |
| 1 | 177.0 | 5.894 | 2 / 24 bytes | 0 |
| 256 | 2239 | 7.461 | 2 / 24 bytes | 0 |
| 4096 | 39499 | 7.705 | 2 / 24 bytes | 0 |

Unchanged CID state now returns after a map lookup and a state comparison.
Actual issuance, retirement, and owner replacement still rebuild routing and
check collisions. A regression test exercises retirement plus replacement with
unchanged active-ID count. Initial routing alias and peer address are immutable
throughout a registration. Future migration APIs must invalidate this cache.

Run from the repository root:

```sh
go test ./internal/transport -run '^$' -bench BenchmarkRouter -benchmem -benchtime=100ms -count=3
```

Raw outputs: [before](router-before.txt), [after](router-after.txt). The
[baseline router source](router-before.go.txt) is retained as a noncompiled
artifact. To reproduce the baseline, replace `internal/transport/router.go` with
that file in a disposable checkout containing the benchmark, then run the same
command. Preserve the benchmark test and all other files unchanged. The baseline
is reconstructed by removing this change's cache fields, check, and assignment;
its routing implementation is the one measured in the before run.

Dispatch remained allocation-free in both runs. Real workloads with CID churn,
mixed peer addresses, socket I/O, and admission load still need measurement.
