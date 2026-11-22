# Packet scratch allocation measurement

2026-09-20; Go 1.26.2, darwin/arm64, Apple M4; default benchmark concurrency
reported as 10. These are sequential local before/after microbenchmark samples,
not an interleaved statistical study or an independent-implementation comparison.

Command for each implementation:

    go test ./internal/packet -run '^$' -bench 'BenchmarkOpenOwnedPacket|BenchmarkInitialSeal' -benchmem -count=5

The change places the header-protection mask and AEAD nonce in one per-call
scratch object. Previously each escaped separately through cipher interfaces.
Keys remain immutable, concurrent calls have independent scratch, and returned
packet/header/plaintext ownership is unchanged. Cipher algorithms and wire
formats are unchanged.

| Benchmark | Before ns/op (five samples) | After ns/op (five samples) | Before allocations | After allocations | Bytes/op, unchanged |
|---|---|---|---:|---:|---:|
| OpenOwnedPacket | 311.1, 314.6, 313.1, 312.4, 312.7 | 305.8, 306.8, 307.5, 306.3, 306.6 | 3 | 2 | 1440 |
| InitialSeal | 249.8, 250.4, 250.2, 252.6, 267.0 | 244.7, 245.3, 245.3, 244.2, 323.7 | 3 | 2 | 1312 |

Medians: open 312.7 → 306.6 ns/op; seal 250.4 → 245.3 ns/op (about 2% each).
The seal samples contain timing outliers. The repeatable result is one fewer
allocation per operation; size-class rounding leaves total bytes unchanged.
These benchmarks exercise the existing AES-128 cases and do not establish
latency, throughput, CPU, or memory superiority over quic-go.

Correctness coverage includes existing RFC vectors, key updates, multipath
nonces, ownership and authentication failures. A new concurrent shared-key test
covers v1/v2 and all three supported cipher suites across 16 workers and four
path IDs, with unique packet numbers for successful seals.
