# Receive occupancy bitmap

2026-09-25, macOS arm64, Apple M4, Go 1.26.2. Three samples per case, same
workspace before/after; no concurrent tests or fuzzing during benchmark runs.

Changed occupancy from one bool per payload byte to one bit. Push validates
occupied overlaps before any state mutation, then copies spans within bitmap
words and counts newly present bits. Read uses contiguous occupied spans. Both
handle arbitrary ring capacities and word/ring wrap boundaries.

| Sequential push/read payload | Before median ns/op | After median ns/op |
| --- | ---: | ---: |
| 64 bytes | 137.8 | 12.85 |
| 1200 bytes | 2673 | 214.6 |
| 16384 bytes | 36604 | 2729 |

Commands: `go test ./internal/stream -run '^$' -bench '^BenchmarkReceiveCycle$' -benchmem -count=3`
and `go test ./internal/stream -run '^$' -bench '^BenchmarkReceive(Cycle|Allocation)$' -benchmem -count=3`.

[Before](receive-bitmap-before.txt), [after](receive-bitmap-after.txt).
A bytewise bitmap intermediate implementation nearly doubled cycle time;
[those samples](receive-bitmap-bytewise.txt) are retained for transparency. The
final version processes contiguous spans within each word.

The 64 KiB receive payload plus occupancy allocation falls from 131225 bytes to
73880 bytes, including flow and receive objects (roughly 44% fewer allocated
bytes). Occupancy itself falls from 65536 to 8192 bytes. Both versions allocate
four objects for the first-byte case. The previous allocation baseline is in
[the lazy-buffer samples](receive-lazy-after.txt).

The cycle benchmark uses a reused 64 KiB ring, new sequential payloads, no
retransmissions, immediate consumption, and window advancement. Speedups of
about 10–13x apply to this microbenchmark only. They do not measure network
throughput, reordered/retransmitted traffic, or superiority to upstream quic-go.
Active streams still allocate their full configured payload capacity even for
one byte; sparse allocation remains unfinished.
