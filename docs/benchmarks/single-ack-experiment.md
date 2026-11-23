# Single-packet ACK lookup experiment — 2026-09-29

Status: candidate reverted; production retains its previous implementation.

OnACK already looks up the largest acknowledged packet for RTT and ECN handling.
The candidate reused that value for a single-packet ACK instead of looking it up
again during counting and retirement. All other ranges retained the existing
bounded lookup or ledger-scan paths. The candidate added two conditional branches
on the general path and did not change allocation behavior.

The existing send/ACK-cycle benchmark ran sequentially on Go 1.26.2,
darwin/arm64, Apple M4, without concurrent tests or benchmarks:

```sh
go test ./internal/recovery -run '^$' -bench '^BenchmarkACKBatch$/^1$' -benchmem -benchtime=100000x -count=3
```

| Implementation | Median ns/op | Sample range ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Control | 175.8 | 154.1–232.6 | 128 | 2 |
| Candidate | 174.1 | 150.4–226.3 | 128 | 2 |

The median difference is about 1%, much smaller than sample variation. Both runs
show declining times across samples, so this short sequence is insufficient to
separate an implementation effect from warmup or machine behavior. It does not
establish a useful throughput improvement, and larger/mixed ACK costs were not
measured. The extra branches were therefore removed pending stronger evidence.
This is an inconclusive experiment, not proof that the idea can never help.

[Control output](single-ack-control.txt),
[candidate output](single-ack-candidate.txt), and
[reproducible candidate patch](single-ack-candidate.patch) are retained.
Apply the patch to an isolated copy to reproduce the proposed code.

Full recovery race passed on the candidate (1.300s). The prior sent.go was then
restored byte-for-byte, checked with cmp, and its recovery race suite passed
(cached). No production change remains. Future optimization work should return
to broader profiles and workloads rather than infer benefit from this tiny
single-packet timing difference.
