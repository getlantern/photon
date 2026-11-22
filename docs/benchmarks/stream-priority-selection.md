# STREAM priority selection microbenchmark

2026-09-29, macOS arm64, Apple M4, Go 1.26.2. Command:

```
env GOCACHE=/Volumes/ExtremeSSD/macdev/quic-go-build-cache go test ./internal/transport -run '^$' -bench BenchmarkStreamPrioritySelection -benchtime=100ms -count=1
```

| Queued items | Initial two-pass selection ns/op | Final one-pass selection ns/op |
| --- | ---: | ---: |
| 1 | 4.470 | 4.596 |
| 16 | 185.8 | 120.1 |
| 256 | 2885 | 1655 |
| 4096 | 49331 | 27242 |

All cases reported zero bytes and allocations per operation. These are short,
single samples, not statistical performance comparisons. The fixture scans a
fixed queue with 16 repeated stream IDs, selecting/committing without removing
frames; after initial commitment it repeatedly selects retransmission work.
It does not measure end-to-end throughput, loss recovery, fairness, or path
migration. Selection remains linear in queued items. There is no upstream
comparison here, and this does not supersede the slower loopback echo result.
An indexed scheduler and matched multi-stream workloads remain needed.
