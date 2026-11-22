# Portable STREAM payload experiment, 2026-09-20

Environment: Apple M4, darwin/arm64, Go 1.26.2. Both endpoints use this
implementation over loopback. This is not an upstream quic-go comparison.
Three sequential 10-iteration samples before and after replacing the 900-byte
STREAM payload cap with 1140 bytes. No race instrumentation during measurement.

Command:

```
go test -run '^$' -bench '^BenchmarkPublicPathTransfer/v1/paths=0$' -benchtime=10x -count=3 -benchmem .
```

Each iteration completes a one-way 256 KiB transfer. Exact observed samples:

| Cap | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| 900 | 4291196 | 2418321 | 20512 |
| 900 | 3440275 | 2409261 | 20501 |
| 900 | 3538608 | 2424748 | 20544 |
| 1140 | 3771225 | 1968464 | 16256 |
| 1140 | 2945467 | 2127304 | 16297 |
| 1140 | 2866558 | 2048633 | 16316 |

Median elapsed time decreased about 16.8%, allocations about 20.6%, and allocated
bytes about 15.3%. Timing is noisy and the sample count is small; these results
are preliminary, not evidence of universal performance improvement. Larger
frames need fewer packets/queue entries for this workload. Packet count for
262144 data bytes falls from ceil(262144/900)=292 to ceil(262144/1140)=230,
excluding control traffic and retransmissions.

The 1140-byte bound reserves 41 bytes for the largest short header plus AEAD tag
and 19 bytes for a STREAM frame's maximum-width ID/offset plus two-byte length.
It therefore remains portable to a 1200-byte path after migration or size
fallback. This does not enable dynamic packetization above 1200 bytes. That
requires retransmission splitting with shared delivery accounting so an ACK of
an original frame and ACKs of its smaller copies release memory/stream state
exactly once.
