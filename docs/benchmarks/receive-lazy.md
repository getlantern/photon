# Lazy stream receive allocation

2026-09-25, Apple M4, macOS arm64, Go 1.26.2. Same implementation before and
after deferring circular receive-buffer allocation until valid bytes arrive.
Three samples per case; benchmark setup and first operation are included.
Receive capacity and advertised credit are 64 KiB. Results include a separately
allocated connection-flow object and escaping receive object.

Command: `go test ./internal/stream -run '^$' -bench '^BenchmarkReceiveAllocation$' -benchmem -count=3`

| Case | Before median ns/op | After median ns/op | Before bytes/op | After bytes/op | Allocations before / after |
| --- | ---: | ---: | ---: | ---: | ---: |
| Idle | 5993 | 24.93 | 131209 | 152 | 4 / 2 |
| First byte | 6009 | 5763 | 131209 | 131225 | 4 / 4 |
| Empty FIN | 5356 | 26.76 | 131208–131209 | 152 | 4 / 2 |
| Reset without data | 6430 | 31.93 | 131216 | 160 | 5 / 3 |

[Before samples](receive-lazy-before.txt), [after samples](receive-lazy-after.txt).
These are local allocation microbenchmarks, not independent-peer throughput
comparisons. Timing differences for the first-byte case are not evidence of a
network performance improvement. A capacity field enlarges the receive object;
first-byte allocation grows by 16 bytes. Active streams still allocate the full
payload ring and per-byte occupancy array even for a single byte. Sparse storage
and allocation proportional to connection data remain future work.

Reset, consuming EOF, and discarding complete canceled data now release existing
arrays immediately. Tests verify terminal duplicates do not recreate them,
invalid frames do not allocate, and flow-control accounting is preserved.
