# Public server stream-echo baseline

Latest follow-up: [statistics publication](statistics-publication.md) compares
value-snapshot publication with a no-op publication control on the matched v0.63.0
fixture. The small timing change is within these short samples' variation; no
performance improvement or negligible-overhead claim is established. Final
exchanges remain about 1.87x slower with 2.60x allocated bytes versus upstream.
The preceding [readiness-retry result](wait-retry.md) and
[short-header result](short-header.md) retain their matched controls.

The preceding [ACK-only history result](ack-only.md#independent-peer-stream-echo-follow-up)
and historical results below retain their original code and reference.

Measured 2026-09-20, Apple M4, darwin/arm64, Go 1.26.2, default GOMAXPROCS=10.
Measurement reference: upstream quic-go v0.62.0, the interop pin when these
results were collected. Historical sections below retain that reference. The
final v0.63.0 checkpoint measures the current interop pin.

The benchmark uses the same upstream client for both servers. Each operation
opens a new bidirectional v1 stream, concurrently sends a 1 MiB request and reads
its identical 1 MiB echo, checks every byte, and checks FIN. TLS/socket setup is
excluded from the timer. The same certificate generator and ALPN are used.
Server receive windows and stream limits are matched: 64 KiB stream windows,
1 MiB connection window, 16 incoming streams per direction. Congestion control,
packetization, socket I/O, and remaining implementation policies differ.

Three short samples, each three operations (medians):

| Server/runtime | Time per exchange | Combined throughput | Allocated bytes/op | Allocations/op |
|---|---:|---:|---:|---:|
| Independent, allocating UDP reader | 21.20 ms | 98.92 MB/s | 82,915,770 | 88,709 |
| Independent, pooled UDP reader | 19.23 ms | 109.08 MB/s | 9,316,514 | 85,124 |
| Upstream, pooled-run reference | 5.62 ms | 373.44 MB/s | 768,845 | 20,101 |

Pooling reduced allocated bytes by about 89% and median time by about 9% in these
samples. Our server remains about 3.4 times slower than the reference here and
allocates about 12 times as many bytes. These are preliminary observations, not
statistically established effects or universal performance claims. This is
loopback with no injected loss, delay, reordering, or parallel connections. Both
client and server run in one process, so allocation counts include both and the
harness. Residual handshake/ACK work can overlap the first timed exchange.
Retry cost, handshake latency, v2, real network behavior, and CPU utilization
are not measured by this comparison. Three operations per sample are too few
for robust performance conclusions; longer trials and multiple environments
remain necessary.

The baseline reader allocated `make([]byte, 65535)` for every UDP receive.
The only transport change between the recorded runs replaces that allocation
with `sync.Pool` buffers. A buffer transfers from reader to the bounded input
queue, then returns to the pool after HandleDatagram finishes. Retained CRYPTO,
stream data, DATAGRAM payloads, and deferred packets own copies. Pool contents
may be discarded by the garbage collector.

Reproduce the current comparison from `interop/`:

```sh
go test -run '^$' -bench '^BenchmarkServerStreamEcho$' -benchtime=3x -count=3 -benchmem
go test -race -run '^$' -bench '^BenchmarkServerStreamEcho$' -benchtime=1x -count=1
```

[Baseline raw samples](stream-echo-baseline.txt) and
[pooled raw samples](stream-echo-pooled.txt) include the upstream measurements
from each run. Race-enabled numbers are deliberately excluded from the table.
Full root race tests/vet, default interop race tests, and both benchmark variants
under the race detector passed with payload checks enabled.

A separate 30-operation allocation-profile run is recorded in
[profile-run output](stream-echo-profile-run.txt), [profile](stream-echo-memory.pprof),
and [allocation summary](stream-echo-allocations.txt). That run includes setup
and benchmark calibration in its profile and is not used for the table.
The largest remaining sampled allocation sites are STREAM frame serialization,
send-payload retention, packet sealing, AEAD plaintext allocation, and residual
receive-pool misses. They are candidates for buffer ownership/allocation work;
this profile alone does not establish where CPU time is spent.

```sh
go test -run '^$' -bench '^BenchmarkServerStreamEcho/independent$' -benchtime=30x \
  -memprofile ../docs/benchmarks/stream-echo-memory.pprof -o /tmp/quic-stream-benchmark.test
go tool pprof -top -alloc_space ../docs/benchmarks/stream-echo-memory.pprof
```

## Frame serialization scratch follow-up

The next change uses a 1200-byte scratch array owned by each serialized connection
when appending a frame payload. Packet sealing still produces separately owned
output, and reliable frames retain their original owned application data. The
scratch is cleared on failure/disposal. This trades 1200 bytes per connection for
fewer transient allocations, including during congestion/pacing retries.

The same three-sample, three-operation command produced these new medians:

| Server/runtime | Time per exchange | Combined throughput | Allocated bytes/op | Allocations/op |
|---|---:|---:|---:|---:|
| Independent, pooled reader plus scratch | 16.43 ms | 127.66 MB/s | 7,977,445 | 81,863 |
| Upstream, same follow-up run | 5.51 ms | 380.85 MB/s | 748,666 | 19,914 |

Compared with the pooled-reader run, median allocated bytes declined about 14%
and time about 15%. The same small-sample limitations apply; this is not a formal
statistical comparison. Upstream remains roughly three times faster in this
workload and allocates substantially fewer bytes.

[Raw samples](stream-echo-scratch.txt),
[separate profile-run output](stream-echo-scratch-profile-run.txt),
[allocation profile](stream-echo-scratch-memory.pprof), and
[allocation summary](stream-echo-scratch-allocations.txt) are retained. STREAM
serialization no longer appears among the leading sampled allocation sites.
Payload retention, packet sealing, and AEAD plaintext allocation remain prominent.
The profiles sample all process allocations, including setup and the reference
client; their totals are not benchmark per-operation figures.

Tests overwrite scratch after serializing multiple packets and verify both the
protected output and retained retransmission payloads in v1/v2. Existing loss and
reordering tests, full root race/vet and default interop race checks pass. Both
benchmark variants also pass full-payload comparisons with the race detector.


## Current implementation after automatic MTU and deferred split metadata

A fresh comparison on the same machine uses five samples of twenty exchanges
per server, with the existing checked 1 MiB echo harness and pinned upstream
v0.62.0. Both implementations use their current default MTU discovery behavior;
no convergence wait is imposed. A connection is reused within each sample, so
these measurements mix early discovery with later transfers.

| Server/runtime | Median time per exchange | Median combined throughput | Median allocated bytes/op | Median allocations/op |
|---|---:|---:|---:|---:|
| Independent, current | 13.70 ms | 153.08 MB/s | 6,973,466 | 71,049 |
| Upstream, same run | 5.49 ms | 382.28 MB/s | 715,830 | 19,659 |

The independent implementation takes about 2.50 times as long and allocates
9.74 times as many bytes and 3.61 times as many objects in this workload.
These remain local sequential samples, not a universal comparison. The older
three-operation measurements above used different code and shorter samples;
this run does not isolate the effect of any one intervening change. Process-wide
allocation counts include the common upstream client and harness. This does not
establish parity with current upstream releases beyond the pinned dependency.

[Raw comparison](stream-echo-mtu-current.txt) and
[race smoke output](stream-echo-mtu-race.txt) are retained. Both benchmark variants
pass the one-operation race smoke with byte and FIN checks. No production code
changed for this measurement checkpoint.

```sh
cd interop
go test -run '^$' -bench '^BenchmarkServerStreamEcho$' -benchtime=20x -count=5 -benchmem
go test -race -run '^$' -bench '^BenchmarkServerStreamEcho$' -benchtime=1x -count=1
```

A separate 300-operation independent-server run records
[profile-run output](stream-echo-mtu-profile-run.txt),
[CPU profile](stream-echo-mtu-cpu.pprof), [CPU summary](stream-echo-mtu-cpu.txt),
[allocation profile](stream-echo-mtu-memory.pprof), and
[allocation summary](stream-echo-mtu-allocations.txt).
Packet sealing accounts for 18.56% of sampled allocation bytes, AES-GCM buffer
allocation for 16.93%, and retained send data for 15.21%. These identify buffer
ownership paths worth investigating; changing them requires preserving queued
packet lifetime, retransmission data and authenticated plaintext ownership.
Profiles include setup, calibration and the upstream client.

The macOS CPU profile is dominated by syscall and runtime wait samples
(rawsyscalln, kevent, usleep, pthread_cond_wait and pthread_cond_signal). Its
summed samples exceed wall duration. It does not by itself demonstrate that
cryptography, serialization, or a particular transport function dominates active
CPU cost, nor justify treating those percentages as elapsed-time savings.


## Shared allocation for decoded header and plaintext

Packet opening now allocates the decoded header and plaintext together. Their
regions do not overlap, and the header slice has a capacity boundary so an append
cannot overwrite plaintext. Each open still owns a fresh allocation; ciphertext
is not mutated and later opens cannot overwrite retained outputs. Retaining only
a header now retains the whole decoded allocation; transport code consumes header
metadata immediately and does not retain that header separately.

A focused AES-128-GCM 1400-byte packet-open benchmark, three samples of 100,000
opens, changed from 4 allocations and 1448 bytes per open to 3 allocations and
1440 bytes. Timings were noisy (before 307.7–463.7 ns, after 311.9–408.7 ns), so
no speedup is established. [Before](packet-open-before.txt) and
[after](packet-open-after.txt) raw outputs are retained.

Five twenty-operation echo samples after the change have these medians:

| Server/runtime | Time per exchange | Combined throughput | Allocated bytes/op | Allocations/op |
|---|---:|---:|---:|---:|
| Independent | 13.79 ms | 152.07 MB/s | 7,016,339 | 70,191 |
| Upstream, same run | 5.40 ms | 388.05 MB/s | 703,497 | 19,452 |

[Raw echo samples](stream-echo-open-buffer.txt) show about 1.2% fewer independent
process-wide allocations than the preceding run, but slightly higher bytes and
time. Those aggregate measurements do not establish an end-to-end improvement;
packetization, receive-pool misses and runtime activity can vary between runs.
The measured benefit is the removed allocation in the focused packet-open test.

Validation includes output ownership and failed-authentication checks across both
versions and all three cipher suites, existing RFC protection vectors and multipath
protection tests, full root and default upstream interop race suites, and vet.
The existing input-immutability packet-open fuzzer passed 2,208,001 executions in
a ten-second, four-worker run. This does not constitute a cryptographic audit.

```sh
go test ./internal/packet -bench '^BenchmarkOpenOwnedPacket$' -benchtime=100000x -count=3
go test ./internal/packet -run '^$' -fuzz '^FuzzOpen$' -fuzztime=10s -parallel=4
```


## Receive bitmap checkpoint and validated-path timer work (2026-09-25)

Five 100-exchange samples per server on the same Apple M4 / macOS arm64 /
Go 1.26.2 environment. Both servers now explicitly specify 16 incoming streams
per type to retain the original benchmark policy despite changed public defaults.
Receive windows, payload validation, and timed-region boundaries are unchanged.

| Server/run | Median time | Median bytes/op | Median allocations/op |
| --- | ---: | ---: | ---: |
| Independent after receive bitmap | 12.143 ms | 6,010,367 | 59,466 |
| Upstream in that run | 5.502 ms | 709,249 | 19,691 |
| Independent after timer optimization | 11.712 ms | 5,919,412 | 51,216 |
| Upstream in that run | 5.428 ms | 708,678 | 19,675 |

[Receive-bitmap checkpoint](stream-echo-receive-bitmap.txt) and
[timer follow-up](stream-echo-validated-timer.txt). The former is a fresh baseline,
not a controlled isolated before/after measurement of the receive changes:
other implementation changes and sample durations differ from historical runs.

The timer optimization stops constructing a probe payload/header merely to test
amplification credit on clients and already validated server paths. Unvalidated
server paths retain the existing size calculation and budget gate. Multipath's
separate recovery arming already calculates its minimum packet size directly.
Median process-wide allocation count falls about 14%, bytes about 1.5%, and time
about 3.6% in these adjacent runs. The smaller timing difference is preliminary;
this is one loopback workload, and the reference timing also changed. The final
independent server remains about 2.16x slower and allocates about 8.35x as many
bytes as upstream here. No universal performance advantage is established.

A separate 200-operation profile of the bitmap checkpoint attributes roughly
58% of sampled bytes to packet sealing (20.7%), opening (19.5%), and retained
stream writes (17.3%). Socket-reader pool misses account for 5.6%. Receive-buffer
allocation is now about 1.5%. These are allocation attribution, not CPU shares.
Both peers and the harness run in one process, and profile totals include setup
and benchmark calibration. Full artifacts:

- [Profile run](stream-echo-bitmap-profile-run.txt)
- [Heap profile](stream-echo-bitmap-memory.pprof)
- [Allocated-byte summary](stream-echo-bitmap-alloc-space.txt)
- [Allocated-object summary](stream-echo-bitmap-alloc-objects.txt)

Reproduce timing with `go test -run '^$' -bench '^BenchmarkServerStreamEcho$'
-benchtime=100x -count=5 -benchmem` in interop. The separate profile command uses
`-benchtime=200x -memprofile ../docs/benchmarks/stream-echo-bitmap-memory.pprof`.
Race-enabled one-operation runs verify both server variants but their timings
are excluded from these comparisons.


## Connection-scoped decode storage (2026-09-25)

Ordinary synchronous receive processing now reuses connection-local packet decode
storage. Packet Open retains its owned-output contract; OpenInto permits separate
caller-owned output storage without modifying ciphertext. The connection grows
storage on demand up to the admitted packet size and clears/releases it on
transport cleanup. Multipath admission and pre-activation retention continue
using owned decoded packets because their frame slices can outlive the call.

The focused 1400-byte AES-128-GCM benchmark measures 1440 bytes / 2 allocations
for owned Open versus 32 bytes / 1 allocation for reuse, with medians 307.7 ns
and 225.5 ns. [Packet samples](packet-open-reuse.txt). Key/packet/storage setup is
outside the timer; this represents warm reuse, not the first packet.

Five 100-exchange samples retain the same v1 loopback fixture:

| Server | Median time | Median bytes/op | Median allocations/op |
| --- | ---: | ---: | ---: |
| Independent | 11.541 ms | 4,654,612 | 50,161 |
| Upstream | 5.430 ms | 711,265 | 19,720 |

[Raw results](stream-echo-decode-reuse.txt). Compared with the immediately prior
timer run, allocated bytes fall about 21.4%, with about 2.1% fewer allocations.
Time changes only about 1.5%; these samples do not establish a robust throughput
improvement. The independent server remains about 2.13x slower and allocates
about 6.54x as many bytes as the reference. Ciphertext sealing and retained send
payloads remain separate major allocations; this change does not address them.

Ownership tests cover v1/v2 and all three cipher suites, buffer growth/shrinkage,
failed-candidate reuse, unchanged ciphertext, and header append isolation.
Transport tests overwrite decode storage after processing and verify retained
STREAM and DATAGRAM contents, then verify cleanup erases/releases the storage.
The packet-open fuzzer compares owned and borrowed outputs/errors/numbers and
checks ciphertext immutability. These gates supplement, not replace, the RFC
vectors and independent interoperability tests.


## Socket-loop encrypted output reuse (2026-09-25)

The synchronous UDP loop now supplies a 2048-byte output buffer while constructing
one datagram, then completes the socket write and error handling before reuse.
Ordinary, path, and migration-probe sealing can borrow it. Larger output falls
back to an allocation. Existing engine/endpoint owned-output entry points pass
no storage. Cached close packets remain independently owned. The temporary
connection reference to socket storage is cleared on every construction exit,
and the socket loop clears its buffer on shutdown.

The 1400-byte sealing microbenchmark drops from 1568 bytes / 2 allocations to
32 bytes / 1 allocation. Median primitive time changes from 322.0 ns to 228.4 ns.
[Primitive samples](packet-seal-reuse.txt), collected with three default-duration
samples using `go test ./internal/packet -run '^$' -bench '^BenchmarkSealPacketStorage$' -benchmem -count=3`.

Five 100-exchange samples of the unchanged v1 echo comparison:

| Server | Median time | Median bytes/op | Median allocations/op |
| --- | ---: | ---: | ---: |
| Independent | 11.405 ms | 3,363,615 | 48,350 |
| Upstream | 5.437 ms | 709,070 | 19,694 |

[Raw samples](stream-echo-seal-reuse.txt). Compared with decode reuse alone,
median process-wide allocated bytes decline 27.7%, and allocation count 3.6%.
The roughly 1.2% timing difference is too small to establish a robust speedup
from these samples. Our implementation remains about 2.10x slower and allocates
about 4.74x as many bytes as upstream for this workload. This optimization does
not remove copying or retention of application send data.

Tests compare owned/borrowed sealing over all three cipher suites, both versions,
base/path nonces, output growth/shrinkage, and input immutability. Endpoint tests
verify owned earlier output survives later borrowed writes, temporary storage
references are cleared, and cached close output does not alias socket storage.
A differential sealing fuzzer varies malformed headers, payload, packet number,
path ID and storage capacity. Protocol vectors and transport recovery tests remain
required; ciphertext equality alone is not independent cryptographic validation.

## ACK result allocation (2026-09-28)

A fresh profile still identifies Send.Write's retained payload copy as the
largest allocation source. That storage cannot be recycled merely when a frame
is acknowledged: old packet mappings and split retransmission references require
an ownership audit first. This change instead removes geometric growth of newly
acknowledged packet result slices. Recovery counts matching retained packets,
then allocates the result once. Both passes use the existing bounded sparse
lookup/dense ledger scan selection. Duplicate ACKs produce no result allocation;
large peer ranges cannot cause allocation beyond the retained packet count.
Returned events remain independently owned.

The new BenchmarkACKBatch includes the full send/ACK cycle. Three samples at the
default duration give these median values:

| Packets per batch | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 205.0 | 207.9 | 232 | 232 | 5 | 5 |
| 16 | 2630 | 2642 | 4312 | 3464 | 56 | 52 |
| 64 | 10273 | 10377 | 17368 | 13448 | 202 | 196 |
| 256 | 39754 | 39930 | 69598 | 53390 | 780 | 772 |

[Before](ack-batch-before.txt), [after](ack-batch-counted.txt).
Reproduce from the root with `go test ./internal/recovery -run '^$' -bench
'^BenchmarkACKBatch$' -benchmem -count=3`. The extra counting pass buys fewer
allocations; these timing samples do not establish a speedup.

The same 100-exchange, three-sample server comparison gives:

| Server / revision | Median ms/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Independent before | 11.558 | 3340695 | 47999 |
| Independent after | 11.485 | 3214724 | 47251 |
| Upstream before | 5.451 | 697241 | 19462 |
| Upstream after | 5.431 | 697761 | 19450 |

[Before](stream-echo-send-before.txt), [after](stream-echo-ack-counted.txt).
Both runs used `go test -run '^$' -bench '^BenchmarkServerStreamEcho$'
-benchtime=100x -count=3 -benchmem -memprofile=/tmp/profile.pprof` from interop.
These are process-wide allocations including the shared upstream client.
Allocated bytes fall 3.8% and allocation count 1.6%; the timing difference is
small and does not establish a robust speedup. We remain about 2.11x slower and
allocate about 4.61x as many bytes as upstream for this workload.
[Before allocation profile](stream-echo-send-before-allocations.txt) and
[after allocation profile](stream-echo-ack-counted-allocations.txt) are sampled
whole-process profiles, including setup, and are not exact per-operation totals.

Recovery tests cover sparse/dense results, ordering, late and duplicate ACKs,
flight accounting and ownership across later calls. Recovery and transport race
suites and root vet passed. The independent/upstream echo comparison also passes
with the race detector. Retained send payload reuse remains open.

## Completed send payload cache (2026-09-28)

The engine now reuses completed unsplit STREAM payload buffers within one
connection. Send.WriteBuffer copies caller data into storage whose ownership is
transferred to reliable output. Successful ACK completion returns the buffer and
removes the old frame reference; late packet mappings and queued probes share
an acknowledged item and cannot retransmit that buffer. Failed completion does
not recycle anything. Split writes, cancellation and rejected early data retain
the existing garbage-collected ownership path. Cache entries are at most 2048
bytes, with at most 64 entries (128 KiB idle capacity) per connection. Transport
cleanup clears the full capacity of cached buffers and releases the cache.
No cache is shared across connections. This changes allocation churn, not the
requirement to copy caller data or retain outstanding reliable payloads.

Three 100-exchange samples with the same command and profile option as above:

| Server / revision | Median ms/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Independent, ACK counting only | 11.485 | 3214724 | 47251 |
| Independent, send cache | 11.406 | 2066808 | 46289 |
| Upstream, send-cache comparison run | 5.416 | 698513 | 19473 |

[Raw samples](stream-echo-send-cache.txt), [allocation profile](stream-echo-send-cache-allocations.txt).
The baseline is the preceding ACK-counting run. Allocated bytes decline 35.7%
and allocation count 2.0%. Timing remains similar and does not establish a robust
speedup. The independent server still takes about 2.11x as long and allocates
about 2.96x as many bytes as upstream in this workload. Idle retained memory can
increase by the bounded cache capacity; allocation reduction is not a claim of
lower memory use for every connection lifecycle.

New tests cover caller-data isolation, blocked writes, completion errors, late
ACKs and loss after actual buffer reuse, split-group exclusion, cache bounds and
v1/v2 cleanup erasure. Full stream/transport race suites and root vet passed.
Independent public stream, migration and HTTP early-recovery race gates passed.
Broader split/cancellation reuse and multi-connection memory measurements remain
future work; this cache does not establish universal performance superiority.


The send cache also has a sequence fuzzer, FuzzSendBufferOwnership, which models
two packet histories sharing retransmitted items. It checks successful/refused,
duplicate and late ACKs, loss, cancellation, pruning and reuse against expected
payloads and exactly-once ownership/accounting. An initial 30-second run passed
719,629 executions. With strengthened canceled-payload checks, a 20-second
race-enabled run passed 15,438 executions. Reproduce from the root:

```sh
go test ./internal/transport -run '^$' -fuzz '^FuzzSendBufferOwnership$' -fuzztime=30s -parallel=4
go test -race ./internal/transport -run '^$' -fuzz '^FuzzSendBufferOwnership$' -fuzztime=20s -parallel=4
```

These are bounded local exploration results, not exhaustive ownership proofs.
The model intentionally omits packet protection and ACK-of-unsent validation;
wire-level loss, migration and multipath tests remain separately necessary.


TestSendBufferCrossPathLateACK additionally exercises v1/v2 packet sealing and
production ACK dispatch. A retransmission on path one is acknowledged over path
zero, its buffer is reused by another stream, and subsequent late/duplicate ACKs
across paths must not retire or corrupt the new write. Delivering the delayed
original ciphertext also verifies that sealed output remains independent of the
recycled plaintext. Five race repetitions passed. Multipath setup and loss are
synthetic in this fixture; independent peer and path-validation tests are separate.

```sh
go test -race ./internal/transport -run '^TestSendBufferCrossPathLateACK$' -count=5
```

## Send-cache capacity tradeoff (2026-09-28)

The current limit is **16 entries / 32 KiB payload capacity per connection**,
replacing the original 64-entry limit. Compared 64, 16 and 4 entries sequentially
with three 100-exchange samples each, without profiling enabled:

| Entries | Maximum cached payload / connection | Median independent ms/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: | ---: |
| 64 | 128 KiB | 11.522 | 2056774 | 46155 |
| 16 (selected) | 32 KiB | 11.544 | 2126802 | 46246 |
| 4 | 8 KiB | 11.421 | 2674958 | 46790 |

[64-entry control](stream-echo-send-cache-64-control.txt),
[16 entries](stream-echo-send-cache-16.txt), [4 entries](stream-echo-send-cache-4.txt).
Each used `go test -run '^$' -bench '^BenchmarkServerStreamEcho$'
-benchtime=100x -count=3 -benchmem` from interop. The entry limit was the only
production change between these runs. Upstream medians were 5.450, 5.394 and
5.386 ms respectively. These small sample counts do not establish timing wins.

Sixteen entries reduce the cache's maximum payload capacity by 75%, while
raising process-wide allocated bytes 3.4% relative to 64 entries. Four entries
raise bytes about 30.1% relative to 64. The selected limit preserves most of the
allocation benefit with less idle retention. At 1000 fully populated caches the
payload capacity bound is 31.25 MiB instead of 125 MiB; those are calculated
capacity bounds, not measured heap/RSS results. Slice tables, allocator overhead,
outstanding payloads and the rest of each connection are additional memory.
Actual multi-connection memory-load measurements remain open.

Focused stream/cache, cross-path late-ACK and fuzz-seed race tests passed with
the selected limit, as did transport vet and the race-enabled echo comparison.

## Public 1280-byte initial default checkpoint (2026-09-28)

Refreshed after public InitialPacketSize integration and the intervening receive
window / timeout work. Same 16-entry send cache and matched receive limits.
Command from interop: `go test -run '^$' -bench '^BenchmarkServerStreamEcho$'
-benchtime=100x -count=3 -benchmem`. No race instrumentation, profiler, or concurrent
test process. [Raw output](stream-echo-initial-1280.txt).

| Runtime | Median ms/op | Median MB/s | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Independent | 12.806 | 163.76 | 2137220 | 45566 |
| Upstream v0.62.0 | 6.235 | 336.36 | 707371 | 19640 |

The independent server remains about 2.05x slower, with 3.02x allocated bytes and
2.32x allocations in this loopback workload. Both runtimes were slower than the
previous checkpoint. These three samples do not isolate the packet-size change
or establish a timing improvement/regression. WAN, tail latency and connection
memory-load comparisons remain open.


## v0.63.0 peer checkpoint (2026-09-28)

Same payload, matched server receive limits and benchmark fixture. No race,
profiler or concurrent test process during the comparison; each sample is 100
operations, three samples per implementation. [Raw output](stream-echo-v063.txt).

| Runtime | Median ms/op | Median MB/s | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Independent | 11.099 | 188.94 | 2160645 | 46503 |
| Upstream v0.63.0 | 5.360 | 391.26 | 702596 | 19565 |

Our implementation is 2.07x slower, with 3.08x allocated bytes and 2.38x
allocations in this workload. The newer checkpoint is not an isolated estimate
of either the peer upgrade or intervening changes. These measurements do not
establish WAN, v2, tail-latency or many-connection performance.

A separate 300-operation independent-server run collected
[CPU](stream-echo-v063-cpu.pprof) and [allocation](stream-echo-v063-memory.pprof)
profiles, with [run output](stream-echo-v063-profile.txt),
[CPU summary](stream-echo-v063-cpu.txt) and
[allocation summary](stream-echo-v063-allocations.txt). Its timing is excluded
from the comparison. Both the upstream client and our server run in the profiled
process; profiles include setup/calibration. Darwin syscall/runtime wait samples
dominate the CPU summary and cannot be read as independent-server CPU alone.

The allocation profile includes socket receive/address conversion, recovery
history, ACK construction, application/runtime handoffs and packet buffers.
`Conn.outboundBaseLarge` alone accounts for 16 MB (2.59%) of sampled allocated
space, motivating examination of per-packet write-error metadata. Moving it into
Outbound would remove a separate allocation but enlarge every Outbound object;
that tradeoff requires measurement, especially for small packets and other paths.
Recovery/ACK ownership and socket I/O remain larger investigation areas.

Reproduce the unprofiled comparison from interop:

```sh
go test -run '^$' -bench '^BenchmarkServerStreamEcho$' -benchtime=100x -count=3 -benchmem
```

All six comparison samples and the profile run completed with exact payload/FIN
checks. No implementation changes were made in this checkpoint.


## Rejected inline MTU metadata experiment (2026-09-28)

Experiment: replace Outbound.baseLarge's pointer with an embedded value, using
size=0 as absence and returning a value from Conn.outboundBaseLarge. Focused MTU,
write-error and path tests passed with race instrumentation (10.787s). The first
benchmark attempt overlapped that process; its [raw record](stream-echo-v063-inline-mtu-overlapped.txt)
is excluded from timing comparisons. An isolated repeat is saved
[here](stream-echo-v063-inline-mtu.txt).

The isolated independent-server medians were 11.082 ms/op, 2,127,079 B/op and
45,628 allocations/op: about 1.9% fewer allocations and 1.6% fewer allocated bytes
than the preceding pointer baseline. Three samples do not establish a timing win.
The same run's upstream median was 5.472 ms/op.

However, BenchmarkOutboundEnvelope, which isolates outbound objects without MTU
metadata, measured 208 B/op with embedding versus 160 B/op with the pointer,
both one allocation. Raw [embedded](outbound-inline-envelope.txt) and
[pointer](outbound-pointer-envelope.txt) samples use 100,000 iterations, three
repetitions. This is a 30% per-envelope byte increase for the small-packet path;
it is not a measurement of total small-packet workload memory or throughput.
Microbenchmark timings varied and are not used to claim a speed difference.

The candidate was reverted byte-for-byte. Production retains pointer metadata.
The reusable envelope benchmark remains. A future design should investigate a
single combined allocation only for packets that need metadata, or another
ownership-safe solution that does not enlarge every packet. This experiment
provides a measured tradeoff, not a delivered optimization.


## Reuse synchronous runtime MTU metadata (2026-09-28)

The UDP owner already reuses one Outbound descriptor after each synchronous
write and error handler. It now also supplies one base-path MTU metadata slot.
The slot is reset after write/error handling to release retained send-item
references. Public NextDatagram callers retain independently allocated metadata;
Outbound itself is unchanged. No shared per-connection mutable metadata is used.

[Metadata/envelope microbenchmarks](outbound-reused-metadata.txt), three samples
of 100,000 operations, measure owned metadata at 64 B/op and one allocation,
versus reused metadata at zero bytes/allocations. Ordinary outbound envelopes
remain 160 B/op and one allocation. These isolate allocation ownership, not
network performance.

[Isolated echo samples](stream-echo-v063-reused-mtu.txt), 100 operations and three
repetitions per server, have these medians:

| Runtime | ms/op | MB/s | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Independent | 11.172 | 187.71 | 2129722 | 45707 |
| Upstream v0.63.0 | 5.514 | 380.32 | 706199 | 19610 |

Compared with the preceding pointer baseline this is about 1.7% fewer allocations
and 1.4% fewer allocated bytes. Timing varies; these samples do not establish a
speedup. The independent server remains about 2.03x slower than the same-run
reference, with 3.02x allocated bytes and 2.33x allocations. Profiling and wider
workload/platform comparisons remain necessary.

MTU/write-failure tests now exercise both owned and borrowed descriptors and
metadata, including v1/v2 STREAM retransmission, DATAGRAM release, baseline
validation and accounting. Unlike the rejected inline layout, this change does
not enlarge each small-packet envelope. It retains one metadata slot per UDP
owner for that owner's lifetime.


## Borrow ACK state during packet encoding (2026-09-28)

Base and multipath packet builders now use a private ACK view over the receive
tracker's existing ranges/ECN counters. State transitions are serialized and the
frame is encoded immediately; it is not queued for retransmission. Received.Ack
continues to return an owned snapshot for callers that retain frames. Regression
tests mutate receive ranges and counters after encoding and verify both owned
snapshots and already encoded bytes remain unchanged.

[ACK encoding microbenchmarks](ack-borrowed-encoding.txt) use four ranges plus ECN,
100,000 iterations and three samples: snapshots allocate 88 B/op in two
allocations; borrowed serialization allocates zero. This is a bounded ownership
optimization, not a concurrency relaxation.

[Isolated echo comparison](stream-echo-v063-borrowed-ack.txt), 100 iterations and
three samples per server:

| Runtime | Median ms/op | Median MB/s | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Independent | 11.307 | 185.48 | 2002468 | 44145 |
| Upstream v0.63.0 | 5.432 | 386.05 | 705890 | 19603 |

Independent allocated bytes are about 6.0% lower and allocation count 3.4% lower
than the preceding reused-MTU checkpoint. Timing is variable and does not show
an established improvement. The same-run gap remains 2.08x time, 2.84x bytes and
2.25x allocations. No profiler/race process or concurrent test ran during these
measurements. Broader latency, CPU, loss, multipath and connection-load measures
remain open.


## Typed recovery packet sorting (2026-09-28)

Recovery now sorts packet events with slices.SortFunc and an unsigned packet-number
comparison. This removes reflection-related allocations from sort.Slice while
retaining ascending event ordering. Existing recovery tests cover ACK/loss,
flight accounting, timers and migration; no ACK or congestion policy changed.

The send/ACK-cycle microbenchmark, 10,000 iterations and three samples per batch,
has [before](ack-sort-before.txt) and [after](ack-sort-after.txt) records:

| Batch | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: |
| 1 | 232 | 208 | 5 | 4 |
| 16 | 3465 | 3329 | 52 | 49 |
| 64 | 13452 | 13316 | 196 | 193 |
| 256 | 53403 | 53267 | 772 | 769 |

Median 64/256-packet timings increased about 2.3%/1.7% in these short sequential
runs; smaller batches were faster. These samples do not establish a general CPU
improvement or regression. The reliable result is removal of allocation sites;
larger-batch CPU behavior remains a tuning question.

[Isolated echo comparison](stream-echo-v063-typed-sort.txt), 100 iterations and
three samples per implementation:

| Runtime | Median ms/op | Median MB/s | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Independent | 11.000 | 190.65 | 1972595 | 43150 |
| Upstream v0.63.0 | 5.352 | 391.84 | 704753 | 19593 |

Independent allocations are about 2.3% lower and allocated bytes 1.5% lower than
the preceding borrowed-ACK checkpoint. The server remains about 2.06x slower in
this workload; no universal performance claim follows from these samples.


## Ordered ACK lookup (2026-09-28)

Bounded packet-number lookup now visits disjoint ACK ranges from lowest to
highest and emits ascending recovery events directly. Only the retained-ledger
scan fallback sorts ACK events. Lookup bounds, RTT sampling, ECN processing,
and loss detection are unchanged. Regression coverage checks mixed duplicate
and newly acknowledged ranges, sorted ACK/loss events, flight accounting,
RTT sampling, and the wide-range scan fallback.

[Send/ACK-cycle measurements](ack-ordered-lookup.txt), 10,000 iterations and
three samples per batch, compared with [typed sorting](ack-sort-after.txt):

| Batch | Previous median ns/op | Ordered lookup median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| 1 | 338.0 | 451.9 | 208 | 4 |
| 16 | 2704 | 2756 | 3329 | 49 |
| 64 | 9837 | 9317 | 13316 | 193 |
| 256 | 39240 | 37619 | 53267 | 769 |

Allocations are unchanged. Larger batches improve about 5.3%/4.1% in these
short runs; smaller batches are slower, with substantial within-run variation.
These samples do not establish a general CPU improvement.

[Isolated echo comparison](stream-echo-v063-ordered-ack.txt), 100 iterations and
three samples per implementation:

| Runtime | Median ms/op | Median MB/s | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Independent | 11.029 | 190.15 | 1977878 | 43151 |
| Upstream v0.63.0 | 5.364 | 391.00 | 703143 | 19577 |

Echo performance and allocations are effectively unchanged from the preceding
checkpoint. The independent server remains about 2.06x slower in this workload.

## Fixture address-family correction (2026-09-28)

After diagnosing IPv6-wildcard/IPv4 port overlap in other interop fixtures on
Darwin, reference clients now use dialUpstreamAddr: resolve the destination,
bind the corresponding UDP family, then call unmodified upstream Dial. Both
benchmark server variants use this same helper, retaining single-use/zero-CID
behavior and setup outside the measurement timer. The historical results above
predate this socket-family change. The 2026-09-29 measurements below use the corrected fixture; historical timings
should not be attributed directly to subsequent production changes.


## Bounded public write batching (2026-09-29)

Public Write previously submitted one owner command per packet-sized fragment.
It now commits at most 32 KiB per owner turn, stopping earlier on flow control,
queue admission or error. A turn that commits some bytes reports progress before
waiting again, preserving wakeups and partial-write counts. Deadline changes and
cancellation remain observable between bounded turns.

Profiling identified payload allocation churn when batching exceeded the old
16-entry send-buffer cache. The final cache allows up to 32 entries while enforcing
an explicit, unchanged **32 KiB retained payload-capacity ceiling**; buffers larger
than 2 KiB remain excluded. It counts actual capacities, returns budget on take,
and clears accounting at teardown. The pointer table may retain more entries;
this is not a claim of unchanged total per-connection memory or peak usage.

Final matched medians (100 iterations, three samples, no race instrumentation or
concurrent tests/fuzzing; Darwin/arm64 Apple M4, Go 1.26.2):

| Runtime/configuration | ms/op | MB/s | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Independent control: one fragment, 16 cache entries | 11.960 | 175.34 | 1860022 | 43126 |
| Independent: 32 KiB batch, byte-bounded cache | 11.083 | 189.23 | 1861372 | 41640 |
| Upstream v0.63.0 during final run | 6.034 | 347.53 | 655559 | 19747 |

Raw [final control](stream-echo-v063-write-control-final.txt) and
[final implementation](stream-echo-v063-batched-write-final.txt). The control
uses a Go build overlay restoring one-fragment Write and a 16-entry cache;
byte-accounting bookkeeping remains in both variants. The exact source difference
is [write-batch-control.patch](write-batch-control.patch). The reference median
in the control run was 5.970 ms/op, showing some between-run variation.

The final independent median is about 7.3% lower in time and 3.4% lower in
allocation count than the control, with effectively unchanged allocated bytes
(+0.07%). It is still about 1.84x slower and allocates about 2.84x as many bytes
as the reference in this workload. These are short localhost samples, not
confidence intervals, WAN/tail results, fairness results or universal superiority.

The first [fresh baseline](stream-echo-v063-public-reset.txt) measured 16.058 ms
versus 7.189 ms for the reference. A later [old-write repeat](stream-echo-v063-write-control-repeat.txt)
measured 12.096 ms versus 5.981 ms. That large shared timing change is why the
initial apparent improvement is not used as the final effect estimate.
[Batch-only](stream-echo-v063-batched-write.txt), its
[repeat](stream-echo-v063-batched-write-repeat.txt), and an
[8 KiB candidate](stream-echo-v063-write-8k.txt) reduced command overhead but
increased allocated bytes. The smaller candidate was not selected. The
[cache-adjusted intermediate run](stream-echo-v063-batched-write-cache.txt)
confirmed that bounded reuse restored allocated bytes near the control range.

Diagnostic profiles [before](write-batch-before-memory.pprof) and
[batch-only after](write-batch-after-memory.pprof), with
[object allocation sites](write-batch-before-allocations.txt) and
[allocated-byte difference](write-batch-allocation-delta.txt), identified reduced
Write/invoke allocation and increased Send.WriteBuffer allocation. These profiles
precede the final cache fix and are not final performance measurements. Their
separate runs are recorded in write-batch-before-profile.txt and
write-batch-after-profile.txt.

Reproduce the final benchmark from interop:

```sh
env GOCACHE=/Volumes/ExtremeSSD/macdev/quic-go-build-cache go test -run '^$' -bench '^BenchmarkServerStreamEcho$' -benchtime=100x -count=3 -benchmem
```

To reproduce the control without altering the worktree, apply the saved patch to
temporary copies of its two files and pass a Go `-overlay` JSON mapping the
original absolute paths to those copies. Keep tests/fuzzing out of benchmark
runs. The receive-window, peer, payload, stream-count, timing and socket-family
conditions remain those of the fixture described above.

Default-policy update (2026-09-29): the independent server fixture now explicitly
sets the same 64 KiB stream / 1 MiB connection initial and maximum windows already
pinned for the reference server. This preserves the workload above despite the
public API's new 512 KiB / 768 KiB initial defaults and enabled growth. No new
performance measurement was made for the changed defaults; historical numbers
must not be presented as their performance or memory profile.

Receive-ring follow-up (2026-09-29): [allocation and matched echo results](receive-ring.md)
measure reduced first-byte allocation separately from the small bulk-echo cost
increase. The final run still trails the pinned reference; it is not a throughput
improvement claim.
