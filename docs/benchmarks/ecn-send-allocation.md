# Native UDP ECN send allocation — 2026-09-30

A fresh 300-iteration allocation profile of the independent server echo fixture
attributed 30.50 MiB of sampled allocation to writeECNDatagram. Compiler escape
analysis traced the local 64-byte ancillary buffer to the generic socket interface
call. Sharing that local variable across branches also made native UDP sends
allocate, although net.UDPConn.WriteMsgUDPAddrPort does not retain it.

The native branch now owns a separate stack buffer. Both branches use the same
encoder, and wrapped sockets still receive their own ancillary buffer through
the interface. No shared mutable ancillary storage or socket-wide options were
introduced. Existing tests check IPv4/IPv6 alternating markings, explicit zero,
wrapped socket dispatch, invalid outgoing CE, and ECN rejection/fallback.

Go 1.26.2, darwin/arm64, Apple M4. BenchmarkSocketSendECN sends to a live loopback
socket and drains each datagram synchronously. It includes receive cost, excludes
setup, and tests native sockets only. Three samples per address:

| Address / implementation | Median ns/op | B/op | allocations/op |
| --- | ---: | ---: | ---: |
| IPv4 / control | 8260 | 64 | 1 |
| IPv4 / final idle rerun | 8412 | 0 | 0 |
| IPv6 / control | 7873 | 64 | 1 |
| IPv6 / final idle rerun | 7840 | 0 | 0 |

The allocation reduction is consistent; these timings establish no speedup.
Raw results: [before](ecn-send-before.txt), [after, idle](ecn-send-after-idle.txt).
The [first after run](ecn-send-after.txt) overlapped the start of race checks near
its end and is excluded from timing comparisons.

The matched 1 MiB request/echo fixture uses pinned upstream v0.63.0 as client
for either server, with matching receive windows and setup excluded. Its metrics
include both client and server. Sequential, idle, 100-iteration runs, three samples
each; control uses the prior implementation via Go overlay:

| Server / implementation | Median ns/op | Median B/op | Median allocations/op |
| --- | ---: | ---: | ---: |
| Independent / control | 10590496 | 1793056 | 37508 |
| Independent / final | 10661452 | 1669301 | 35759 |
| Upstream / control run | 5657083 | 651639 | 19714 |
| Upstream / final run | 5698948 | 655567 | 19809 |

Final independent allocated bytes are about 6.9% lower and allocation count 4.7%
lower than control. Time is about 0.7% higher, within observed run variation;
no transfer speed improvement is established. Independent remains about 1.87x
time, 2.55x allocated bytes and 1.81x allocation count versus upstream. This does
not measure WAN behavior, server-only CPU, peak memory, fairness or tail latency.

Raw results: [control](stream-echo-ecn-control.txt),
[final](stream-echo-ecn-final.txt), [initial baseline](stream-echo-v063-priority-current.txt),
[sampled allocation profile](ecn-send-before-alloc-top.txt).
The [reverse control patch](ecn-send-control.patch) recreates the previous send
implementation; apply in an isolated copy or use a Go overlay.

```sh
# Root module:
env GOCACHE=/Volumes/ExtremeSSD/macdev/quic-go-build-cache go test -run '^$' -bench '^BenchmarkSocketSendECN$' -benchmem -count=3 ./internal/transport
# interop module (add -overlay=/absolute/path/to/control.json for control):
env GOCACHE=/Volumes/ExtremeSSD/macdev/quic-go-build-cache go test -run '^$' -bench '^BenchmarkServerStreamEcho$' -benchmem -benchtime=100x -count=3 .
```

Full root race passes (root 12.753s, HTTP3 1.503s, transport 43.849s;
other packages cached), focused ECN race passes (1.472s), and tests matching
Independent pass under race (21.040s). The full default interop race suite also
passes (88.425s). Both modules pass vet. Linux/amd64
transport test binary cross-compiles; this is not a Linux execution result.
