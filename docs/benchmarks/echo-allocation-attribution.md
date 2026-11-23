# Echo allocation attribution — 2026-09-29

The independent-server echo benchmark runs the server and pinned upstream
v0.63.0 client in one process. An unfiltered allocation profile includes both.
In the profile collected for the [short-header experiment](short-header.md),
the `socket.(*Conn).recvMsg` subtree accounts for 3,991,253 sampled allocation
objects, or 39.76% of the whole profile. Its callers trace to upstream's
`Transport.listen`, `oobConn.ReadPacket`, and `ipv4.ReadBatch`.
The [call tree](current-echo-peer-receive.txt) records that attribution.

Our `internal/transport/udp_server.go` receiver already uses
`net.UDPConn.ReadMsgUDPAddrPort` with pooled datagram buffers and a stack ancillary
buffer. The large x/net receive stack is therefore not evidence that replacing
our receive API will remove those allocations. It also does not establish which
implementation has lower receive costs in a matched workload.

The [filtered allocation summary](current-echo-own-alloc-top.txt) selects stacks
containing our root or internal packages. Its filter deliberately excludes the
`github.com/getlantern/photon/interop` fixture package: matching our module prefix
alone would also retain upstream client work called by that fixture.

```sh
go tool pprof -alloc_objects -tree -focus='socket.*recvMsg' -nodecount=16 /tmp/quic-current-echo.test /tmp/quic-current-echo-mem.pprof
go tool pprof -alloc_objects -top -focus='github.com/getlantern/photon(/internal/|[.])' -nodecount=20 /tmp/quic-current-echo.test /tmp/quic-current-echo-mem.pprof
```

These commands analyze the same pre-header-change binary/profile used in the
short-header experiment. To collect a fresh profile, run from `interop`:

```sh
env GOCACHE=/Volumes/ExtremeSSD/macdev/quic-go-build-cache go test -run '^$' -bench '^BenchmarkServerStreamEcho$/^independent$' -benchmem -benchtime=300x -count=1 -cpuprofile=/tmp/quic-current-echo-cpu.pprof -memprofile=/tmp/quic-current-echo-mem.pprof -o /tmp/quic-current-echo.test .
```

A fresh profile uses the current header implementation and will not reproduce
the old header counts. Profiling timings are not comparison benchmarks.

The filtered summary highlights recovery record storage, packet protection,
stream queues, frame parsing, owner-command replies, and deadline contexts.
Header construction and AppendUint32 entries predate the completed header fix.
Percentages still use the whole-profile denominator. The top 20 nodes account
for 47.65% of whole-profile objects; this is neither a complete server allocation
total nor isolated server CPU attribution. Cumulative entries overlap and must
not be added. Stack filtering also cannot assign independently scheduled runtime
work to an endpoint.

Two implementation constraints matter for subsequent experiments:

- Packet-protection scratch passed through the AEAD interface is a candidate
  allocation source, but Keys permits concurrent use. Shared mutable scratch
  would require a concurrency design and validation.
- `streamDeadline.run` creates and cancels a context per operation. Its generation
  check permits changed deadlines to retry only uncommitted operations. Any
  reuse design must preserve deadline changes during blocked operations, cleanup,
  and partial-write accounting. The independent-peer
  `TestWriteDeadlineChangeAfterPartialCommit` exercises deadline extension,
  expiration, clearing, and delivery across QUIC versions.

No production change or new performance result follows from this attribution
pass. The latest matched performance comparison remains the short-header result.
