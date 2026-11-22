# Stream echo after ECN and ACK lookup work

Go 1.26.2, macOS arm64, Apple M4, GOMAXPROCS=10. Five sequential samples,
20 iterations each, without simultaneous testing. Each operation opens a stream,
sends 1 MiB and checks the identical 1 MiB response. Setup and handshake are
excluded. Both server implementations use the same upstream client; upstream
server receive-window/stream limits match our current defaults. These are
whole-process allocation counts, including that client.

```
cd interop
go test . -run '^$' -bench '^BenchmarkServerStreamEcho$' -benchtime=20x -count=5
```

The initial measurement includes ECN support and sparse ACK lookup optimization.
Our median was 13.997 ms versus upstream's 5.209 ms. Thus the ACK microbenchmark
improvement did not remove the end-to-end performance gap in this workload.

A separate 100-iteration CPU/allocation profile identified per-packet Outbound
allocation as a smaller removable cost. Its binaries/profiles are on the external
SSD at `/Volumes/ExtremeSSD/macdev/quic-echo-profile-C3zsFk`. Profiling timings
are not included in the before/after comparison below.

The UDP owner now reuses its synchronous outbound descriptor, clearing all
references after the write/error callback. Existing NextDatagram callers
still receive independent descriptors; packet byte ownership is unchanged.

| Sample | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 14032883 | 14004910 | 7183464 | 7018052 | 68836 | 67112 |
| 2 | 13996940 | 13987394 | 7156139 | 6837255 | 68877 | 67027 |
| 3 | 13911429 | 13931008 | 7187164 | 6927176 | 68787 | 67120 |
| 4 | 13880906 | 13918212 | 7170882 | 6935560 | 68743 | 67050 |
| 5 | 14041027 | 13897556 | 7168721 | 6840445 | 68882 | 67043 |

Median allocations decreased 2.6% (68,836 to 67,050), and median bytes decreased
3.4% (7,170,882 to 6,927,176). Median time changed 13.997 to 13.931 ms; no
throughput improvement is claimed at this scale. The after-run upstream medians
were 5.152 ms, 712,922 B/op, and 19,653 allocs/op: our implementation still trails
substantially. This is one v1 loopback workload, not a universal comparison.

## Lazy owner notifications

A subsequent change allocates a change-notification channel only when an owner
command returns a would-block result. Signaling closes that channel and clears
it; successful operations and packets without registered waiters allocate none.
Registration and signaling stay serialized on the endpoint owner. The existing
Wait and Accept APIs continue to share a broadcast notification.

Same command, five samples after the change:

| Sample | Independent ns/op | B/op | allocs/op | Upstream ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 13668994 | 6625332 | 64742 | 5194117 | 705094 | 19370 |
| 2 | 13745248 | 6554234 | 64812 | 5202733 | 705616 | 19586 |
| 3 | 13782275 | 6526009 | 64856 | 5270706 | 708504 | 19559 |
| 4 | 13775742 | 6592735 | 64888 | 5266092 | 711523 | 19594 |
| 5 | 13598292 | 6528397 | 64809 | 5135804 | 701000 | 19425 |

Against the immediately preceding outbound-reuse measurements, independent
median allocations decrease 3.3% (67,050 to 64,812), and median bytes decrease
5.4% (6,927,176 to 6,554,234). Median time is 13.745 ms versus 13.931 ms before;
this small timing difference is not evidence of a reproducible throughput gain.
The new upstream median is 5.203 ms, 705,616 B/op, 19,559 allocs/op. All previous
scope limitations apply. Benchmarking finished before full race testing began.

## One deadline context per Write

SendStream.Write now holds one deadline context across partial flow-control
commits, instead of constructing and canceling a context for each commit. If a
concurrent deadline change interrupts a blocked operation, retry resumes from
the committed byte offset. Contexts are still canceled when Write returns;
there are no idle per-stream deadline timers retained by this change.

Same command and environment, five samples after the change:

| Sample | Independent ns/op | B/op | allocs/op | Upstream ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 13614412 | 6232737 | 60210 | 5188696 | 711583 | 19586 |
| 2 | 13269296 | 6239698 | 60041 | 5233652 | 712100 | 19670 |
| 3 | 13352079 | 6256072 | 60104 | 5264260 | 716648 | 19581 |
| 4 | 13413040 | 6116764 | 60075 | 5189135 | 702324 | 19434 |
| 5 | 13496612 | 6138990 | 60123 | 5113198 | 697047 | 19276 |

Independent median allocations decreased 7.3% (64,812 to 60,104), bytes decreased
4.9% (6,554,234 to 6,232,737), and time changed from 13.745 to 13.413 ms. No
reliable throughput improvement is claimed from this small timing difference.
Upstream medians were 5.189 ms, 711,583 B/op, and 19,581 allocs/op. The large
performance gap remains. All previous scope limitations apply.

The new independent-peer test changes deadlines after the receiver observes
partial data while its fixed flow-control window prevents full completion.
It covers expiry with a correct partial byte count and resumed writing, and
clearing a deadline while the original write continues, checking the exact
payload and FIN without duplicate bytes for both QUIC versions.
