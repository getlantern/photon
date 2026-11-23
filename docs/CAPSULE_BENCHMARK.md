# Capsule codec benchmark

Measured 2026-09-25 on Apple M4, macOS arm64, Go 1.26.2, upstream quic-go
v0.62.0. The nested interop module pins the comparison dependency.

`BenchmarkCapsuleCodec` processes 64 capsules per operation. Parsing includes
parser creation, both header varints and payload discard. Source bytes are prepared
outside timing. Writing reuses a pre-sized bytes.Buffer; allocations shown include
header scratch escaping through io.Writer. No QUIC encryption, network I/O, HTTP
DATA framing or tunnel cleanup is timed. Reported MB/s is encoded bytes processed
in memory, not network throughput. Both parsers implement bounded payload reads;
existing interoperability tests check their wire compatibility.

Run from `interop` (set GOCACHE as needed for the environment):

```sh
go test -run '^$' -bench '^BenchmarkCapsuleCodec$' -benchmem -benchtime=200ms -count=3
go test -run '^$' -bench '^BenchmarkCapsuleCodec/parse/' -benchmem -benchtime=500ms -count=3
```

## Results

Median ns per 64-capsule parsing batch, three repetitions. Before measurements use
200ms samples; final comparisons use 500ms samples in one run without concurrent
agent-started tests. Host load is not controlled and no significance claim is made.

| Payload bytes | Independent before | Independent after | Upstream final | Final time reduction vs upstream |
| --- | ---: | ---: | ---: | ---: |
| 0 | 2976 | 1629 | 2506 | 35.0% |
| 64 | 3710 | 2231 | 3165 | 29.5% |
| 1200 | 4489 | 2949 | 3893 | 24.2% |

Independent parsing fell from 195 allocations and approximately 2196 bytes per
batch to 2 allocations and 112 bytes. Upstream measured 130 allocations and
approximately 2645 bytes. The changes reuse eight bytes of wire-reader scratch
for header integers and avoid boxing each public payload reader during discard.
The reader remains serialized; source readers must obey io.Reader's buffer
ownership contract. Payload bounds, stale-reader checks and error/abort behavior
remain enforced.

Writing was not optimized. Baseline median times for independent/upstream were
810/869ns (empty), 963/893ns (64 bytes), and 1671/1584ns (1200 bytes), with 64
allocations/1024 bytes per batch for both. Independent writing remains slower for
nonempty payloads in these measurements. These results do not establish broad
QUIC performance superiority or cross-platform gains.

Raw outputs:

- [Before](benchmarks/capsule-before.txt)
- [Initial after run](benchmarks/capsule-after-initial.txt), which overlapped race tests at startup and contains timing outliers
- [Final isolated parsing run](benchmarks/capsule-after-parse.txt)
