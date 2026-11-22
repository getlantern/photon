# Automatic discovery: short-transfer comparison

Measured on 2026-09-20, Apple M4, darwin/arm64, Go 1.26.2.
Both peers use this implementation on concrete IPv4 loopback sockets. Each
iteration completes a public-API 256 KiB unidirectional stream, checking exact
payload and FIN. Handshake/setup is outside the timed region. Connections are
fresh per benchmark sample; discovery is not awaited or sampled for convergence.
These are short post-handshake transfers, not a steady-state MTU comparison or
an upstream quic-go comparison. Race instrumentation is off for measurements.

Reproduce:

```
go test -run '^$' -bench '^BenchmarkPublicMTUTransfer$' -benchtime=10x -count=5 -benchmem .
```

Median of five samples, ten iterations each:

| Version | Discovery | ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| v1 | automatic | 3074433 | 3659784 | 16945 |
| v1 | disabled | 2860362 | 2011324 | 16302 |
| v2 | automatic | 3011654 | 3706012 | 16930 |
| v2 | disabled | 2865779 | 2025512 | 16332 |

Automatic discovery increased median elapsed time about 7.5% for v1 and 5.1% for
v2. Allocated bytes increased about 82.0% and 83.0%, respectively. Allocation
counts increased about 3.9% and 3.7%. This is a regression for this workload.
Small samples, fixed case order, shared-host activity and discovery timing limit
inference; no statistical significance or universal performance claim is made.
Raw samples: [mtu-startup-transfer.txt](mtu-startup-transfer.txt).

A separate 100-iteration v1 automatic run collected an allocation profile:

```
go test -run '^$' -bench '^BenchmarkPublicMTUTransfer/v1/disabled=false$' -benchtime=100x -count=1 -memprofile=mtu-startup-memory.pprof .
go tool pprof -top -alloc_space quic-go.test mtu-startup-memory.pprof
```

The profile attributes 84.22 MiB (24.54% flat, 26.14% cumulative) of 343.25 MiB
sampled allocations to reserveStreamItem. This identifies eager split-accounting
allocation as a concrete optimization target; it does not establish that it
explains the entire regression. Preserve reserved fallback capacity and late-ACK
semantics when optimizing it.

Artifacts: [profile run](mtu-startup-profile-run.txt),
[allocation summary](mtu-startup-allocations.txt),
[profile](mtu-startup-memory.pprof).

All four benchmark cases also pass a one-iteration race-instrumented smoke run;
those timings are excluded from the comparison. Vet passes. Production code was
unchanged for this measurement.

## Deferred split metadata follow-up

On the same host/toolchain, the implementation now charges the same fallback
reservation at write admission but delays the coverage ledger, branch map and
fragment table until an actual split. Unsplit ACK/cancellation releases the full
logical charge. The command and sample sizes above were repeated after race and
interop checks completed, with no concurrent checks launched by this agent.

| Version | Discovery | ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| v1 | automatic, deferred | 2853283 | 2456852 | 15454 |
| v1 | disabled, follow-up | 2897454 | 2121212 | 16338 |
| v2 | automatic, deferred | 2757829 | 2415556 | 15472 |
| v2 | disabled, follow-up | 2896104 | 2110813 | 16319 |

Compared with the earlier automatic-mode medians, allocated bytes fell about
32.9% (v1) and 34.8% (v2); allocation counts fell 8.8% and 8.6%. Elapsed medians
fell about 7.2% and 8.4%. The disabled control changed too, so timing differences
remain preliminary. Automatic mode still allocates about 15.8%/14.4% more bytes
than its disabled follow-up control; this does not eliminate all MTU-related
overhead or demonstrate universal improvement.

Raw follow-up: [mtu-startup-lazy-transfer.txt](mtu-startup-lazy-transfer.txt).
Full root race tests, vet and default upstream interop race tests pass. Existing
full-buffer splitting, maximum-payload fallback, late original ACK, cancellation,
MTU-rejection and live-relay shrink tests remain part of validation. A new test
checks that unsplit reserved writes allocate no ledger/map/table and refund their
charge on ACK or cancellation.

## Scratch buffer sized to the default discovery ceiling

A refreshed lazy-metadata profile attributed 32.56 MiB (13.72% flat) to
StreamFrame.Append. The reusable payload buffer was still 1200 bytes, so larger
STREAM frames allocated encoding storage. It now shares the 1452-byte default
probe ceiling constant. This adds 252 bytes of inline scratch per connection;
larger internally configured MTUs can still outgrow it. Close still clears it.

Same command, five ten-iteration samples on the same host/toolchain:

| Version | Discovery | ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| v1 | automatic, sized scratch | 2758208 | 1950069 | 15240 |
| v1 | disabled, control | 2937233 | 2036509 | 16341 |
| v2 | automatic, sized scratch | 2759888 | 1989736 | 15208 |
| v2 | disabled, control | 3142079 | 2089281 | 16476 |

Automatic-mode median allocated bytes fell another 20.6%/17.6% versus the lazy
metadata samples. V1 median elapsed time fell about 3.3%; v2 was essentially
unchanged. These remain sequential small-sample observations with noisy controls.
The benchmark excludes setup and does not measure the increased per-connection
resident footprint. No universal improvement or upstream advantage is established.

A separate 100-iteration profile no longer lists StreamFrame.Append above the
default reporting threshold; this is not proof of zero allocation in all cases.
Sampled total allocation was 203.15 MiB versus 237.25 MiB in the refreshed lazy
profile. Root race tests, vet and default upstream interoperability race tests pass.

Artifacts: [samples](mtu-startup-sized-scratch.txt),
[lazy profile summary](mtu-lazy-allocations.txt),
[sized-scratch profile summary](mtu-scratch-allocations.txt),
[profile run](mtu-scratch-profile-run.txt),
[profile](mtu-scratch-memory.pprof).
