# Independent dynamic QPACK peer

The optional tests use unmodified ls-qpack CLI tools and library pinned to
`91567706c41c0d97ab8dc576873ecd472d7869fa` (2.7.0). This is an external test peer,
not an implementation dependency. Build with Git, a C compiler and CMake:

```sh
./interop/lsqpack/build.sh /tmp/quic-lsqpack-peer-build
cd interop
LSQPACK_BIN=/tmp/quic-lsqpack-peer-build/build/bin go test -race -count=1 -run 'Test(LSQPACK|IndependentEncoderToLSQPACK)' -v ./...
```

The script requires a new destination, checks out the exact revision and does not
install anything. Without LSQPACK_BIN the tests skip; an invalid configured path
fails. The initial build was verified on macOS arm64 with AppleClang and CMake.

Both directions compare 80 ordered field sections, asserting actual dynamic
references. The input changes entries enough to exercise eviction and Required
Insert Count wraparound. Our encoder uses capacity 128 against the peer maximum
256. ls-qpack decoding runs with one-byte reads both in order and with its default
header/update swapping. Its encoder generated 108 insertions and 80 dynamic
sections in the initial run.

The original CLI tests are offline codec tests. The CLI container uses big-endian uint64 stream IDs
and uint32 lengths; record ID zero carries encoder bytes. Stream IDs are fixture
identifiers, not QUIC stream-role tests. Its encoder omits the initial capacity
instruction, so our test decoder explicitly initializes the out-of-band -t
capacity. Our production decoder still starts at zero capacity. Acknowledgments
are modeled promptly by the offline drivers; interactive feedback exchange, malformed inputs, HTTP/3 semantics and live
transport are not proved by those offline tests.

Peer source/API: https://github.com/litespeedtech/ls-qpack

The delayed-update test runs the independent encoder's 80 sections through our
FieldDecoder owner before delivering each section's preceding encoder bytes.
All 80 must block in the pinned fixture. In the normal mode all sections resume
and are acknowledged; in the cancellation mode every third blocked section is
canceled (26 cancellations, 54 acknowledgments). Fields, completion and feedback
identity/uniqueness are checked. Encoder bytes remain in encoder-stream order;
only relative delivery against field sections changes. Feedback is validated
locally, not returned to the offline peer process.

The build also compiles `encoder-peer.c`, a small test-only framing adapter around
the unmodified library's public encoder API. `TestLSQPACKInteractiveFeedback`
returns our decoder's actual feedback to this process and checks every API return
status. It transmits the peer's initial capacity instruction. Eight cases cover
duplication enabled/disabled, normal/canceled sections, and whole/one-byte feedback.
With duplication disabled, all 80 sections block; cancellation mode cancels 27.
Every surviving section must decode correctly and every feedback call must succeed.
This exchanges real codec feedback through process pipes, not live QUIC/HTTP3.

With duplication enabled, the pinned encoder switches to literal sections after
three blocked sections (one cancellation in cancellation mode). The default test
records this exact behavior and checks all 80 results; it does not claim sustained
dynamic compression in this mode. The strict gate below currently fails by
requiring the same dynamic coverage as the no-duplication case:

```sh
LSQPACK_BIN=/tmp/quic-lsqpack-peer-build/build/bin QUIC_INTEROP_KNOWN_FAILURES=1 go test -run 'TestLSQPACKInteractiveFeedback/duplicates=true' -v
```

See [known failures](../KNOWN_FAILURES.md) for the isolated peer accounting mismatch.
`check-feedback.py` reproduces it using only peer encoding and hand-authored
feedback, without our decoder. It exits 1 against the pin. Run from the workspace:

```sh
LSQPACK_BIN=/tmp/quic-lsqpack-peer-build/build/bin python3 interop/lsqpack/check-feedback.py
```

The driver has a read-only diagnostic command exposing three pinned private
counters. `accounting-experiment.patch` is a test artifact for a **separate** peer
checkout; the build script never applies it. Applying it, rebuilding the library
and relinking the driver made the reduced diagnostic and the strict interactive
gate pass. The canonical peer and our production decoder remain unchanged. Always
use `go test -count=1` after rebuilding an external peer because Go's test cache
does not reliably track changes to subprocess binaries.


`TestLSQPACKLive` now carries the independent encoder's output over real QUIC v1
and v2, using upstream quic-go as the server transport and our client transport,
framing, critical-stream dispatch and FieldDecoder. Each connection sends eight
GET requests and receives eight dynamic response sections. Encoder updates are
withheld until each response section is observably blocked; all sections must
resume with the expected fields. Response semantics and FIN are checked. Actual
feedback is read from the QUIC decoder stream and passed unchanged to ls-qpack,
which must accept it before encoding the next response. Ten insertions are
acknowledged in each current fixture.

The peer uses its no-duplication option due to the documented pinned accounting
issue. HTTP3 framing on the server is test-owned, so this is independent live
QPACK plus QUIC coverage, not a complete independent HTTP3 endpoint test. Broader concurrency and public endpoint ownership remain gaps.


The live test also runs cancellation mode on both QUIC versions. It cancels three
of the eight observably blocked sections through FieldStream, calls Cancel twice
and verifies subsequent Decode is rejected. The transport sends STOP_SENDING with
H3_REQUEST_CANCELLED; the upstream peer must observe that exact remote stream
error. Encoder updates still arrive after cancellation, so insert-count feedback
must advance independently of Section Acknowledgment. The real feedback stream
must carry cancellation for canceled IDs and acknowledgment for surviving IDs;
ls-qpack must accept each batch before continuing. Five subsequent/surviving
responses decode correctly on the same connection. This covers explicit owner
cancellation, not concurrent Decode/context-cancellation timing.
