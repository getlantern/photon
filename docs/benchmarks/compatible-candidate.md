# Compatible-version invalid Initial processing

2026-09-25, macOS arm64, Apple M4, Go 1.26.2. Same workspace before and
after bounded candidate Initial key caching. No concurrent test workload during
benchmark sampling. Three samples per direction, default benchmark duration.

Command: `go test ./internal/transport -run '^$' -bench '^BenchmarkCompatibleCandidateAuthenticationFailure$' -benchmem -count=3`

| Initial version | Before ns/op | After ns/op | Before B/op | After B/op | Before allocations | After allocations |
| --- | --- | --- | --- | --- | --- | --- |
| v1 | 3403, 3357, 3349 | 305.9, 302.4, 302.6 | 10237 | 96 | 101 | 2 |
| v2 | 3348, 3355, 3361 | 301.8, 302.8, 304.0 | 10237 | 96 | 101 | 2 |

This benchmark repeatedly submits an alternative-version Initial with a corrupt
AEAD tag to one client connection. It measures the warm receive failure path,
including header parsing and authentication-failure accounting; it excludes
connection setup and does not represent throughput or comparison with upstream.
The first candidate still requires key derivation. Median time improves about
11x in this narrow workload. The cache retains one candidate key pair until
selection, Retry, original-version CRYPTO, Initial retirement, or cleanup.

Retry invalidation follows the changed Initial secret input described in
[RFC 9001 section 5.2](https://www.rfc-editor.org/rfc/rfc9001.html#section-5.2).
Tests reject pre-Retry ciphertext after Retry and accept fresh ciphertext using
the replacement DCID, in both version directions. Cache population does not
change active keys, version selection, peer CID, or failure-count accounting.
