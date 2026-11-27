# Hysteria2's congestion control, for comparison

`bbr` and `common` are copied unchanged from Hysteria2 core v2.14.0
(`github.com/apernet/hysteria/core/v2`, `internal/congestion/bbr` and
`internal/congestion/common`), except for the `common` import path, so the
interop benchmarks and pathbench can run Hysteria's BBR. It is an internal
package upstream and can't be imported. It runs over Hysteria's fork of
quic-go (`github.com/apernet/quic-go`), at the version v2.14.0 pins. The
copied code is under Hysteria's MIT license, in `LICENSE.md`.
