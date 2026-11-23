package transport

import "github.com/getlantern/photon/internal/recovery"

// One serialized QUIC packet currently makes up each outbound datagram.
// Coalescing must extend this to roll back every packet's marking together.
type ecnCommit struct {
	sent   *recovery.Sent
	space  recovery.Space
	number uint64
}

// writeWithECNFallback retries only an atomic local rejection of ancillary
// marking. A successful or partially reported send must never be repeated.
func writeWithECNFallback(out *Outbound, write func(recovery.ECNMark) (int, error)) (int, error) {
	n, err := write(out.ECN)
	if err == nil || n != 0 || out.ECN == recovery.NotECT || !unsupportedECNError(err) || out.ecn.sent == nil {
		return n, err
	}
	if !out.ecn.sent.RejectECNMarking(out.ecn.space, out.ecn.number) {
		return n, err
	}
	out.ECN = recovery.NotECT
	return write(recovery.NotECT)
}
