package quic

import "github.com/getlantern/photon/internal/recovery"

// CongestionControl selects a connection's congestion controller.
type CongestionControl uint8

const (
	// CongestionControlNewReno is RFC 9002's NewReno, the default. It
	// treats each packet loss as congestion and halves its window.
	CongestionControlNewReno CongestionControl = CongestionControl(recovery.NewRenoAlgorithm)
	// CongestionControlBBR is BBRv3, as in draft-ietf-ccwg-bbr-07: it
	// models the path's bottleneck bandwidth and minimum RTT and paces at
	// its bandwidth estimate, so random loss affects it far less than
	// NewReno. Experimental: the draft is not yet final, and BBR's
	// coexistence with loss-based flows on a shared bottleneck is still
	// studied.
	CongestionControlBBR CongestionControl = CongestionControl(recovery.BBRAlgorithm)
)
