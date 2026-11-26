package quic

import "github.com/getlantern/photon/internal/recovery"

// CongestionControl selects a connection's congestion controller.
type CongestionControl uint8

const (
	// CongestionControlBBR is BBRv3, as in draft-ietf-ccwg-bbr-07, and the
	// default. It models the path's bottleneck bandwidth and minimum RTT and
	// paces at its bandwidth estimate, so random loss affects it far less
	// than a loss-based controller. The draft specifies no response to
	// ECN-CE, so a BBR connection doesn't mark its packets ECN-capable.
	CongestionControlBBR CongestionControl = iota
	// CongestionControlNewReno is RFC 9002's NewReno. It treats each packet
	// loss as congestion and halves its window, and it marks packets
	// ECN-capable where the socket supports it.
	CongestionControlNewReno
)

// algorithm returns the internal controller c selects, or false if c is
// unknown.
func (c CongestionControl) algorithm() (recovery.Algorithm, bool) {
	switch c {
	case CongestionControlBBR:
		return recovery.BBRAlgorithm, true
	case CongestionControlNewReno:
		return recovery.NewRenoAlgorithm, true
	}
	return 0, false
}

// congestionAlgorithm is the internal controller of a config that
// prepareConfig validated.
func (cfg *Config) congestionAlgorithm() recovery.Algorithm {
	a, ok := cfg.CongestionControl.algorithm()
	if !ok {
		panic("quic: unvalidated congestion control")
	}
	return a
}
