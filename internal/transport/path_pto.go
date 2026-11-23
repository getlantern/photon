package transport

import "time"

// retentionPTO excludes exponential probe backoff, just as base QUIC's key and
// closing retention calculations do. Prepared CID-only paths have no RTT state.
// Retained abandoned paths still count while delayed packets can be processed.
// This does not change per-path loss/PTO deadlines or congestion control.
func (c *Conn) retentionPTO(application bool, maxDelay time.Duration) time.Duration {
	pto := c.recovery.RTT.PTO(application, c.confirmed, maxDelay, 0)
	if c.multipath == nil || !application || !c.confirmed {
		return pto
	}
	var acrossPaths time.Duration
	for _, state := range c.multipath.paths {
		if state.application != nil {
			acrossPaths = max(acrossPaths, state.application.sent.RTT.PTO(true, true, maxDelay, 0))
		}
	}
	if acrossPaths > 0 {
		return acrossPaths
	}
	return pto
}
