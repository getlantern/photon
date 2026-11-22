package quic

import "github.com/getlantern/photon/internal/transport"

// ConnectionStats contains a snapshot of connection traffic and RTT estimates.
// Sent totals count committed attempts, including retransmissions and locally
// failed socket writes. Receive totals count routed arrivals, including duplicate
// and unprocessable packets; an unparseable remainder counts as one packet attempt.
// Bytes include QUIC headers/protection, but exclude UDP and outer framing.
// Loss totals decrease for late ACKs while recovery retains their history;
// corrections outside that bounded horizon can be omitted.
// RTT describes the lowest-numbered surviving prepared path (path zero normally),
// and is zero until that path has an RTT sample. Totals aggregate all paths.
type ConnectionStats = transport.ConnectionStats

// ConnectionStats returns the latest completed network/timer-event snapshot.
// It is safe concurrently with connection operations and from application
// callbacks. After connection disposal it retains the final snapshot.
func (c *Conn) ConnectionStats() ConnectionStats {
	if c.raw == nil {
		return ConnectionStats{}
	}
	return c.raw.Statistics()
}
