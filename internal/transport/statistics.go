package transport

import (
	"sync"
	"time"
)

// ConnectionStats is an immutable snapshot of connection accounting and one
// selected path's RTT. Counters exclude UDP and other outer framing.
type ConnectionStats struct {
	MinRTT, LatestRTT, SmoothedRTT, MeanDeviation time.Duration
	BytesSent, PacketsSent                        uint64
	BytesReceived, PacketsReceived                uint64
	BytesLost, PacketsLost                        uint64
}

type statisticsPublication struct {
	mu    sync.RWMutex
	value ConnectionStats
}

// Statistics is safe concurrently with owner operations, including shutdown.
func (c *Conn) Statistics() ConnectionStats {
	c.statistics.mu.RLock()
	defer c.statistics.mu.RUnlock()
	return c.statistics.value
}

// publishStatistics runs only on the owner, at completed network/timer events.
// Readers never inspect mutable recovery/path state or submit owner commands.
func (c *Conn) publishStatistics() {
	if c.closed {
		return
	} // Retain the snapshot taken before disposal.
	s := ConnectionStats{
		BytesSent: c.sentTotals.bytes, PacketsSent: c.sentTotals.packets,
		BytesReceived: c.receivedTotals.bytes, PacketsReceived: c.receivedTotals.packets,
		BytesLost:   c.loss.declaredBytes - min(c.loss.declaredBytes, c.loss.recoveredBytes),
		PacketsLost: c.loss.declaredPackets - min(c.loss.declaredPackets, c.loss.recoveredPackets),
	}
	r := c.recovery
	if c.multipath != nil {
		if id, ok := c.survivingPath(^uint64(0)); ok {
			r = c.multipath.paths[id].application.sent
		}
	}
	if r != nil && r.RTT.Sampled {
		s.MinRTT, s.LatestRTT = r.RTT.Minimum, r.RTT.Latest
		s.SmoothedRTT, s.MeanDeviation = r.RTT.Smoothed, r.RTT.Variation
	}
	c.statistics.mu.Lock()
	c.statistics.value = s
	c.statistics.mu.Unlock()
}
