package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestStatisticsPathSelectionAndFinalSnapshot(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		c, _ := sendingPathPair(t, version)
		zero := c.multipath.paths[0].application.sent
		one := c.multipath.paths[1].application.sent
		zero.RTT = recovery.RTT{Sampled: true, Minimum: 10 * time.Millisecond, Latest: 12 * time.Millisecond, Smoothed: 11 * time.Millisecond, Variation: time.Millisecond}
		one.RTT = recovery.RTT{Sampled: true, Minimum: 20 * time.Millisecond, Latest: 24 * time.Millisecond, Smoothed: 22 * time.Millisecond, Variation: 2 * time.Millisecond}
		c.sentTotals = sendAccounting{packets: 7, bytes: 700}
		c.receivedTotals = receiveAccounting{packets: 8, bytes: 800}
		c.loss = lossAccounting{declaredPackets: 3, declaredBytes: 300, recoveredPackets: 1, recoveredBytes: 100}
		c.publishStatistics()
		want := ConnectionStats{MinRTT: 10 * time.Millisecond, LatestRTT: 12 * time.Millisecond, SmoothedRTT: 11 * time.Millisecond, MeanDeviation: time.Millisecond, PacketsSent: 7, BytesSent: 700, PacketsReceived: 8, BytesReceived: 800, PacketsLost: 2, BytesLost: 200}
		if got := c.Statistics(); got != want {
			t.Fatal(got, want)
		}
		if err := c.abandonFailedPath(0); err != nil {
			t.Fatal(err)
		}
		c.publishStatistics()
		want.MinRTT, want.LatestRTT, want.SmoothedRTT, want.MeanDeviation = 20*time.Millisecond, 24*time.Millisecond, 22*time.Millisecond, 2*time.Millisecond
		if got := c.Statistics(); got != want {
			t.Fatal("retired path selected", got, want)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		c.publishStatistics()
		if got := c.Statistics(); got != want {
			t.Fatal("path disposal lost final RTT", got, want)
		}
	}
}

func TestStatisticsUnsampledRTT(t *testing.T) {
	c, _ := testPair(t, wire.Version1)
	c.publishStatistics()
	if got := c.Statistics(); got != (ConnectionStats{}) {
		t.Fatal("initial estimator exposed as observation", got)
	}
}
