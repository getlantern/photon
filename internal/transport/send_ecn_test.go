package transport

import (
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestEndpointSerializedECNMatchesRecovery(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, peer := testPair(t, v)
		c.recovery.EnableECN()
		endpoint, err := NewClientEndpoint(c, netip.MustParseAddrPort("127.0.0.1:443"))
		if err != nil {
			t.Fatal(err)
		}
		at := time.Unix(100, 0)
		out, err := endpoint.NextDatagram(at)
		if err != nil || out == nil {
			t.Fatal(out, err)
		}
		if out.ECN != recovery.ECT0 || c.recovery.ECNSent(recovery.Initial) != [2]uint64{1, 0} {
			t.Fatal("serialized marking mismatch", out.ECN)
		}
		if err := peer.HandleDatagramECN(out.Data, at, ECN(out.ECN)); err != nil {
			t.Fatal(err)
		}
		frame, err := peer.spaces[initialSpace].received.Ack(at, 3)
		if err != nil || frame.ECN == nil || *frame.ECN != [3]uint64{1, 0, 0} {
			t.Fatal("peer received inconsistent marking", frame, err)
		}
		// A valid data ACK without ECN fails path testing. The next queued packet
		// must have neither marked recovery evidence nor stale output metadata.
		frame.ECN = nil
		if err := c.handleAck(initialSpace, frame, at.Add(time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		if err := c.enqueue(initialSpace, wire.PingFrame{}, 1); err != nil {
			t.Fatal(err)
		}
		out, err = endpoint.NextDatagram(at.Add(time.Second))
		if err != nil || out == nil {
			t.Fatal(out, err)
		}
		if out.ECN != recovery.NotECT || c.recovery.ECNSent(recovery.Initial) != [2]uint64{1, 0} {
			t.Fatal("failed path still marked")
		}
	}
}

func TestMultipathSerializedECNMatchesRecovery(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, _ := sendingPathPair(t, v)
		app := c.multipath.paths[1].application
		app.sent.EnableECN()
		at := time.Unix(101, 0)
		if err := c.validateMultipathPath(1); err != nil {
			t.Fatal(err)
		}
		if data, err := c.nextPacketOnPath(1, at); err != nil || data != nil {
			t.Fatal("sent without credit", err)
		}
		if app.sent.ECNSent(recovery.Application) != [2]uint64{} || c.datagramECN != recovery.NotECT {
			t.Fatal("blocked send consumed marking")
		}
		app.budget.Received(400)
		data, err := c.nextPacketOnPath(1, at)
		if err != nil || len(data) != 1200 {
			t.Fatal("validation probe", len(data), err)
		}
		if c.datagramECN != recovery.ECT0 || app.sent.ECNSent(recovery.Application) != [2]uint64{1, 0} {
			t.Fatal("multipath marking mismatch")
		}
		if c.multipath.paths[0].application.sent.ECNSent(recovery.Application) != [2]uint64{} {
			t.Fatal("wrong path accounting")
		}
	}
}

// Both controllers answer CE marks, so connections with either mark their
// packets ECN-capable where the socket can.
func TestConnectionsMarkECN(t *testing.T) {
	for _, tc := range []struct {
		algorithm recovery.Algorithm
		want      recovery.ECNMark
	}{{recovery.NewRenoAlgorithm, recovery.ECT0}, {recovery.BBRAlgorithm, recovery.ECT0}} {
		c, _ := testPair(t, wire.Version1)
		c.config.enableECN, c.config.CongestionControl = true, tc.algorithm
		endpoint, err := NewClientEndpoint(c, netip.MustParseAddrPort("127.0.0.1:443"))
		if err != nil {
			t.Fatal(err)
		}
		out, err := endpoint.NextDatagram(time.Unix(100, 0))
		if err != nil || out == nil {
			t.Fatal(out, err)
		}
		if out.ECN != tc.want {
			t.Fatalf("algorithm %d marked %v, want %v", tc.algorithm, out.ECN, tc.want)
		}
	}
}
