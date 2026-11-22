package transport

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestPathMTUEncryptedExchange(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			client, server := sendingPathPair(t, version)
			for _, c := range []*Conn{client, server} {
				a := c.multipath.paths[1].application
				a.validation.state = PathValidated
				a.budget.Validate()
			}
			if err := client.startPathMTUSearch(1, 1500); err != nil {
				t.Fatal(err)
			}
			app := client.multipath.paths[1].application
			now := time.Unix(101, 0)
			for app.mtu.confirmed < 1500 {
				size, ok := app.mtu.next()
				if !ok {
					t.Fatal("search stalled")
				}
				packet, err := client.nextPacketOnPath(1, now)
				if err != nil || len(packet) != size {
					t.Fatalf("probe size %d want %d: %v", len(packet), size, err)
				}
				if !app.mtu.pending || app.sent.BytesInFlight != uint64(size) || len(app.space.sent) != 0 {
					t.Fatal("probe accounting")
				}
				if err := server.handleMultipathPacket(packet, now); err != nil {
					t.Fatal(err)
				}
				now = now.Add(time.Second)
				ack, err := server.nextPacketOnPath(1, now)
				if err != nil || ack == nil {
					t.Fatal("missing encrypted ACK", err)
				}
				if err := client.handleMultipathPacket(ack, now); err != nil {
					t.Fatal(err)
				}
				if app.mtu.confirmed != size || app.mtu.pending || app.sent.BytesInFlight != 0 {
					t.Fatal("ACK did not confirm size/release flight")
				}
				now = now.Add(time.Second)
			}
			if packet, err := client.nextPacketOnPath(1, now); err != nil || packet != nil {
				t.Fatal("probe after ceiling", err)
			}
		})
	}
}

func TestPathMTUSendGates(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version1)
	app := client.multipath.paths[1].application
	if err := client.startPathMTUSearch(1, 1500); err == nil {
		t.Fatal("unvalidated search")
	}
	app.validation.state = PathValidated
	client.peerParams.MaxUDPPayloadSize = 1400
	if err := client.startPathMTUSearch(1, 1500); err == nil {
		t.Fatal("peer ceiling bypass")
	}
	if err := client.startPathMTUSearch(1, 1400); err != nil {
		t.Fatal(err)
	}
	if err := client.startPathMTUSearch(1, 1300); err == nil {
		t.Fatal("search replaced")
	}
	now := time.Unix(101, 0)
	blocked := func() {
		t.Helper()
		packet, err := client.nextPacketOnPath(1, now)
		if err != nil || packet != nil || app.space.nextPN != 0 || app.mtu.pending || app.mtu.attempts != 0 {
			t.Fatal("blocked probe committed state", err)
		}
	}
	blocked() // No amplification credit.
	app.budget.Validate()
	app.sent.BytesInFlight = app.congestion.Window()
	blocked()
	app.sent.BytesInFlight = 0
	app.pacer.Delay(now, app.congestion.Window(), app.sent.RTT.Smoothed, 1200)
	app.pacer.Sent(12000)
	blocked()
	now = now.Add(time.Second)
	packet, err := client.nextPacketOnPath(1, now)
	if err != nil || len(packet) != 1300 || !app.mtu.pending {
		t.Fatal("unblocked probe", err)
	}
}

func TestPathMTULossAndLateACK(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version2)
	app := client.multipath.paths[1].application
	app.validation.state = PathValidated
	app.budget.Validate()
	if err := client.startPathMTUSearch(1, 1500); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(101, 0)
	if b, err := client.nextPacketOnPath(1, now); err != nil || len(b) != 1350 {
		t.Fatal(err)
	}
	// Three later ordinary packets permit packet-threshold loss of probe zero.
	for i := 0; i < 3; i++ {
		now = now.Add(time.Millisecond)
		if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
		if b, err := client.nextPacketOnPath(1, now); err != nil || b == nil {
			t.Fatal(err)
		}
	}
	window := app.congestion.Window()
	ack := func(n uint64) {
		t.Helper()
		smallest := n
		if n == 3 {
			smallest = 1
		}
		f := wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: smallest, Largest: n}}}}
		if _, err := client.multipath.receiveACK(f, now, 0, 0, false); err != nil {
			t.Fatal(err)
		}
	}
	ack(3)
	if app.mtu.pending || app.mtu.confirmed != 1200 || app.mtu.attempts != 1 {
		t.Fatal("loss feedback missing")
	}
	if app.congestion.Window() < window {
		t.Fatal("MTU loss cut congestion window")
	}
	now = now.Add(time.Second)
	if b, err := client.nextPacketOnPath(1, now); err != nil || len(b) != 1350 {
		t.Fatal("retry", err)
	}
	if app.mtu.attempts != 2 {
		t.Fatal("retry ledger")
	}
	ack(0) // The original lost probe has already left recovery's packet map.
	if app.mtu.confirmed != 1350 || app.mtu.pending {
		t.Fatal("late ACK did not confirm size")
	}
}

func TestPathMTUSchedulerPrioritizesSharedData(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, backup := range []bool{false, true} {
			c, _ := sendingPathPair(t, version)
			app := c.multipath.paths[1].application
			app.validation.state = PathValidated
			app.budget.Validate()
			c.multipath.paths[1].backup = backup
			if err := c.startPathMTUSearch(1, 1500); err != nil {
				t.Fatal(err)
			}
			if err := c.queueFrameOnPath(0, wire.StreamFrame{StreamID: 0, Data: []byte("data before discovery")}); err != nil {
				t.Fatal(err)
			}
			c.budget = Amplification{}
			now := time.Unix(101, 0)
			id, b, err := c.nextMultipathPacket(now)
			if err != nil || id != 1 || b == nil || len(b) > 1200 || app.mtu.pending || app.mtu.attempts != 0 {
				t.Fatal("probe displaced shared application data", id, len(b), err)
			}
			item := app.space.sent[0]
			if item == nil {
				t.Fatal("shared data was not transmitted")
			}
			if _, ok := item.frame.(wire.StreamFrame); !ok {
				t.Fatal("wrong output priority")
			}
			id, b, err = c.nextMultipathPacket(now.Add(time.Second))
			if err != nil || id != 1 || len(b) != 1350 || !app.mtu.pending {
				t.Fatal("idle path discovery starved", id, len(b), err)
			}
		}
	}
}

func TestPathMTURebindingInvalidatesEvidence(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		c, _ := sendingPathPair(t, version)
		local := netip.MustParseAddrPort("192.0.2.1:443")
		remote := netip.MustParseAddrPort("192.0.2.2:444")
		changed := netip.MustParseAddrPort("192.0.2.2:445")
		if err := c.bindPreparedPath(1, local, remote); err != nil {
			t.Fatal(err)
		}
		app := c.multipath.paths[1].application
		app.validation.state = PathValidated
		app.budget.Validate()
		if err := c.startPathMTUSearch(1, 1500); err != nil {
			t.Fatal(err)
		}
		now := time.Unix(101, 0)
		send := func(size int) uint64 {
			t.Helper()
			pn := app.space.nextPN
			b, err := c.nextPacketOnPath(1, now)
			if err != nil || len(b) != size {
				t.Fatal("probe", len(b), size, err)
			}
			now = now.Add(time.Second)
			return pn
		}
		ack := func(pn uint64) {
			t.Helper()
			f := wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: pn, Largest: pn}}}}
			if _, err := c.multipath.receiveACK(f, now, 0, 0, false); err != nil {
				t.Fatal(err)
			}
		}
		first := send(1350)
		ack(first)
		old := send(1425)
		app.migration, _ = app.space.migrationSelection(*app.network)
		candidate, err := app.migration.observeAuthenticated(pathTuple{local, changed}, 0, true, 1200, now, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		candidate.validation.state = PathValidated
		candidate.budget.Validate()
		if committed, err := c.commitPeerRebinding(1, candidate); err != nil || !committed {
			t.Fatal("commit", err)
		}
		if app.mtu.confirmed != 1200 || app.mtu.pending || app.mtu.lastNumber != old {
			t.Fatal("rebinding retained MTU evidence or reset PN high-water")
		}
		fresh := send(1350)
		if fresh <= old {
			t.Fatal("reused packet number")
		}
		ack(old)
		if app.mtu.confirmed != 1200 || !app.mtu.pending {
			t.Fatal("old-tuple ACK confirmed new path")
		}
		ack(fresh)
		if app.mtu.confirmed != 1350 || app.mtu.pending {
			t.Fatal("new-tuple ACK failed")
		}
	}
}
