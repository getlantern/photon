package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func sendingPathPair(t *testing.T, v wire.Version, parameters ...wire.TransportParameters) (*Conn, *Conn) {
	t.Helper()
	p := streamParameters()
	if len(parameters) > 0 {
		p = parameters[0]
	}
	client, server := testPair(t, v, p)
	drivePair(t, client, server, false, false)
	// This synthetic multipath owner represents a server allowing active paths.
	client.peerParams.DisableActiveMigration = false
	for _, c := range []*Conn{client, server} {
		p, err := adoptPathZero(c, 3, 3, 4)
		if err != nil {
			t.Fatal(err)
		}
		c.multipath = p
		if _, err = p.issue(1); err != nil {
			t.Fatal(err)
		}
		if _, err = p.application(1); err != nil {
			t.Fatal(err)
		}
	}
	for _, pair := range [][2]*Conn{{client, server}, {server, client}} {
		for seq, id := range pair[0].multipath.paths[1].local.active {
			f := wire.PathNewConnectionIDFrame{PathID: 1, NewConnectionIDFrame: wire.NewConnectionIDFrame{Sequence: seq, ConnectionID: id.cid, StatelessResetToken: id.token}}
			if _, err := pair[1].multipath.receiveNew(f); err != nil {
				t.Fatal(err)
			}
			pair[0].multipath.markSent(1, seq)
		}
	}
	return client, server
}

func TestMultipathValidationPacketExchange(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, v)
		// Client is not constrained by server anti-amplification. Server starts
		// with credit from an authenticated initial new-path datagram.
		client.multipath.paths[1].application.budget.Validate()
		server.multipath.paths[1].application.budget.Received(400)
		for _, c := range []*Conn{client, server} {
			if err := c.validateMultipathPath(1); err != nil {
				t.Fatal(err)
			}
		}
		now := time.Unix(101, 0)
		for round := 0; round < 12; round++ {
			for _, pair := range [][2]*Conn{{client, server}, {server, client}} {
				b, err := pair[0].nextPacketOnPath(1, now)
				if err != nil {
					t.Fatal(err)
				}
				if b == nil {
					continue
				}
				if err := pair[1].multipath.paths[1].application.budget.Received(len(b)); err != nil {
					t.Fatal(err)
				}
				if err := pair[1].handleMultipathPacket(b, now); err != nil {
					t.Fatal(err)
				}
			}
			now = now.Add(time.Millisecond)
		}
		for _, c := range []*Conn{client, server} {
			app := c.multipath.paths[1].application
			if app.validation.state != PathValidated || !app.budget.Validated() {
				t.Fatal("real packet exchange did not validate path")
			}
			if app.space.nextPN == 0 || app.space.received.Expected() == 0 {
				t.Fatal("missing per-path packet state")
			}
			if len(app.space.sent) != 0 {
				t.Fatal("unreliable validation frames retained for retransmission")
			}
		}
	}
}

func TestMultipathSendCreditAndCIDCommit(t *testing.T) {
	client, server := sendingPathPair(t, wire.Version1)
	app := client.multipath.paths[1].application
	now := time.Unix(101, 0)
	if err := client.validateMultipathPath(1); err != nil {
		t.Fatal(err)
	}
	queued, sealed := client.queuedBytes, client.sealed[applicationSpace]
	if b, err := client.nextPacketOnPath(1, now); err != nil || b != nil {
		t.Fatal("sent without credit", err)
	}
	if app.space.nextPN != 0 || len(app.validation.probes) != 0 || client.queuedBytes != queued || client.sealed[applicationSpace] != sealed {
		t.Fatal("blocked send committed state")
	}
	app.budget.Received(400)
	b, err := client.nextPacketOnPath(1, now)
	if err != nil || len(b) != 1200 {
		t.Fatal("expanded probe", len(b), err)
	}
	if app.space.nextPN != 1 || len(app.validation.probes) != 1 || !app.validation.probes[0].expanded || app.budget.credit != 0 || client.sealed[applicationSpace] != sealed+1 {
		t.Fatal("send accounting")
	}
	h, err := wire.ParseHeader(b, len(server.config.LocalCID))
	if err != nil {
		t.Fatal(err)
	}
	_, _, pn, err := server.readKeys(applicationSpace).OpenPath(b, h.PNOffset, 0, 1)
	if err != nil || pn != 0 {
		t.Fatal("path nonce/packet number", err)
	}
	app.budget.Validate()
	frames, err := client.multipath.issue(2)
	if err != nil {
		t.Fatal(err)
	}
	f := frames[0]
	if err := client.queueFrameOnPath(1, f); err != nil {
		t.Fatal(err)
	}
	if _, ok := client.multipath.lookupLocal(f.ConnectionID); ok {
		t.Fatal("unsent CID routable")
	}
	if b, err = client.nextPacketOnPath(1, now.Add(time.Millisecond)); err != nil || b == nil {
		t.Fatal(err)
	}
	if path, ok := client.multipath.lookupLocal(f.ConnectionID); !ok || path != 2 {
		t.Fatal("send did not commit advertised CID")
	}
	if app.space.sent[1] == nil {
		t.Fatal("reliable CID frame not tracked")
	}
}

func TestMultipathSenderCongestionAndAbandonment(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version2)
	app := client.multipath.paths[1].application
	app.budget.Validate()
	now := time.Unix(101, 0)
	if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	app.sent.BytesInFlight = app.congestion.Window()
	if b, err := client.nextPacketOnPath(1, now); err != nil || b != nil || app.space.nextPN != 0 {
		t.Fatal("congestion bypass", err)
	}
	app.sent.BytesInFlight = 0
	app.pacer.Delay(now, app.congestion.Window(), app.sent.RTT.Smoothed, 1200)
	app.pacer.Sent(12000)
	if b, err := client.nextPacketOnPath(1, now); err != nil || b != nil || app.pacingAt.IsZero() || app.space.nextPN != 0 {
		t.Fatal("pacing bypass", err)
	}
	client.multipath.beginAbandon(1)
	if b, err := client.nextPacketOnPath(1, now.Add(time.Second)); err == nil || b != nil {
		t.Fatal("abandoned path sent")
	}
}

func TestMultipathValidationPrecedesData(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version1)
	app := client.multipath.paths[1].application
	app.budget.Validate()
	now := time.Unix(101, 0)
	if err := client.queueFrameOnPath(1, wire.StreamFrame{StreamID: 0, Data: []byte("wait for validation")}); err != nil {
		t.Fatal(err)
	}
	if b, err := client.nextPacketOnPath(1, now); err != nil || b != nil {
		t.Fatal("application data preceded validation", err)
	}
	if err := client.validateMultipathPath(1); err != nil {
		t.Fatal(err)
	}
	if _, ok := app.space.queue[0].frame.(wire.PathChallengeFrame); !ok {
		t.Fatal("data blocked validation probe")
	}
	if b, err := client.nextPacketOnPath(1, now); err != nil || len(b) != 1200 {
		t.Fatal("probe not sent ahead of data", err)
	}
	if len(app.space.queue) != 1 {
		t.Fatal("blocked data lost")
	}
}
