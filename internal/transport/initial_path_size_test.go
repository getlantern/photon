package transport

import (
	"github.com/getlantern/photon/internal/recovery"

	"bytes"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestInitialPathSizeAdoption(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, discovery := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/discovery=%t", version, discovery), func(t *testing.T) {
				// Configure the handoff independently of Initial padding coverage.
				c, peer := testPair(t, version)
				drivePair(t, c, peer, false, false)
				c.config.InitialPacketSize = 1452
				c.config.enablePathMTUDiscovery = true
				c.config.DisablePathMTUDiscovery = !discovery
				c.peerParams.MaxUDPPayloadSize = 1400
				p, err := adoptPathZero(c, 3, 3, 4)
				if err != nil {
					t.Fatal(err)
				}
				c.multipath = p
				app := p.paths[0].application
				if app.packetPayloadLimit() != 1400 || app.mtu.fixed == discovery {
					t.Fatal("initial size/peer clamp/discovery policy", app.mtu)
				}
				if _, ok := app.mtu.next(); ok {
					t.Fatal("probe above peer ceiling")
				}
				if _, err := p.application(1); err == nil {
					t.Fatal("fixture unexpectedly has path 1 CIDs")
				}
				if newPathApplication(recovery.NewRenoAlgorithm).packetPayloadLimit() != 1200 {
					t.Fatal("new path inherited original tuple size")
				}
				app.resetMTU()
				if app.packetPayloadLimit() != 1200 {
					t.Fatal("migration/failure reset retained original size")
				}
				if _, ok := app.mtu.next(); ok != discovery {
					t.Fatal("reset lost discovery policy")
				}
				app.mtu.upper = 1200
				app.updateMTURaise(time.Unix(100, 0))
				if app.mtuRaiseDeadline().IsZero() == discovery {
					t.Fatal("fixed mode reopened discovery")
				}
			})
		}
	}
}

func TestConfiguredPathSizeCarriesStreamData(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		c, peer := testPair(t, version, streamParameters())
		drivePair(t, c, peer, false, false)
		for _, conn := range []*Conn{c, peer} {
			conn.config.InitialPacketSize = 1452
			conn.config.DisablePathMTUDiscovery = true
			p, err := adoptPathZero(conn, 3, 3, 4)
			if err != nil {
				t.Fatal(err)
			}
			conn.multipath = p
		}
		id, err := c.OpenStream(false)
		if err != nil {
			t.Fatal(err)
		}
		payload := bytes.Repeat([]byte{42}, 1252)
		n, err := c.WriteStream(id, payload, false)
		if err != nil || n != len(payload) {
			t.Fatal("configured stream admission", n, err)
		}
		now := time.Unix(101, 0)
		large := false
		for i := 0; i < 10; i++ {
			b, err := c.nextPacketOnPath(0, now)
			if err != nil {
				t.Fatal(err)
			}
			if len(b) > 1452 {
				t.Fatal("size exceeded", len(b))
			}
			if len(b) > 1200 {
				large = true
			}
			if len(b) > 0 {
				if err := peer.handleMultipathPacket(b, now); err != nil {
					t.Fatal(err)
				}
			}
			now = now.Add(time.Millisecond)
		}
		if !large {
			t.Fatal("no larger ordinary packet")
		}
		buf := make([]byte, len(payload))
		n, err = peer.ReadStream(id, buf)
		if err != nil || n != len(payload) || !bytes.Equal(buf, payload) {
			t.Fatal("stream delivery", n, err)
		}
		if c.multipath.paths[0].application.mtu.pending {
			t.Fatal("disabled discovery sent probe")
		}
	}
}

func TestFixedInitialSizeWriteFallback(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		e, c := mtuWriteEndpoint(t, version)
		defer e.Close()
		app := c.multipath.paths[1].application
		// Exercise the configured-size state through the endpoint write-failure
		// fixture; primary-path adoption is separately checked above.
		c.config.InitialPacketSize = 1452
		c.config.DisablePathMTUDiscovery = true
		c.seedInitialPathMTU(app)
		id, err := c.OpenStream(false)
		if err != nil {
			t.Fatal(err)
		}
		if n, err := c.WriteStream(id, make([]byte, 1252), true); n != 1252 || err != nil {
			t.Fatal(n, err)
		}
		now := time.Unix(101, 0)
		out, err := e.NextDatagram(now)
		if err != nil || out == nil || out.large == nil || len(out.Data) <= 1200 {
			t.Fatal("missing large stream packet", err)
		}
		item := out.large.item
		e.HandleWriteError(out, syscall.EMSGSIZE, now)
		if c.closed || !item.queued || app.packetPayloadLimit() != 1200 || !app.mtuNeedsValidation {
			t.Fatal("fixed-size fallback failed")
		}
		if _, ok := app.mtu.next(); ok || !app.mtuRaiseDeadline().IsZero() {
			t.Fatal("failure enabled discovery")
		}
		out, err = e.NextDatagram(now.Add(time.Second))
		if err != nil || out == nil || len(out.Data) != 1200 || app.validation.state != PathValidating {
			t.Fatal("missing baseline validation", err)
		}
		if err := c.splitQueuedStream(app.space, 0, portableStreamPayload); err != nil {
			t.Fatal(err)
		}
		if len(app.space.queue) < 2 {
			t.Fatal("large frame not split for fallback")
		}
	}
}
