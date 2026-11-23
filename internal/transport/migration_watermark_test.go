package transport

import (
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestBoundMigrationWatermark(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, version)
		local := netip.MustParseAddrPort("192.0.2.1:443")
		remote := netip.MustParseAddrPort("192.0.2.2:444")
		if err := client.bindPreparedPath(1, local, remote); err != nil {
			t.Fatal(err)
		}
		if err := server.bindPreparedPath(1, remote, local); err != nil {
			t.Fatal(err)
		}
		client.multipath.paths[1].application.budget.Validate()
		receiver := server.multipath.paths[1].application.space
		now := time.Unix(101, 0)
		send := func(f wire.Frame) []byte {
			t.Helper()
			if err := client.queueFrameOnPath(1, f); err != nil {
				t.Fatal(err)
			}
			raw, err := client.nextPacketOnPath(1, now)
			now = now.Add(time.Millisecond)
			if err != nil || raw == nil {
				t.Fatal(err)
			}
			return raw
		}
		probing := send(wire.PathResponseFrame{})
		if err := server.handleBoundMultipathPacket(probing, remote, local, now); err != nil {
			t.Fatal(err)
		}
		if receiver.haveNonProbing {
			t.Fatal("probing-only packet advanced migration watermark")
		}
		older := send(wire.PingFrame{})
		newer := send(wire.PingFrame{})
		forged := append([]byte(nil), newer...)
		forged[len(forged)-1] ^= 1
		if err := server.handleBoundMultipathPacket(forged, remote, local, now); err != nil {
			t.Fatal(err)
		}
		if receiver.haveNonProbing {
			t.Fatal("unauthenticated packet advanced watermark")
		}
		for _, raw := range [][]byte{newer, older, newer} {
			if err := server.handleBoundMultipathPacket(raw, remote, local, now); err != nil {
				t.Fatal(err)
			}
			if !receiver.haveNonProbing || receiver.highestNonProbing != 2 {
				t.Fatal("reordered packet changed watermark", receiver.highestNonProbing)
			}
		}
		m, err := receiver.migrationSelection(pathTuple{remote, local})
		if err != nil {
			t.Fatal(err)
		}
		changed := pathTuple{remote, netip.MustParseAddrPort("192.0.2.1:445")}
		candidate, err := m.observeAuthenticated(changed, 1, true, 1200, now, time.Second)
		if err != nil || candidate != nil {
			t.Fatal("seed admitted old candidate", err)
		}
		candidate, err = m.observeAuthenticated(changed, 3, true, 1200, now, time.Second)
		if err != nil || candidate == nil {
			t.Fatal("seed rejected newer candidate", err)
		}
	}
}

func TestMigrationWatermarkSurvivesPathZeroAdoption(t *testing.T) {
	c, peer := testPair(t, wire.Version1, streamParameters())
	drivePair(t, c, peer, false, false)
	s := &c.spaces[applicationSpace]
	if !s.haveNonProbing {
		t.Fatal("legacy authenticated 1-RTT did not record ordering")
	}
	before := s.highestNonProbing
	p, err := adoptPathZero(c, 3, 3, 4)
	if err != nil {
		t.Fatal(err)
	}
	if p.paths[0].application.space != s || p.paths[0].application.space.highestNonProbing != before {
		t.Fatal("handoff lost migration ordering")
	}
}
