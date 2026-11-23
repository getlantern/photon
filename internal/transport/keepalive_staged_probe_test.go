package transport

import (
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestKeepAliveWithRetainedStagedProbe(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, peer := testPair(t, v, streamParameters())
		drivePair(t, c, peer, false, false)
		old := netip.MustParseAddrPort("192.0.2.1:443")
		next := netip.MustParseAddrPort("192.0.2.1:445")
		remote := netip.MustParseAddrPort("192.0.2.2:444")
		if err := c.activateBasePath(old, remote); err != nil {
			t.Fatal(err)
		}
		if err := peer.activateBasePath(remote, old); err != nil {
			t.Fatal(err)
		}
		c.peerParams.DisableActiveMigration = false
		c.config.KeepAlivePeriod = time.Second
		now := time.Unix(101, 0)
		candidate, err := c.prepareLocalPathProbe(0, next, now)
		if err != nil {
			t.Fatal(err)
		}
		raw, _, err := c.nextLocalMigrationProbe(0, now)
		if err != nil || raw == nil {
			t.Fatal(err)
		}
		decoded, err := peer.authenticateMultipathPacket(raw, now)
		if err != nil || decoded == nil {
			t.Fatal(err)
		}
		var response []byte
		for _, f := range decoded.packet.frames {
			if challenge, ok := f.(wire.PathChallengeFrame); ok {
				response, _ = (wire.PathResponseFrame{Data: challenge.Data}).Append(nil)
			}
		}
		if response == nil {
			t.Fatal("no challenge")
		}
		proof := sealPathPayload(t, peer.writeKeys(applicationSpace), peer.peerCID, 0, peer.spaces[applicationSpace].nextPN+20, response)
		if err := c.handleBoundMultipathPacket(proof, next, remote, now); err != nil {
			t.Fatal(err)
		}
		app := c.multipath.paths[0].application
		if candidate.validation.state != PathValidated || candidate.committed || app.localMigration != candidate {
			t.Fatal("fixture: validated candidate not retained")
		}
		// The handle stays staged until Switch or Close; the established tuple
		// is the only path able to keep the connection alive meanwhile.
		for round := 0; round < 3; round++ {
			at := c.keepAliveDeadline()
			if at.IsZero() {
				t.Fatal("no keepalive deadline", round)
			}
			if err := c.queueKeepAlive(at); err != nil {
				t.Fatal(err)
			}
			ping := false
			for i := 0; i < 40 && !ping; i++ {
				_, where, pkt, err := c.nextBoundMultipathPacket(at)
				if err != nil {
					t.Fatal(err)
				}
				if pkt == nil {
					at = at.Add(time.Millisecond)
					continue
				}
				got, err := peer.authenticateMultipathPacket(pkt, at)
				if err != nil || got == nil {
					t.Fatal("keepalive authentication", err)
				}
				for _, f := range got.packet.frames {
					if _, ok := f.(wire.PingFrame); ok {
						ping = got.ackEliciting && where.local == old
					}
				}
			}
			if !ping {
				t.Fatal("retained staged probe suppressed keepalive on the established tuple", round)
			}
			// Any authenticated receive re-arms keepalive for the next round.
			reply := sealPathPayload(t, peer.writeKeys(applicationSpace), peer.peerCID, 0, peer.spaces[applicationSpace].nextPN+30+uint64(round), []byte{1})
			if err := c.handleBoundMultipathPacket(reply, old, remote, at); err != nil {
				t.Fatal(err)
			}
		}
	}
}
