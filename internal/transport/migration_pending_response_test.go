package transport

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestLocalMigrationAllowsEstablishedPathResponse(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			client, server := testPair(t, version, streamParameters())
			defer client.Close()
			defer server.Close()
			drivePair(t, client, server, false, false)
			local := netip.MustParseAddrPort("192.0.2.1:443")
			remote := netip.MustParseAddrPort("192.0.2.2:444")
			next := netip.MustParseAddrPort("192.0.2.1:445")
			if err := client.activateBasePath(local, remote); err != nil {
				t.Fatal(err)
			}
			if err := server.activateBasePath(remote, local); err != nil {
				t.Fatal(err)
			}
			client.peerParams.DisableActiveMigration = false
			now := time.Now().Add(time.Second)
			if err := server.validateMultipathPath(0); err != nil {
				t.Fatal(err)
			}
			// Capture the peer challenge before migration begins, but deliver it after
			// preparation. This models independently completing validation in each role.
			var challenge []byte
			for i := 0; i < 100; i++ {
				b, err := server.nextPacketOnPath(0, now)
				if err != nil {
					t.Fatal(err)
				}
				if b == nil {
					now = now.Add(time.Millisecond)
					continue
				}
				auth, err := client.authenticateMultipathPacket(b, now)
				if err != nil || auth == nil {
					t.Fatal("decode", err)
				}
				for _, f := range auth.packet.frames {
					if _, ok := f.(wire.PathChallengeFrame); ok {
						challenge = append([]byte(nil), b...)
					}
				}
				if challenge != nil {
					break
				}
			}
			if challenge == nil {
				t.Fatal("no peer challenge")
			}
			candidate, err := client.prepareLocalMigration(0, next, now)
			if err != nil {
				t.Fatal(err)
			}
			if err := client.handleBoundMultipathPacket(challenge, local, remote, now); err != nil {
				t.Fatal(err)
			}
			app := client.multipath.paths[0].application
			// ACK and ordinary traffic must stay held: they can cancel the peer's
			// migration nomination. Only the probing response may use the old tuple.
			app.space.ackPending = true
			flight, pn := app.sent.BytesInFlight, app.space.nextPN
			app.sent.BytesInFlight = app.congestion.Window()
			if b, err := client.nextPacketOnPath(0, now); err != nil || b != nil || app.space.nextPN != pn {
				t.Fatal("blocked response leaked ACK or packet number", err)
			}
			app.sent.BytesInFlight = flight
			response, err := client.nextPacketOnPath(0, now.Add(time.Millisecond))
			if err != nil || response == nil {
				t.Fatal("migration blocked old-path response", err)
			}
			auth, err := server.authenticateMultipathPacket(response, now)
			if err != nil || auth == nil {
				t.Fatal("response decode", err)
			}
			haveResponse := false
			for _, f := range auth.packet.frames {
				if !wire.ProbingFrame(f) {
					t.Fatalf("non-probing frame escaped hold: %T", f)
				}
				if _, ok := f.(wire.PathResponseFrame); ok {
					haveResponse = true
				}
			}
			if !haveResponse {
				t.Fatal("missing PATH_RESPONSE")
			}
			if err := server.handleBoundMultipathPacket(response, remote, local, now); err != nil {
				t.Fatal(err)
			}
			if server.multipath.paths[0].application.validation.state != PathValidated {
				t.Fatal("peer validation remained blocked")
			}
			if app.localMigration != candidate || *app.network != (pathTuple{local, remote}) || !app.space.ackPending {
				t.Fatal("response changed migration or ACK state")
			}
			if b, err := client.nextPacketOnPath(0, now.Add(2*time.Millisecond)); err != nil || b != nil {
				t.Fatal("ordinary output escaped hold", err)
			}
		})
	}
}
