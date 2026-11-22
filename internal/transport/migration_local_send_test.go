package transport

import (
	"bytes"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestLocalMigrationProbeOutputAndExpiry(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, base := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/base=%v", version, base), func(t *testing.T) {
				client, server := testPair(t, version, streamParameters())
				drivePair(t, client, server, false, false)
				local := netip.MustParseAddrPort("192.0.2.1:443")
				next := netip.MustParseAddrPort("192.0.2.1:445")
				remote := netip.MustParseAddrPort("192.0.2.2:444")
				if err := client.activateBasePath(local, remote); err != nil {
					t.Fatal(err)
				}
				if err := server.activateBasePath(remote, local); err != nil {
					t.Fatal(err)
				}
				client.multipath.baseOnly, server.multipath.baseOnly = base, base
				client.peerParams.DisableActiveMigration = false
				server.config.allowCandidateMigration = true
				server.config.allowFreshCIDMigration = true
				now := time.Unix(101, 0)
				candidate, err := client.prepareLocalMigration(0, next, now)
				if err != nil {
					t.Fatal(err)
				}
				app := client.multipath.paths[0].application
				peers := client.multipath.paths[0].peer
				pn, seals := app.space.nextPN, client.sealed[applicationSpace]
				oldFlight := app.sent.BytesInFlight
				app.sent.BytesInFlight = app.congestion.Window()
				if packet, _, err := client.nextLocalMigrationProbe(0, now); err != nil || packet != nil {
					t.Fatal("congestion-blocked probe sent", err)
				}
				if app.space.nextPN != pn || client.sealed[applicationSpace] != seals || peers.active[candidate.peerSequence].used || len(candidate.validation.probes) != 0 {
					t.Fatal("blocked probe consumed state")
				}
				app.sent.BytesInFlight = oldFlight
				id, tuple, packet, err := client.nextBoundMultipathPacket(now)
				if err != nil || id != 0 || tuple != (pathTuple{next, remote}) || len(packet) != 1200 {
					t.Fatal("incorrect candidate output", id, tuple, len(packet), err)
				}
				reserved := peers.active[candidate.peerSequence]
				if !reserved.used || !bytes.Equal(packet[1:1+len(reserved.cid)], reserved.cid) || app.space.nextPN != pn+1 || client.sealed[applicationSpace] != seals+1 {
					t.Fatal("probe did not commit shared counters and reserved CID")
				}
				if app.network.local != local {
					t.Fatal("probe switched established binding")
				}
				if deadline := client.multipathPathsDeadline(); deadline.IsZero() || deadline.After(candidate.validation.expires) {
					t.Fatal("candidate missing retry/expiry timer")
				}
				if err := server.handleBoundMultipathPacket(packet, remote, next, now); err != nil {
					t.Fatal(err)
				}
				remoteCandidate := server.multipath.paths[0].application.migration
				if remoteCandidate == nil || remoteCandidate.candidate == nil {
					t.Fatal("encrypted local probe did not nominate server-side candidate")
				}
				// Expiry under queue pressure retains ownership for retry without leaving
				// an already expired deadline that would spin the runtime timer.
				beforeBytes := client.queuedBytes
				client.queuedBytes = maxSendBytes
				if _, _, err := client.nextLocalMigrationProbe(0, candidate.validation.expires); err != nil {
					t.Fatal(err)
				}
				if app.localMigration != candidate || !client.multipathPathsDeadline().IsZero() {
					t.Fatal("blocked expiry lost state or retained expired deadline")
				}
				client.queuedBytes = beforeBytes
				if _, _, err := client.nextLocalMigrationProbe(0, candidate.validation.expires); err != nil {
					t.Fatal(err)
				}
				if app.localMigration != nil || app.network.local != local || client.queuedBytes != beforeBytes+32 {
					t.Fatal("expiry did not retire CID on established path")
				}
			})
		}
	}
}
