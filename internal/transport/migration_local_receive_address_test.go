package transport

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestLocalMigrationCandidateRejectsUnknownServerBeforeAuthentication(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, staged := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/staged=%v", version, staged), func(t *testing.T) {
				client, server := testPair(t, version, streamParameters())
				drivePair(t, client, server, false, false)
				old := netip.MustParseAddrPort("192.0.2.1:443")
				next := netip.MustParseAddrPort("192.0.2.1:445")
				remote := netip.MustParseAddrPort("192.0.2.2:444")
				wrong := netip.MustParseAddrPort("192.0.2.3:444")
				if err := client.activateBasePath(old, remote); err != nil {
					t.Fatal(err)
				}
				client.peerParams.DisableActiveMigration = false
				now := time.Unix(101, 0)
				prepare := client.prepareLocalMigration
				if staged {
					prepare = client.prepareLocalPathProbe
				}
				if _, err := prepare(0, next, now); err != nil {
					t.Fatal(err)
				}
				app := client.multipath.paths[0].application
				// Valid ciphertext above every number seen so far: authentication
				// would advance the key-phase and packet-number observations.
				pn := server.spaces[applicationSpace].nextPN + 10
				ping, _ := (wire.PingFrame{}).Append(nil)
				raw := sealPathPayload(t, server.writeKeys(applicationSpace), server.peerCID, 0, pn, ping)
				idle, revision, deadline := client.idle, client.idleRevision, client.IdleDeadline()
				packets, expected, numbers := client.authenticatedPackets, app.space.authenticatedExpected, *client.keyNumbers(0)
				later := now.Add(10 * time.Second)
				for _, local := range []netip.AddrPort{next, old} {
					for replay := 0; replay < 3; replay++ {
						if err := client.handleBoundMultipathPacket(raw, local, wrong, later); err != nil {
							t.Fatal(err)
						}
					}
				}
				if client.idle != idle || client.idleRevision != revision || client.IdleDeadline() != deadline {
					t.Fatal("unknown server address refreshed idle activity")
				}
				if client.authenticatedPackets != packets || app.space.authenticatedExpected != expected || *client.keyNumbers(0) != numbers {
					t.Fatal("unknown server address changed authentication or key-phase state")
				}
				// The same bytes from the real server on the candidate tuple
				// are still authenticated.
				if err := client.handleBoundMultipathPacket(raw, next, remote, later); err != nil {
					t.Fatal(err)
				}
				if client.authenticatedPackets != packets+1 || app.space.authenticatedExpected != pn+1 || client.idle == idle {
					t.Fatal("candidate tuple packet from the server was not authenticated")
				}
			})
		}
	}
}
