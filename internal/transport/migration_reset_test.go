package transport

import (
	"errors"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestLocalProbeResetEligibility(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []string{"live", "validated", "unsent", "expired", "failed", "detached", "retired", "wrong-peer", "server", "unchecked", "abandoned"} {
			t.Run(fmt.Sprintf("%s/%s", version, mode), func(t *testing.T) {
				_, c, source, _, route := endpointForeignFixture(t, version)
				now := time.Unix(101, 0)
				candidate, err := c.prepareLocalPathProbe(1, route.local, now)
				if err != nil {
					t.Fatal(err)
				}
				state := c.multipath.paths[1]
				peer := state.peer.active[candidate.peerSequence]
				if !peer.hasToken {
					t.Fatal("missing token")
				}
				reset := resetDatagram(peer.token)
				tuple := candidate.tuple
				if mode != "unsent" {
					raw, _, err := c.nextLocalMigrationProbe(1, now)
					if err != nil || raw == nil {
						t.Fatal("no probe sent", err)
					}
				}
				switch mode {
				case "validated":
					candidate.validation.state = PathValidated
					now = candidate.validation.expires.Add(time.Second)
				case "expired":
					now = candidate.validation.expires
				case "unchecked":
					candidate.validation.state = PathUnchecked
				case "abandoned":
					if _, err := c.multipath.beginAbandon(1); err != nil {
						t.Fatal(err)
					}
				case "failed":
					candidate.validation.state = PathValidationFailed
				case "detached":
					state.application.localMigration = nil
				case "retired":
					seq := candidate.peerSequence + 1
					replacement := state.peer.active[seq]
					if len(replacement.cid) == 0 {
						replacement.cid = []byte("resetcid")
						replacement.token = [16]byte{0x77}
					}
					if _, err := state.peer.add(wire.NewConnectionIDFrame{Sequence: seq, RetirePriorTo: seq, ConnectionID: replacement.cid, StatelessResetToken: replacement.token}); err != nil {
						t.Fatal(err)
					}
				case "wrong-peer":
					tuple.remote = netip.MustParseAddrPort("192.0.2.99:443")
				case "server":
					c.config.Server = true
				}
				route.publishResetHints(c)
				allowed := mode == "live" || mode == "validated"
				if allowed && source.forwardResetRoute(reset, tuple.local, tuple.remote) != route {
					t.Fatal("live probe lacks reset route")
				}
				if !allowed && mode != "expired" && source.forwardResetRoute(reset, tuple.local, tuple.remote) != nil {
					t.Fatal("ineligible probe published hint")
				}
				// Final receipt rechecks expiry even when a previously published hint exists.
				err = c.handleBoundMultipathPacket(reset, tuple.local, tuple.remote, now)
				if allowed {
					if !errors.Is(err, ErrStatelessReset) || !c.Draining() {
						t.Fatal("live probe reset ignored", err)
					}
				} else if c.Draining() || c.closed || errors.Is(err, ErrStatelessReset) {
					t.Fatal("ineligible tuple accepted reset", err)
				}
			})
		}
	}
}
