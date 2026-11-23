package transport

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestLocalMigrationSocketFailure(t *testing.T) {
	for _, scenario := range []string{"candidate", "candidate-queue-full", "old-after-validation"} {
		t.Run(scenario, func(t *testing.T) {
			c, _ := sendingPathPair(t, wire.Version1)
			old := netip.MustParseAddrPort("192.0.2.1:443")
			next := netip.MustParseAddrPort("192.0.2.1:444")
			remote := netip.MustParseAddrPort("192.0.2.2:443")
			if err := c.bindPreparedPath(0, old, remote); err != nil {
				t.Fatal(err)
			}
			e, err := NewClientEndpoint(c, remote)
			if err != nil {
				t.Fatal(err)
			}
			e.networks = map[*Conn]*networkOwner{c: {conn: c, primary: pathTuple{old, remote}, activationChecked: true}}
			e.locals = map[netip.AddrPort]bool{old: true, next: true}
			candidate, err := c.prepareLocalMigration(0, next, time.Unix(101, 0))
			if err != nil {
				t.Fatal(err)
			}
			c.multipath.paths[0].peer.sentMigrationID(candidate, candidate.peerSequence)
			app := c.multipath.paths[0].application
			before := c.queuedBytes
			failed := next
			if scenario == "old-after-validation" {
				candidate.validation.state = PathValidated
				failed = old
			}
			if scenario == "candidate-queue-full" {
				c.queuedBytes = maxSendBytes
			}
			e.failLocalSocket(failed, errors.New("reader failed"))
			if c.closed || len(e.conns) != 1 {
				t.Fatal("socket failure destroyed viable connection")
			}
			if scenario == "old-after-validation" {
				if app.network.local != next || app.localMigration != nil || c.multipath.abandoned[0] != nil {
					t.Fatal("validated replacement lost on old socket failure")
				}
				return
			}
			if app.network.local != old {
				t.Fatal("dead candidate became established")
			}
			if scenario == "candidate-queue-full" {
				if app.localMigration != candidate || candidate.validation.state != PathValidationFailed {
					t.Fatal("blocked cancellation lost ownership")
				}
				raw, _, err := c.nextLocalMigrationProbe(0, time.Unix(102, 0))
				if err != nil || raw != nil {
					t.Fatal("dead candidate generated output", err)
				}
				c.queuedBytes = before
				if err := e.serviceLocalMigrations(c); err != nil {
					t.Fatal(err)
				}
			}
			if app.localMigration != nil {
				t.Fatal("failed candidate retained")
			}
			if _, ok := c.multipath.paths[0].peer.active[candidate.peerSequence]; ok {
				t.Fatal("spent failed candidate CID not retired")
			}
		})
	}
}
