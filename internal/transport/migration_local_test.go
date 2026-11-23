package transport

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestLocalMigrationAdmissionAndCancellation(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		c, _ := sendingPathPair(t, version)
		local := netip.MustParseAddrPort("192.0.2.1:443")
		remote := netip.MustParseAddrPort("192.0.2.2:444")
		next := netip.MustParseAddrPort("192.0.2.1:445")
		if err := c.bindPreparedPath(0, local, remote); err != nil {
			t.Fatal(err)
		}
		app := c.multipath.paths[0].application
		peers := c.multipath.paths[0].peer
		now := time.Unix(101, 0)
		beforeBytes, pn, current := c.queuedBytes, app.space.nextPN, peers.current
		candidate, err := c.prepareLocalMigration(0, next, now)
		if err != nil {
			t.Fatal(err)
		}
		if !candidate.budget.Validated() || candidate.validation.state != PathValidating || !candidate.hasPeerSequence || candidate.peerSequence == current || app.network.local != local || app.space.nextPN != pn || c.queuedBytes != beforeBytes || peers.current != current {
			t.Fatal("preparation changed established state")
		}
		if _, err := c.prepareLocalMigration(0, next, now); err == nil {
			t.Fatal("parallel local migration admitted")
		}
		if err := c.commitLocalMigration(0, candidate); !errors.Is(err, errMigrationState) {
			t.Fatal("unvalidated commit accepted", err)
		}
		candidate.validation.state = PathValidated
		if err := c.commitLocalMigration(0, candidate); !errors.Is(err, errMigrationState) {
			t.Fatal("unsent CID commit accepted", err)
		}
		reserved := peers.active[candidate.peerSequence]
		delete(peers.active, candidate.peerSequence)
		if err := c.commitLocalMigration(0, candidate); !errors.Is(err, errMigrationState) {
			t.Fatal("revoked CID commit accepted", err)
		}
		peers.active[candidate.peerSequence] = reserved
		candidate.validation.state = PathValidating
		if app.network.local != local || app.localMigration != candidate || peers.current != current || c.queuedBytes != beforeBytes || app.space.nextPN != pn {
			t.Fatal("rejected commit mutated established state")
		}
		if err := c.retireMigrationReservations(0, nil); err != nil {
			t.Fatal(err)
		}
		if _, ok := peers.migrationID(candidate, candidate.peerSequence); !ok {
			t.Fatal("generic cleanup retired live local reservation")
		}
		if err := c.cancelLocalMigration(0, candidate); err != nil {
			t.Fatal(err)
		}
		if app.localMigration != nil || peers.active[candidate.peerSequence].reservation != nil || c.queuedBytes != beforeBytes {
			t.Fatal("unsent cancellation consumed state")
		}
		candidate, err = c.prepareLocalMigration(0, next, now)
		if err != nil {
			t.Fatal(err)
		}
		peers.sentMigrationID(candidate, candidate.peerSequence)
		c.queuedBytes = maxSendBytes
		if err := c.cancelLocalMigration(0, candidate); !errors.Is(err, ErrSendBuffer) || app.localMigration != candidate {
			t.Fatal("blocked cancellation lost reservation", err)
		}
		c.queuedBytes = beforeBytes
		if err := c.cancelLocalMigration(0, candidate); err != nil {
			t.Fatal(err)
		}
		if app.localMigration != nil || c.queuedBytes != beforeBytes+32 {
			t.Fatal("spent cancellation did not queue retirement")
		}
		if _, ok := peers.active[candidate.peerSequence]; ok {
			t.Fatal("spent CID still active")
		}
	}
}

func TestLocalMigrationPolicyRejectionIsAtomic(t *testing.T) {
	for _, scenario := range []string{"server", "unconfirmed", "disabled", "same-local", "wrong-family", "invalid-address", "zero-time", "no-spare", "unvalidated"} {
		t.Run(scenario, func(t *testing.T) {
			client, server := sendingPathPair(t, wire.Version1)
			c := client
			if scenario == "server" {
				c = server
			}
			local := netip.MustParseAddrPort("192.0.2.1:443")
			remote := netip.MustParseAddrPort("192.0.2.2:444")
			next := netip.MustParseAddrPort("192.0.2.1:445")
			now := time.Unix(101, 0)
			id := uint64(0)
			if scenario == "unvalidated" {
				id = 1
			}
			if err := c.bindPreparedPath(id, local, remote); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "unconfirmed":
				c.confirmed = false
			case "disabled":
				c.peerParams.DisableActiveMigration = true
			case "same-local":
				next = local
			case "wrong-family":
				next = netip.MustParseAddrPort("[2001:db8::1]:443")
			case "invalid-address":
				next = netip.AddrPort{}
			case "zero-time":
				now = time.Time{}
			case "no-spare":
				for seq, peer := range c.multipath.paths[id].peer.active {
					peer.used = true
					c.multipath.paths[id].peer.active[seq] = peer
				}
			}
			app := c.multipath.paths[id].application
			before, pn := c.queuedBytes, app.space.nextPN
			if candidate, err := c.prepareLocalMigration(id, next, now); err == nil || candidate != nil {
				t.Fatal("invalid local migration admitted")
			}
			if app.localMigration != nil || c.queuedBytes != before || app.space.nextPN != pn || app.network.local != local {
				t.Fatal("rejected operation mutated state")
			}
			for _, peer := range c.multipath.paths[id].peer.active {
				if peer.reservation != nil {
					t.Fatal("rejected operation reserved CID")
				}
			}
		})
	}
}
