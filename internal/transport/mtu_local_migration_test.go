package transport

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMTULocalMigrationExchange(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, base := range []bool{true, false} {
			t.Run(fmt.Sprintf("%x/base=%v", version, base), func(t *testing.T) {
				client, server := testPair(t, version, streamParameters())
				drivePair(t, client, server, false, false)
				old := netip.MustParseAddrPort("192.0.2.1:443")
				next := netip.MustParseAddrPort("192.0.2.1:445")
				remote := netip.MustParseAddrPort("192.0.2.2:444")
				if err := client.activateBasePath(old, remote); err != nil {
					t.Fatal(err)
				}
				if err := server.activateBasePath(remote, old); err != nil {
					t.Fatal(err)
				}
				client.multipath.baseOnly, server.multipath.baseOnly = base, base
				client.peerParams.DisableActiveMigration = false
				server.config.allowCandidateMigration, server.config.allowFreshCIDMigration = true, true
				if err := client.startPathMTUSearch(0, 1500); err != nil {
					t.Fatal(err)
				}
				app := client.multipath.paths[0].application
				now := time.Unix(101, 0)
				// Confirm one larger size with real encrypted output and ACKs.
				for i := 0; i < 10 && app.mtu.confirmed == 1200; i++ {
					for _, pair := range [][2]*Conn{{client, server}, {server, client}} {
						_, tuple, b, err := pair[0].nextBoundMultipathPacket(now)
						if err != nil {
							t.Fatal(err)
						}
						if b != nil {
							if err := pair[1].handleBoundMultipathPacket(b, tuple.remote, tuple.local, now); err != nil {
								t.Fatal(err)
							}
						}
						now = now.Add(time.Second)
					}
				}
				if app.mtu.confirmed != 1350 {
					t.Fatal("initial size not confirmed", app.mtu.confirmed)
				}
				// Leave a larger encrypted probe outstanding across the address move.
				_, _, probe, err := client.nextBoundMultipathPacket(now)
				if err != nil || len(probe) != 1425 || !app.mtu.pending {
					t.Fatal("pending old probe", len(probe), err)
				}
				oldPN := app.space.largestSent
				now = now.Add(time.Second)
				candidate, err := client.prepareLocalMigration(0, next, now)
				if err != nil {
					t.Fatal(err)
				}
				searchBefore := *app.mtu
				if err := client.commitLocalMigration(0, candidate); err == nil {
					t.Fatal("unvalidated migration committed")
				}
				if *app.mtu != searchBefore {
					t.Fatal("failed migration changed MTU evidence")
				}
				for i := 0; i < 12; i++ {
					b, tuple, err := client.nextLocalMigrationProbe(0, now)
					if err != nil {
						t.Fatal(err)
					}
					if b != nil {
						if err := server.handleBoundMultipathPacket(b, tuple.remote, tuple.local, now); err != nil {
							t.Fatal(err)
						}
					}
					b, tuple, err = server.nextMigrationProbe(0, now)
					if err != nil {
						t.Fatal(err)
					}
					if b != nil {
						if err := client.handleBoundMultipathPacket(b, tuple.remote, tuple.local, now); err != nil {
							t.Fatal(err)
						}
					}
					now = now.Add(100 * time.Millisecond)
				}
				if candidate.validation.state != PathValidated || server.multipath.paths[0].application.network.remote != next {
					t.Fatal("local migration not validated")
				}
				beforePN := app.space.nextPN
				if err := client.commitLocalMigration(0, candidate); err != nil {
					t.Fatal(err)
				}
				if app.mtu.confirmed != 1200 || app.mtu.pending || app.mtu.lastNumber != oldPN || app.space.nextPN != beforePN || app.sent.BytesInFlight != 0 {
					t.Fatal("commit retained old size/flight evidence or reset packet numbers")
				}
				// Model a delayed peer ACK after the move. Recovery has discarded the old
				// packet metadata, but still knows this packet number was allocated.
				ack := wire.AckFrame{Ranges: []wire.AckRange{{Smallest: oldPN, Largest: oldPN}}}
				var frame wire.Frame = ack
				if !base {
					frame = wire.PathAckFrame{PathID: 0, AckFrame: ack}
				}
				if _, err := client.multipath.receiveACK(frame, now, 0, 0, false); err != nil {
					t.Fatal(err)
				}
				if app.mtu.confirmed != 1200 || app.mtu.pending {
					t.Fatal("pre-migration ACK restored old size")
				}
				// Drain migration control output and prove discovery restarts on the new
				// source tuple, with fresh numbers and the conservative initial proposal.
				for i := 0; i < 20 && !app.mtu.pending; i++ {
					id, tuple, b, err := client.nextBoundMultipathPacket(now)
					if err != nil {
						t.Fatal(err)
					}
					if b != nil && (id != 0 || tuple.local != next) {
						t.Fatal("output used old tuple")
					}
					if b != nil {
						if err := server.handleBoundMultipathPacket(b, tuple.remote, tuple.local, now); err != nil {
							t.Fatal(err)
						}
					}
					if app.mtu.pending && (len(b) != 1350 || app.mtu.lastNumber < beforePN) {
						t.Fatal("wrong fresh probe")
					}
					now = now.Add(time.Second)
				}
				if !app.mtu.pending {
					t.Fatal("discovery did not restart")
				}
				for i := 0; i < 10 && app.mtu.pending; i++ {
					_, tuple, b, err := server.nextBoundMultipathPacket(now)
					if err != nil {
						t.Fatal(err)
					}
					if b != nil {
						if tuple.remote != next {
							t.Fatal("fresh ACK sent to old local address")
						}
						if err := client.handleBoundMultipathPacket(b, tuple.remote, tuple.local, now); err != nil {
							t.Fatal(err)
						}
					}
					now = now.Add(time.Second)
				}
				if app.mtu.confirmed != 1350 || app.mtu.pending {
					t.Fatal("fresh encrypted ACK did not confirm new tuple")
				}

			})
		}
	}
}
