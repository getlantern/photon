package transport

import (
	"bytes"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// Nomination uses authenticated packets, while addresses emulate a NAT changing
// its external port. These tests deliberately withhold validation responses.
func migrationLifecyclePair(t *testing.T, version wire.Version) (*Conn, *Conn, func(netip.AddrPort, time.Time), netip.AddrPort) {
	t.Helper()
	client, server := sendingPathPair(t, version)
	ca := netip.MustParseAddrPort("192.0.2.1:4000")
	sa := netip.MustParseAddrPort("192.0.2.2:443")
	if err := client.bindPreparedPath(1, ca, sa); err != nil {
		t.Fatal(err)
	}
	if err := server.bindPreparedPath(1, sa, ca); err != nil {
		t.Fatal(err)
	}
	client.multipath.paths[1].application.budget.Validate()
	app := server.multipath.paths[1].application
	app.validation.state = PathValidated
	app.budget.Validate()
	server.config.allowCandidateMigration = true
	server.config.allowFreshCIDMigration = true
	nominate := func(remote netip.AddrPort, now time.Time) {
		t.Helper()
		if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
		raw, err := client.nextPacketOnPath(1, now)
		if err != nil || raw == nil {
			t.Fatal("nomination packet", err)
		}
		if err := server.handleBoundMultipathPacket(raw, sa, remote, now); err != nil {
			t.Fatal(err)
		}
	}
	return client, server, nominate, ca
}

func TestFreshCIDCandidateReplacementAndExpiry(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, sent := range []bool{false, true} {
			for _, expire := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/sent=%v/expire=%v", version, sent, expire), func(t *testing.T) {
					_, server, nominate, established := migrationLifecyclePair(t, version)
					now := time.Unix(101, 0)
					first := netip.MustParseAddrPort("192.0.2.1:4001")
					second := netip.MustParseAddrPort("192.0.2.1:4002")
					nominate(first, now)
					state := server.multipath.paths[1]
					app := state.application
					old := app.migration.candidate
					if old == nil || !old.freshCID {
						t.Fatal("fresh nomination missing")
					}
					if !sent {
						old.budget.credit = 0
					}
					raw, _, err := server.nextMigrationProbe(1, now)
					if err != nil || (raw != nil) != sent || !old.hasPeerSequence {
						t.Fatal("unexpected probe outcome", err)
					}
					seq := old.peerSequence
					if expire {
						now = old.validation.expires
					} else {
						now = now.Add(100 * time.Millisecond)
						nominate(second, now)
					}
					// Block replacement output so cleanup is observed separately from its first probe.
					if !expire {
						app.migration.candidate.budget.credit = 0
					}
					before := server.queuedBytes
					if raw, _, err := server.nextMigrationProbe(1, now); err != nil || raw != nil {
						t.Fatal("unexpected cleanup output", err)
					}
					if expire && app.migration.candidate != nil {
						t.Fatal("candidate survived absolute expiry")
					}
					if !expire && (app.migration.candidate == old || app.migration.candidate.tuple.remote != second) {
						t.Fatal("newer nomination did not replace old candidate")
					}
					if id, ok := state.peer.active[seq]; sent {
						if ok || server.queuedBytes != before+32 {
							t.Fatal("sent reservation not reliably retired")
						}
					} else {
						if !ok || id.used || id.reservation == old || server.queuedBytes != before {
							t.Fatal("unsent reservation consumed or leaked")
						}
					}
					if app.network.remote != established {
						t.Fatal("unvalidated candidate changed binding")
					}
					oldCID := state.peer.active[state.peer.current].cid
					if len(oldCID) == 0 {
						t.Fatal("established CID lost")
					}
					if err := server.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
						t.Fatal(err)
					}
					packet, err := server.nextPacketOnPath(1, now.Add(100*time.Millisecond))
					if err != nil || packet == nil || !bytes.Equal(packet[1:1+len(oldCID)], oldCID) {
						t.Fatal("cleanup blocked established output", err)
					}
				})
			}
		}
	}
}

func TestFreshCIDExhaustionPreservesEstablishedPath(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		_, server, nominate, established := migrationLifecyclePair(t, version)
		state := server.multipath.paths[1]
		app := state.application
		now := time.Unix(101, 0)
		// Exercise actual packet construction with every available CID, leaving no
		// unused CID eligible for a new address. Restore the established current ID.
		current := state.peer.current
		for seq := range state.peer.active {
			state.peer.current = seq
			if err := server.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
				t.Fatal(err)
			}
			packet, err := server.nextPacketOnPath(1, now)
			if err != nil || packet == nil {
				t.Fatal("CID use", err)
			}
			now = now.Add(100 * time.Millisecond)
		}
		state.peer.current = current
		nominate(netip.MustParseAddrPort("192.0.2.1:4001"), now)
		candidate := app.migration.candidate
		pn, seals := app.space.nextPN, server.sealed[applicationSpace]
		for range 3 {
			packet, _, err := server.nextMigrationProbe(1, now)
			if err != nil || packet != nil || candidate.hasPeerSequence || app.space.nextPN != pn || server.sealed[applicationSpace] != seals {
				t.Fatal("CID exhaustion consumed output state", err)
			}
			now = now.Add(100 * time.Millisecond)
		}
		if _, _, err := server.nextMigrationProbe(1, candidate.validation.expires); err != nil {
			t.Fatal(err)
		}
		if app.migration.candidate != nil || app.network.remote != established || server.Err() != nil {
			t.Fatal("CID exhaustion damaged established path")
		}
	}
}
