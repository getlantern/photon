package transport

import (
	"github.com/getlantern/photon/internal/wire"
	"net/netip"
	"testing"
	"time"
)

// Exercise reconstruction independently of receive admission and ACK policy.
func TestAuthenticatedPathPacketNumberWithoutAdmission(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, v)
		a := server.multipath.paths[1].application
		zeroExpected := server.multipath.paths[0].application.space.authenticatedExpected
		state := client.multipath.paths[1]
		cid := state.peer.active[state.peer.current].cid
		now := time.Unix(101, 0)
		seal := func(pn uint64, width int) []byte {
			t.Helper()
			header := append([]byte{0x40 | byte(width-1)}, cid...)
			for shift := width - 1; shift >= 0; shift-- {
				header = append(header, byte(pn>>uint(shift*8)))
			}
			payload := []byte{1, 0, 0, 0}
			b, err := client.writeKeys(applicationSpace).SealPath(header, payload, pn, 1)
			if err != nil {
				t.Fatal(err)
			}
			return b
		}
		authenticate := func(pn uint64, width int) {
			t.Helper()
			decoded, err := server.authenticateMultipathPacket(seal(pn, width), now)
			if err != nil || decoded == nil || decoded.packet.number != pn {
				t.Fatal("packet reconstruction", pn, err)
			}
			if a.space.received.Expected() != 0 || a.space.ackPending {
				t.Fatal("authentication admitted packet")
			}
		}
		authenticate(65534, 4)
		authenticate(65535, 2)
		authenticate(65536, 2)
		authenticate(65537, 1)
		authenticate(65535, 1) // reordering must not lower the reconstruction reference.
		if a.space.authenticatedExpected != 65538 {
			t.Fatal("authenticated reference regressed")
		}
		forged := seal(90000, 4)
		forged[len(forged)-1] ^= 1
		if packet, err := server.authenticateMultipathPacket(forged, now); err != nil || packet != nil {
			t.Fatal("forgery accepted", err)
		}
		if a.space.authenticatedExpected != 65538 || server.multipath.paths[0].application.space.authenticatedExpected != zeroExpected {
			t.Fatal("forged or cross-path reference")
		}
		a.resetMigrationRecovery()
		authenticate(65538, 1)
	}
}

func TestAuthenticatedPacketNumberSurvivesOwnerHandoff(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		maximum := uint64(3)
		client, server := negotiatedPathPair(t, v, &maximum, &maximum)
		drivePair(t, client, server, false, false)
		space := &client.spaces[applicationSpace]
		admitted := space.received.Expected()
		// Saturated pre-activation retention authenticates but does not ACK/admit.
		retainedBytes := client.pendingMultipathBytes
		client.pendingMultipathBytes = maxDeferredBytes
		now := time.Unix(101, 0)
		send := func(pn uint64, width int) {
			t.Helper()
			header := append([]byte{0x40 | byte(width-1)}, client.config.LocalCID...)
			for shift := width - 1; shift >= 0; shift-- {
				header = append(header, byte(pn>>uint(shift*8)))
			}
			raw, err := server.writeKeys(applicationSpace).SealPath(header, []byte{1, 0, 0, 0}, pn, 0)
			if err != nil {
				t.Fatal(err)
			}
			if client.multipath == nil {
				err = client.HandleDatagram(raw, now)
			} else {
				err = client.handleMultipathPacket(raw, now)
			}
			if err != nil {
				t.Fatal(err)
			}
			if space.authenticatedExpected != pn+1 {
				t.Fatal("stale decoding reference", pn, space.authenticatedExpected)
			}
		}
		send(65534, 4)
		send(65535, 2)
		send(65536, 2)
		if space.received.Expected() != admitted {
			t.Fatal("retained packet prematurely admitted")
		}
		client.pendingMultipathBytes = retainedBytes
		local := netip.MustParseAddrPort("192.0.2.1:443")
		remote := netip.MustParseAddrPort("192.0.2.2:443")
		if active, err := client.activateNegotiatedMultipath(local, remote); err != nil || !active {
			t.Fatal(err)
		}
		if client.multipath.paths[0].application.space != space || space.authenticatedExpected != 65537 {
			t.Fatal("handoff reset packet space")
		}
		send(65537, 1)
		if space.received.Expected() != 65538 {
			t.Fatal("post-handoff admission failed")
		}
	}
}
