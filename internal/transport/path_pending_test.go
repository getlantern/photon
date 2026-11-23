package transport

import (
	"github.com/getlantern/photon/internal/wire"
	"net/netip"
	"testing"
	"time"
)

func TestMultipathFramesBeforeOwnerActivation(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		maximum := uint64(3)
		client, server := negotiatedPathPair(t, version, &maximum, &maximum)
		confirmed := false
		if err := client.WatchHandshakeConfirmed(func() {
			if !client.SupportsMultipath() {
				t.Error("confirmation snapshot lacks negotiated multipath")
			}
			confirmed = true
		}); err != nil {
			t.Fatal(err)
		}
		drivePair(t, client, server, false, false)
		if !confirmed {
			t.Fatal("multipath handoff lost confirmation notification")
		}
		local := netip.MustParseAddrPort("192.0.2.1:443")
		remote := netip.MustParseAddrPort("192.0.2.2:444")
		if active, err := server.activateNegotiatedMultipath(remote, local); err != nil || !active {
			t.Fatal(err)
		}
		if err := server.advertisePathCIDs(1); err != nil {
			t.Fatal(err)
		}
		now := time.Unix(101, 0)
		before := len(client.pendingMultipath)
		// Server can already be using PATH_ACK/PATH_NEW_CONNECTION_ID while the
		// client owner still delivers packets through the original receive entry.
		for i := 0; i < 8; i++ {
			_, _, b, err := server.nextBoundMultipathPacket(now)
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			if err := client.HandleDatagram(b, now); err != nil {
				t.Fatal(err)
			}
			count := len(client.pendingMultipath)
			if err := client.HandleDatagram(b, now); err != nil {
				t.Fatal(err)
			}
			if len(client.pendingMultipath) != count {
				t.Fatal("duplicate retained twice")
			}
		}
		if len(client.pendingMultipath) <= before {
			t.Fatal("no early frames retained")
		}
		if active, err := client.activateNegotiatedMultipath(local, remote); err != nil || !active {
			t.Fatal(err)
		}
		if state := client.multipath.paths[1]; state == nil || len(state.peer.active) == 0 || state.application != nil {
			t.Fatal("early CID frames not applied")
		}
		if client.pendingMultipath != nil || client.pendingMultipathBytes != 0 {
			t.Fatal("retained frames not released")
		}
	}
}

func TestMultipathPendingCapacityDoesNotAcknowledge(t *testing.T) {
	maximum := uint64(1)
	client, server := negotiatedPathPair(t, wire.Version1, &maximum, &maximum)
	drivePair(t, client, server, false, false)
	client.pendingMultipathBytes = maxDeferredBytes
	before := client.spaces[applicationSpace].received.Expected()
	if err := client.retainMultipathBeforeActivation(client.config.LocalCID, before+100, []wire.Frame{wire.PingFrame{}}, true, 32, time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	if client.spaces[applicationSpace].received.Expected() != before {
		t.Fatal("capacity-dropped packet acknowledged")
	}
}
