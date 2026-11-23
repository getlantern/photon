package transport

import (
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestFinalPathAbandonmentEmitsClose(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, version)
		ca := netip.MustParseAddrPort("192.0.2.1:443")
		sa := netip.MustParseAddrPort("192.0.2.2:444")
		// Additional CIDs exist, but no secondary network path has been opened.
		client.multipath.paths[1].application = nil
		server.multipath.paths[1].application = nil
		if err := client.bindPreparedPath(0, ca, sa); err != nil {
			t.Fatal(err)
		}
		if err := server.bindPreparedPath(0, sa, ca); err != nil {
			t.Fatal(err)
		}
		now := time.Unix(101, 0)
		if err := client.queueFrameOnPath(0, wire.PathAbandonFrame{PathID: 0}); err != nil {
			t.Fatal(err)
		}
		_, _, raw, err := client.nextBoundMultipathPacket(now)
		if err != nil || raw == nil {
			t.Fatal(err)
		}
		err = server.handleBoundMultipathPacket(raw, sa, ca, now)
		requirePathError(t, err, 0x10, wire.FramePathAbandon)
		if !server.Closing() || len(server.multipath.paths[0].peer.active) != 0 {
			t.Fatal("close not prepared before CID retirement")
		}
		a := server.multipath.abandoned[0]
		if a == nil || !a.received || a.sent {
			t.Fatal("invalid abandonment accounting")
		}
		id, tuple, closePacket, err := server.nextBoundMultipathPacket(now)
		if err != nil || id != 0 || tuple != (pathTuple{sa, ca}) || closePacket == nil {
			t.Fatal("missing final close", err)
		}
		decoded, err := client.decodeMultipathPacket(closePacket, now)
		if err != nil || decoded == nil {
			t.Fatal("close authentication", err)
		}
		found := false
		for _, frame := range decoded.frames {
			if f, ok := frame.(wire.ConnectionCloseFrame); ok {
				if f.Application || f.ErrorCode != 0x10 || f.TriggerFrame != wire.FramePathAbandon {
					t.Fatal("wrong close", f)
				}
				found = true
			}
		}
		if !found {
			t.Fatal("missing NO_VIABLE_PATH")
		}
	}
}
