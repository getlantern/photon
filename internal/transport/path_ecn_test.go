package transport

import (
	"bytes"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathECNAdmission(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			c, keys, cid := pathDecodeFixture(t, v)
			local := netip.MustParseAddrPort("192.0.2.1:1234")
			remote := netip.MustParseAddrPort("192.0.2.2:443")
			foreign := netip.MustParseAddrPort("192.0.2.3:443")
			now := time.Unix(101, 0)
			for _, id := range []uint64{0, 1} {
				if err := c.bindPreparedPath(id, local, remote); err != nil {
					t.Fatal(err)
				}
			}
			payload, _ := (wire.PingFrame{}).Append(nil)
			payload = append(payload, 0, 0) // Header protection sample for a four-byte PN.
			raw := sealPathPayload(t, keys, cid, 1, 100, payload)
			// Authentication alone is not address admission and must not count.
			authenticated, err := c.authenticateMultipathPacketECN(raw, now, ECNCE)
			if err != nil || authenticated == nil {
				t.Fatal(err)
			}
			r := c.multipath.paths[1].application.space.received
			if r.haveECN {
				t.Fatal("authentication counted before admission")
			}
			if err := c.handleBoundMultipathPacketECN(raw, local, foreign, now, ECNCE); err != nil {
				t.Fatal(err)
			}
			if r.haveECN {
				t.Fatal("unadmitted server tuple counted")
			}
			bad := bytes.Clone(raw)
			bad[len(bad)-1] ^= 1
			if err := c.handleBoundMultipathPacketECN(bad, local, remote, now, ECNCE); err != nil {
				t.Fatal(err)
			}
			if r.haveECN {
				t.Fatal("damaged packet counted")
			}
			if err := c.handleBoundMultipathPacketECN(raw, local, remote, now, ECNECT0); err != nil {
				t.Fatal(err)
			}
			if err := c.handleBoundMultipathPacketECN(raw, local, remote, now, ECNCE); err != nil {
				t.Fatal(err)
			}
			// Packet numbers and ECN counters are independent on each path.
			other := sealPathPayload(t, keys, c.config.LocalCID, 0, 100, payload)
			if err := c.handleBoundMultipathPacketECN(other, local, remote, now, ECNECT1); err != nil {
				t.Fatal(err)
			}
			for id, want := range map[uint64][3]uint64{0: {0, 1, 0}, 1: {1, 0, 0}} {
				ack, err := c.multipath.paths[id].application.space.received.Ack(now, 0)
				if err != nil || ack.ECN == nil || *ack.ECN != want {
					t.Fatal("path counts", id, ack.ECN, err)
				}
				encoded, err := (wire.PathAckFrame{PathID: id, AckFrame: ack}).Append(nil)
				if err != nil || len(encoded) == 0 {
					t.Fatal("PATH_ACK ECN encoding", err)
				}
			}
			// A staged authentication result carries the original mark until commit.
			fresh := sealPathPayload(t, keys, cid, 1, 101, payload)
			staged, err := c.authenticateMultipathPacketECN(fresh, now, ECNCE)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.recordMultipathPacket(staged, now.Add(time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			ack, _ := r.Ack(now, 0)
			if *ack.ECN != [3]uint64{1, 0, 1} {
				t.Fatal("staged metadata lost")
			}
		})
	}
}
