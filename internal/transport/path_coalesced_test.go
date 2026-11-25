package transport

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestBoundCoalescedPostConfirmation(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, base := range []bool{false, true} {
			for _, scenario := range []string{"valid", "candidate", "forged-tail", "different-cid", "other-known-version", "unknown-version", "truncated-prefix", "oversized"} {
				t.Run(fmt.Sprintf("%x/base=%v/%s", version, base, scenario), func(t *testing.T) {
					client, server := testPair(t, version, streamParameters())
					drivePair(t, client, server, false, false)
					local := netip.MustParseAddrPort("192.0.2.1:443")
					remote := netip.MustParseAddrPort("192.0.2.2:444")
					if base {
						if err := server.activateBasePath(local, remote); err != nil {
							t.Fatal(err)
						}
					} else {
						p, err := adoptPathZero(server, 0, 0, 1)
						if err != nil {
							t.Fatal(err)
						}
						server.multipath = p
						if err := server.bindPreparedPath(0, local, remote); err != nil {
							t.Fatal(err)
						}
						p.paths[0].local.bindAuthenticated(pathTuple{local, remote})
					}
					app := server.multipath.paths[0].application
					*app.budget = Amplification{}
					app.space.clearACK()
					if err := client.prepareApplicationKeys(); err != nil {
						t.Fatal(err)
					}
					pn := client.spaces[applicationSpace].nextPN
					payload, _ := (wire.PingFrame{}).Append(nil)
					tail := sealPathPayload(t, client.writeKeys(applicationSpace), client.peerCID, 0, pn, payload)
					prefixCID := clone(client.peerCID)
					prefixVersion := version
					if scenario == "different-cid" {
						prefixCID[0] ^= 1
					}
					if scenario == "other-known-version" {
						if version == wire.Version1 {
							prefixVersion = wire.Version2
						} else {
							prefixVersion = wire.Version1
						}
					}
					var datagram []byte
					// These levels are retired: only their framing is needed, not their keys.
					for _, typ := range []wire.PacketType{wire.Initial, wire.Handshake} {
						header, err := wire.AppendLongHeader(nil, prefixVersion, typ, prefixCID, client.config.LocalCID, nil, 0, 4, 16)
						if err != nil {
							t.Fatal(err)
						}
						datagram = append(datagram, header...)
						datagram = append(datagram, make([]byte, 16)...)
					}
					if scenario == "unknown-version" {
						copy(datagram[1:5], []byte{0xde, 0xad, 0xbe, 0xef})
					}
					if scenario == "truncated-prefix" {
						datagram = []byte{0xc0}
					}
					if scenario == "forged-tail" {
						tail[len(tail)-1] ^= 1
					}
					datagram = append(datagram, tail...)
					if scenario == "oversized" {
						server.config.Parameters.MaxUDPPayloadSize = uint64(len(datagram) - 1)
					}
					if scenario == "candidate" {
						server.config.allowCandidateMigration = true
						server.config.allowFreshCIDMigration = true
						remote = netip.MustParseAddrPort("192.0.2.2:445")
					}
					before := server.authenticatedPackets
					if err := server.handleBoundMultipathPacket(datagram, local, remote, time.Unix(101, 0)); err != nil {
						t.Fatal(err)
					}
					if scenario == "valid" || scenario == "other-known-version" {
						if server.authenticatedPackets != before+1 || !ackScheduled(app.space) || app.budget.credit != uint64(3*len(datagram)) {
							t.Fatal("trailing packet not admitted or datagram credit wrong")
						}
					} else if scenario == "candidate" {
						if app.migration == nil || app.migration.candidate == nil || app.migration.candidate.budget.credit != uint64(3*len(datagram)) || app.budget.credit != 0 || ackScheduled(app.space) {
							t.Fatal("candidate coalescing crossed admission boundary")
						}
					} else if server.authenticatedPackets != before || app.budget.credit != 0 || ackScheduled(app.space) {
						t.Fatal("invalid coalesced datagram mutated authenticated state")
					}
				})
			}
		}
	}
}
