package transport

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// Draft-21 sections 3.1 and 5.5 scope validation frames to a path ID. This
// differs from a changed address tuple within the same migrating path ID.
func TestMigrationResponsePathIDIsolation(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, id := range []uint64{0, 1} {
			t.Run(fmt.Sprintf("%x/path=%d", version, id), func(t *testing.T) {
				client, server := sendingPathPair(t, version)
				locals := [2]netip.AddrPort{netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.1:444")}
				remotes := [2]netip.AddrPort{netip.MustParseAddrPort("192.0.2.2:5000"), netip.MustParseAddrPort("192.0.2.2:5001")}
				for path := uint64(0); path < 2; path++ {
					if err := client.bindPreparedPath(path, locals[path], remotes[path]); err != nil {
						t.Fatal(err)
					}
					if err := server.bindPreparedPath(path, remotes[path], locals[path]); err != nil {
						t.Fatal(err)
					}
					for _, c := range []*Conn{client, server} {
						app := c.multipath.paths[path].application
						app.validation.state = PathValidated
						app.budget.Validate()
					}
				}
				next := netip.MustParseAddrPort("192.0.2.1:445")
				now := time.Unix(101, 0)
				candidate, err := client.prepareLocalMigration(id, next, now)
				if err != nil {
					t.Fatal(err)
				}
				probe, tuple, err := client.nextLocalMigrationProbe(id, now)
				if err != nil || probe == nil || tuple.local != next {
					t.Fatal("missing migration probe", err)
				}
				data := candidate.validation.probes[0].token
				payload, _ := (wire.PathResponseFrame{Data: data}).Append(nil)
				receive := func(path uint64) {
					t.Helper()
					dcid := client.multipath.paths[path].local.active[0].cid
					pn := server.multipath.paths[path].application.space.nextPN + 10
					raw := sealPathPayload(t, server.writeKeys(applicationSpace), dcid, uint32(path), pn, payload)
					if err := client.handleBoundMultipathPacket(raw, locals[path], remotes[path], now); err != nil {
						t.Fatal(err)
					}
				}
				other := 1 - id
				before := client.multipath.paths[other].peer.current
				receive(other)
				if candidate.validation.state != PathValidating || candidate.committed {
					t.Fatal("other path ID validated migration")
				}
				if err := client.commitLocalMigration(id, candidate); err == nil {
					t.Fatal("cross-ID response authorized commit")
				}
				receive(id)
				if candidate.validation.state != PathValidated {
					t.Fatal("same path ID rejected proof on old tuple")
				}
				if err := client.commitLocalMigration(id, candidate); err != nil {
					t.Fatal(err)
				}
				if client.multipath.paths[id].application.network.local != next {
					t.Fatal("migration did not commit")
				}
				untouched := client.multipath.paths[other]
				if untouched.application.network.local != locals[other] || untouched.peer.current != before {
					t.Fatal("migration changed another path")
				}
			})
		}
	}
}
