package transport

import (
	"errors"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMigrationHandleRejectsRetiredProof(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []string{"handle", "service", "output"} {
			t.Run(fmt.Sprintf("%s/%s", version, mode), func(t *testing.T) {
				params := streamParameters()
				params.ActiveConnectionIDLimit = 4
				c, _ := sendingPathPair(t, version, params)
				local := netip.MustParseAddrPort("192.0.2.1:443")
				next := netip.MustParseAddrPort("192.0.2.1:444")
				remote := netip.MustParseAddrPort("192.0.2.2:443")
				if err := c.bindPreparedPath(0, local, remote); err != nil {
					t.Fatal(err)
				}
				e, err := NewClientEndpoint(c, remote)
				if err != nil {
					t.Fatal(err)
				}
				defer e.Close()
				e.locals = map[netip.AddrPort]bool{local: true, next: true}
				now := time.Unix(101, 0)
				candidate, err := c.prepareLocalPathProbe(0, next, now)
				if err != nil {
					t.Fatal(err)
				}
				state := c.multipath.paths[0]
				if !state.peer.sentMigrationID(candidate, candidate.peerSequence) {
					t.Fatal("probe CID")
				}
				candidate.validation.state = PathValidated
				candidate.responsePending = &wire.PathResponseFrame{Data: [8]byte{1}}
				h := newPacketHandoff()
				defer h.close()
				route := &handoffRoute{target: h, local: next}
				handle := &MigrationPath{conn: c, route: route, local: next, candidate: candidate}
				if err := handle.available(e); err != nil {
					t.Fatal("fresh proof unavailable", err)
				}

				// Keep another spent reservation awaiting retirement. Its reliable frame
				// forces cancellation to retain the revoked candidate under queue pressure.
				stale := &migrationCandidate{}
				seq, ok := state.peer.reserveMigrationID(stale)
				if !ok || !state.peer.sentMigrationID(stale, seq) {
					t.Fatal("stale reservation")
				}
				replacement := state.peer.active[seq]
				if _, err := state.peer.add(wire.NewConnectionIDFrame{Sequence: seq, RetirePriorTo: seq, ConnectionID: replacement.cid, StatelessResetToken: replacement.token}); err != nil {
					t.Fatal(err)
				}
				if _, owned := state.peer.migrationID(candidate, candidate.peerSequence); owned {
					t.Fatal("reservation not revoked")
				}
				savedBytes := c.queuedBytes
				c.queuedBytes = maxSendBytes
				if mode == "handle" {
					if err := handle.available(e); !errors.Is(err, errMigrationState) {
						t.Fatal("retired proof advertised as usable", err)
					}
					c.queuedBytes = savedBytes
					return
				}
				if mode == "service" {
					if err := e.serviceLocalMigrations(c); err != nil {
						t.Fatal(err)
					}
				} else {
					raw, _, err := c.nextLocalMigrationProbe(0, now)
					if err != nil || raw != nil {
						t.Fatal("retired CID produced output", err)
					}
				}
				if state.application.localMigration != candidate || candidate.validation.state != PathValidationFailed || !errors.Is(candidate.failure, errMigrationState) || candidate.responsePending != nil {
					t.Fatal("retained canceled candidate still claims proof")
				}
				if err := handle.available(e); !errors.Is(err, errMigrationState) {
					t.Fatal("failed handle available", err)
				}
				if err := c.commitLocalMigration(0, candidate); !errors.Is(err, errMigrationState) {
					t.Fatal("stale switch", err)
				}
				c.queuedBytes = savedBytes
				if err := e.serviceLocalMigrations(c); err != nil {
					t.Fatal(err)
				}
				if state.application.localMigration != nil || state.application.network.local != local || c.closed {
					t.Fatal("cleanup failed or changed primary")
				}
				if c.queuedBytes != savedBytes+32 {
					t.Fatal("missing reliable stale CID retirement")
				}
			})
		}
	}
}
