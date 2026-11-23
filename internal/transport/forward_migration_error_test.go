package transport

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func queuedForeignMigration(t *testing.T, v wire.Version) (*ServerEndpoint, *Conn, *packetWriter, *handoffRoute, *migrationCandidate, *foreignWrite) {
	t.Helper()
	e, c, source, writer, route := endpointForeignFixture(t, v)
	if err := e.bindForeignWriter(c, writer, route); err != nil {
		t.Fatal(err)
	}
	e.foreignPacketRoutes(c)
	source.applyForwardUpdates()
	e.foreignPacketRoutes(c)
	candidate, err := c.prepareLocalPathProbe(1, route.local, time.Unix(101, 0))
	if err != nil {
		t.Fatal(err)
	}
	out, err := e.NextDatagram(time.Unix(101, 0))
	if err != nil || out != nil || c.pendingWrite == nil || c.pendingWrite.out.migration == nil {
		t.Fatal("probe was not queued with metadata", err)
	}
	return e, c, writer, route, candidate, <-writer.writes
}

func TestForeignMigrationWriteFailurePreservesPrimary(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, n := range []int{0, 1} {
			t.Run(fmt.Sprintf("%x/bytes=%d", uint32(v), n), func(t *testing.T) {
				e, c, _, route, candidate, request := queuedForeignMigration(t, v)
				app := c.multipath.paths[1].application
				pn := c.pendingWrite.out.migration.number
				old := *app.network
				request.complete(n, syscall.ENETUNREACH)
				out, err := e.NextDatagram(time.Unix(101, 0).Add(time.Millisecond))
				if err != nil || c.closed || c.pendingWrite != nil || *app.network != old || app.localMigration != nil {
					t.Fatal("alternative failure destroyed established path", err)
				}
				if !route.revoked.Load() || e.foreignWriterCount != 0 || candidate.validation.state != PathValidationFailed || !errors.Is(candidate.failure, syscall.ENETUNREACH) {
					t.Fatal("failed alternative retained")
				}
				if out == nil || out.Local != old.local {
					t.Fatal("primary output did not resume")
				}
				expected := uint64(len(out.Data))
				if n != 0 {
					expected += uint64(len(request.data))
				}
				if app.sent.BytesInFlight != expected {
					t.Fatal("incorrect send rollback", app.sent.BytesInFlight, expected)
				}
				_, ackErr := app.sent.OnACK(recovery.Application, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: pn, Largest: pn}}}, time.Unix(102, 0), 0, 0, true)
				if n == 0 && !errors.Is(ackErr, recovery.ErrACK) {
					t.Fatal("locally unsent probe remained ACKable", ackErr)
				}
				if n != 0 && ackErr != nil {
					t.Fatal("partial result discarded possible transmission", ackErr)
				}
			})
		}
	}
}

func TestForeignMigrationFailureQueuePressure(t *testing.T) {
	e, c, _, _, candidate, request := queuedForeignMigration(t, wire.Version1)
	before := c.queuedBytes
	c.queuedBytes = maxSendBytes
	request.complete(0, syscall.ECONNREFUSED)
	_, err := e.NextDatagram(time.Unix(101, 0).Add(time.Millisecond))
	if err != nil || c.closed || candidate.validation.state != PathValidationFailed || c.multipath.paths[1].application.localMigration != candidate {
		t.Fatal("queue pressure lost cancellation ownership", err)
	}
	c.queuedBytes = before
	if err := e.serviceLocalMigrations(c); err != nil {
		t.Fatal(err)
	}
	if c.multipath.paths[1].application.localMigration != nil {
		t.Fatal("cancellation did not retry")
	}
}

func TestForeignMigrationSwitchWaitsForWrite(t *testing.T) {
	e, c, _, _, candidate, request := queuedForeignMigration(t, wire.Version1)
	candidate.validation.state = PathValidated
	if err := c.commitLocalMigration(1, candidate); !errors.Is(err, errOperationWouldBlock) || candidate.committed {
		t.Fatal("switch overtook write completion", err)
	}
	request.complete(len(request.data), nil)
	if _, err := e.NextDatagram(time.Unix(101, 0).Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := c.commitLocalMigration(1, candidate); err != nil || !candidate.committed {
		t.Fatal("completed write still blocked switch", err)
	}
}

func TestForeignMigrationLateFailurePreservesReplacement(t *testing.T) {
	e, c, _, oldRoute, oldCandidate, request := queuedForeignMigration(t, wire.Version1)
	if err := c.cancelLocalMigration(1, oldCandidate); err != nil {
		t.Fatal(err)
	}
	e.removeForeignWriter(c, oldRoute.local)
	source, _ := NewRouter(8, 8)
	route := updateTestRoute(t, source, oldRoute.target, nil)
	route.local = oldRoute.local
	if err := route.target.bindConnection(route, c); err != nil {
		t.Fatal(err)
	}
	writer := newPacketWriter(source)
	defer writer.close()
	if err := e.bindForeignWriter(c, writer, route); err != nil {
		t.Fatal(err)
	}
	// Replenish the peer CID consumed by the canceled probe.
	if _, err := c.multipath.receiveNew(wire.PathNewConnectionIDFrame{PathID: 1, NewConnectionIDFrame: wire.NewConnectionIDFrame{Sequence: 100, ConnectionID: []byte("replace1"), StatelessResetToken: [16]byte{99}}}); err != nil {
		t.Fatal(err)
	}
	replacement, err := c.prepareLocalPathProbe(1, route.local, time.Unix(101, 0))
	if err != nil {
		t.Fatal(err)
	}
	request.complete(0, syscall.ENETUNREACH)
	if _, err := e.NextDatagram(time.Unix(101, 0).Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if c.closed || c.multipath.paths[1].application.localMigration != replacement || replacement.validation.state != PathValidating || replacement.failure != nil {
		t.Fatal("old failure canceled replacement candidate")
	}
	if route.revoked.Load() || e.foreignWriters[c][route.local].route != route || e.foreignWriterCount != 1 {
		t.Fatal("old failure revoked replacement registration")
	}
}

func TestForeignMigrationRejectionPreservesLaterActivity(t *testing.T) {
	e, c, _, _, _, request := queuedForeignMigration(t, wire.Version1)
	// Equal timestamps must still invalidate the earlier idle rollback revision.
	c.receivedIdleActivity(time.Unix(101, 0))
	before, revision := c.idle, c.idleRevision
	request.complete(0, syscall.EMSGSIZE)
	// Inspect completion handling before the next ordinary packet updates idle.
	pending := c.pendingWrite
	out, result, ready := pending.poll()
	if !ready || !e.rejectForeignMigration(out, result, time.Unix(101, 0)) {
		t.Fatal("rejection not handled")
	}
	if c.closed || c.idle != before || c.idleRevision != revision {
		t.Fatal("rejection rewound receive activity")
	}
}

func TestForeignMigrationAutomaticCommitWaitIsRetryable(t *testing.T) {
	e, c, _, _, candidate, request := queuedForeignMigration(t, wire.Version1)
	candidate.explicitSwitch = false
	candidate.validation.state = PathValidated
	if err := e.serviceLocalMigrations(c); err != nil || candidate.committed {
		t.Fatal("pending write wait became a migration failure", err)
	}
	request.complete(len(request.data), nil)
	if _, err := e.NextDatagram(time.Unix(101, 0).Add(time.Millisecond)); err != nil || !candidate.committed || c.closed {
		t.Fatal("automatic commit did not resume", err)
	}
}
