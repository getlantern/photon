package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestPathStatusSend(t *testing.T) {
	c, peer := sendingPathPair(t, wire.Version1)
	app := c.multipath.paths[1].application
	app.budget.Validate()
	if err := c.setPathBackup(1, true); err != nil {
		t.Fatal(err)
	}
	first := app.space.queue[len(app.space.queue)-1]
	if c.multipath.paths[1].backup {
		t.Fatal("receive preference changed local scheduling")
	}
	charge := c.queuedBytes
	if err := c.setPathBackup(1, true); err != nil || c.queuedBytes != charge {
		t.Fatal("duplicate preference", err)
	}
	packet, err := c.nextPacketOnPath(1, time.Unix(101, 0))
	if err != nil || packet == nil {
		t.Fatal(err)
	}
	if err := peer.handleMultipathPacket(packet, time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	if !peer.multipath.paths[1].backup {
		t.Fatal("peer did not apply backup")
	}
	if err := c.setPathBackup(1, false); err != nil {
		t.Fatal(err)
	}
	if !first.acked {
		t.Fatal("obsolete sent status still recoverable")
	}
	second := app.space.queue[len(app.space.queue)-1].frame.(wire.PathStatusFrame)
	if second.Sequence != 1 || second.Backup {
		t.Fatal(second)
	}
	if _, err := peer.multipath.receiveStatus(second); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.multipath.receiveStatus(first.frame.(wire.PathStatusFrame)); err != nil {
		t.Fatal(err)
	}
	if peer.multipath.paths[1].backup {
		t.Fatal("reordered old status overwrote new preference")
	}
	state := c.multipath.paths[1]
	c.queuedBytes = maxSendBytes
	if err := c.setPathBackup(1, true); !errors.Is(err, ErrSendBuffer) {
		t.Fatal(err)
	}
	if state.localBackup || state.localStatusSequence != 1 || c.Err() != nil {
		t.Fatal("failed queue admission mutated state")
	}
	state.localStatusSequence = wire.MaxVarint
	if err := c.setPathBackup(1, true); err == nil {
		t.Fatal("sequence wrapped")
	}
	if state.localStatusSequence != wire.MaxVarint || state.localBackup {
		t.Fatal("exhaustion mutated state")
	}
	if err := c.setPathBackup(2, true); err == nil {
		t.Fatal("accepted uninitiated path")
	}
}
