package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestEncryptedPathAbandonment(t *testing.T) {
	c, keys, cid := pathDecodeFixture(t, wire.Version1)
	now := time.Unix(101, 0)
	f := pathNew(1, 0, 0, "remote10")
	f.StatelessResetToken = [16]byte{1, 2, 3}
	if _, err := c.multipath.receiveNew(f); err != nil {
		t.Fatal(err)
	}
	peer := c.multipath.paths[1].peer.active[0]
	peer.used = true
	c.multipath.paths[1].peer.active[0] = peer
	reset := append(make([]byte, 5), peer.token[:]...)
	if !c.isStatelessReset(reset) {
		t.Fatal("secondary-path reset token ignored")
	}
	payload, _ := (wire.PathAbandonFrame{PathID: 1, ErrorCode: 0x3e}).Append(nil)
	// Receive the abandonment on the very path being abandoned. Its response
	// must use another path, while its local CID stays routable for late packets.
	before := len(c.spaces[applicationSpace].queue)
	raw := sealPathPayload(t, keys, cid, 1, 100, payload)
	if err := c.handleMultipathPacket(raw, now); err != nil {
		t.Fatal(err)
	}
	if c.isStatelessReset(reset) {
		t.Fatal("abandoned token still recognized")
	}
	a := c.multipath.abandoned[1]
	if a == nil || !a.sent || !a.received {
		t.Fatal("abandonment handshake not recorded")
	}
	q := c.spaces[applicationSpace].queue
	if len(q) != before+1 {
		t.Fatal("missing response")
	}
	response, ok := q[len(q)-1].frame.(wire.PathAbandonFrame)
	if !ok || response.PathID != 1 || response.ErrorCode != 0 {
		t.Fatal("incorrect response")
	}
	if len(c.multipath.paths[1].application.space.queue) != 0 {
		t.Fatal("response queued on abandoned path")
	}
	deadline := a.retainUntil
	raw = sealPathPayload(t, keys, c.config.LocalCID, 0, 100, payload)
	if err := c.handleMultipathPacket(raw, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if len(c.spaces[applicationSpace].queue) != before+1 || !a.retainUntil.Equal(deadline) {
		t.Fatal("duplicate extended retention or queued response")
	}
	// Late authenticated input can still be acknowledged on a surviving path.
	raw = sealPathPayload(t, keys, cid, 1, 101, []byte{1})
	if err := c.handleMultipathPacket(raw, now.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if !c.multipath.paths[1].application.space.ackPending {
		t.Fatal("late ACK not pending")
	}
	if removed := c.multipath.collectAbandoned(deadline); len(removed) != 1 || removed[0].ID != 1 {
		t.Fatal(removed)
	}
	if out, err := c.decodeMultipathPacket(raw, deadline); err != nil || out != nil {
		t.Fatal("disposed CID accepted", err)
	}
}

func TestEncryptedAbandonmentFinalPath(t *testing.T) {
	c, keys, _ := pathDecodeFixture(t, wire.Version2)
	// Path one has no peer CID, so it cannot carry a response after path zero
	// is abandoned. Report NO_VIABLE_PATH while preparing connection close.
	payload, _ := (wire.PathAbandonFrame{PathID: 0}).Append(nil)
	err := c.handleMultipathPacket(sealPathPayload(t, keys, c.config.LocalCID, 0, 100, payload), time.Unix(101, 0))
	requirePathError(t, err, 0x10, wire.FramePathAbandon)
	if c.multipath.abandoned[0].sent {
		t.Fatal("claimed an unqueued response")
	}
}

func TestMultipathResetTokenEligibility(t *testing.T) {
	c, _, _ := pathDecodeFixture(t, wire.Version1)
	f := pathNew(1, 0, 0, "remote10")
	f.StatelessResetToken = [16]byte{9, 8, 7}
	if _, err := c.multipath.receiveNew(f); err != nil {
		t.Fatal(err)
	}
	reset := append(make([]byte, 5), f.StatelessResetToken[:]...)
	if c.isStatelessReset(reset) {
		t.Fatal("unused token recognized")
	}
	id := c.multipath.paths[1].peer.active[0]
	id.used = true
	c.multipath.paths[1].peer.active[0] = id
	if !c.isStatelessReset(reset) {
		t.Fatal("used token ignored")
	}
	if _, err := c.multipath.receiveNew(pathNew(1, 1, 1, "remote11")); err != nil {
		t.Fatal(err)
	}
	if c.isStatelessReset(reset) {
		t.Fatal("retired token recognized from history")
	}
}
