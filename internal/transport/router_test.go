package transport

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestRouterCIDLifecycle(t *testing.T) {
	c, s := testPair(t, wire.Version1)
	peer := netip.MustParseAddrPort("192.0.2.1:1234")
	r, _ := NewRouter(8, 10)
	if err := r.Add(s, peer); err != nil {
		t.Fatal(err)
	}
	initial := makePeerPacket(t, c, initialSpace, 0, wire.PingFrame{})
	if got, known := r.Route(initial, peer); got != s || !known {
		t.Fatal("Initial alias not routed")
	}
	drivePair(t, c, s, false, false)
	if err := r.Sync(s); err != nil {
		t.Fatal(err)
	}
	if err := c.RotatePeerID(); err != nil {
		t.Fatal(err)
	}
	raw := makePeerPacket(t, c, applicationSpace, 1000, wire.PingFrame{})
	if got, known := r.Route(raw, peer); got != s || !known {
		t.Fatal("issued CID not routed")
	}
	wrong := netip.MustParseAddrPort("192.0.2.1:1235")
	if got, known := r.Route(raw, wrong); got != nil || !known {
		t.Fatal("known CID readmitted from new path")
	}
	mapped := netip.MustParseAddrPort("[::ffff:192.0.2.1]:1234")
	if got, _ := r.Route(raw, mapped); got != s {
		t.Fatal("mapped address routing")
	}
	s.beginDraining(ErrPeerClosed, time.Unix(200, 0))
	r.Sync(s)
	for _, b := range [][]byte{initial, raw} {
		if got, known := r.Route(b, peer); got != s || !known {
			t.Fatal("draining route released early")
		}
	}
	s.HandleTimeout(s.RecoveryDeadline())
	r.Sync(s)
	if got, known := r.Route(raw, peer); got != nil || known {
		t.Fatal("expired route retained")
	}
}

func TestRouterCollisionAtomicity(t *testing.T) {
	_, s := testPair(t, wire.Version1)
	_, other := testPair(t, wire.Version1)
	peer := netip.MustParseAddrPort("192.0.2.1:1234")
	r, _ := NewRouter(8, 2)
	r.Add(s, peer)
	if err := r.Add(other, peer); !errors.Is(err, ErrRouteCollision) {
		t.Fatal(err)
	}
	if len(r.entries) != 1 {
		t.Fatal("failed Add retained entry")
	}
	other.config.LocalCID = []byte("othercid")
	other.config.OriginalDCID = []byte("otherorg")
	other.localIDs = newLocalIDs(other.config.LocalCID)
	if err := r.Add(other, peer); err != nil {
		t.Fatal(err)
	}
	other.localIDs.active[1] = peerID{cid: s.config.LocalCID}
	if err := r.Sync(other); !errors.Is(err, ErrRouteCollision) {
		t.Fatal(err)
	}
	if r.ids["server00"] != s || r.ids["othercid"] != other {
		t.Fatal("collision changed existing routes")
	}
	delete(other.localIDs.active, 1)
	other.localIDs.active[1] = peerID{cid: []byte("sparecid")}
	r.Sync(other)
	delete(other.localIDs.active, 1)
	r.Sync(other)
	if r.ids["sparecid"] != nil {
		t.Fatal("retired CID retained")
	}
	r.Remove(other)
	if len(r.entries) != 1 || r.ids["othercid"] != nil {
		t.Fatal("Remove failed")
	}
}

func TestRouterResetAndZeroCID(t *testing.T) {
	c, s := testPair(t, wire.Version1)
	drivePair(t, c, s, false, false)
	peer := netip.MustParseAddrPort("192.0.2.1:4433")
	r, _ := NewRouter(8, 2)
	r.Add(c, peer)
	id := c.peerIDs.active[c.peerIDs.current]
	id.hasToken = true
	id.token = [16]byte{8}
	id.used = true
	c.peerIDs.active[c.peerIDs.current] = id
	if got, known := r.Route(resetDatagram(id.token), peer); got != c || !known {
		t.Fatal("unknown-CID reset lost")
	}
	if got, known := r.Route(resetDatagram(id.token), netip.MustParseAddrPort("192.0.2.2:4433")); got != nil || known {
		t.Fatal("reset accepted from wrong address")
	}
	r.Remove(c)
	c.config.LocalCID = nil
	c.localIDs = newLocalIDs(nil)
	if err := r.Add(c, peer); err != nil {
		t.Fatal(err)
	}
	if got, known := r.Route([]byte{0x40, 1, 2}, peer); got != c || !known {
		t.Fatal("zero CID dispatch")
	}
	if err := r.Add(s, peer); !errors.Is(err, ErrRouteCollision) {
		t.Fatal("ambiguous zero CID", err)
	}
}

func TestRouterSyncDetectsReplacementAtSameCIDCount(t *testing.T) {
	_, c := testPair(t, wire.Version1)
	r, _ := NewRouter(8, 2)
	peer := netip.MustParseAddrPort("192.0.2.1:1234")
	frames, err := c.localIDs.issue(4)
	if err != nil || len(frames) != 3 {
		t.Fatal(err)
	}
	if err := r.Add(c, peer); err != nil {
		t.Fatal(err)
	}
	retired := frames[0]
	c.localIDs.markSent(retired.Sequence)
	if _, err := c.localIDs.retire(retired.Sequence, c.config.LocalCID); err != nil {
		t.Fatal(err)
	}
	replacement, err := c.localIDs.issue(4)
	if err != nil || len(replacement) != 1 {
		t.Fatal(err)
	}
	if err := r.Sync(c); err != nil {
		t.Fatal(err)
	}
	if r.ids[string(retired.ConnectionID)] != nil || r.ids[string(replacement[0].ConnectionID)] != c {
		t.Fatal("same-count replacement missed")
	}
	// A repeated Sync must leave this result intact.
	if err := r.Sync(c); err != nil {
		t.Fatal(err)
	}
	if r.ids[string(replacement[0].ConnectionID)] != c {
		t.Fatal("unchanged sync lost route")
	}
}
