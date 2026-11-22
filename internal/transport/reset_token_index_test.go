package transport

import (
	"crypto/hmac"
	"crypto/sha256"
	"net/netip"
	"testing"
	"time"
)

func indexedResetConn(cid string, token [16]byte, used bool) *Conn {
	c := &Conn{config: Config{LocalCID: []byte(cid)}, localIDs: newLocalIDs([]byte(cid)), peerIDs: newPeerIDs([]byte("peer-cid"), 4)}
	entry := c.peerIDs.active[0]
	entry.token, entry.hasToken, entry.used = token, true, used
	c.peerIDs.active[0] = entry
	return c
}

func TestResetTokenIndexKeyedLookup(t *testing.T) {
	index, err := newResetTokenIndex()
	if err != nil {
		t.Fatal(err)
	}
	// A deterministic key checks that lookup uses the standard HMAC construction.
	key := []byte("0123456789abcdef0123456789abcdef")
	index.mac = hmac.New(sha256.New, key)
	token := [16]byte{1, 2, 3}
	want := hmac.New(sha256.New, key)
	want.Write(token[:])
	got := index.key(token[:])
	if !hmac.Equal(got[:], want.Sum(nil)) {
		t.Fatal("lookup is not keyed HMAC-SHA256")
	}
	other, err := newResetTokenIndex()
	if err != nil {
		t.Fatal(err)
	}
	if got == other.key(token[:]) {
		t.Fatal("independent index reused secret transform")
	}
	c := indexedResetConn("localcid", token, true)
	index.sync(c)
	raw := resetDatagram(token)
	if len(index.candidates(raw)) != 1 {
		t.Fatal("used token absent")
	}
	if allocations := testing.AllocsPerRun(100, func() { index.candidates(raw) }); allocations != 0 {
		t.Fatalf("lookup allocations: %g", allocations)
	}
	if index.candidates(raw[:20]) != nil {
		t.Fatal("short input indexed")
	}
	raw[len(raw)-1] ^= 1
	if len(index.candidates(raw)) != 0 {
		t.Fatal("unrelated token matched")
	}
}

func TestRouterResetIndexLifecycle(t *testing.T) {
	peer := netip.MustParseAddrPort("192.0.2.1:443")
	r, _ := NewRouter(8, 4)
	token := [16]byte{1}
	c := indexedResetConn("firstcid", token, true)
	if err := r.Add(c, peer); err != nil {
		t.Fatal(err)
	}
	if r.resetIndex != nil {
		t.Fatal("index allocated without foreign traffic")
	}
	r.enableResetIndex()
	index := r.resetIndex
	if index == nil || len(index.candidates(resetDatagram(token))) != 1 {
		t.Fatal("bootstrap missed existing token")
	}
	b := indexedResetConn("othercid", token, false)
	if err := r.Add(b, peer); err != nil {
		t.Fatal(err)
	}
	if len(index.candidates(resetDatagram(token))) != 1 {
		t.Fatal("unused token indexed")
	}
	entry := b.peerIDs.active[0]
	entry.used = true
	b.peerIDs.active[0] = entry
	// No local CID revision changed: the normal unchanged-Sync fast path must
	// still publish first-use reset tokens before output reaches a socket.
	if err := r.Sync(b); err != nil {
		t.Fatal(err)
	}
	if len(index.candidates(resetDatagram(token))) != 2 {
		t.Fatal("first use lost or duplicate token overwrote an owner")
	}
	old := token
	token[0] = 2
	entry.token = token
	b.peerIDs.active[0] = entry
	if err := r.Sync(b); err != nil {
		t.Fatal(err)
	}
	if len(index.candidates(resetDatagram(old))) != 1 || len(index.candidates(resetDatagram(token))) != 1 {
		t.Fatal("token replacement not reflected")
	}
	delete(b.peerIDs.active, 0)
	if err := r.Sync(b); err != nil {
		t.Fatal(err)
	}
	if len(index.candidates(resetDatagram(token))) != 0 {
		t.Fatal("retired token retained")
	}
	r.Remove(c)
	if len(index.tokens) != 0 || len(index.connections) != 0 {
		t.Fatal("router removal leaked indexed identities")
	}
}

func TestResetTokenIndexLiveEligibility(t *testing.T) {
	peer := netip.MustParseAddrPort("192.0.2.1:443")
	wrong := netip.MustParseAddrPort("192.0.2.2:443")
	token := [16]byte{1}
	r, _ := NewRouter(8, 4)
	c := indexedResetConn("firstcid", token, true)
	other := indexedResetConn("othercid", token, true)
	if err := r.Add(c, peer); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(other, wrong); err != nil {
		t.Fatal(err)
	}
	r.enableResetIndex()
	e := &ServerEndpoint{router: r}
	raw := resetDatagram(token)
	if got, ambiguous := e.localResetMatch(raw, netip.AddrPort{}, peer, time.Now(), nil); got != c || ambiguous {
		t.Fatal("peer-scoped selection failed")
	}
	// Stale index entries never grant reset authority. Deliberately do not Sync
	// this retirement: final receipt must consult the live connection state.
	delete(c.peerIDs.active, 0)
	if got, ambiguous := e.localResetMatch(raw, netip.AddrPort{}, peer, time.Now(), nil); got != nil || ambiguous {
		t.Fatal("stale token authorized receipt")
	}
	entry := other.peerIDs.active[0]
	entry.token[0] = 99
	other.peerIDs.active[0] = entry
	// Keep a stale/colliding index bucket but use the correct peer tuple. The
	// final constant-time comparison must reject its mismatched real token.
	if got, _ := e.localResetMatch(raw, netip.AddrPort{}, wrong, time.Now(), nil); got != nil {
		t.Fatal("index hit bypassed token verification")
	}
}

func TestResetTokenIndexMultipathRemoval(t *testing.T) {
	local := netip.MustParseAddrPort("192.0.2.1:443")
	c := resetPublicationBenchmarkConn(local, netip.MustParseAddrPort("192.0.2.2:444"), 2)
	index, err := newResetTokenIndex()
	if err != nil {
		t.Fatal(err)
	}
	index.sync(c)
	if len(index.tokens) != 2 {
		t.Fatal("path tokens missing")
	}
	c.multipath.abandoned = map[uint64]*pathAbandonment{1: {}}
	index.sync(c)
	if len(index.tokens) != 1 {
		t.Fatal("abandoned path retained")
	}
	c.closed = true
	index.sync(c)
	if len(index.tokens) != 0 || len(index.connections) != 0 {
		t.Fatal("closed owner retained")
	}
}

func TestRouterIndexedResetLiveEligibility(t *testing.T) {
	local := netip.MustParseAddrPort("192.0.2.1:443")
	peer := netip.MustParseAddrPort("192.0.2.2:444")
	wrong := netip.MustParseAddrPort("192.0.2.3:444")
	token := [16]byte{42}
	router, _ := NewRouter(8, 4)
	a := indexedResetConn("firstcid", token, true)
	b := indexedResetConn("othercid", token, false)
	for _, c := range []*Conn{a, b} {
		if err := router.Add(c, peer); err != nil {
			t.Fatal(err)
		}
	}
	router.enableResetIndex()
	raw := resetDatagram(token)
	check := func(want *Conn, reserved bool) {
		t.Helper()
		for _, network := range []bool{false, true} {
			got, known := router.Route(raw, peer)
			if network {
				got, known = router.RouteNetwork(raw, local, peer)
			}
			if got != want || known != reserved {
				t.Fatalf("network=%v: route=%p known=%v, want=%p known=%v", network, got, known, want, reserved)
			}
		}
	}
	check(a, true)
	entry := b.peerIDs.active[0]
	entry.used = true
	b.peerIDs.active[0] = entry
	if err := router.Sync(b); err != nil {
		t.Fatal(err)
	}
	check(nil, true)
	// Indexed membership is only a hint. Live state must reject stale entries.
	entry.used = false
	b.peerIDs.active[0] = entry
	check(a, true)
	delete(router.entries, a)
	check(nil, false)
	router.entries[a] = connectionRoute{peer: wrong}
	check(nil, false)
	router.entries[a] = connectionRoute{peer: peer}
	a.closed = true
	check(nil, false)
	a.closed = false
	check(a, true)
}

func TestRouterResetIndexPopulationTransitions(t *testing.T) {
	local := netip.MustParseAddrPort("192.0.2.1:443")
	peer := netip.MustParseAddrPort("192.0.2.2:444")
	router, _ := NewRouter(8, 4)
	token := [16]byte{42}
	a := indexedResetConn("firstcid", token, true)
	if err := router.Add(a, peer); err != nil {
		t.Fatal(err)
	}
	router.enableResetIndex()
	check := func(token [16]byte, want *Conn, reserved bool) {
		t.Helper()
		raw := resetDatagram(token)
		got, known := router.Route(raw, peer)
		if got != want || known != reserved {
			t.Fatalf("fixed: got %p/%v want %p/%v", got, known, want, reserved)
		}
		got, known = router.RouteNetwork(raw, local, peer)
		if got != want || known != reserved {
			t.Fatalf("network: got %p/%v want %p/%v", got, known, want, reserved)
		}
	}
	check(token, a, true)
	// Publish replacement while only one owner exists. Growing the population
	// must not resurrect the old token or lose the new token in indexed routing.
	next := [16]byte{43}
	id := a.peerIDs.active[0]
	id.token = next
	a.peerIDs.active[0] = id
	if err := router.Sync(a); err != nil {
		t.Fatal(err)
	}
	b := indexedResetConn("othercid", next, true)
	if err := router.Add(b, peer); err != nil {
		t.Fatal(err)
	}
	check(token, nil, false)
	check(next, nil, true)
	router.Remove(b)
	check(next, a, true)
	router.Remove(a)
	check(next, nil, false)
	if len(router.resetIndex.tokens) != 0 || len(router.resetIndex.connections) != 0 {
		t.Fatal("last owner removal retained index state")
	}
	if err := router.Add(b, peer); err != nil {
		t.Fatal(err)
	}
	check(next, b, true)
	if err := router.Add(a, peer); err != nil {
		t.Fatal(err)
	}
	check(next, nil, true)
}
