package transport

import (
	"bytes"
	"testing"
)

func TestLocalIDs(t *testing.T) {
	l := newLocalIDs([]byte("initial0"))
	frames, err := l.issue(2)
	if err != nil || len(frames) != 1 || frames[0].Sequence != 1 || len(frames[0].ConnectionID) != 8 {
		t.Fatal(frames, err)
	}
	if l.accept(frames[0].ConnectionID) {
		t.Fatal("unsent CID routed")
	}
	if _, err := l.retire(1, []byte("initial0")); err == nil {
		t.Fatal("unsent CID retired")
	}
	l.markSent(1)
	if !l.accept(frames[0].ConnectionID) || !l.accept([]byte("initial0")) {
		t.Fatal("routing IDs")
	}
	if _, err := l.retire(0, []byte("initial0")); err == nil {
		t.Fatal("retired packet destination")
	}
	if _, err := l.retire(2, frames[0].ConnectionID); err == nil {
		t.Fatal("retired unissued ID")
	}
	if changed, err := l.retire(0, frames[0].ConnectionID); err != nil || !changed {
		t.Fatal(changed, err)
	}
	if l.accept([]byte("initial0")) {
		t.Fatal("retired ID still routed")
	}
	if changed, err := l.retire(0, frames[0].ConnectionID); err != nil || changed {
		t.Fatal("duplicate retirement", changed, err)
	}
	next, err := l.issue(2)
	if err != nil || len(next) != 1 || next[0].Sequence != 2 || bytes.Equal(next[0].ConnectionID, frames[0].ConnectionID) {
		t.Fatal(next, err)
	}
	saved := append([]byte(nil), l.active[1].cid...)
	frames[0].ConnectionID[0] ^= 1
	if !bytes.Equal(l.active[1].cid, saved) {
		t.Fatal("owned CID changed")
	}
	zero := newLocalIDs(nil)
	if frames, err := zero.issue(100); err != nil || len(frames) != 0 {
		t.Fatal("issued after zero-length CID")
	}
	if _, err := zero.retire(0, nil); err == nil {
		t.Fatal("retired zero-length CID")
	}
}
func TestEngineCIDRotation(t *testing.T) {
	client, server := testPair(t, 1)
	drivePair(t, client, server, false, false)
	for round := 0; round < 4; round++ {
		for _, c := range []*Conn{client, server} {
			if err := c.RotatePeerID(); err != nil {
				t.Fatal(err)
			}
		}
		drivePair(t, client, server, true, false)
		if !server.localIDs.accept(client.peerCID) || !client.localIDs.accept(server.peerCID) {
			t.Fatal("rotated CID not routed")
		}
		if len(client.peerIDs.active) > 2 || len(server.peerIDs.active) > 2 {
			t.Fatal("peer ID limit")
		}
	}
}

func TestLocalIDLifetimeBound(t *testing.T) {
	l := newLocalIDs([]byte("initial0"))
	for i := 0; i < 4096; i++ {
		l.used[string([]byte{byte(i >> 8), byte(i)})] = struct{}{}
	}
	frames, err := l.issue(4)
	if err != nil || len(frames) != 0 || !l.accept([]byte("initial0")) {
		t.Fatal("lifetime limit must preserve active IDs", err)
	}
}
