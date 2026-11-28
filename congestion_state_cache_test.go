package quic

import (
	"fmt"
	"testing"
	"time"
)

// Take removes what it returns, so one connection at a time resumes from a
// state; Put replaces a client's state; Delete and Flush drop states.
func TestMemoryCongestionStateCacheTakePut(t *testing.T) {
	c := NewMemoryCongestionStateCache(0, 0)
	if c.maxEntries != defaultCongestionStateEntries || c.lifetime != defaultCongestionStateLifetime {
		t.Fatal("defaults", c.maxEntries, c.lifetime)
	}
	a := CongestionState{Bandwidth: 1e6, MinRTT: 50 * time.Millisecond}
	b := CongestionState{Bandwidth: 2e6, MinRTT: 60 * time.Millisecond}
	c.Put("192.0.2.1", a)
	c.Put("192.0.2.1", b)
	if c.Len() != 1 {
		t.Fatal("Put didn't replace", c.Len())
	}
	if s, ok := c.Take("192.0.2.1"); !ok || s.Bandwidth != b.Bandwidth || s.MinRTT != b.MinRTT {
		t.Fatal("take", s, ok)
	}
	if _, ok := c.Take("192.0.2.1"); ok {
		t.Fatal("a second Take of one state")
	}
	c.Put("192.0.2.1", a)
	c.Delete("192.0.2.1")
	c.Put("192.0.2.2", a)
	c.Put("192.0.2.3", a)
	c.Flush()
	if c.Len() != 0 {
		t.Fatal("after Delete and Flush", c.Len())
	}
}

// The cache holds at most maxEntries states, evicting the least recently
// saved, and Take drops a state older than its lifetime.
func TestMemoryCongestionStateCacheBounds(t *testing.T) {
	now := time.Unix(1000, 0)
	c := NewMemoryCongestionStateCache(2, time.Minute)
	c.now = func() time.Time { return now }
	s := CongestionState{Bandwidth: 1e6, MinRTT: 50 * time.Millisecond}
	c.Put("a", s)
	c.Put("b", s)
	c.Put("a", s)
	c.Put("c", s)
	if _, ok := c.Take("b"); ok || c.Len() != 2 {
		t.Fatal("least recently saved not evicted", c.Len())
	}
	now = now.Add(time.Minute + time.Second)
	if _, ok := c.Take("a"); ok {
		t.Fatal("an expired state taken")
	}
	if c.Len() != 1 {
		t.Fatal("expired state kept", c.Len())
	}
	// A state put back keeps its measured time, and expires by it.
	c.Put("d", s)
	got, _ := c.Take("d")
	if !got.Saved.Equal(now) {
		t.Fatal("taken state's time", got.Saved, now)
	}
	now = now.Add(50 * time.Second)
	c.Put("d", got)
	now = now.Add(20 * time.Second)
	if _, ok := c.Take("d"); ok {
		t.Fatal("putting a state back extended its lifetime")
	}
	// A state measured longer ago than the lifetime isn't kept.
	c.Put("f", CongestionState{Bandwidth: 1e6, MinRTT: time.Millisecond, Saved: now.Add(-2 * time.Minute)})
	if _, ok := c.Take("f"); ok {
		t.Fatal("an expired state put")
	}
	// Putting back a state doesn't undo a newer one saved meanwhile.
	old := CongestionState{Bandwidth: 9e6, MinRTT: time.Millisecond, Saved: now.Add(-time.Minute / 2)}
	c.Put("e", CongestionState{Bandwidth: 1e6, MinRTT: time.Millisecond})
	c.Put("e", old)
	if got, _ := c.Take("e"); got.Bandwidth != 1e6 {
		t.Fatal("an older state put back replaced a newer one")
	}
}

// One address holds at most 32 resume-ID states, its least recently saved
// evicted first, so it can't evict other clients' states; address keys and
// other addresses' IDs are untouched.
func TestMemoryCongestionStateCacheGroups(t *testing.T) {
	c := NewMemoryCongestionStateCache(0, 0)
	s := CongestionState{Bandwidth: 1e6, MinRTT: time.Millisecond}
	c.Put("192.0.2.1", s)
	c.Put("id:198.51.100.9/aa", s)
	for i := range 40 {
		c.Put(fmt.Sprintf("id:192.0.2.1/%02d", i), s)
	}
	if c.Len() != 2+maxCongestionStatesPerGroup || c.groupLen("id:192.0.2.1") != maxCongestionStatesPerGroup {
		t.Fatal("entries", c.Len(), c.groups)
	}
	for _, key := range []string{"192.0.2.1", "id:198.51.100.9/aa", "id:192.0.2.1/39", "id:192.0.2.1/08"} {
		if _, ok := c.Take(key); !ok {
			t.Fatal("evicted", key)
		}
	}
	if _, ok := c.Take("id:192.0.2.1/07"); ok {
		t.Fatal("the group's oldest kept")
	}
	if c.groupLen("id:192.0.2.1") != maxCongestionStatesPerGroup-2 {
		t.Fatal("group count", c.groups)
	}
	c.Flush()
	if len(c.groups) != 0 {
		t.Fatal("Flush kept groups")
	}
}

// In a small cache one address may hold at most an eighth of it, so its
// resume IDs can't evict the others.
func TestMemoryCongestionStateCacheSmallGroups(t *testing.T) {
	c := NewMemoryCongestionStateCache(16, 0)
	s := CongestionState{Bandwidth: 1e6, MinRTT: time.Millisecond}
	for i := range 14 {
		c.Put(fmt.Sprintf("192.0.2.%d", i), s)
	}
	for i := range 20 {
		c.Put(fmt.Sprintf("id:198.51.100.1/%02d", i), s)
	}
	if c.groupLen("id:198.51.100.1") != 2 || c.Len() != 16 {
		t.Fatal("entries", c.Len(), c.groups)
	}
	for i := range 14 {
		if _, ok := c.Take(fmt.Sprintf("192.0.2.%d", i)); !ok {
			t.Fatal("evicted another address's state", i)
		}
	}
}

// The listener adapter skips states that couldn't be resumed from.
func TestCongestionStatesAdapter(t *testing.T) {
	c := NewMemoryCongestionStateCache(0, 0)
	cfg := Config{CongestionStateCache: c}
	states := cfg.congestionStates()
	c.Put("a", CongestionState{MinRTT: time.Millisecond})
	if _, ok := states.Take("a"); ok {
		t.Fatal("a state without bandwidth")
	}
	c.Put("a", CongestionState{Bandwidth: 1e6, MinRTT: 20 * time.Millisecond})
	if s, ok := states.Take("a"); !ok || s.Bandwidth != 1e6 || s.MinRTT != 20*time.Millisecond {
		t.Fatal("take", s, ok)
	}
	if (&Config{}).congestionStates() != nil {
		t.Fatal("no cache, yet states")
	}
	if (&Config{}).carefulResumeMaxJump() != 4<<20 || (&Config{CarefulResumeMaxJump: 1 << 20}).carefulResumeMaxJump() != 1<<20 {
		t.Fatal("jump cap")
	}
}
