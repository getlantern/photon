package quic

import (
	"container/list"
	"sync"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/transport"
)

// CongestionState is a client path's congestion state, as a server saves
// it for Careful Resume.
type CongestionState struct {
	// Bandwidth is the path's bottleneck bandwidth in bytes per second.
	Bandwidth uint64
	MinRTT    time.Duration
	// Saved is when the state was measured. A connection that took a state
	// and measured none of its own puts it back with its Saved unchanged,
	// so that this doesn't extend its lifetime; zero means now.
	Saved time.Time
}

// CongestionStateCache keeps servers' saved congestion state for Careful
// Resume (draft-ietf-tsvwg-careful-resume), keyed by client IP address. A
// connection Takes its client's state on admission, so that one connection
// at a time resumes from it, and Puts its own at close. Put must replace a
// state only if it was measured no later than the one put, by Saved: a
// connection puts back a state it took and didn't use, which mustn't undo
// a newer one another connection from the address saved meanwhile. Delete
// drops a state whose jump met congestion. Implementations must be safe
// for concurrent use.
type CongestionStateCache interface {
	Take(key string) (CongestionState, bool)
	Put(key string, s CongestionState)
	Delete(key string)
}

// MemoryCongestionStateCache is an in-memory CongestionStateCache that
// holds at most a fixed number of states, evicting the least recently
// saved, and drops each a fixed lifetime after it was measured. It is never
// persisted. Create one with NewMemoryCongestionStateCache.
type MemoryCongestionStateCache struct {
	mu         sync.Mutex
	maxEntries int
	lifetime   time.Duration
	order      *list.List // most recently saved first
	entries    map[string]*list.Element
	now        func() time.Time
}

type savedCongestionState struct {
	key   string
	state CongestionState
	saved time.Time
}

const (
	defaultCongestionStateEntries  = 4096
	defaultCongestionStateLifetime = 10 * time.Minute
)

// NewMemoryCongestionStateCache returns a cache of at most maxEntries
// states, each kept for lifetime. Zero or negative values select 4096
// entries and 10 minutes: the draft asks for lifetimes of minutes on paths
// whose capacity changes, as mobile ones do.
func NewMemoryCongestionStateCache(maxEntries int, lifetime time.Duration) *MemoryCongestionStateCache {
	if maxEntries <= 0 {
		maxEntries = defaultCongestionStateEntries
	}
	if lifetime <= 0 {
		lifetime = defaultCongestionStateLifetime
	}
	return &MemoryCongestionStateCache{maxEntries: maxEntries, lifetime: lifetime, order: list.New(), entries: map[string]*list.Element{}, now: time.Now}
}

// Take implements CongestionStateCache.
func (c *MemoryCongestionStateCache) Take(key string) (CongestionState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return CongestionState{}, false
	}
	c.remove(e)
	s := e.Value.(savedCongestionState)
	if c.now().Sub(s.saved) > c.lifetime {
		return CongestionState{}, false
	}
	s.state.Saved = s.saved
	return s.state, true
}

// Put implements CongestionStateCache.
func (c *MemoryCongestionStateCache) Put(key string, s CongestionState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	saved := s.Saved
	if saved.IsZero() {
		saved = c.now()
	}
	if e, ok := c.entries[key]; ok {
		if e.Value.(savedCongestionState).saved.After(saved) {
			return
		}
		c.remove(e)
	}
	c.entries[key] = c.order.PushFront(savedCongestionState{key, s, saved})
	for c.order.Len() > c.maxEntries {
		c.remove(c.order.Back())
	}
}

// Delete implements CongestionStateCache.
func (c *MemoryCongestionStateCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[key]; ok {
		c.remove(e)
	}
}

// Flush drops every saved state.
func (c *MemoryCongestionStateCache) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.order.Init()
	clear(c.entries)
}

// Len reports how many states the cache holds, expired ones included.
func (c *MemoryCongestionStateCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

func (c *MemoryCongestionStateCache) remove(e *list.Element) {
	delete(c.entries, e.Value.(savedCongestionState).key)
	c.order.Remove(e)
}

// congestionStates adapts a CongestionStateCache to the transport's.
type congestionStates struct{ cache CongestionStateCache }

func (s congestionStates) Take(key string) (recovery.ResumeState, bool) {
	v, ok := s.cache.Take(key)
	if !ok || v.Bandwidth == 0 || v.MinRTT <= 0 {
		return recovery.ResumeState{}, false
	}
	return recovery.ResumeState{Bandwidth: float64(v.Bandwidth), MinRTT: v.MinRTT, Saved: v.Saved}, true
}

func (s congestionStates) Put(key string, v recovery.ResumeState) {
	s.cache.Put(key, CongestionState{Bandwidth: uint64(v.Bandwidth), MinRTT: v.MinRTT, Saved: v.Saved})
}

func (s congestionStates) Delete(key string) { s.cache.Delete(key) }

const defaultCarefulResumeMaxJump = 4 << 20

func (c *Config) carefulResumeMaxJump() uint64 {
	if c.CarefulResumeMaxJump == 0 {
		return defaultCarefulResumeMaxJump
	}
	return c.CarefulResumeMaxJump
}

func (c *Config) congestionStates() transport.CongestionStates {
	if c.CongestionStateCache == nil {
		return nil
	}
	return congestionStates{c.CongestionStateCache}
}
