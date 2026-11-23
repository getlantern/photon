package quic

import (
	"container/list"
	"crypto/sha256"
	"sync"
	"time"
)

const maxClientTokenSize = 512
const clientTokenLifetime = 24 * time.Hour

// ClientToken is opaque address-validation evidence received from a server.
// It is unrelated to TLS session tickets or permission to send early data.
type ClientToken struct {
	data     []byte
	version  Version
	received time.Time
}

// TokenStore stores tokens under an authoritative server/version key supplied by
// the client. Implementations must be concurrency-safe. Pop consumes the token;
// a token must never be returned to two concurrent connection attempts.
// Put runs on the connection owner; it must return promptly and must not call
// connection methods synchronously.
type TokenStore interface {
	Pop(key string) *ClientToken
	Put(key string, token *ClientToken)
}

type tokenEntry struct {
	token    *ClientToken
	hash     [32]byte
	received time.Time
}
type tokenOrigin struct {
	key     string
	entries []tokenEntry
}
type lruTokenStore struct {
	mu                    sync.Mutex
	origins               map[string]*list.Element
	order                 list.List
	maxOrigins, perOrigin int
	now                   func() time.Time
}

// NewLRUTokenStore creates a bounded token store. It retains the most recent
// tokens, consumes each at most once, and keeps bounded duplicate history even
// after consumption. Evicting an origin or old entry also evicts that history.
// Tokens larger than 512 bytes or older than 24 hours are discarded. Limits
// must be positive; invalid limits panic. Returned tokens own their byte storage.
func NewLRUTokenStore(maxOrigins, tokensPerOrigin int) TokenStore {
	if maxOrigins <= 0 || tokensPerOrigin <= 0 {
		panic("quic: token store limits must be positive")
	}
	return &lruTokenStore{origins: make(map[string]*list.Element), maxOrigins: maxOrigins, perOrigin: tokensPerOrigin, now: time.Now}
}
func cloneClientToken(t *ClientToken) *ClientToken {
	if t == nil {
		return nil
	}
	return &ClientToken{data: append([]byte(nil), t.data...), version: t.version, received: t.received}
}
func tokenCurrent(received, now time.Time) bool {
	return !received.IsZero() && !received.After(now) && now.Sub(received) < clientTokenLifetime
}
func (s *lruTokenStore) Put(key string, t *ClientToken) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if key == "" || t == nil || len(t.data) == 0 || len(t.data) > maxClientTokenSize || !t.version.Supported() || !tokenCurrent(t.received, now) {
		return
	}
	hash := sha256.Sum256(t.data)
	e := s.origins[key]
	if e == nil {
		if len(s.origins) == s.maxOrigins {
			old := s.order.Back()
			delete(s.origins, old.Value.(*tokenOrigin).key)
			s.order.Remove(old)
		}
		e = s.order.PushFront(&tokenOrigin{key: key})
		s.origins[key] = e
	}
	origin := e.Value.(*tokenOrigin)
	live := origin.entries[:0]
	duplicate := false
	for _, entry := range origin.entries {
		if tokenCurrent(entry.received, now) {
			live = append(live, entry)
			duplicate = duplicate || entry.hash == hash
		}
	}
	clear(origin.entries[len(live):])
	origin.entries = live
	if duplicate {
		return
	}
	s.order.MoveToFront(e)
	if len(origin.entries) == s.perOrigin {
		copy(origin.entries, origin.entries[1:])
		origin.entries = origin.entries[:len(origin.entries)-1]
	}
	origin.entries = append(origin.entries, tokenEntry{token: cloneClientToken(t), hash: hash, received: t.received})
}
func (s *lruTokenStore) Pop(key string) *ClientToken {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.origins[key]
	if e == nil {
		return nil
	}
	s.order.MoveToFront(e)
	origin := e.Value.(*tokenOrigin)
	now := s.now()
	for i := len(origin.entries) - 1; i >= 0; i-- {
		entry := &origin.entries[i]
		if !tokenCurrent(entry.received, now) {
			entry.token = nil
			continue
		}
		if entry.token != nil {
			result := entry.token
			entry.token = nil
			return result
		}
	}
	return nil
}
