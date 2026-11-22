package quic

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestTokenStoreOwnershipConsumptionAndExpiry(t *testing.T) {
	now := time.Unix(100000, 0)
	s := NewLRUTokenStore(2, 2).(*lruTokenStore)
	s.now = func() time.Time { return now }
	original := &ClientToken{data: []byte("first"), version: Version1, received: now}
	s.Put("server/v1", original)
	original.data[0] = 'X'
	got := s.Pop("server/v1")
	if got == nil || string(got.data) != "first" {
		t.Fatal("input ownership")
	}
	if s.Pop("server/v1") != nil {
		t.Fatal("token reused")
	}
	duplicate := &ClientToken{data: []byte("first"), version: Version1, received: now}
	s.Put("server/v1", duplicate)
	if s.Pop("server/v1") != nil {
		t.Fatal("consumed duplicate resurrected")
	}
	got.data[0] = 'Y'
	s.Put("server/v1", duplicate)
	if s.Pop("server/v1") != nil {
		t.Fatal("returned bytes alias duplicate history")
	}
	s.Put("server/v2", &ClientToken{data: []byte("v2"), version: Version2, received: now})
	if s.Pop("server/v1") != nil {
		t.Fatal("cross-key token")
	}
	if got := s.Pop("server/v2"); got == nil || got.version != Version2 {
		t.Fatal("version lost")
	}
	s.Put("server/v1", &ClientToken{data: []byte("expires"), version: Version1, received: now})
	now = now.Add(clientTokenLifetime)
	if s.Pop("server/v1") != nil {
		t.Fatal("expired token")
	}
	for _, bad := range []*ClientToken{nil, {}, {data: []byte{1}, version: Version1, received: now.Add(time.Second)}, {data: []byte{1}, version: Version(42), received: now}, {data: make([]byte, maxClientTokenSize+1), version: Version1, received: now}} {
		s.Put("invalid", bad)
		if s.Pop("invalid") != nil {
			t.Fatal("invalid token retained")
		}
	}
}

func TestTokenStoreBoundsAndConcurrentPop(t *testing.T) {
	now := time.Now()
	s := NewLRUTokenStore(2, 2)
	put := func(key, value string) {
		s.Put(key, &ClientToken{data: []byte(value), version: Version1, received: now})
	}
	put("a", "a1")
	put("a", "a2")
	put("a", "a3")
	if got := s.Pop("a"); got == nil || string(got.data) != "a3" {
		t.Fatal("newest token not preferred")
	}
	if got := s.Pop("a"); got == nil || string(got.data) != "a2" {
		t.Fatal("per-origin capacity")
	}
	if s.Pop("a") != nil {
		t.Fatal("old token retained")
	}
	put("b", "b")
	s.Pop("a")
	put("c", "c")
	if s.Pop("b") != nil {
		t.Fatal("LRU origin retained")
	}
	concurrent := NewLRUTokenStore(1, 64)
	for i := 0; i < 64; i++ {
		concurrent.Put("origin", &ClientToken{data: []byte(fmt.Sprint(i)), version: Version1, received: now})
	}
	results := make(chan string, 128)
	var wg sync.WaitGroup
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if token := concurrent.Pop("origin"); token != nil {
				results <- string(token.data)
			}
		}()
	}
	wg.Wait()
	close(results)
	seen := make(map[string]bool)
	for value := range results {
		if seen[value] {
			t.Fatal("concurrent reuse", value)
		}
		seen[value] = true
	}
	if len(seen) != 64 {
		t.Fatal("lost tokens", len(seen))
	}
}

func TestTokenStoreMaximumPayload(t *testing.T) {
	s := NewLRUTokenStore(1, 1)
	payload := bytes.Repeat([]byte{7}, maxClientTokenSize)
	s.Put("origin", &ClientToken{data: payload, version: Version1, received: time.Now()})
	got := s.Pop("origin")
	if got == nil || !bytes.Equal(got.data, payload) {
		t.Fatal("maximum payload lost")
	}
}

func TestTokenStoreConcurrentDuplicatePut(t *testing.T) {
	s := NewLRUTokenStore(1, 1)
	token := &ClientToken{data: []byte("same token"), version: Version1, received: time.Now()}
	var wg sync.WaitGroup
	results := make(chan *ClientToken, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Put("origin", token)
			if got := s.Pop("origin"); got != nil {
				results <- got
			}
		}()
	}
	wg.Wait()
	close(results)
	count := 0
	for range results {
		count++
	}
	if count != 1 {
		t.Fatal("duplicate delivery", count)
	}
}

func TestTokenStoreInvalidLimits(t *testing.T) {
	for _, limits := range [][2]int{{0, 1}, {1, 0}, {-1, 1}, {1, -1}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("invalid capacity accepted", limits)
				}
			}()
			NewLRUTokenStore(limits[0], limits[1])
		}()
	}
}
