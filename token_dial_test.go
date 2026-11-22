package quic

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"
)

type signalingTokenStore struct {
	TokenStore
	stored chan struct{}
	popped chan bool
}

func (s *signalingTokenStore) Put(key string, token *ClientToken) {
	s.TokenStore.Put(key, token)
	select {
	case s.stored <- struct{}{}:
	default:
	}
}
func (s *signalingTokenStore) Pop(key string) *ClientToken {
	token := s.TokenStore.Pop(key)
	s.popped <- (token != nil)
	return token
}

func TestDialAddressTokenReuse(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			ct, st := dialTLS(t)
			listener, err := ListenAddr("127.0.0.1:0", st, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cache := &signalingTokenStore{TokenStore: NewLRUTokenStore(4, 4), stored: make(chan struct{}, 4), popped: make(chan bool, 4)}
			for round := 0; round < 3; round++ {
				c, err := DialAddr(ctx, listener.Addr().String(), ct, &Config{Versions: []Version{version}, TokenStore: cache})
				if err != nil {
					t.Fatal(round, err)
				}
				if used := <-cache.popped; used != (round > 0) {
					t.Fatalf("round %d cached token %v", round, used)
				}
				if _, err := listener.Accept(ctx); err != nil {
					t.Fatal(err)
				}
				select {
				case <-cache.stored:
				case <-ctx.Done():
					t.Fatal("NEW_TOKEN not stored", ctx.Err())
				}
				if err := c.CloseWithError(0, ""); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestClientTokenAuthority(t *testing.T) {
	key := clientTokenKey("Example.COM", 443, Version1)
	if key != clientTokenKey("example.com", 443, Version1) {
		t.Fatal("case normalization")
	}
	for _, other := range []string{
		clientTokenKey("other.example", 443, Version1),
		clientTokenKey("example.com", 8443, Version1),
		clientTokenKey("example.com", 443, Version2),
	} {
		if key == other {
			t.Fatal("token authority collision")
		}
	}
}

func TestCompatibleDialTokenAndSessionReuse(t *testing.T) {
	for _, original := range []Version{Version1, Version2} {
		selected := Version1
		if selected == original {
			selected = Version2
		}
		t.Run(fmt.Sprintf("%x", original), func(t *testing.T) {
			ct, st := dialTLS(t)
			ct.ClientSessionCache = tls.NewLRUClientSessionCache(4)
			listener, err := ListenAddr("127.0.0.1:0", st, &Config{Versions: []Version{selected, original}, EnableCompatibleVersionNegotiation: true})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cache := &signalingTokenStore{TokenStore: NewLRUTokenStore(4, 4), stored: make(chan struct{}, 4), popped: make(chan bool, 4)}
			for round, first := range []Version{original, selected, original} {
				other := original
				if first == original {
					other = selected
				}
				c, err := DialAddr(ctx, listener.Addr().String(), ct, &Config{Versions: []Version{first, other}, EnableCompatibleVersionNegotiation: true, TokenStore: cache})
				if err != nil {
					t.Fatal(round, err)
				}
				server, err := listener.Accept(ctx)
				if err != nil {
					c.CloseWithError(0, "")
					t.Fatal(err)
				}
				wantReuse := round == 1
				if used := <-cache.popped; used != wantReuse {
					t.Fatalf("round=%d token reused=%t want=%t", round, used, wantReuse)
				}
				if c.ConnectionState().Version != selected || server.ConnectionState().Version != selected {
					t.Fatal("wrong negotiated version")
				}
				if c.ConnectionState().TLS.DidResume != wantReuse || server.ConnectionState().TLS.DidResume != wantReuse {
					t.Fatalf("round=%d wrong session provenance", round)
				}
				select {
				case <-cache.stored:
				case <-ctx.Done():
					t.Fatal("NEW_TOKEN not stored", ctx.Err())
				}
				// Ticket processing runs on the owner; wait until a negotiated-version ticket
				// is visible before starting the next attempt.
				for {
					saved, ok := ct.ClientSessionCache.Get("127.0.0.1")
					if ok && saved != nil {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("session ticket not cached")
					case <-time.After(time.Millisecond):
					}
				}
				if err := c.CloseWithError(0, ""); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
