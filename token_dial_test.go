package quic

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

type signalingTokenStore struct {
	TokenStore
	stored chan struct{}
	popped chan bool
	issued sync.Map // token bytes, as a string
}

func (s *signalingTokenStore) Put(key string, token *ClientToken) {
	s.issued.Store(string(token.data), true)
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

// initialTrace records, per client port, the token of every Initial datagram
// that reached the server and the number of Retry packets sent back.
type initialTrace struct {
	mu      sync.Mutex
	tokens  map[int][]string
	retries map[int]int
}

// retryListener matches ListenAddr's policy of requiring Retry from a source
// without valid address evidence, on a socket whose packets are recorded.
func retryListener(t *testing.T, tlsConfig *tls.Config, config *Config) (*Listener, *initialTrace) {
	t.Helper()
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { socket.Close() })
	trace := &initialTrace{tokens: make(map[int][]string), retries: make(map[int]int)}
	record := func(sent bool) func([]byte, net.Addr) {
		return func(b []byte, addr net.Addr) {
			h, err := wire.ParseHeaderWithQUICBit(b, 0, true)
			if err != nil || !h.Long {
				return
			}
			port := addr.(*net.UDPAddr).Port
			trace.mu.Lock()
			defer trace.mu.Unlock()
			if sent && h.Type == wire.Retry {
				trace.retries[port]++
			} else if !sent && h.Type == wire.Initial {
				trace.tokens[port] = append(trace.tokens[port], string(h.Token))
			}
		}
	}
	tr := &Transport{Conn: &packetTap{PacketConn: socket, in: record(false), out: record(true)}, VerifySourceAddress: func(net.Addr) bool { return true }}
	t.Cleanup(func() { tr.Close() })
	listener, err := tr.Listen(tlsConfig, config)
	if err != nil {
		t.Fatal(err)
	}
	return listener, trace
}

// check requires a reusing dial to open with a cached NEW_TOKEN and draw no
// Retry, and any other dial to open without a token and be sent a Retry.
func (r *initialTrace) check(t *testing.T, c *Conn, cache *signalingTokenStore, reuse bool) {
	t.Helper()
	port := c.LocalAddr().(*net.UDPAddr).Port
	r.mu.Lock()
	defer r.mu.Unlock()
	tokens, retries := r.tokens[port], r.retries[port]
	if len(tokens) == 0 {
		t.Fatal("no Initial observed")
	}
	if !reuse {
		if tokens[0] != "" || retries == 0 {
			t.Fatalf("first Initial token length %d, Retry packets %d; want no token and a Retry", len(tokens[0]), retries)
		}
		return
	}
	if retries != 0 {
		t.Fatalf("cached token did not avoid Retry: %d sent", retries)
	}
	// Handshake packets coalesced behind an Initial are not separate entries.
	for _, token := range tokens {
		if _, ok := cache.issued.Load(token); !ok || token == "" {
			t.Fatalf("Initial carried a %d-byte token that is not a cached NEW_TOKEN", len(token))
		}
	}
}

func TestDialAddressTokenReuse(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			ct, st := dialTLS(t)
			listener, trace := retryListener(t, st, nil)
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
				trace.check(t, c, cache, round > 0)
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
			listener, trace := retryListener(t, st, &Config{Versions: []Version{selected, original}, EnableCompatibleVersionNegotiation: true})
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
				trace.check(t, c, cache, wantReuse)
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
