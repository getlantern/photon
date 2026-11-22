package interop

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

// Freeze one upstream-issued ticket with a fixture-enabled EarlyData flag so concurrent consumers
// exercise ownership of the same cache object. Deliberate reuse is a test fixture,
// not a suggested application ticket policy.
type sharedTicketCache struct {
	mu      sync.Mutex
	key     string
	session *tls.ClientSessionState
	frozen  bool
	stored  chan struct{}
}

func (c *sharedTicketCache) Get(key string) (*tls.ClientSessionState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.session, key == c.key && c.session != nil
}
func (c *sharedTicketCache) Put(key string, session *tls.ClientSessionState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		return
	}
	c.key, c.session = key, session
	if session != nil {
		select {
		case c.stored <- struct{}{}:
		default:
		}
	}
}
func (c *sharedTicketCache) freeze() *tls.ClientSessionState {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.frozen = true
	return c.session
}

func TestConcurrentClientResumptionSharedEarlyTicket(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			cache := &sharedTicketCache{stored: make(chan struct{}, 1)}
			clientTLS.ClientSessionCache = cache
			cfg := &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, Allow0RTT: true}
			l, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			seed, err := quic.DialAddr(ctx, l.Addr().String(), clientTLS, &quic.Config{Versions: []quic.Version{v}})
			if err != nil {
				t.Fatal(err)
			}
			defer seed.CloseWithError(0, "")
			seedPeer, err := l.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer seedPeer.CloseWithError(0, "")
			select {
			case <-cache.stored:
			case <-ctx.Done():
				t.Fatal("missing upstream ticket", ctx.Err())
			}
			saved := cache.freeze()
			ticket, state, err := saved.ResumptionState()
			if err != nil || state == nil {
				t.Fatal("missing seeded session", err)
			}
			// The ticket came from this version via our client, which recorded its
			// origin. Simulate another cache consumer permitting early data locally.
			// This mutation occurs before concurrent readers start.
			state.EarlyData = true
			before, err := state.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			ticketBefore := bytes.Clone(ticket)
			seed.CloseWithError(0, "")
			seedPeer.CloseWithError(0, "")

			const concurrent = 6
			type result struct {
				conn *quic.Conn
				err  error
			}
			results := make(chan result, concurrent)
			var wg sync.WaitGroup
			defer func() {
				cancel()
				wg.Wait()
				close(results)
				for r := range results {
					if r.conn != nil {
						r.conn.CloseWithError(0, "")
					}
				}
			}()
			for range concurrent {
				wg.Add(1)
				go func() {
					defer wg.Done()
					c, err := quic.DialAddr(ctx, l.Addr().String(), clientTLS, &quic.Config{Versions: []quic.Version{v}, LocalAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}})
					results <- result{c, err}
				}()
			}
			peers := make(map[string]*upstream.Conn)
			for range concurrent {
				p, err := l.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer p.CloseWithError(0, "")
				if !p.ConnectionState().TLS.DidResume || p.ConnectionState().Used0RTT {
					t.Fatal("upstream did not resume without early data")
				}
				peers[p.RemoteAddr().String()] = p
			}
			for range concurrent {
				r := <-results
				if r.conn != nil {
					defer r.conn.CloseWithError(0, "")
				}
				if r.err != nil {
					t.Fatal(r.err)
				}
				c := r.conn
				if !c.ConnectionState().TLS.DidResume || len(c.ConnectionState().TLS.VerifiedChains) == 0 {
					t.Fatal("missing resumed authenticated identity")
				}
				p := peers[c.LocalAddr().String()]
				if p == nil {
					t.Fatal("missing independent peer")
				}
				tx, err := c.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				tx.SetWriteDeadline(time.Now().Add(time.Second))
				if _, err := tx.Write([]byte("shared session")); err != nil {
					t.Fatal(err)
				}
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				rx, err := p.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				rx.SetReadDeadline(time.Now().Add(time.Second))
				if b, err := io.ReadAll(rx); err != nil || string(b) != "shared session" {
					t.Fatalf("payload %q: %v", b, err)
				}
			}
			after, err := state.Bytes()
			if err != nil || !state.EarlyData || !bytes.Equal(before, after) || !bytes.Equal(ticket, ticketBefore) {
				t.Fatal("shared source session mutated", err)
			}
		})
	}
}
