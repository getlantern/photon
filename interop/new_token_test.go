package interop

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/wire"
	upstream "github.com/quic-go/quic-go"
)

type observedTokenStore struct {
	upstream.TokenStore
	stored chan struct{}
}

func (s *observedTokenStore) Put(key string, token *upstream.ClientToken) {
	s.TokenStore.Put(key, token)
	select {
	case s.stored <- struct{}{}:
	default:
	}
}

type tokenPacketObserver struct {
	net.PacketConn
	retries       atomic.Int32
	initialSeen   atomic.Bool
	firstHasToken atomic.Bool
}

func (s *tokenPacketObserver) ReadFrom(b []byte) (int, net.Addr, error) {
	n, a, err := s.PacketConn.ReadFrom(b)
	if err == nil {
		if h, e := wire.ParseHeader(b[:n], 0); e == nil && h.Type == wire.Retry {
			s.retries.Add(1)
		}
	}
	return n, a, err
}
func (s *tokenPacketObserver) WriteTo(b []byte, a net.Addr) (int, error) {
	n, err := s.PacketConn.WriteTo(b, a)
	if err == nil && n == len(b) {
		if h, e := wire.ParseHeader(b, 0); e == nil && h.Type == wire.Initial && s.initialSeen.CompareAndSwap(false, true) {
			s.firstHasToken.Store(len(h.Token) > 0)
		}
	}
	return n, err
}

func TestServerNewTokenUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			listener, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{v}, EnableDatagrams: true})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cache := &observedTokenStore{TokenStore: upstream.NewLRUTokenStore(4, 4), stored: make(chan struct{}, 4)}
			var previousPort int
			for round := 0; round < 4; round++ {
				if round == 2 {
					address := listener.Addr().String()
					if err := listener.Close(); err != nil {
						t.Fatal(err)
					}
					listener, err = quic.ListenAddr(address, st, &quic.Config{Versions: []quic.Version{v}, EnableDatagrams: true})
					if err != nil {
						t.Fatal(err)
					}
					defer listener.Close()
				}
				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer socket.Close()
				port := socket.LocalAddr().(*net.UDPAddr).Port
				if port == previousPort {
					t.Fatal("fixture reused port")
				}
				previousPort = port
				observer := &tokenPacketObserver{PacketConn: socket}
				tr := &upstream.Transport{Conn: observer}
				defer tr.Close()
				c, err := tr.Dial(ctx, listener.Addr(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, TokenStore: cache, EnableDatagrams: true})
				if err != nil {
					t.Fatal(err)
				}
				defer c.CloseWithError(0, "")
				peer, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "")
				select {
				case <-cache.stored:
				case <-ctx.Done():
					t.Fatal("NEW_TOKEN not stored", ctx.Err())
				}
				if observer.firstHasToken.Load() != (round > 0) {
					t.Fatal("wrong first Initial token state", round)
				}
				if count := observer.retries.Load(); (round%2 == 0 && count == 0) || (round%2 == 1 && count != 0) {
					t.Fatal("Retry count", round, count)
				}
				if err := c.SendDatagram([]byte("token reuse")); err != nil {
					t.Fatal(err)
				}
				payload, err := peer.ReceiveDatagram(ctx)
				if err != nil || string(payload) != "token reuse" {
					t.Fatal(string(payload), err)
				}
				c.CloseWithError(0, "")
				peer.CloseWithError(0, "")
			}
		})
	}
}

type ownObservedTokenStore struct {
	quic.TokenStore
	stored chan struct{}
}

func (s *ownObservedTokenStore) Put(key string, token *quic.ClientToken) {
	s.TokenStore.Put(key, token)
	select {
	case s.stored <- struct{}{}:
	default:
	}
}

type tokenServerObserver struct {
	net.PacketConn
	retries atomic.Int32
	seen    map[string]bool // one socket reader
	first   chan bool
}

func (s *tokenServerObserver) ReadFrom(b []byte) (int, net.Addr, error) {
	n, a, err := s.PacketConn.ReadFrom(b)
	if err == nil {
		if h, e := wire.ParseHeader(b[:n], 0); e == nil && h.Type == wire.Initial && !s.seen[a.String()] {
			s.seen[a.String()] = true
			s.first <- (len(h.Token) > 0)
		}
	}
	return n, a, err
}
func (s *tokenServerObserver) WriteTo(b []byte, a net.Addr) (int, error) {
	n, err := s.PacketConn.WriteTo(b, a)
	if err == nil && n == len(b) {
		if h, e := wire.ParseHeader(b, 0); e == nil && h.Type == wire.Retry {
			s.retries.Add(1)
		}
	}
	return n, err
}
func TestClientNewTokenUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			observer := &tokenServerObserver{PacketConn: socket, seen: make(map[string]bool), first: make(chan bool, 8)}
			tr := &upstream.Transport{Conn: observer, VerifySourceAddress: func(net.Addr) bool { return true }}
			defer tr.Close()
			listener, err := tr.Listen(st, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, EnableDatagrams: true})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cache := &ownObservedTokenStore{TokenStore: quic.NewLRUTokenStore(4, 4), stored: make(chan struct{}, 8)}
			for round := 0; round < 3; round++ {
				before := observer.retries.Load()
				c, err := quic.DialAddr(ctx, listener.Addr().String(), ct, &quic.Config{Versions: []quic.Version{v}, TokenStore: cache, EnableDatagrams: true})
				if err != nil {
					t.Fatal(round, err)
				}
				defer c.CloseWithError(0, "")
				peer, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "")
				select {
				case <-cache.stored:
				case <-ctx.Done():
					t.Fatal("no NEW_TOKEN", ctx.Err())
				}
				select {
				case has := <-observer.first:
					if has != (round > 0) {
						t.Fatal("first Initial token", round, has)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				retries := observer.retries.Load() - before
				if (round == 0 && retries == 0) || (round > 0 && retries != 0) {
					t.Fatal("Retry count", round, retries)
				}
				if err := c.SendDatagram([]byte("independent client token reuse")); err != nil {
					t.Fatal(err)
				}
				b, err := peer.ReceiveDatagram(ctx)
				if err != nil || string(b) != "independent client token reuse" {
					t.Fatal(string(b), err)
				}
			}
		})
	}
}
