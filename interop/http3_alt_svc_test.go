package interop

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

type altSvcListener struct {
	address          net.Addr
	entered, stopped chan struct{}
	once             sync.Once
}

func (l *altSvcListener) Addr() net.Addr { return l.address }
func (l *altSvcListener) Close() error   { l.once.Do(func() { close(l.stopped) }); return nil }
func (l *altSvcListener) accept(ctx context.Context) error {
	close(l.entered)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.stopped:
		return io.EOF
	}
}

type ownAltSvcListener struct{ *altSvcListener }

func (l ownAltSvcListener) Accept(ctx context.Context) (*quic.Conn, error) { return nil, l.accept(ctx) }

type peerAltSvcListener struct{ *altSvcListener }

func (l peerAltSvcListener) Accept(ctx context.Context) (*upstream.Conn, error) {
	return nil, l.accept(ctx)
}

func TestHTTP3AltSvcCompatibility(t *testing.T) {
	for _, test := range []struct {
		name, address string
		port          int
		listeners     []net.Addr
		expected      string
	}{
		{name: "listener", address: ":9999", listeners: []net.Addr{&net.UDPAddr{Port: 443}}, expected: `h3=":443"; ma=2592000`},
		{name: "override", port: 8443, listeners: []net.Addr{&net.UDPAddr{Port: 443}}, expected: `h3=":8443"; ma=2592000`},
		{name: "fallback", address: ":8443", listeners: []net.Addr{&net.UnixAddr{Name: "unused", Net: "unix"}}, expected: `h3=":8443"; ma=2592000`},
		{name: "missing", listeners: []net.Addr{&net.UnixAddr{Name: "unused", Net: "unix"}}},
		{name: "multiple", listeners: []net.Addr{&net.UDPAddr{Port: 8443}, &net.UDPAddr{Port: 443}, &net.UDPAddr{Port: 8443}}, expected: `h3=":8443"; ma=2592000,h3=":443"; ma=2592000,h3=":8443"; ma=2592000`},
	} {
		t.Run(test.name, func(t *testing.T) {
			own := &ownh3.Server{Addr: test.address, Port: test.port}
			peer := &peerh3.Server{Addr: test.address, Port: test.port}
			defer own.Close()
			defer peer.Close()
			type endpoint interface{ SetQUICHeaders(http.Header) error }
			for _, server := range []endpoint{own, peer} {
				h := make(http.Header)
				if err := server.SetQUICHeaders(h); err == nil {
					t.Fatal("advertised before listening")
				}
			}
			var owners, peers []*altSvcListener
			var ownDone, peerDone []chan error
			start := func(address net.Addr, ours bool) (*altSvcListener, chan error) {
				l := &altSvcListener{address: address, entered: make(chan struct{}), stopped: make(chan struct{})}
				done := make(chan error, 1)
				go func() {
					if ours {
						done <- own.ServeListener(ownAltSvcListener{l})
					} else {
						done <- peer.ServeListener(peerAltSvcListener{l})
					}
				}()
				t.Cleanup(func() {
					l.Close()
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Error("listener did not join")
					}
				})
				select {
				case <-l.entered:
				case err := <-done:
					done <- err
					t.Fatal("serve failed", err)
				case <-time.After(time.Second):
					t.Fatal("listener not registered")
				}
				return l, done
			}
			for _, address := range test.listeners {
				l, done := start(address, true)
				owners = append(owners, l)
				ownDone = append(ownDone, done)
				l, done = start(address, false)
				peers = append(peers, l)
				peerDone = append(peerDone, done)
			}
			check := func(expected string) {
				a, b := http.Header{"Alt-Svc": {"existing"}}, http.Header{"Alt-Svc": {"existing"}}
				errA, errB := own.SetQUICHeaders(a), peer.SetQUICHeaders(b)
				if expected == "" {
					if !errors.Is(errA, ownh3.ErrNoAltSvcPort) || !errors.Is(errB, peerh3.ErrNoAltSvcPort) {
						t.Fatal("missing-port errors", errA, errB)
					}
				} else if errA != nil || errB != nil || a.Values("Alt-Svc")[len(a.Values("Alt-Svc"))-1] != expected {
					t.Fatal("advertisement", a, errA, errB)
				}
				if !reflect.DeepEqual(a, b) {
					t.Fatal("header mismatch", a, b)
				}
			}
			check(test.expected)
			for i := range owners {
				owners[i].Close()
				peers[i].Close()
				for _, done := range []chan error{ownDone[i], peerDone[i]} {
					select {
					case err := <-done:
						done <- err
					case <-time.After(time.Second):
						t.Fatal("listener stop timed out")
					}
				}
				if test.name == "multiple" && i == 0 {
					check(`h3=":443"; ma=2592000,h3=":8443"; ma=2592000`)
				}
			}
			check("")
		})
	}
}
