package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func TestSharedClientEndpointUDP(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			clientTLS, serverTLS := testTLS(t)
			tokens, err := token.NewRetryTokens([32]byte{42}, nil, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			remote, err := NewServerEndpoint(Config{Version: version, TLS: serverTLS, Parameters: wire.DefaultTransportParameters()}, tokens, 4)
			if err != nil {
				t.Fatal(err)
			}
			start := func(e *ServerEndpoint) *UDPServer {
				socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				runtime, err := NewUDPServer(socket, e)
				if err != nil {
					socket.Close()
					e.Close()
					t.Fatal(err)
				}
				t.Cleanup(func() { runtime.Close() })
				return runtime
			}
			server := start(remote)
			local, err := NewClientOnlyEndpoint(8, 2)
			if err != nil {
				t.Fatal(err)
			}
			// Bind before runtime startup so newly added clients acquire network owners.
			local.config.EnableMigration = true
			client := start(local)
			peer := server.Addr().(*net.UDPAddr).AddrPort()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var clients []*Conn
			for i := 0; i < 2; i++ {
				c, err := NewConn(Config{Version: version, TLS: clientTLS, EnableMigration: true, LocalCID: []byte(fmt.Sprintf("client%02d", i)), PeerCID: []byte("original"), OriginalDCID: []byte("original"), Parameters: wire.DefaultTransportParameters()})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { client.Close(); c.Close() })
				if err := c.Start(ctx); err != nil {
					t.Fatal(err)
				}
				if err := client.Do(ctx, func(e *ServerEndpoint) error { return e.AddClient(c, peer) }); err != nil {
					t.Fatal(err)
				}
				clients = append(clients, c)
			}
			for _, c := range clients {
				if err := client.Wait(ctx, func(*ServerEndpoint) (bool, error) { return c.confirmed, c.Err() }); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 2; i++ {
				if _, _, err := server.Accept(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := client.Do(ctx, func(e *ServerEndpoint) error {
				if _, _, err := e.Accept(); !errors.Is(err, ErrAcceptWouldBlock) {
					return fmt.Errorf("outgoing client accepted: %v", err)
				}
				if len(e.networks) != 2 {
					return fmt.Errorf("network owners: %d", len(e.networks))
				}
				e.Remove(clients[0])
				if clients[1].closed {
					return errors.New("removing one client closed another")
				}
				return clients[1].enqueue(applicationSpace, wire.PingFrame{}, 1)
			}); err != nil {
				t.Fatal(err)
			}
			if err := client.Wait(ctx, func(*ServerEndpoint) (bool, error) { return !clients[1].HasOutstanding(), clients[1].Err() }); err != nil {
				t.Fatal(err)
			}
			if err := client.Do(ctx, func(e *ServerEndpoint) error { e.Remove(clients[1]); return nil }); err != nil {
				t.Fatal(err)
			}
			// A shared endpoint remains usable after its last connection is removed.
			if err := client.Do(ctx, func(e *ServerEndpoint) error {
				if e.closed {
					return ErrEndpointClosed
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			client.Close() // Join owner before the per-connection cleanup callbacks.
		})
	}
}

func TestAddClientFailureOwnership(t *testing.T) {
	c, _ := testPair(t, wire.Version1)
	e, err := NewClientOnlyEndpoint(8, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	peer := netip.MustParseAddrPort("192.0.2.1:443")
	if err := e.AddClient(c, peer); err != nil {
		t.Fatal(err)
	}
	duplicate, _ := testPair(t, wire.Version1)
	if err := e.AddClient(duplicate, peer); !errors.Is(err, ErrRouteCollision) {
		t.Fatal(err)
	}
	if duplicate.closed || len(e.conns) != 1 || len(e.router.entries) != 1 {
		t.Fatal("failed registration changed ownership")
	}
	e.CloseWhenIdle()
	if err := e.AddClient(duplicate, peer); err == nil || duplicate.closed {
		t.Fatal("dedicated endpoint accepted client or disposed caller ownership")
	}
	e.Close()
	if err := e.AddClient(duplicate, peer); !errors.Is(err, ErrEndpointClosed) || duplicate.closed {
		t.Fatal("closed endpoint registration", err)
	}
}

func TestAddClientNetworkRollback(t *testing.T) {
	c, _ := testPair(t, wire.Version1)
	e, err := NewClientOnlyEndpoint(8, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := e.bindNetworkOwner(netip.MustParseAddrPort("192.0.2.10:1234")); err != nil {
		t.Fatal(err)
	}
	// The router accepts this address, but address-aware ownership cannot bind it.
	if err := e.AddClient(c, netip.MustParseAddrPort("0.0.0.0:443")); err == nil {
		t.Fatal("accepted unspecified peer")
	}
	if c.closed || len(e.router.entries) != 0 || len(e.router.ids) != 0 || len(e.router.peers) != 0 || len(e.networks) != 0 || len(e.conns) != 0 {
		t.Fatal("failed network handoff retained ownership or routes")
	}
	if err := e.AddClient(c, netip.MustParseAddrPort("192.0.2.20:443")); err != nil {
		t.Fatal("retry after failed handoff", err)
	}
	other, _ := testPair(t, wire.Version1)
	if err := e.AddClient(other, netip.MustParseAddrPort("192.0.2.30:443")); !errors.Is(err, ErrRouterCapacity) || other.closed {
		t.Fatal("capacity failure ownership", err)
	}
}
