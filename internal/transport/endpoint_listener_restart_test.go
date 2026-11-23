package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func TestUDPListenerRestart(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			clientTLS, serverTLS := testTLS(t)
			parameters := wire.DefaultTransportParameters()
			parameters.InitialMaxStreamsUni = 4
			parameters.InitialMaxStreamDataUni = 65536
			parameters.InitialMaxData = 65536
			tokens, err := token.NewRetryTokens([32]byte{29}, nil, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			e, err := NewClientOnlyEndpoint(8, 4)
			if err != nil {
				t.Fatal(err)
			}
			socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			runtime, err := NewUDPServerBorrowed(socket, e)
			if err != nil {
				socket.Close()
				t.Fatal(err)
			}
			defer runtime.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			config := Config{Version: v, TLS: serverTLS, Parameters: parameters, HandshakeTimeout: 3 * time.Second}
			var generation uint64
			if err := runtime.Do(ctx, func(e *ServerEndpoint) error {
				var err error
				generation, err = e.StartListener(config, tokens)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			firstGeneration := generation
			var accepted []*Conn
			var peers []*Conn
			var peerOwners []*UDPServer
			for i := 0; i < 2; i++ {
				c, err := NewConn(Config{Version: v, TLS: clientTLS, LocalCID: []byte(fmt.Sprintf("client%02d", i)), PeerCID: []byte("original"), OriginalDCID: []byte("original"), Parameters: parameters})
				if err != nil {
					t.Fatal(err)
				}
				if err := c.Start(ctx); err != nil {
					c.Close()
					t.Fatal(err)
				}
				ce, err := NewClientEndpoint(c, runtime.Addr().(*net.UDPAddr).AddrPort())
				if err != nil {
					c.Close()
					t.Fatal(err)
				}
				cs, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					ce.Close()
					t.Fatal(err)
				}
				cr, err := NewUDPServer(cs, ce)
				if err != nil {
					cs.Close()
					ce.Close()
					t.Fatal(err)
				}
				defer cr.Close()
				peers = append(peers, c)
				peerOwners = append(peerOwners, cr)
				got, _, err := runtime.AcceptListener(ctx, generation)
				if err != nil {
					t.Fatal(err)
				}
				accepted = append(accepted, got)
				if err := cr.Wait(ctx, func(*ServerEndpoint) (bool, error) { return c.confirmed, c.Err() }); err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					// Close and restart in one command: old waiters may see only the final
					// state, so a boolean closed flag cannot protect their ownership.
					config.HandshakeTimeout = 4 * time.Second
					if err := runtime.Do(ctx, func(e *ServerEndpoint) error {
						e.CloseListenerGeneration(generation, time.Now())
						var err error
						generation, err = e.StartListener(config, tokens)
						return err
					}); err != nil {
						t.Fatal(err)
					}
					if generation == firstGeneration {
						t.Fatal("generation reused")
					}
					if _, _, err := runtime.AcceptListener(ctx, firstGeneration); !errors.Is(err, ErrListenerClosed) {
						t.Fatal("stale accept", err)
					}
					if err := runtime.Do(ctx, func(e *ServerEndpoint) error { e.CloseListenerGeneration(firstGeneration, time.Now()); return nil }); err != nil {
						t.Fatal(err)
					}
				}
			}
			for i, sender := range accepted {
				payload := fmt.Sprintf("connection %d after listener restart", i)
				if err := runtime.Do(ctx, func(*ServerEndpoint) error {
					id, err := sender.OpenStream(true)
					if err != nil {
						return err
					}
					n, err := sender.WriteStream(id, []byte(payload), true)
					if err == nil && n != len(payload) {
						return io.ErrShortWrite
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
				var id uint64
				if err := peerOwners[i].Wait(ctx, func(*ServerEndpoint) (bool, error) {
					var err error
					id, err = peers[i].AcceptStream(true)
					if errors.Is(err, stream.ErrWouldBlock) {
						return false, nil
					}
					return true, err
				}); err != nil {
					t.Fatal(err)
				}
				var received []byte
				if err := peerOwners[i].Wait(ctx, func(*ServerEndpoint) (bool, error) {
					var buf [128]byte
					n, err := peers[i].ReadStream(id, buf[:])
					received = append(received, buf[:n]...)
					if errors.Is(err, io.EOF) {
						return true, nil
					}
					if errors.Is(err, stream.ErrWouldBlock) {
						return false, nil
					}
					return false, err
				}); err != nil {
					t.Fatal(err)
				}
				if string(received) != payload {
					t.Fatal("post-restart payload", string(received))
				}
			}
			if err := runtime.Do(ctx, func(e *ServerEndpoint) error {
				for i, c := range accepted {
					if c.closed || c.Err() != nil {
						return fmt.Errorf("accepted connection %d lost: %v", i, c.Err())
					}
					if c.config.HandshakeTimeout != time.Duration(3+i)*time.Second {
						return errors.New("listener restart mutated existing configuration")
					}
				}
				if e.listenerClosed || len(e.conns) != 2 {
					return errors.New("stale close changed replacement")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStartListenerValidationAtomic(t *testing.T) {
	_, tlsConfig := testTLS(t)
	tokens, err := token.NewRetryTokens([32]byte{30}, nil, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	e, err := NewClientOnlyEndpoint(8, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	valid := Config{Version: wire.Version1, TLS: tlsConfig}
	for _, config := range []Config{
		{},
		{Version: wire.Version1, TLS: tlsConfig, EnableMultipath: true},
		{Version: wire.Version1, TLS: tlsConfig, EnableMigration: true, PreferredAddresses: []netip.AddrPort{netip.MustParseAddrPort("192.0.2.1:443")}},
	} {
		if generation, err := e.StartListener(config, tokens); err == nil || generation != 0 {
			t.Fatal("invalid listener accepted", err)
		}
		if !e.clientOnly || e.listenerGeneration != 0 || e.config.TLS != nil || e.tokens != nil {
			t.Fatal("failed start changed endpoint")
		}
	}
	generation, err := e.StartListener(valid, tokens)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := e.config.TLS
	if _, err := e.StartListener(valid, tokens); err == nil {
		t.Fatal("second active listener accepted")
	}
	if e.listenerGeneration != generation || e.config.TLS != snapshot {
		t.Fatal("failed duplicate start mutated listener")
	}
	e.CloseListenerGeneration(generation, time.Unix(100, 0))
	if _, err := e.StartListener(Config{}, tokens); err == nil {
		t.Fatal("bad replacement accepted")
	}
	if !e.listenerClosed || e.listenerGeneration != generation || e.config.TLS != snapshot {
		t.Fatal("bad replacement mutated old state")
	}
}
