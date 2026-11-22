package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func TestUDPActorNegotiatedMultipath(t *testing.T) {
	t.Run("reader failure", func(t *testing.T) { testUDPActorMultipathRetirement(t, false) })
	t.Run("explicit removal", func(t *testing.T) { testUDPActorMultipathRetirement(t, true) })
}
func testUDPActorMultipathRetirement(t *testing.T, planned bool) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		maximum := uint64(3)
		client, template := negotiatedPathPair(t, version, &maximum, &maximum)
		codec, _ := token.NewRetryTokens([32]byte{17}, nil, time.Minute)
		endpoint, err := NewServerEndpoint(template.config, codec, 4)
		if err != nil {
			t.Fatal(err)
		}
		openSocket := func() *net.UDPConn {
			t.Helper()
			s, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			return s
		}
		ss, cs := openSocket(), openSocket()
		ss2, cs2 := openSocket(), openSocket()
		sr, err := NewUDPServer(ss, endpoint)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { sr.Close() })
		remote := ss.LocalAddr().(*net.UDPAddr).AddrPort()
		local := cs2.LocalAddr().(*net.UDPAddr).AddrPort()
		secondaryRemote := ss2.LocalAddr().(*net.UDPAddr).AddrPort()
		ce, err := NewClientEndpoint(client, remote)
		if err != nil {
			t.Fatal(err)
		}
		cr, err := NewUDPServer(cs, ce)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cr.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server, _, err := sr.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := cr.Wait(ctx, func(*ServerEndpoint) (bool, error) { return client.confirmed && client.multipath != nil, nil }); err != nil {
			t.Fatal(err)
		}
		if err := sr.addSocket(ctx, ss2); err != nil {
			t.Fatal(err)
		}
		if err := cr.addSocket(ctx, cs2); err != nil {
			t.Fatal(err)
		}
		if err := cr.addSocket(ctx, cs2); err == nil {
			t.Fatal("duplicate socket accepted")
		}
		rejected := openSocket()
		canceled, cancelAdd := context.WithCancel(ctx)
		cancelAdd()
		if err := cr.addSocket(canceled, rejected); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := rejected.SetReadDeadline(time.Now()); err != nil {
			t.Fatal("rejected socket ownership changed", err)
		}

		for _, side := range []struct {
			r *UDPServer
			c *Conn
		}{{cr, client}, {sr, server}} {
			if err := side.r.Do(ctx, func(*ServerEndpoint) error { return side.c.advertisePathCIDs(1) }); err != nil {
				t.Fatal(err)
			}
		}
		if err := cr.Wait(ctx, func(*ServerEndpoint) (bool, error) {
			s := client.multipath.paths[1]
			return s != nil && len(s.peer.active) > 0, nil
		}); err != nil {
			t.Fatal(err)
		}
		unregistered := rejected.LocalAddr().(*net.UDPAddr).AddrPort()
		if err := cr.prepareLocalMigration(ctx, client, 0, unregistered); !errors.Is(err, errPathBinding) {
			t.Fatal("local migration accepted unregistered socket", err)
		}
		if err := cr.prepareLocalMigration(ctx, server, 0, local); !errors.Is(err, errConnectionOwner) {
			t.Fatal("local migration accepted foreign connection", err)
		}
		if err := cr.initiatePath(ctx, client, 1, unregistered, secondaryRemote); !errors.Is(err, errPathBinding) {
			t.Fatal("unregistered local address", err)
		}
		if err := cr.initiatePath(ctx, server, 1, local, secondaryRemote); !errors.Is(err, errConnectionOwner) {
			t.Fatal("foreign connection", err)
		}
		if err := cr.Do(ctx, func(*ServerEndpoint) error {
			if client.err != nil || client.multipath.paths[1].application != nil {
				return errors.New("rejected initiation changed connection state")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := cr.initiatePath(ctx, client, 1, local, secondaryRemote); err != nil {
			t.Fatal(err)
		}
		for _, side := range []struct {
			r *UDPServer
			c *Conn
		}{{cr, client}, {sr, server}} {
			if err := side.r.Wait(ctx, func(*ServerEndpoint) (bool, error) {
				s := side.c.multipath.paths[1]
				return s != nil && s.application != nil && s.application.validation.state == PathValidated, nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		// Exercise both unexpected reader failure and explicit removal.
		// Either transition must preserve the secondary reader/connection.
		if planned {
			primary := cs.LocalAddr().(*net.UDPAddr).AddrPort()
			for i := 0; i < 2; i++ {
				if err := cr.removeSocket(ctx, primary); err != nil {
					t.Fatal(err)
				}
			}
			if err := cr.removeSocket(ctx, local); err == nil {
				t.Fatal("last socket removal accepted")
			}
		} else if err := cs.Close(); err != nil {
			t.Fatal(err)
		}
		if err := cr.Wait(ctx, func(*ServerEndpoint) (bool, error) {
			return client.multipath != nil && client.multipath.abandoned[0] != nil, nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := sr.Wait(ctx, func(*ServerEndpoint) (bool, error) { return server.multipath.abandoned[0] != nil, nil }); err != nil {
			t.Fatal(err)
		}
		if err := cr.Do(ctx, func(*ServerEndpoint) error {
			id, err := client.OpenStream(false)
			if err != nil {
				return err
			}
			_, err = client.WriteStream(id, []byte("actor failover"), true)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		var accepted uint64
		haveStream := false
		if err := sr.Wait(ctx, func(*ServerEndpoint) (bool, error) {
			if !haveStream {
				id, err := server.AcceptStream(false)
				if errors.Is(err, stream.ErrWouldBlock) {
					return false, nil
				}
				if err != nil {
					return false, err
				}
				accepted, haveStream = id, true
			}
			var out [64]byte
			n, err := server.ReadStream(accepted, out[:])
			if errors.Is(err, stream.ErrWouldBlock) {
				return false, nil
			}
			if err != io.EOF || string(out[:n]) != "actor failover" {
				return false, errors.New("unexpected stream result")
			}
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := cr.Close(); err != nil {
			t.Fatal(err)
		}
		if cr.readers.Load() != 0 {
			t.Fatal("dynamic reader survived close")
		}
		if err := cs2.SetReadDeadline(time.Now()); !errors.Is(err, net.ErrClosed) {
			t.Fatal("dynamic socket survived close", err)
		}
	}
}
