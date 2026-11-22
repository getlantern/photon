package quic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestPublicMultipath(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			clientTLS, serverTLS := dialTLS(t)
			listener, err := ListenAddr("127.0.0.1:0", serverTLS, &Config{EnableMultipath: true})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			bind := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}
			client, err := DialAddr(ctx, listener.Addr().String(), clientTLS, &Config{Versions: []Version{version}, EnableMultipath: true, LocalAddr: bind})
			if err != nil {
				t.Fatal(err)
			}
			defer client.server.Close()
			server, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !client.ConnectionState().SupportsMultipath || !server.ConnectionState().SupportsMultipath {
				t.Fatal("multipath not negotiated")
			}
			remote := listener.Addr().(*net.UDPAddr)
			if _, err := server.OpenPath(ctx, 1, bind, client.LocalAddr().(*net.UDPAddr)); err == nil {
				t.Fatal("server initiated path")
			}
			canceled, stop := context.WithCancel(ctx)
			stop()
			if _, err := client.OpenPath(canceled, 1, bind, remote); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			path, err := client.OpenPath(ctx, 1, bind, remote)
			if err != nil {
				t.Fatal(err)
			}
			if err := client.SetPathBackup(ctx, 1, true); err != nil {
				t.Fatal(err)
			}
			if err := client.SetPathBackup(ctx, 1, false); err != nil {
				t.Fatal(err)
			}
			if err := server.SetPathBackup(ctx, 1, true); err != nil {
				t.Fatal(err)
			}
			if err := client.SetPathBackup(ctx, 4, true); err == nil {
				t.Fatal("accepted unknown status path")
			}
			if path.ID() != 1 || path.LocalAddr().String() == client.LocalAddr().String() {
				t.Fatal("invalid path handle")
			}
			if _, err := client.OpenPath(ctx, 1, bind, remote); err == nil {
				t.Fatal("reused path")
			}
			if err := path.Close(); err != nil {
				t.Fatal(err)
			}
			if err := path.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := client.OpenPath(ctx, 1, bind, remote); err == nil {
				t.Fatal("reused abandoned path")
			}

			second, err := client.OpenPath(ctx, 2, bind, remote)
			if err != nil {
				t.Fatal(err)
			}
			defer second.Close()
			blackhole, err := net.ListenUDP("udp4", bind)
			if err != nil {
				t.Fatal(err)
			}
			defer blackhole.Close()
			attempt, stop := context.WithCancel(ctx)
			defer stop()
			result := make(chan error, 1)
			go func() {
				_, err := client.OpenPath(attempt, 3, bind, blackhole.LocalAddr().(*net.UDPAddr))
				result <- err
			}()
			// A queued challenge proves initiation happened before cancellation.
			if err := blackhole.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			packet := make([]byte, 1500)
			_, source, err := blackhole.ReadFromUDP(packet)
			if err != nil {
				t.Fatal(err)
			}
			stop()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("validation cancellation: %v", err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if _, err := client.OpenPath(ctx, 3, bind, remote); err == nil {
				t.Fatal("reused canceled probe ID")
			}
			released, err := net.ListenUDP("udp4", source)
			if err != nil {
				t.Fatalf("canceled path retained socket: %v", err)
			}
			released.Close()

			// Retire the primary path using only the public path-control API.
			if err := client.AbandonPath(ctx, 0); err != nil {
				t.Fatal(err)
			}
			if err := client.AbandonPath(ctx, 0); err != nil {
				t.Fatal(err)
			}
			if err := client.AbandonPath(ctx, 2); err == nil {
				t.Fatal("abandoned last viable path")
			}
			if err := second.Close(); err == nil {
				t.Fatal("closed last viable path handle")
			}
			send, err := client.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := send.Write([]byte("public multipath")); err != nil {
				t.Fatal(err)
			}
			if err := send.Close(); err != nil {
				t.Fatal(err)
			}
			receive, err := server.AcceptUniStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := receive.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(receive)
			if err != nil || string(data) != "public multipath" {
				t.Fatalf("data=%q err=%v", data, err)
			}
		})
	}
}

func TestPublicMultipathFallback(t *testing.T) {
	clientTLS, serverTLS := dialTLS(t)
	l, err := ListenAddr("127.0.0.1:0", serverTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bind := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}
	c, err := DialAddr(ctx, l.Addr().String(), clientTLS, &Config{EnableMultipath: true, LocalAddr: bind})
	if err != nil {
		t.Fatal(err)
	}
	defer c.server.Close()
	if c.ConnectionState().SupportsMultipath {
		t.Fatal("unilateral negotiation")
	}
	if _, err := c.OpenPath(ctx, 1, bind, l.Addr().(*net.UDPAddr)); err == nil {
		t.Fatal("opened unnegotiated path")
	}
	if _, err := DialAddr(ctx, l.Addr().String(), clientTLS, &Config{EnableMultipath: true}); err == nil {
		t.Fatal("accepted wildcard client")
	}
	if wildcard, err := ListenAddr("0.0.0.0:0", serverTLS, &Config{EnableMultipath: true}); err == nil {
		wildcard.Close()
		t.Fatal("accepted wildcard listener")
	}
}

func TestServerAbandonKeepsListenerSocket(t *testing.T) {
	clientTLS, serverTLS := dialTLS(t)
	listener, err := ListenAddr("127.0.0.1:0", serverTLS, &Config{EnableMultipath: true})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bind := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}
	client, err := DialAddr(ctx, listener.Addr().String(), clientTLS, &Config{EnableMultipath: true, LocalAddr: bind})
	if err != nil {
		t.Fatal(err)
	}
	defer client.server.Close()
	server, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	path, err := client.OpenPath(ctx, 1, bind, listener.Addr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer path.Close()
	// The server's paths share the listener socket. Abandonment must not close it.
	if err := server.AbandonPath(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := server.AbandonPath(ctx, 0); err == nil {
		t.Fatal("accepted final server path abandonment")
	}
	another, err := DialAddr(ctx, listener.Addr().String(), clientTLS, nil)
	if err != nil {
		t.Fatalf("listener stopped accepting: %v", err)
	}
	defer another.server.Close()
	peer, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]*Conn{{client, server}, {another, peer}} {
		send, err := pair[0].OpenUniStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := send.Write([]byte("listener alive")); err != nil {
			t.Fatal(err)
		}
		if err := send.Close(); err != nil {
			t.Fatal(err)
		}
		receive, err := pair[1].AcceptUniStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		receive.SetReadDeadline(time.Now().Add(time.Second))
		b, err := io.ReadAll(receive)
		if err != nil || string(b) != "listener alive" {
			t.Fatal(string(b), err)
		}
	}
}

func TestConcurrentPathClosePreservesSurvivor(t *testing.T) {
	clientTLS, serverTLS := dialTLS(t)
	l, err := ListenAddr("127.0.0.1:0", serverTLS, &Config{EnableMultipath: true})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bind := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}
	c, err := DialAddr(ctx, l.Addr().String(), clientTLS, &Config{EnableMultipath: true, LocalAddr: bind})
	if err != nil {
		t.Fatal(err)
	}
	defer c.server.Close()
	peer, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	paths := make([]*Path, 2)
	for i := range paths {
		paths[i], err = c.OpenPath(ctx, uint64(i+1), bind, l.Addr().(*net.UDPAddr))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := c.AbandonPath(ctx, 0); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 2)
	start := make(chan struct{})
	for _, p := range paths {
		go func() { <-start; result <- p.Close() }()
	}
	close(start)
	successes := 0
	for range paths {
		select {
		case err := <-result:
			if err == nil {
				successes++
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if successes != 1 {
		t.Fatalf("successful closes=%d; want one", successes)
	}
	send, err := c.OpenUniStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := send.Write([]byte("survived")); err != nil {
		t.Fatal(err)
	}
	if err := send.Close(); err != nil {
		t.Fatal(err)
	}
	receive, err := peer.AcceptUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := receive.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(receive)
	if err != nil || string(data) != "survived" {
		t.Fatalf("data=%q err=%v", data, err)
	}
}
