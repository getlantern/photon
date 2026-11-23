package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestTransportListenerLifetime(t *testing.T)           { testTransportListenerLifetime(t, "udp") }
func TestTransportPacketConnListenerLifetime(t *testing.T) { testTransportListenerLifetime(t, "plain") }

func testTransportListenerLifetime(t *testing.T, wrapperKind string) {
	for _, version := range []Version{Version1, Version2} {
		for _, early := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/early=%v", version, early), func(t *testing.T) {
				ct, st := dialTLS(t)
				socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				defer socket.Close()
				tr := &Transport{Conn: socket}
				tr.Conn = transportTestWrapSocket(socket, wrapperKind)
				defer tr.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				listen := func(capacity int) *Listener {
					cfg := &Config{Versions: []Version{version}, MaxConnections: capacity}
					if early {
						l, err := tr.ListenEarly(st, cfg)
						if err != nil {
							t.Fatal(err)
						}
						return l.Listener
					}
					l, err := tr.Listen(st, cfg)
					if err != nil {
						t.Fatal(err)
					}
					return l
				}
				l := listen(1)
				if l.Addr().String() != socket.LocalAddr().String() {
					t.Fatal("listener changed socket")
				}
				if other, err := tr.Listen(st, nil); err == nil {
					other.Close()
					t.Fatal("duplicate listener accepted")
				}
				var clients, servers []*Conn
				var old *Listener
				for i := 0; i < 2; i++ {
					c, err := DialAddr(ctx, l.Addr().String(), ct, &Config{Versions: []Version{version}})
					if err != nil {
						t.Fatal(err)
					}
					defer c.CloseWithError(0, "")
					s, err := l.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					clients = append(clients, c)
					servers = append(servers, s)
					if i == 0 {
						old = l
						if err := l.Close(); err != nil {
							t.Fatal(err)
						}
						if _, err := l.Accept(ctx); !errors.Is(err, ErrServerClosed) || !errors.Is(err, net.ErrClosed) {
							t.Fatal("closed listener accept", err)
						}
						l = listen(4)
						if err := old.Close(); err != nil {
							t.Fatal(err)
						}
						if _, err := old.Accept(ctx); !errors.Is(err, ErrServerClosed) {
							t.Fatal("stale listener consumed replacement", err)
						}
					}
				}
				// Both connections must carry complete streams after replacing the listener.
				for i, c := range clients {
					payload := fmt.Sprintf("survived restart: %d", i)
					tx, err := c.OpenUniStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					tx.SetWriteDeadline(time.Now().Add(time.Second))
					if _, err := tx.Write([]byte(payload)); err != nil {
						t.Fatal(err)
					}
					if err := tx.Close(); err != nil {
						t.Fatal(err)
					}
					rx, err := servers[i].AcceptUniStream(ctx)
					if err != nil {
						t.Fatal(err)
					}
					rx.SetReadDeadline(time.Now().Add(time.Second))
					got, err := io.ReadAll(rx)
					if err != nil || string(got) != payload {
						t.Fatal("post-restart stream", string(got), err)
					}
				}
				blocked := make(chan error, 1)
				go func() { _, err := l.Accept(ctx); blocked <- err }()
				if err := tr.Close(); err != nil {
					t.Fatal(err)
				}
				if err := <-blocked; !errors.Is(err, ErrServerClosed) {
					t.Fatal("transport close did not end accept", err)
				}
				for _, c := range servers {
					select {
					case <-c.Context().Done():
					default:
						t.Fatal("transport retained connection")
					}
				}
				if _, err := tr.Listen(st, nil); !errors.Is(err, ErrTransportClosed) || !errors.Is(err, net.ErrClosed) {
					t.Fatal("reopened closed transport", err)
				}
				if err := l.Close(); err != nil {
					t.Fatal("listener close after transport", err)
				}
				// Borrowed socket remains available, including its write deadline.
				if _, err := socket.WriteToUDP([]byte("caller"), socket.LocalAddr().(*net.UDPAddr)); err != nil {
					t.Fatal("socket ownership lost", err)
				}
			})
		}
	}
}

func TestTransportInitializationFailures(t *testing.T) {
	_, tlsConfig := dialTLS(t)
	tr := new(Transport)
	if _, err := tr.Listen(tlsConfig, nil); err == nil {
		t.Fatal("nil socket accepted")
	}
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	tr.Conn = socket
	for _, bad := range []*tls.Config{nil, {}, {NextProtos: []string{"test"}}} {
		if _, err := tr.Listen(bad, nil); err == nil {
			t.Fatal("invalid TLS accepted")
		}
		if tr.runtime != nil {
			t.Fatal("invalid configuration started socket reader")
		}
	}
	l, err := tr.Listen(tlsConfig, nil)
	if err != nil {
		t.Fatal("retry after invalid configuration", err)
	}
	l.Close()
	tr.Close()
	unopened := &Transport{Conn: socket}
	unopened.Close()
	if _, err := unopened.Listen(tlsConfig, nil); !errors.Is(err, ErrTransportClosed) {
		t.Fatal(err)
	}
	if _, err := socket.WriteToUDP([]byte{1}, socket.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal("unopened close consumed socket", err)
	}
}

func TestTransportListenCloseRace(t *testing.T) {
	_, tlsConfig := dialTLS(t)
	for i := 0; i < 16; i++ {
		socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		tr := &Transport{Conn: socket}
		start := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			<-start
			l, err := tr.Listen(tlsConfig, nil)
			if err == nil {
				err = l.Close()
			}
			result <- err
		}()
		close(start)
		if err := tr.Close(); err != nil {
			socket.Close()
			t.Fatal(err)
		}
		if err := <-result; err != nil && !errors.Is(err, ErrTransportClosed) {
			socket.Close()
			t.Fatal(err)
		}
		tr.Close()
		if tr.runtime != nil {
			select {
			case <-tr.runtime.Done():
			default:
				socket.Close()
				t.Fatal("close left reader owner running")
			}
		}
		if _, err := socket.WriteToUDP([]byte{1}, socket.LocalAddr().(*net.UDPAddr)); err != nil {
			socket.Close()
			t.Fatal(err)
		}
		socket.Close()
	}
}

func TestTransportOOBListenerLifetime(t *testing.T) { testTransportListenerLifetime(t, "oob") }
