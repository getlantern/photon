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

func helperSocket(t *testing.T) *net.UDPConn {
	t.Helper()
	s, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func helperSocketReusable(t *testing.T, s *net.UDPConn) {
	t.Helper()
	// Do not reset the write deadline: runtime cleanup must have cleared it.
	if _, err := s.WriteToUDP([]byte("caller owns socket"), s.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	if err := s.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	defer s.SetReadDeadline(time.Time{})
	var b [2048]byte
	for {
		n, _, err := s.ReadFromUDPAddrPort(b[:])
		if err != nil {
			t.Fatal("socket reader not released", err)
		}
		if string(b[:n]) == "caller owns socket" {
			return
		}
	}
}

func TestPacketConnHelpersLifetime(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		for _, early := range []bool{false, true} {
			for _, remoteClose := range []bool{false, true} {
				for _, kind := range []string{"udp", "plain", "oob"} {
					t.Run(fmt.Sprintf("%x/early=%v/%s/remoteClose=%v", version, early, kind, remoteClose), func(t *testing.T) {
						ct, st := dialTLS(t)
						ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
						defer cancel()
						serverSocket, clientSocket := helperSocket(t), helperSocket(t)
						cfg := &Config{Versions: []Version{version}}
						var l *Listener
						var err error
						if early {
							el, e := ListenEarly(transportTestWrapSocket(serverSocket, kind), st, cfg)
							err = e
							if el != nil {
								l = el.Listener
							}
						} else {
							l, err = Listen(transportTestWrapSocket(serverSocket, kind), st, cfg)
						}
						if err != nil {
							t.Fatal(err)
						}
						defer l.Close()
						var c *Conn
						if early {
							c, err = DialEarly(ctx, transportTestWrapSocket(clientSocket, kind), l.Addr(), ct, cfg)
						} else {
							c, err = Dial(ctx, transportTestWrapSocket(clientSocket, kind), l.Addr(), ct, cfg)
						}
						if err != nil {
							t.Fatal(err)
						}
						defer c.CloseWithError(0, "")
						s, err := l.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
						if c.LocalAddr().String() != clientSocket.LocalAddr().String() {
							t.Fatal("wrong client socket")
						}
						tx, err := c.OpenUniStreamSync(ctx)
						if err != nil {
							t.Fatal(err)
						}
						if _, err = tx.Write([]byte("helper transfer")); err != nil {
							t.Fatal(err)
						}
						if err = tx.Close(); err != nil {
							t.Fatal(err)
						}
						rx, err := s.AcceptUniStream(ctx)
						if err != nil {
							t.Fatal(err)
						}
						rx.SetReadDeadline(time.Now().Add(time.Second))
						data, err := io.ReadAll(rx)
						if err != nil || string(data) != "helper transfer" {
							t.Fatal(string(data), err)
						}
						// Local and remote terminal events must stop the dedicated reader.
						closing := c
						if remoteClose {
							closing = s
						}
						if err = closing.CloseWithError(12, "done"); err != nil {
							t.Fatal(err)
						}
						select {
						case <-c.Context().Done():
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
						select {
						case <-c.server.Done():
						case <-ctx.Done():
							t.Fatal("dial runtime leaked", ctx.Err())
						}
						helperSocketReusable(t, clientSocket)
						// A second accepted connection must be terminated by convenience Close.
						c2, err := DialAddr(ctx, l.Addr().String(), ct, cfg)
						if err != nil {
							t.Fatal(err)
						}
						defer c2.CloseWithError(0, "")
						s2, err := l.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
						if err = l.Close(); err != nil {
							t.Fatal(err)
						}
						select {
						case <-s2.Context().Done():
						default:
							t.Fatal("accepted connection survived helper close")
						}
						if _, err = l.Accept(ctx); !errors.Is(err, ErrServerClosed) {
							t.Fatal(err)
						}
						helperSocketReusable(t, serverSocket)
					})
				}
			}
		}
	}

}

func TestPacketConnHelpersFailureCleanup(t *testing.T) {
	ct, st := dialTLS(t)
	for _, early := range []bool{false, true} {
		t.Run(fmt.Sprint(early), func(t *testing.T) {
			s, silent := helperSocket(t), helperSocket(t)
			dial := Dial
			if early {
				dial = DialEarly
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			_, err := dial(ctx, s, silent.LocalAddr(), ct, nil)
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			helperSocketReusable(t, s)
			if _, err = dial(context.Background(), s, silent.LocalAddr(), nil, nil); err == nil {
				t.Fatal("nil TLS")
			}
			helperSocketReusable(t, s)
			// Invalid server configuration must not consume the borrowed socket.
			if early {
				_, err = ListenEarly(s, nil, nil)
			} else {
				_, err = Listen(s, nil, nil)
			}
			if err == nil {
				t.Fatal("nil TLS")
			}
			helperSocketReusable(t, s)
			l, err := Listen(s, st, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = l.Close(); err != nil {
				t.Fatal(err)
			}
			helperSocketReusable(t, s)
		})
	}
}
