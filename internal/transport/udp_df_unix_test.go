//go:build linux || darwin

package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func assertUDPNoFragment(t *testing.T, socket *net.UDPConn) {
	t.Helper()
	raw, err := socket.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Control(func(fd uintptr) {
		address, err := unix.Getsockname(int(fd))
		if err != nil {
			t.Error(err)
			return
		}
		check := func(level, option int) {
			value, err := unix.GetsockoptInt(int(fd), level, option)
			if err != nil || value != udpDFValue {
				t.Errorf("fragmentation option %d/%d: value=%d err=%v", level, option, value, err)
			}
		}
		switch address.(type) {
		case *unix.SockaddrInet4:
			check(unix.IPPROTO_IP, udpDFOption4)
		case *unix.SockaddrInet6:
			check(unix.IPPROTO_IPV6, udpDFOption6)
			only6, err := unix.GetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_V6ONLY)
			if err != nil {
				t.Error(err)
				return
			}
			if only6 == 0 && udpDFDualNeedsIPv4 {
				check(unix.IPPROTO_IP, udpDFOption4)
			}
		default:
			t.Error("unexpected socket family")
		}
	}); err != nil {
		t.Fatal(err)
	}
}
func TestUDPSocketNoFragment(t *testing.T) {
	for _, network := range []string{"udp4", "udp6", "udp"} {
		t.Run(network, func(t *testing.T) {
			socket, err := net.ListenUDP(network, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			if err := configureUDPSocket(socket); err != nil {
				t.Fatal(err)
			}
			assertUDPNoFragment(t, socket)
			if err := configureUDPSocket(socket); err != nil {
				t.Fatal("repeat configuration", err)
			}
		})
	}
}
func TestUDPServerNoFragment(t *testing.T) { assertUDPNoFragment(t, testUDPServer(t).socket) }
func TestClosedUDPSocketConfiguration(t *testing.T) {
	socket, err := net.ListenUDP("udp4", nil)
	if err != nil {
		t.Fatal(err)
	}
	socket.Close()
	if err := configureUDPSocket(socket); err == nil {
		t.Fatal("closed socket accepted")
	}
}

func TestAddedUDPSocketNoFragment(t *testing.T) {
	server := testUDPServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	address := server.socket.LocalAddr().(*net.UDPAddr).AddrPort()
	if err := server.Do(ctx, func(e *ServerEndpoint) error { return e.bindNetworkOwner(address) }); err != nil {
		t.Fatal(err)
	}
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	if err := server.addSocket(ctx, socket); err != nil {
		t.Fatal(err)
	}
	assertUDPNoFragment(t, socket)
	rejected, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	rejectedAddress := canonicalPeer(rejected.LocalAddr().(*net.UDPAddr).AddrPort())
	rejected.Close()
	if err := server.addSocket(ctx, rejected); err == nil {
		t.Fatal("closed socket registered")
	}
	server.socketMu.Lock()
	_, registered := server.sockets[rejectedAddress]
	server.socketMu.Unlock()
	if registered {
		t.Fatal("failed setup transferred ownership")
	}
}
