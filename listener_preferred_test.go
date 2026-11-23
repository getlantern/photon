package quic

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"
)

func TestListenerPreferredAddressStreams(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		for _, host := range []string{"127.0.0.1", "::1"} {
			t.Run(fmt.Sprintf("%x/%s", version, host), func(t *testing.T) {
				clientTLS, serverTLS := dialTLS(t)
				clientTLS.ServerName = "127.0.0.1" // The fixture certificate authenticates this identity over either family.
				requested := netip.AddrPortFrom(netip.MustParseAddr(host), 0)
				cfg := &Config{Versions: []Version{version}, PreferredAddresses: []netip.AddrPort{requested}}
				listener, err := ListenAddr(requested.String(), serverTLS, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				addresses := listener.PreferredAddresses()
				if len(addresses) != 1 || addresses[0].Port() == 0 {
					t.Fatal("missing bound preferred address", addresses)
				}
				preferred := addresses[0]
				addresses[0] = netip.AddrPort{}
				cfg.PreferredAddresses[0] = netip.AddrPort{}
				if listener.PreferredAddresses()[0] != preferred {
					t.Fatal("preferred address aliases caller memory")
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				client, err := DialAddr(ctx, listener.Addr().String(), clientTLS, &Config{Versions: []Version{version}, LocalAddr: &net.UDPAddr{IP: net.ParseIP(host)}})
				if err != nil {
					t.Fatal(err)
				}
				defer client.server.Close()
				server, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for client.RemoteAddr().String() != preferred.String() {
					select {
					case <-ctx.Done():
						t.Fatal("client did not migrate", ctx.Err())
					case <-ticker.C:
					}
				}
				stream, err := client.OpenStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				payload := bytes.Repeat([]byte("preferred-address"), 32)
				stream.SetDeadline(time.Now().Add(3 * time.Second))
				if _, err := stream.Write(payload); err != nil {
					t.Fatal(err)
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				incoming, err := server.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				incoming.SetDeadline(time.Now().Add(3 * time.Second))
				got, err := io.ReadAll(incoming)
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatal("request", len(got), err)
				}
				if _, err := incoming.Write(got); err != nil {
					t.Fatal(err)
				}
				if err := incoming.Close(); err != nil {
					t.Fatal(err)
				}
				echoed, err := io.ReadAll(stream)
				if err != nil || !bytes.Equal(echoed, payload) {
					t.Fatal("echo", len(echoed), err)
				}
				for server.LocalAddr().String() != preferred.String() {
					select {
					case <-ctx.Done():
						t.Fatal("server did not migrate", ctx.Err())
					case <-ticker.C:
					}
				}
				if err := listener.Close(); err != nil {
					t.Fatal(err)
				}
				rebound, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(preferred))
				if err != nil {
					t.Fatal("preferred socket leaked after close", err)
				}
				rebound.Close()
			})
		}
	}
}

func TestPreferredListenerConstructionCleanup(t *testing.T) {
	_, serverTLS := dialTLS(t)
	reserve := func(network, host string) *net.UDPConn {
		t.Helper()
		s, err := net.ListenUDP(network, &net.UDPAddr{IP: net.ParseIP(host)})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	primary := reserve("udp4", "127.0.0.1")
	primaryAddr := primary.LocalAddr().(*net.UDPAddr).AddrPort()
	primary.Close()
	first := reserve("udp4", "127.0.0.1")
	firstAddr := first.LocalAddr().(*net.UDPAddr).AddrPort()
	first.Close()
	occupied := reserve("udp6", "::1")
	defer occupied.Close()
	l, err := ListenAddr(primaryAddr.String(), serverTLS, &Config{PreferredAddresses: []netip.AddrPort{firstAddr, occupied.LocalAddr().(*net.UDPAddr).AddrPort()}})
	if err == nil {
		l.Close()
		t.Fatal("occupied preferred socket accepted")
	}
	for _, address := range []netip.AddrPort{primaryAddr, firstAddr} {
		s, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(address))
		if err != nil {
			t.Fatal("partial construction leaked socket", address, err)
		}
		s.Close()
	}
}
