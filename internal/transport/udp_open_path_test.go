package transport

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func TestOpenPathRejectsMixedAddressFamilies(t *testing.T) {
	maximum := uint64(3)
	client, template := negotiatedPathPair(t, wire.Version1, &maximum, &maximum)
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
	sr, err := NewUDPServer(ss, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sr.Close() })
	remote := ss.LocalAddr().(*net.UDPAddr).AddrPort()
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
	for _, side := range []struct {
		r *UDPServer
		c *Conn
	}{{cr, client}, {sr, server}} {
		if err := side.r.Do(ctx, func(*ServerEndpoint) error { return side.c.advertisePathCIDs(1) }); err != nil {
			t.Fatal(err)
		}
	}
	bind := netip.MustParseAddrPort("127.0.0.1:0")
	mixed := netip.AddrPortFrom(netip.IPv6Loopback(), remote.Port())
	if _, err := cr.OpenPath(ctx, client, 1, bind, mixed); !errors.Is(err, errPathBinding) {
		t.Fatal("mixed-family path", err)
	}
	if err := cr.Do(ctx, func(e *ServerEndpoint) error {
		if _, ok := e.router.entries[client]; !ok || client.err != nil {
			return errors.New("rejected path removed the connection")
		}
		if client.multipath.highestInitiated != 0 || client.multipath.paths[1].application != nil || len(e.locals) != 1 {
			return errors.New("rejected path consumed its ID or kept a socket")
		}
		// The engine also refuses a registered local address of the other family.
		if err := client.initiateMultipathPath(1, canonicalPeer(cs.LocalAddr().(*net.UDPAddr).AddrPort()), mixed); !errors.Is(err, errPathBinding) {
			return errors.New("engine accepted a mixed-family tuple")
		}
		if client.multipath.highestInitiated != 0 || client.multipath.paths[1].application != nil {
			return errors.New("engine rejection consumed the path ID")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := cr.OpenPath(ctx, client, 1, bind, remote); err != nil {
		t.Fatal("path ID unusable after rejection", err)
	}
}
