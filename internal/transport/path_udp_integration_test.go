package transport

import (
	"fmt"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// This exercises the complete TLS handshake and multipath failover over UDP
// through the address-aware owner. Public runtime integration remains separate.
func TestMultipathLoopbackUDPFailover(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprintf("%08x", version), func(t *testing.T) {
			maximum := uint64(3)
			client, server := negotiatedPathPair(t, version, &maximum, &maximum)
			type socketOwner struct {
				socket *net.UDPConn
				conn   *Conn
			}
			sockets := make(map[netip.AddrPort]socketOwner)
			addresses := make(map[*Conn][2]netip.AddrPort)
			for _, c := range []*Conn{client, server} {
				var pair [2]netip.AddrPort
				for i := range pair {
					sock, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { sock.Close() })
					pair[i] = sock.LocalAddr().(*net.UDPAddr).AddrPort()
					sockets[pair[i]] = socketOwner{sock, c}
				}
				addresses[c] = pair
			}
			owners := make(map[*Conn]*networkOwner)
			for _, pair := range [][2]*Conn{{client, server}, {server, client}} {
				owner, err := newNetworkOwner(pair[0], addresses[pair[0]][0], addresses[pair[1]][0])
				if err != nil {
					t.Fatal(err)
				}
				owners[pair[0]] = owner
			}
			routers := make(map[netip.AddrPort]*Router)
			ownerRouters := make(map[*Conn][]*Router)
			for address, socket := range sockets {
				r, err := NewRouter(8, 4)
				if err != nil {
					t.Fatal(err)
				}
				if err := r.Add(socket.conn, owners[socket.conn].primary.remote); err != nil {
					t.Fatal(err)
				}
				routers[address] = r
				ownerRouters[socket.conn] = append(ownerRouters[socket.conn], r)
			}
			sentByPath := map[uint64]int{}
			pump := func() {
				t.Helper()
				for round := 0; round < 200; round++ {
					sent := false
					for _, c := range []*Conn{client, server} {
						id, tuple, b, err := owners[c].nextRouted(time.Now(), ownerRouters[c]...)
						if err != nil {
							t.Fatal(err)
						}
						if b == nil {
							continue
						}
						source, ok := sockets[tuple.local]
						if !ok || source.conn != c {
							t.Fatal("wrong source socket")
						}
						destination, ok := sockets[tuple.remote]
						if !ok {
							t.Fatal("unknown destination socket")
						}
						if n, err := source.socket.WriteToUDPAddrPort(b, tuple.remote); err != nil || n != len(b) {
							t.Fatal(n, err)
						}
						if err := destination.socket.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
							t.Fatal(err)
						}
						var buffer [65535]byte
						n, peer, err := destination.socket.ReadFromUDPAddrPort(buffer[:])
						if err != nil {
							t.Fatal(err)
						}
						if peer != tuple.local {
							t.Fatal("UDP source address mismatch")
						}
						routed, known := routers[tuple.remote].RouteNetwork(buffer[:n], tuple.remote, peer)
						if !known || routed != destination.conn {
							t.Fatal("socket router selected wrong connection")
						}
						if err := owners[routed].receive(buffer[:n], tuple.remote, peer, time.Now()); err != nil {
							t.Fatal(err)
						}
						sentByPath[id]++
						sent = true
					}
					if !sent {
						return
					}
				}
				t.Fatal("UDP exchange failed to quiesce")
			}
			pump()
			for _, c := range []*Conn{client, server} {
				if !c.confirmed || c.multipath == nil {
					t.Fatal("UDP handshake/activation incomplete")
				}
				if err := c.advertisePathCIDs(1); err != nil {
					t.Fatal(err)
				}
			}
			pump()
			if sentByPath[0] == 0 {
				t.Fatal("no primary CID exchange")
			}
			if err := client.initiateMultipathPath(1, addresses[client][1], addresses[server][1]); err != nil {
				t.Fatal(err)
			}
			pump()
			for _, c := range []*Conn{client, server} {
				app := c.multipath.paths[1].application
				if app == nil || app.validation.state != PathValidated {
					t.Fatal("UDP secondary validation failed")
				}
			}
			if err := client.abandonFailedPath(0); err != nil {
				t.Fatal(err)
			}
			pump()
			for _, c := range []*Conn{client, server} {
				if c.multipath.abandoned[0] == nil {
					t.Fatal("primary not abandoned")
				}
			}
			primaryPackets := sentByPath[0]
			id, err := client.OpenStream(false)
			if err != nil {
				t.Fatal(err)
			}
			payload := []byte("stream delivery across a second UDP address pair")
			if n, err := client.WriteStream(id, payload, true); err != nil || n != len(payload) {
				t.Fatal(n, err)
			}
			pump()
			accepted, err := server.AcceptStream(false)
			if err != nil {
				t.Fatal(err)
			}
			var out [128]byte
			n, err := server.ReadStream(accepted, out[:])
			if err != io.EOF || string(out[:n]) != string(payload) {
				t.Fatal(n, err, string(out[:n]))
			}
			if sentByPath[0] != primaryPackets || sentByPath[1] == 0 {
				t.Fatal("failover used wrong socket path")
			}
		})
	}
}
