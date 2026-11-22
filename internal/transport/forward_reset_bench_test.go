package transport

import (
	"encoding/binary"
	"fmt"
	"net/netip"
	"testing"
	"time"
)

// Isolates alternate-reset selection, not authentication, UDP throughput or
// comparison with another QUIC implementation. Synthetic state is outside timing.
func BenchmarkForeignResetSelection(b *testing.B) {
	for _, count := range []int{1, 256, 4096} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			local := netip.MustParseAddrPort("192.0.2.1:443")
			remote := netip.MustParseAddrPort("192.0.2.2:444")
			r, _ := NewRouter(8, count)
			e := &ServerEndpoint{router: r, networks: make(map[*Conn]*networkOwner)}
			for i := 0; i < count; i++ {
				c := resetPublicationBenchmarkConn(local, remote, 1)
				entry := c.multipath.paths[0].peer.active[0]
				binary.BigEndian.PutUint64(entry.token[8:], uint64(i+1))
				c.multipath.paths[0].peer.active[0] = entry
				r.entries[c] = connectionRoute{peer: remote}
				e.networks[c] = &networkOwner{conn: c, primary: pathTuple{local, remote}, activationChecked: true}
			}
			first := &handoffRoute{local: local}
			r.forwardRegistrations = map[*handoffRoute]struct{}{first: {}}
			s := &UDPServer{endpoint: e}
			raw := make([]byte, 29)
			raw[0] = 0x40
			p := receivedUDP{data: raw, local: local, peer: remote}
			now := time.Unix(100, 0)
			r.enableResetIndex() // Bootstrap excluded from steady lookup timing.
			b.ReportAllocs()
			for b.Loop() {
				if s.reserveResetReturn(p, first, now) != nil {
					b.Fatal("unexpected reset match")
				}
			}
		})
	}
}

func resetPublicationBenchmarkConn(local, remote netip.AddrPort, paths int) *Conn {
	c := &Conn{multipath: &pathCIDs{paths: make(map[uint64]*pathCIDState)}}
	for i := 0; i < paths; i++ {
		tuple := pathTuple{local, remote}
		token := [16]byte{0x77}
		binary.BigEndian.PutUint64(token[8:], uint64(i+1))
		c.multipath.paths[uint64(i)] = &pathCIDState{
			application: &pathApplication{network: &tuple},
			peer:        &peerIDs{active: map[uint64]peerID{0: {used: true, hasToken: true, token: token}}},
		}
	}
	return c
}

func BenchmarkForeignResetPublication(b *testing.B) {
	for _, paths := range []int{1, 2, 8} {
		b.Run(fmt.Sprint(paths), func(b *testing.B) {
			local := netip.MustParseAddrPort("192.0.2.1:443")
			c := resetPublicationBenchmarkConn(local, netip.MustParseAddrPort("192.0.2.2:444"), paths)
			route := &handoffRoute{local: local}
			route.publishResetHints(c)
			b.ReportAllocs()
			for b.Loop() {
				route.publishResetHints(c)
			}
		})
	}
}

// Rebuild cost complements the stable-publication benchmark: retaining identity
// records trades larger changed snapshots for allocation-free unchanged checks.
func BenchmarkForeignResetPublicationChange(b *testing.B) {
	for _, paths := range []int{1, 2, 8} {
		b.Run(fmt.Sprint(paths), func(b *testing.B) {
			local := netip.MustParseAddrPort("192.0.2.1:443")
			c := resetPublicationBenchmarkConn(local, netip.MustParseAddrPort("192.0.2.2:444"), paths)
			route := &handoffRoute{local: local}
			route.publishResetHints(c)
			ids := c.multipath.paths[0].peer
			b.ReportAllocs()
			for b.Loop() {
				entry := ids.active[0]
				entry.token[0] ^= 1
				ids.active[0] = entry
				route.publishResetHints(c)
			}
		})
	}
}

func BenchmarkResetTokenIndexSync(b *testing.B) {
	for _, paths := range []int{1, 8} {
		b.Run(fmt.Sprint(paths), func(b *testing.B) {
			c := resetPublicationBenchmarkConn(netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.2:444"), paths)
			index, err := newResetTokenIndex()
			if err != nil {
				b.Fatal(err)
			}
			index.sync(c)
			b.ReportAllocs()
			for b.Loop() {
				index.sync(c)
			}
		})
	}
}

func BenchmarkResetTokenIndexBootstrap(b *testing.B) {
	for _, count := range []int{1, 256, 4096} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			r, _ := NewRouter(8, count)
			for i := 0; i < count; i++ {
				token := [16]byte{0x77}
				binary.BigEndian.PutUint64(token[8:], uint64(i+1))
				c := indexedResetConn("localcid", token, true)
				r.entries[c] = connectionRoute{}
			}
			b.ReportAllocs()
			for b.Loop() {
				r.resetIndex = nil
				r.enableResetIndex()
				if len(r.resetIndex.connections) != count {
					b.Fatal("bootstrap incomplete")
				}
			}
		})
	}
}

func BenchmarkResetTokenIndexHit(b *testing.B) {
	for _, count := range []int{1, 256, 4096} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			local := netip.MustParseAddrPort("192.0.2.1:443")
			remote := netip.MustParseAddrPort("192.0.2.2:444")
			r, _ := NewRouter(8, count)
			e := &ServerEndpoint{router: r, networks: make(map[*Conn]*networkOwner)}
			var selected *Conn
			var token [16]byte
			for i := 0; i < count; i++ {
				c := resetPublicationBenchmarkConn(local, remote, 1)
				entry := c.multipath.paths[0].peer.active[0]
				binary.BigEndian.PutUint64(entry.token[8:], uint64(i+1))
				c.multipath.paths[0].peer.active[0] = entry
				r.entries[c] = connectionRoute{peer: remote}
				e.networks[c] = &networkOwner{conn: c, primary: pathTuple{local, remote}, activationChecked: true}
				selected, token = c, entry.token
			}
			raw := resetDatagram(token)
			now := time.Unix(100, 0)
			r.enableResetIndex()
			b.ReportAllocs()
			for b.Loop() {
				c, ambiguous := e.localResetMatch(raw, local, remote, now, nil)
				if c != selected || ambiguous {
					b.Fatal("live indexed hit missing")
				}
			}
		})
	}
}
