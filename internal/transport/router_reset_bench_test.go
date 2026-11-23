package transport

import (
	"encoding/binary"
	"fmt"
	"net/netip"
	"testing"
)

// Synthetic, warmed lookup only. Index construction, publication, packet
// authentication and UDP I/O are excluded. All owners share a remote address.
func BenchmarkRouterUnknownReset(b *testing.B) {
	for _, count := range []int{1, 256, 4096} {
		for _, mode := range []string{"miss", "unique", "duplicate"} {
			b.Run(fmt.Sprintf("%d/%s", count, mode), func(b *testing.B) {
				local := netip.MustParseAddrPort("192.0.2.1:443")
				peer := netip.MustParseAddrPort("192.0.2.2:444")
				router, _ := NewRouter(8, count)
				router.peers[peer] = make(map[*Conn]struct{}, count)
				var target *Conn
				query := [16]byte{0xff}
				for i := 0; i < count; i++ {
					token := [16]byte{}
					binary.BigEndian.PutUint64(token[8:], uint64(i+1))
					if mode == "duplicate" {
						token = [16]byte{1}
					}
					c := indexedResetConn(fmt.Sprintf("%08x", i), token, true)
					router.entries[c] = connectionRoute{peer: peer}
					router.peers[peer][c] = struct{}{}
					if i == count-1 && mode != "miss" {
						target, query = c, token
					}
				}
				if mode == "duplicate" && count > 1 {
					target = nil
				}
				router.enableResetIndex()
				if router.resetIndex == nil {
					b.Fatal("index unavailable")
				}
				raw := resetDatagram(query)
				b.ReportAllocs()
				for b.Loop() {
					got, known := router.RouteNetwork(raw, local, peer)
					if got != target || known != (mode != "miss") {
						b.Fatal("incorrect reset route")
					}
				}
			})
		}
	}
}
