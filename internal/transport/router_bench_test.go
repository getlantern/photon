package transport

import (
	"encoding/binary"
	"fmt"
	"net/netip"
	"testing"
)

// These microbenchmarks measure routing only, not QUIC throughput or parity
// with another implementation. Setup and synthetic connection allocation are
// outside the timed loop.
func BenchmarkRouter(b *testing.B) {
	for _, count := range []int{1, 256, 4096} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			r, _ := NewRouter(8, count)
			peer := netip.MustParseAddrPort("192.0.2.1:1234")
			var selected *Conn
			var cid []byte
			for i := 0; i < count; i++ {
				cid = binary.BigEndian.AppendUint64(nil, uint64(i+1))
				c := &Conn{config: Config{LocalCID: cid}, localIDs: newLocalIDs(cid)}
				if err := r.Add(c, peer); err != nil {
					b.Fatal(err)
				}
				selected = c
			}
			raw := append([]byte{0x40}, cid...)
			raw = append(raw, make([]byte, 20)...)
			b.Run("dispatch", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if c, known := r.Route(raw, peer); c != selected || !known {
						b.Fatal("route")
					}
				}
			})
			b.Run("unchanged-sync", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if err := r.Sync(selected); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
