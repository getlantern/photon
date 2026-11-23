package transport

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestShortHeaderOwnedStorage(t *testing.T) {
	for _, size := range []int{0, 4, 8, 16, 20} {
		for _, phase := range []bool{false, true} {
			t.Run(fmt.Sprintf("cid=%d/phase=%t", size, phase), func(t *testing.T) {
				c := &Conn{peerCID: bytes.Repeat([]byte{0xa5}, size)}
				c.applicationKeys.phase = phase
				const pn = uint64(0x123456789)
				header, err := c.makeHeader(applicationSpace, pn, 17)
				if err != nil {
					t.Fatal(err)
				}
				parsed, err := wire.ParseHeader(header, size)
				if err != nil || parsed.Type != wire.OneRTT || parsed.PNOffset != 1+size || !bytes.Equal(parsed.DCID, c.peerCID) {
					t.Fatal("short header parse", parsed, err)
				}
				if len(header) != 1+size+4 || binary.BigEndian.Uint32(header[1+size:]) != uint32(pn&0xffffffff) || (header[0]&4 != 0) != phase {
					t.Fatal("header fields", header)
				}
				saved := bytes.Clone(header)
				for i := range c.peerCID {
					c.peerCID[i] = 0x5a
				}
				next, err := c.makeHeader(applicationSpace, pn+1, 100)
				if err != nil {
					t.Fatal(err)
				}
				next[0] ^= 0x40
				if !bytes.Equal(header, saved) {
					t.Fatal("header aliases caller CID or later header")
				}
			})
		}
	}
}

var shortHeaderBenchmarkSink []byte

func BenchmarkShortHeaderConstruction(b *testing.B) {
	for _, size := range []int{0, 4, 8, 16, 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			c := &Conn{peerCID: bytes.Repeat([]byte{1}, size)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				header, err := c.makeHeader(applicationSpace, uint64(i), 1200)
				if err != nil {
					b.Fatal(err)
				}
				shortHeaderBenchmarkSink = header
			}
		})
	}
}
