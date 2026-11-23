package interop

import (
	"bytes"
	"fmt"
	"testing"

	own "github.com/getlantern/photon/http3"
	peer "github.com/quic-go/quic-go/http3"
)

// Both implementations encode/decode the same 64-capsule batch. Timing includes
// parser setup and payload discard, but excludes generating the source batch.
// Writers reuse a pre-sized sink so buffer growth does not dominate the result.
func BenchmarkCapsuleCodec(b *testing.B) {
	const count = 64
	for _, size := range []int{0, 64, 1200} {
		value := bytes.Repeat([]byte{0x5a}, size)
		var source bytes.Buffer
		for i := 0; i < count; i++ {
			if err := own.WriteCapsule(&source, 0, value); err != nil {
				b.Fatal(err)
			}
		}
		encoded := source.Bytes()
		for _, implementation := range []string{"independent", "upstream"} {
			b.Run(fmt.Sprintf("parse/%s/%d", implementation, size), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(encoded)))
				for b.Loop() {
					reader := bytes.NewReader(encoded)
					if implementation == "independent" {
						parser := own.NewCapsuleParser(reader)
						for i := 0; i < count; i++ {
							typ, payload, err := parser.Next()
							if err != nil || typ != 0 {
								b.Fatal(typ, err)
							}
							if err := payload.Discard(); err != nil {
								b.Fatal(err)
							}
						}
					} else {
						parser := peer.NewCapsuleParser(reader)
						for i := 0; i < count; i++ {
							typ, payload, err := parser.Next()
							if err != nil || typ != 0 {
								b.Fatal(typ, err)
							}
							if err := payload.Discard(); err != nil {
								b.Fatal(err)
							}
						}
					}
				}
			})
			b.Run(fmt.Sprintf("write/%s/%d", implementation, size), func(b *testing.B) {
				var sink bytes.Buffer
				sink.Grow(len(encoded))
				b.ReportAllocs()
				b.SetBytes(int64(len(encoded)))
				for b.Loop() {
					sink.Reset()
					for i := 0; i < count; i++ {
						var err error
						if implementation == "independent" {
							err = own.WriteCapsule(&sink, 0, value)
						} else {
							err = peer.WriteCapsule(&sink, 0, value)
						}
						if err != nil {
							b.Fatal(err)
						}
					}
					if sink.Len() != len(encoded) {
						b.Fatal("incomplete encoding")
					}
				}
			})
		}
	}
}
