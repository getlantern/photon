package packet

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestSealIntoStorage(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, suite := range []uint16{tls.TLS_AES_128_GCM_SHA256, tls.TLS_AES_256_GCM_SHA384, tls.TLS_CHACHA20_POLY1305_SHA256} {
			t.Run(fmt.Sprintf("%x/%x", v, suite), func(t *testing.T) {
				hash, _, _ := suiteParams(suite)
				k, err := TrafficKeys(v, suite, bytes.Repeat([]byte{42}, hash().Size()))
				if err != nil {
					t.Fatal(err)
				}
				storage := make([]byte, 2048)
				for _, path := range []uint32{0, 3} {
					for i, size := range []int{3, 1400, 65500, 31} {
						pn := uint64(i + 1)
						h := []byte{0x43, 0, 0, 0, byte(pn)}
						p := bytes.Repeat([]byte{byte(size)}, size)
						originalH, originalP := bytes.Clone(h), bytes.Clone(p)
						owned, err := k.SealPath(h, p, pn, path)
						if err != nil {
							t.Fatal(err)
						}
						// Same inputs compare deterministic encodings; neither copy is sent.
						for _, dst := range [][]byte{nil, make([]byte, 1), storage[:0]} {
							var out []byte
							if path == 0 {
								out, err = k.SealInto(dst, h, p, pn)
							} else {
								out, err = k.SealPathInto(dst, h, p, pn, path)
							}
							if err != nil || !bytes.Equal(out, owned) {
								t.Fatal("seal mismatch", err)
							}
							if cap(dst) >= len(out) && &out[0] != &storage[0] {
								t.Fatal("storage not reused")
							}
							dh, dp, n, err := k.OpenPath(out, 1, pn, path)
							if err != nil || n != pn || !bytes.Equal(dh, h) || !bytes.Equal(dp, p) {
								t.Fatal("round trip", err)
							}
						}
						clear(storage)
						if !bytes.Equal(h, originalH) || !bytes.Equal(p, originalP) {
							t.Fatal("mutated input")
						}
						if _, _, _, err := k.OpenPath(owned, 1, pn, path); err != nil {
							t.Fatal("owned output aliases storage", err)
						}
					}
				}
			})
		}
	}
}

func BenchmarkSealPacketStorage(b *testing.B) {
	for _, borrowed := range []bool{false, true} {
		b.Run(fmt.Sprint(borrowed), func(b *testing.B) {
			k, _, err := InitialKeys(wire.Version1, []byte("12345678"))
			if err != nil {
				b.Fatal(err)
			}
			header := []byte{0x43, 0, 0, 0, 1}
			payload := make([]byte, 1400)
			var dst []byte
			if borrowed {
				dst = make([]byte, 1500)
			}
			b.ReportAllocs()
			b.SetBytes(1400)
			b.ResetTimer()
			// Local deterministic primitive benchmark; packets are never transmitted.
			for b.Loop() {
				if _, err := k.SealInto(dst, header, payload, 1); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func FuzzSealStorage(f *testing.F) {
	f.Add([]byte{0x43, 0, 0, 0, 1}, []byte{1, 2, 3}, uint64(1), uint32(3), uint16(2048))
	f.Add([]byte{}, []byte{}, uint64(0), uint32(0), uint16(0))
	k, _, _ := InitialKeys(wire.Version1, []byte("12345678"))
	f.Fuzz(func(t *testing.T, h, p []byte, pn uint64, path uint32, capacity uint16) {
		if len(h) > 128 || len(p) > 65535 {
			return
		}
		beforeH, beforeP := bytes.Clone(h), bytes.Clone(p)
		dst := make([]byte, int(capacity))
		owned, err := k.Seal(h, p, pn)
		borrowed, be := k.SealInto(dst, h, p, pn)
		if err != be || !bytes.Equal(owned, borrowed) {
			t.Fatal("ordinary output mismatch")
		}
		clear(dst)
		owned, err = k.SealPath(h, p, pn, path)
		borrowed, be = k.SealPathInto(dst, h, p, pn, path)
		if err != be || !bytes.Equal(owned, borrowed) {
			t.Fatal("path output mismatch")
		}
		if !bytes.Equal(h, beforeH) || !bytes.Equal(p, beforeP) {
			t.Fatal("mutated input")
		}
	})
}
