package packet

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestOpenOutputOwnership(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, suite := range []uint16{tls.TLS_AES_128_GCM_SHA256, tls.TLS_AES_256_GCM_SHA384, tls.TLS_CHACHA20_POLY1305_SHA256} {
			t.Run(fmt.Sprintf("%x/%x", version, suite), func(t *testing.T) {
				hash, _, _ := suiteParams(suite)
				k, err := TrafficKeys(version, suite, bytes.Repeat([]byte{42}, hash().Size()))
				if err != nil {
					t.Fatal(err)
				}
				for _, size := range []int{3, 31, 1140, 1400, 65500} {
					originalHeader := []byte{0x40, 1}
					originalPayload := bytes.Repeat([]byte{0x5a}, size)
					ciphertext, err := k.Seal(originalHeader, originalPayload, 1)
					if err != nil {
						t.Fatal(err)
					}
					saved := bytes.Clone(ciphertext)
					header, payload, _, err := k.Open(ciphertext, 1, 1)
					if err != nil {
						t.Fatal(err)
					}
					// Appending to either output must not corrupt the other, even when
					// both occupy distinct regions of a shared allocation.
					_ = append(header, 0xff)
					_ = append(payload, 0xee)
					if !bytes.Equal(header, originalHeader) || !bytes.Equal(payload, originalPayload) {
						t.Fatal("output append changed sibling")
					}
					otherHeader, otherPayload, _, err := k.Open(ciphertext, 1, 1)
					if err != nil {
						t.Fatal(err)
					}
					clear(otherHeader)
					clear(otherPayload)
					bad := bytes.Clone(ciphertext)
					bad[len(bad)-1] ^= 1
					if h, p, _, err := k.Open(bad, 1, 1); err == nil || h != nil || p != nil {
						t.Fatal("failed authentication exposed output")
					}
					if !bytes.Equal(header, originalHeader) || !bytes.Equal(payload, originalPayload) || !bytes.Equal(ciphertext, saved) {
						t.Fatal("open mutated retained output or input")
					}
				}
			})
		}
	}
}

func BenchmarkOpenOwnedPacket(b *testing.B) {
	k, _, err := InitialKeys(wire.Version1, []byte("12345678"))
	if err != nil {
		b.Fatal(err)
	}
	packet, err := k.Seal([]byte{0x43, 0, 0, 0, 1}, bytes.Repeat([]byte{1}, 1400), 1)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(1400)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, payload, _, err := k.Open(packet, 1, 1)
		if err != nil || len(payload) != 1400 {
			b.Fatal(err)
		}
	}
}

func TestOpenIntoStorage(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, suite := range []uint16{tls.TLS_AES_128_GCM_SHA256, tls.TLS_AES_256_GCM_SHA384, tls.TLS_CHACHA20_POLY1305_SHA256} {
			t.Run(fmt.Sprintf("%x/%x", v, suite), func(t *testing.T) {
				hash, _, _ := suiteParams(suite)
				k, err := TrafficKeys(v, suite, bytes.Repeat([]byte{42}, hash().Size()))
				if err != nil {
					t.Fatal(err)
				}
				storage := make([]byte, 65535)
				for _, size := range []int{3, 1400, 65500, 31} {
					wantHeader := []byte{0x43, 0, 0, 0, 1}
					wantPayload := bytes.Repeat([]byte{0x5a}, size)
					raw, err := k.Seal(wantHeader, wantPayload, 1)
					if err != nil {
						t.Fatal(err)
					}
					original := bytes.Clone(raw)
					for _, dst := range [][]byte{nil, make([]byte, 1), storage[:0]} {
						h, p, n, err := k.OpenInto(dst, raw, 1, 1)
						if err != nil || n != 1 || !bytes.Equal(h, wantHeader) || !bytes.Equal(p, wantPayload) {
							t.Fatal("decode mismatch", err)
						}
						if cap(dst) >= len(raw) && &h[0] != &storage[0] {
							t.Fatal("did not reuse storage")
						}
						_ = append(h, 0xff)
						if !bytes.Equal(p, wantPayload) {
							t.Fatal("header append overwrote payload")
						}
					}
					bad := bytes.Clone(raw)
					bad[len(bad)-1] ^= 1
					if h, p, _, err := k.OpenInto(storage, bad, 1, 1); err != ErrAuthentication || h != nil || p != nil {
						t.Fatal("authentication failure exposed output", err)
					}
					if !bytes.Equal(raw, original) {
						t.Fatal("mutated ciphertext")
					}
					// A failed candidate can dirty storage; the next successful open must
					// fully restore its own views, including when packet lengths shrink.
					h, p, _, err := k.OpenInto(storage, raw, 1, 1)
					if err != nil || !bytes.Equal(h, wantHeader) || !bytes.Equal(p, wantPayload) {
						t.Fatal("reuse after failure", err)
					}
				}
			})
		}
	}
}

func BenchmarkOpenBorrowedPacket(b *testing.B) {
	k, _, err := InitialKeys(wire.Version1, []byte("12345678"))
	if err != nil {
		b.Fatal(err)
	}
	raw, err := k.Seal([]byte{0x43, 0, 0, 0, 1}, bytes.Repeat([]byte{1}, 1400), 1)
	if err != nil {
		b.Fatal(err)
	}
	dst := make([]byte, len(raw))
	b.ReportAllocs()
	b.SetBytes(1400)
	b.ResetTimer()
	for b.Loop() {
		if _, p, _, err := k.OpenInto(dst, raw, 1, 1); err != nil || len(p) != 1400 {
			b.Fatal(err)
		}
	}
}
