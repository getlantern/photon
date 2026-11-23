package packet

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

type vectorPacket struct {
	Payload, Header, Packet string
	PN                      uint64
}
type vectors struct {
	Version        int
	DCID           string
	Client, Server vectorPacket
	Retry          string
	ChaChaPacket   string `json:"chacha_packet"`
	UpdatedSecret  string `json:"updated_secret"`
}

func readVectors(t testing.TB, name string) vectors {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var v vectors
	if err = json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestRFCVectors(t *testing.T) {
	for _, name := range []string{"rfc9001", "rfc9369"} {
		t.Run(name, func(t *testing.T) {
			v := readVectors(t, name)
			version := wire.Version1
			if v.Version == 2 {
				version = wire.Version2
			}
			client, server, err := InitialKeys(version, unhex(v.DCID))
			if err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				name string
				k    *Keys
				p    vectorPacket
			}{{"client", client, v.Client}, {"server", server, v.Server}} {
				t.Run(tc.name, func(t *testing.T) {
					header, payload, want := unhex(tc.p.Header), unhex(tc.p.Payload), unhex(tc.p.Packet)
					got, err := tc.k.Seal(header, payload, tc.p.PN)
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("does not match published protected packet: %v\ngot %x\nwant %x", err, got, want)
					}
					h, err := wire.ParseHeader(want, 0)
					if err != nil {
						t.Fatal(err)
					}
					decoded, plain, pn, err := tc.k.Open(want, h.PNOffset, tc.p.PN)
					if err != nil || pn != tc.p.PN || !bytes.Equal(decoded, header) || !bytes.Equal(plain, payload) {
						t.Fatalf("RFC decryption failed: %v", err)
					}
					if !bytes.Equal(want, unhex(tc.p.Packet)) {
						t.Fatal("mutated input")
					}
					// Every bit of ciphertext and associated header data must be authenticated.
					for i := range want {
						bad := bytes.Clone(want)
						bad[i] ^= 1
						if _, _, _, err := tc.k.Open(bad, h.PNOffset, tc.p.PN); err == nil {
							t.Fatalf("accepted tampering at %d", i)
						}
					}
					for i := 0; i < len(want); i++ {
						if _, _, _, err := tc.k.Open(want[:i], h.PNOffset, tc.p.PN); err == nil {
							t.Fatalf("accepted truncated packet %d", i)
						}
					}
				})
			}
			if !VerifyRetry(version, unhex(v.DCID), unhex(v.Retry)) {
				t.Fatal("Retry RFC vector")
			}
			for i := range unhex(v.Retry) {
				bad := unhex(v.Retry)
				bad[i] ^= 1
				if VerifyRetry(version, unhex(v.DCID), bad) {
					t.Fatal("accepted altered Retry")
				}
			}
			if VerifyRetry(version, []byte("wrong CID"), unhex(v.Retry)) {
				t.Fatal("Retry not bound to CID")
			}
			k, err := TrafficKeys(version, tls.TLS_CHACHA20_POLY1305_SHA256, unhex("9ac312a7f877468ebe69422748ad00a15443f18203a07d6060f688f30f21632b"))
			if err != nil {
				t.Fatal(err)
			}
			got, err := k.Seal(unhex("4200bff4"), []byte{1}, 654360564)
			if err != nil || !bytes.Equal(got, unhex(v.ChaChaPacket)) {
				t.Fatalf("ChaCha20 RFC vector: %x %v", got, err)
			}
			_, plain, pn, err := k.Open(unhex(v.ChaChaPacket), 1, 654360564)
			if err != nil || pn != 654360564 || !bytes.Equal(plain, []byte{1}) {
				t.Fatal("ChaCha20 RFC decryption", err)
			}
			next, err := k.Next()
			if err != nil || !bytes.Equal(next.secret, unhex(v.UpdatedSecret)) {
				t.Fatal("key update RFC vector", err)
			}
			if !bytes.Equal(next.chachaHP, k.chachaHP) {
				t.Fatal("key update changed header protection")
			}
		})
	}
}

func TestSuitesAndKeyUpdates(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, suite := range []uint16{tls.TLS_AES_128_GCM_SHA256, tls.TLS_AES_256_GCM_SHA384, tls.TLS_CHACHA20_POLY1305_SHA256} {
			h, _, _ := suiteParams(suite)
			secret := bytes.Repeat([]byte{0x42}, h().Size())
			k, err := TrafficKeys(version, suite, secret)
			if err != nil {
				t.Fatal(err)
			}
			next, err := k.Next()
			if err != nil {
				t.Fatal(err)
			}
			for n := 1; n <= 4; n++ {
				header := append([]byte{0x40 | byte(n-1)}, make([]byte, n)...)
				header[n] = 1
				payload := []byte("test packet")
				p, err := k.Seal(header, payload, 1)
				if err != nil {
					t.Fatal(err)
				}
				_, out, pn, err := k.Open(p, 1, 0)
				if err != nil || pn != 1 || !bytes.Equal(out, payload) {
					t.Fatal("roundtrip", err)
				}
				if _, _, _, err = next.Open(p, 1, 0); err == nil {
					t.Fatal("next keys accepted old ciphertext")
				}
				updated, err := next.Seal(header, payload, 1)
				if err != nil {
					t.Fatal(err)
				}
				_, out, _, err = next.Open(updated, 1, 0)
				if err != nil || !bytes.Equal(out, payload) {
					t.Fatal("updated packet", err)
				}
			}
		}
	}
}

func TestInvalidProtectionInputs(t *testing.T) {
	k, _, _ := InitialKeys(wire.Version1, []byte("12345678"))
	for _, offset := range []int{-1, 0, 1, 100, int(^uint(0) >> 1)} {
		if _, _, _, err := k.Open(nil, offset, 0); err == nil {
			t.Fatal("invalid offset")
		}
	}
	for _, header := range [][]byte{nil, {0x40}, {0x43, 0}, {0x40, 1}, {0x48, 0}} {
		if _, err := k.Seal(header, nil, 0); err == nil {
			t.Fatalf("bad header %x", header)
		}
	}
	if _, _, err := InitialKeys(42, nil); !errors.Is(err, wire.ErrVersion) {
		t.Fatal(err)
	}
	if _, _, err := InitialKeys(wire.Version1, make([]byte, 21)); err == nil {
		t.Fatal("CID length")
	}
	if _, err := TrafficKeys(wire.Version1, 0, make([]byte, 32)); !errors.Is(err, ErrCipherSuite) {
		t.Fatal(err)
	}
	if _, err := TrafficKeys(wire.Version1, tls.TLS_AES_128_GCM_SHA256, nil); err == nil {
		t.Fatal("secret length")
	}
}

func TestAuthenticatedReservedBits(t *testing.T) {
	k, _, _ := InitialKeys(wire.Version1, []byte("12345678"))
	// Construct an authenticated protocol violation without using Seal, which
	// deliberately refuses to generate nonzero reserved bits.
	header := []byte{0x48, 0}
	nonce := k.nonce(0)
	p := k.aead.Seal(bytes.Clone(header), nonce[:], []byte{0, 0, 0}, header)
	mask := k.mask(p[5:])
	p[0] ^= mask[0] & 0x1f
	p[1] ^= mask[1]
	if _, _, _, err := k.Open(p, 1, 0); !errors.Is(err, ErrReservedBits) {
		t.Fatalf("expected authenticated violation: %v", err)
	}
	p[len(p)-1] ^= 1
	if _, _, _, err := k.Open(p, 1, 0); err == nil || errors.Is(err, ErrReservedBits) {
		t.Fatal("reserved bits checked before authentication")
	}
}

func FuzzOpen(f *testing.F) {
	v := readVectors(f, "rfc9001")
	f.Add(unhex(v.Client.Packet), uint16(18), uint64(0))
	f.Add([]byte{1}, uint16(0), uint64(0))
	k, _, _ := InitialKeys(wire.Version1, unhex(v.DCID))
	f.Fuzz(func(t *testing.T, b []byte, offset uint16, expected uint64) {
		before := bytes.Clone(b)
		h, p, pn, err := k.Open(b, int(offset), expected)
		bh, bp, bn, be := k.OpenInto(make([]byte, len(b)), b, int(offset), expected)
		if err != be || pn != bn || !bytes.Equal(h, bh) || !bytes.Equal(p, bp) {
			t.Fatal("owned/borrowed decode mismatch")
		}
		if !bytes.Equal(b, before) {
			t.Fatal("mutated input")
		}
	})
}

func BenchmarkInitialSeal(b *testing.B) {
	v := readVectors(b, "rfc9001")
	k, _, _ := InitialKeys(wire.Version1, unhex(v.DCID))
	header, payload := unhex(v.Client.Header), unhex(v.Client.Payload)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := k.Seal(header, payload, 2); err != nil {
			b.Fatal(err)
		}
	}
}
