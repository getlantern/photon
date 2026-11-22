package packet

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathNonceVector(t *testing.T) {
	// draft-ietf-quic-multipath-21 section 2.4, published nonce example.
	k := &Keys{}
	copy(k.iv[:], unhex("6b26114b9cba2b63a9e8dd4f"))
	got := k.nonceForPath(3, 54321)
	if !bytes.Equal(got[:], unhex("6b2611489cba2b63a9e8097e")) {
		t.Fatalf("nonce: %x", got)
	}
}

func TestMultipathProtection(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, suite := range []uint16{tls.TLS_AES_128_GCM_SHA256, tls.TLS_AES_256_GCM_SHA384, tls.TLS_CHACHA20_POLY1305_SHA256} {
			t.Run(fmt.Sprintf("%x/%x", v, suite), func(t *testing.T) {
				h, _, _ := suiteParams(suite)
				k, err := TrafficKeys(v, suite, bytes.Repeat([]byte{0x73}, h().Size()))
				if err != nil {
					t.Fatal(err)
				}
				for generation := 0; generation < 3; generation++ {
					for _, pn := range []uint64{0, 54321, wire.MaxVarint} {
						header := binary.BigEndian.AppendUint32([]byte{0x43 | byte(generation&1)<<2}, uint32(pn))
						payload := []byte("authenticated multipath payload")
						base, err := k.Seal(header, payload, pn)
						if err != nil {
							t.Fatal(err)
						}
						seen := map[string]bool{}
						for _, pathID := range []uint32{0, 1, 3, ^uint32(0)} {
							sealed, err := k.SealPath(header, payload, pn, pathID)
							if err != nil {
								t.Fatal(err)
							}
							if pathID == 0 && !bytes.Equal(base, sealed) {
								t.Fatal("path zero differs from base QUIC")
							}
							if seen[string(sealed)] {
								t.Fatal("distinct paths produced identical ciphertext")
							}
							seen[string(sealed)] = true
							original := bytes.Clone(sealed)
							gotHeader, gotPayload, gotPN, err := k.OpenPath(sealed, 1, pn, pathID)
							if err != nil || gotPN != pn || !bytes.Equal(gotHeader, header) || !bytes.Equal(gotPayload, payload) {
								t.Fatalf("path %d packet %d: %v", pathID, pn, err)
							}
							if !bytes.Equal(original, sealed) {
								t.Fatal("mutated packet")
							}
							if _, _, _, err := k.OpenPath(sealed, 1, pn, pathID^1); !errors.Is(err, ErrAuthentication) {
								t.Fatalf("wrong path authentication: %v", err)
							}
						}
					}
					k, err = k.Next()
					if err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func TestMultipathRejectsInvalidInput(t *testing.T) {
	k, _, err := InitialKeys(wire.Version1, []byte("test CID"))
	if err != nil {
		t.Fatal(err)
	}
	for _, header := range [][]byte{nil, {0xc3, 0, 0, 0, 0}, {0xe3, 0, 0, 0, 0}, {0xd3, 0, 0, 0, 0}} {
		if _, err := k.SealPath(header, make([]byte, 32), 0, 1); !errors.Is(err, ErrPacket) {
			t.Fatalf("seal: %v", err)
		}
		if _, _, _, err := k.OpenPath(header, 1, 0, 1); !errors.Is(err, ErrPacket) {
			t.Fatalf("open: %v", err)
		}
	}
	if _, err := k.SealPath([]byte{0x43, 0, 0, 0, 0}, make([]byte, 32), wire.MaxVarint+1, 1); !errors.Is(err, ErrPacket) {
		t.Fatalf("packet number overflow: %v", err)
	}
}
