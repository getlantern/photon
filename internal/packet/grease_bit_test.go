package packet

import (
	"bytes"
	"crypto/tls"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestGreasedQUICBitAuthenticated(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, suite := range []uint16{tls.TLS_AES_128_GCM_SHA256, tls.TLS_AES_256_GCM_SHA384, tls.TLS_CHACHA20_POLY1305_SHA256} {
			hash, _, _ := suiteParams(suite)
			keys, err := TrafficKeys(v, suite, bytes.Repeat([]byte{7}, hash().Size()))
			if err != nil {
				t.Fatal(err)
			}
			for _, long := range []bool{false, true} {
				header := []byte{0, 1}
				payload := []byte("authenticated greased QUIC bit")
				if long {
					header, err = wire.AppendLongHeader(nil, v, wire.Handshake, nil, nil, nil, 1, 1, len(payload)+16)
					if err != nil {
						t.Fatal(err)
					}
					header[0] &= ^byte(0x40)
				}
				sealed, err := keys.Seal(header, payload, 1)
				if err != nil {
					t.Fatal(err)
				}
				parsed, err := wire.ParseHeaderWithQUICBit(sealed, 0, true)
				if err != nil {
					t.Fatal(err)
				}
				_, plain, pn, err := keys.Open(sealed, parsed.PNOffset, 0)
				if err != nil || pn != 1 || !bytes.Equal(plain, payload) {
					t.Fatal(v, suite, long, err)
				}
				sealed[0] ^= 0x40
				if _, _, _, err := keys.Open(sealed, parsed.PNOffset, 0); err == nil {
					t.Fatal("QUIC bit tampering authenticated")
				}
			}
		}
	}
}
