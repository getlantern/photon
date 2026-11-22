package packet

import (
	"crypto/tls"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestAEADUsageLimits(t *testing.T) {
	for _, tc := range []struct {
		suite                      uint16
		size                       int
		confidentiality, integrity uint64
	}{
		{tls.TLS_AES_128_GCM_SHA256, 32, 1 << 23, 1 << 52},
		{tls.TLS_AES_256_GCM_SHA384, 48, 1 << 23, 1 << 52},
		{tls.TLS_CHACHA20_POLY1305_SHA256, 32, 1 << 62, 1 << 36},
	} {
		keys, err := TrafficKeys(wire.Version2, tc.suite, make([]byte, tc.size))
		if err != nil {
			t.Fatal(err)
		}
		c, i := keys.UsageLimits()
		if c != tc.confidentiality || i != tc.integrity {
			t.Fatal(tc.suite, c, i)
		}
	}
}
