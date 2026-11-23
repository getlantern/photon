package packet

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestSharedKeysConcurrentProtection(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, suite := range []uint16{tls.TLS_AES_128_GCM_SHA256, tls.TLS_AES_256_GCM_SHA384, tls.TLS_CHACHA20_POLY1305_SHA256} {
			t.Run(fmt.Sprintf("%x/%x", v, suite), func(t *testing.T) {
				hash, _, err := suiteParams(suite)
				if err != nil {
					t.Fatal(err)
				}
				keys, err := TrafficKeys(v, suite, bytes.Repeat([]byte{7}, hash().Size()))
				if err != nil {
					t.Fatal(err)
				}
				var wg sync.WaitGroup
				for worker := 0; worker < 16; worker++ {
					wg.Go(func() {
						for iteration := 0; iteration < 64; iteration++ {
							// Unique numbers prevent key/nonce reuse even across concurrent writers.
							pn := uint64(worker*64 + iteration)
							header := binary.BigEndian.AppendUint32([]byte{0x43}, uint32(pn))
							payload := bytes.Repeat([]byte{byte(worker)}, 64+iteration)
							path := uint32(worker % 4)
							ciphertext, err := keys.SealPath(header, payload, pn, path)
							if err != nil {
								t.Error(err)
								return
							}
							h, p, n, err := keys.OpenPath(ciphertext, 1, pn, path)
							if err != nil || n != pn || !bytes.Equal(h, header) || !bytes.Equal(p, payload) {
								t.Errorf("concurrent packet %d: %v", pn, err)
								return
							}
							// Keep each returned plaintext intact across another operation using keys.
							again, err := keys.SealPath(header, payload, pn+1024, path)
							// Header packet number must match; this intentionally invalid call must
							// neither alter the prior output nor affect other operations.
							if err == nil || again != nil || !bytes.Equal(p, payload) {
								t.Error("invalid seal affected retained output")
								return
							}
						}
					})
				}
				wg.Wait()
			})
		}
	}
}
