package transport

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// Exercise encrypted packet parsing and dispatch, not just the storage helper.
// Paths are installed by the existing engine fixture; this is not UDP/path
// discovery or independent-peer multipath coverage.
func TestMultipathAddressTokenReceipt(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			c, keys, cid := pathDecodeFixture(t, v)
			now := time.Unix(101, 0)
			var tokens [][]byte
			c.config.OnToken = func(b []byte, at time.Time) {
				if !at.Equal(now) {
					t.Fatal("incorrect receipt time")
				}
				tokens = append(tokens, b)
			}
			token := []byte("opaque address token on secondary path")
			payload, err := (wire.NewTokenFrame{Token: token}).Append(nil)
			if err != nil {
				t.Fatal(err)
			}
			packet := sealPathPayload(t, keys, cid, 1, 100, payload)
			damaged := bytes.Clone(packet)
			damaged[len(damaged)-1] ^= 1
			if err := c.handleMultipathPacket(damaged, now); err != nil {
				t.Fatal(err)
			}
			if len(tokens) != 0 {
				t.Fatal("unauthenticated token retained")
			}
			if err := c.handleMultipathPacket(packet, now); err != nil {
				t.Fatal(err)
			}
			if len(tokens) != 1 || !bytes.Equal(tokens[0], token) {
				t.Fatal("secondary path token not retained")
			}
			// A repeated packet and fresh packets carrying the same frame on either
			// path must not call the store again, even if the first token was consumed.
			for _, raw := range [][]byte{
				packet,
				sealPathPayload(t, keys, cid, 1, 101, payload),
				sealPathPayload(t, keys, c.config.LocalCID, 0, 100, payload),
			} {
				if err := c.handleMultipathPacket(raw, now); err != nil {
					t.Fatal(err)
				}
			}
			if len(tokens) != 1 {
				t.Fatal("cross-path retransmission stored twice")
			}
			// A distinct token arriving on path zero is still useful.
			next, err := (wire.NewTokenFrame{Token: []byte("fresh token")}).Append(nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.handleMultipathPacket(sealPathPayload(t, keys, c.config.LocalCID, 0, 101, next), now); err != nil {
				t.Fatal(err)
			}
			clear(packet)
			clear(payload)
			if len(tokens) != 2 || !bytes.Equal(tokens[0], token) || string(tokens[1]) != "fresh token" {
				t.Fatal("token ownership or fresh delivery")
			}
		})
	}
}
