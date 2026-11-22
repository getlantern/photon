package packet

import (
	"bytes"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestBuildRetry(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, cid := range [][]byte{[]byte("retrycid"), {}} {
			original := []byte("original")
			client := []byte("client00")
			token := []byte("opaque-token")
			b, err := BuildRetry(v, original, client, cid, token)
			if err != nil {
				t.Fatal(err)
			}
			h, err := wire.ParseHeader(b, 8)
			if err != nil || h.Type != wire.Retry || h.Version != v || !bytes.Equal(h.DCID, client) || !bytes.Equal(h.SCID, cid) || !bytes.Equal(h.Token, token) {
				t.Fatal(h, err)
			}
			if !VerifyRetry(v, original, b) {
				t.Fatal("invalid integrity tag")
			}
			token[0] ^= 1
			if !VerifyRetry(v, original, b) {
				t.Fatal("packet aliases input")
			}
		}
	}
	for _, tc := range []struct{ original, client, retry, token []byte }{
		{[]byte("original"), nil, []byte("original"), []byte{1}},
		{[]byte("short"), nil, []byte("retry"), []byte{1}},
		{[]byte("original"), nil, []byte("retry"), nil},
		{[]byte("original"), make([]byte, 21), []byte("retry"), []byte{1}},
		{[]byte("original"), nil, []byte("retry"), make([]byte, 65527)},
	} {
		if _, err := BuildRetry(wire.Version1, tc.original, tc.client, tc.retry, tc.token); err == nil {
			t.Fatal("invalid Retry accepted")
		}
	}
}
