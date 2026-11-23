package interop

import (
	"bytes"
	"testing"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestConnectionIDReferenceValues(t *testing.T) {
	for size := 0; size <= 20; size++ {
		for seed := 0; seed < 256; seed++ {
			input := make([]byte, size)
			for i := range input {
				input[i] = byte(seed + i*31)
			}
			own, peer := quic.ConnectionIDFromBytes(input), upstream.ConnectionIDFromBytes(input)
			if own.Len() != peer.Len() || own.String() != peer.String() || !bytes.Equal(own.Bytes(), peer.Bytes()) {
				t.Fatal("reference mismatch", size, seed)
			}
			if size > 0 {
				original := bytes.Clone(input)
				input[0] ^= 0xff
				own.Bytes()[0] ^= 0xff
				peer.Bytes()[0] ^= 0xff
				if !bytes.Equal(own.Bytes(), original) || !bytes.Equal(peer.Bytes(), original) {
					t.Fatal("reference ownership mismatch")
				}
			}
		}
	}
}
