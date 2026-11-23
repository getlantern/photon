package interop

import (
	"bytes"
	"testing"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestConnectionIDReferenceValues(t *testing.T) {
	var own quic.ConnectionID
	var peer upstream.ConnectionID
	if own.Valid() != peer.Valid() || own.Len() != peer.Len() || own.String() != peer.String() || !bytes.Equal(own.Bytes(), peer.Bytes()) {
		t.Fatal("zero value mismatch", own.Valid(), own.Len(), own.String())
	}
	for size := 0; size <= 20; size++ {
		for seed := 0; seed < 256; seed++ {
			input := make([]byte, size)
			for i := range input {
				input[i] = byte(seed + i*31)
			}
			own, peer := quic.ConnectionIDFromBytes(input), upstream.ConnectionIDFromBytes(input)
			if own.Valid() != peer.Valid() || (own == quic.ConnectionID{}) != (peer == upstream.ConnectionID{}) {
				t.Fatal("validity mismatch", size, seed)
			}
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
