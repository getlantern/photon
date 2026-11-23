package interop

import (
	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"math/rand/v2"
	"testing"
)

func TestVersionAPIIndependent(t *testing.T) {
	ours, reference := quic.SupportedVersions(), upstream.SupportedVersions()
	if len(ours) != len(reference) {
		t.Fatal(ours, reference)
	}
	for i, v := range ours {
		if uint32(v) != uint32(reference[i]) {
			t.Fatal(ours, reference)
		}
	}
	values := []uint32{0, 1, 2, 0x6b3343cf, 0xff00001d, 0xffffffff, 0x51303030, 0x5130343a}
	for v := uint32(0x51303031); v <= 0x51303439; v++ {
		values = append(values, v)
	}
	rng := rand.New(rand.NewPCG(0, 1))
	for i := 0; i < 10000; i++ {
		values = append(values, rng.Uint32())
	}
	for _, v := range values {
		if got, want := quic.Version(v).String(), upstream.Version(v).String(); got != want {
			t.Fatalf("%08x: %q != %q", v, got, want)
		}
	}
}
