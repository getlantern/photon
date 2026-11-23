package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

func candidatePacket(t testing.TB, client *Conn, version wire.Version) []byte {
	t.Helper()
	_, keys, err := packet.InitialKeys(version, client.initialKeyDCID())
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 32)
	payload[0] = 1
	header, err := wire.AppendLongHeader(nil, version, wire.Initial, client.config.LocalCID, []byte("server00"), nil, 0, 4, len(payload)+16)
	if err != nil {
		t.Fatal(err)
	}
	data, err := keys.Seal(header, payload, 0)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func BenchmarkCompatibleCandidateAuthenticationFailure(b *testing.B) {
	for _, original := range []wire.Version{wire.Version1, wire.Version2} {
		b.Run(vName(original), func(b *testing.B) {
			selected := wire.Version1
			if selected == original {
				selected = wire.Version2
			}
			client, _ := compatiblePair(b, original, selected)
			bad := candidatePacket(b, client, selected)
			bad[len(bad)-1] ^= 1
			now := time.Unix(100, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := client.HandleDatagram(bad, now); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestCompatibleCandidateAuthenticationAndRetry(t *testing.T) {
	for _, original := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(original), func(t *testing.T) {
			selected := wire.Version1
			if original == selected {
				selected = wire.Version2
			}
			client, _ := compatiblePair(t, original, selected)
			now := time.Unix(100, 0)
			drain(t, client, now)
			good := candidatePacket(t, client, selected)
			bad := append([]byte(nil), good...)
			bad[len(bad)-1] ^= 1
			read, write := client.initialRead, client.initialWrite
			for i := 0; i < 3; i++ {
				cached := client.candidateInitial.read
				if err := client.HandleDatagram(bad, now); err != nil {
					t.Fatal(err)
				}
				if client.config.Version != original || client.versionChosen || client.peerCIDKnown || client.initialRead != read || client.initialWrite != write {
					t.Fatal("failed authentication changed active handshake state")
				}
				if client.failedAuth != uint64(i+1) {
					t.Fatalf("failed authentication count: %d", client.failedAuth)
				}
				if client.candidateInitial.read == nil || (cached != nil && cached != client.candidateInitial.read) {
					t.Fatal("candidate keys not reused")
				}
			}
			cached := client.candidateInitial.read
			retry := retryPacket(t, client, []byte("retrycid"), []byte("token"))
			if err := client.HandleDatagram(retry, now); err != nil {
				t.Fatal(err)
			}
			if !client.retryUsed || client.candidateInitial.read != nil {
				t.Fatal("Retry retained candidate keys")
			}
			if err := client.HandleDatagram(good, now); err != nil {
				t.Fatal(err)
			}
			if client.versionChosen || client.failedAuth != 4 || client.candidateInitial.read == cached {
				t.Fatal("pre-Retry candidate accepted or cache reused")
			}
			cachedRead, cachedWrite := client.candidateInitial.read, client.candidateInitial.write
			if err := client.HandleDatagram(candidatePacket(t, client, selected), now); err != nil {
				t.Fatal(err)
			}
			if client.config.Version != selected || !client.versionChosen || client.initialRead != cachedRead || client.initialWrite != cachedWrite {
				t.Fatal("authenticated candidate did not install cached keys")
			}
			if client.candidateInitial.read != nil || client.failedAuth != 4 {
				t.Fatal("selection retained cache or reset failure accounting")
			}
		})
	}
}

func TestCompatibleCandidateRelease(t *testing.T) {
	for _, cleanup := range []string{"discard", "release", "original-crypto"} {
		t.Run(cleanup, func(t *testing.T) {
			client, server := compatiblePair(t, wire.Version1, wire.Version2)
			server.config.CompatibleVersions = []wire.Version{wire.Version1, wire.Version2}
			now := time.Unix(100, 0)
			bad := candidatePacket(t, client, wire.Version2)
			bad[len(bad)-1] ^= 1
			if err := client.HandleDatagram(bad, now); err != nil {
				t.Fatal(err)
			}
			if client.candidateInitial.read == nil {
				t.Fatal("cache not populated")
			}
			switch cleanup {
			case "discard":
				client.discard(initialSpace)
			case "release":
				client.releaseTransportBuffers()
			case "original-crypto":
				for _, data := range drain(t, client, now) {
					if err := server.HandleDatagram(data, now); err != nil {
						t.Fatal(err)
					}
				}
				for _, data := range drain(t, server, now) {
					if err := client.HandleDatagram(data, now); err != nil {
						t.Fatal(err)
					}
				}
				if !client.versionChosen {
					t.Fatal("original CRYPTO did not lock selection")
				}
			}
			if client.candidateInitial != (candidateInitialKeys{}) {
				t.Fatal("candidate keys retained")
			}
		})
	}
}
