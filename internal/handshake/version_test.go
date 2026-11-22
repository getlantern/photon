package handshake

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

func TestCompatibleTLSVersionSelection(t *testing.T) {
	for _, original := range []wire.Version{wire.Version1, wire.Version2} {
		selected := wire.Version1
		if original == selected {
			selected = wire.Version2
		}
		t.Run(fmt.Sprintf("%x-to-%x", original, selected), func(t *testing.T) {
			cc, sc := configs(t)
			cp := wire.DefaultTransportParameters()
			cp.InitialSourceConnectionID = []byte("client00")
			cp.VersionInformation = &wire.VersionInformation{Chosen: original, Available: []wire.Version{selected, original}}
			sp := wire.DefaultTransportParameters()
			sp.InitialSourceConnectionID = []byte("server00")
			sp.OriginalDestinationConnectionID = []byte("original")
			sp.VersionInformation = &wire.VersionInformation{Chosen: original, Available: []wire.Version{selected, original}}
			cb, err := cp.Marshal(false)
			if err != nil {
				t.Fatal(err)
			}
			sb, err := sp.Marshal(true)
			if err != nil {
				t.Fatal(err)
			}
			client, err := New(false, original, cc, cb)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			calls := 0
			server, err := NewNegotiatingServer(original, sc, sb, func(peer wire.TransportParameters) (wire.Version, []byte, error) {
				calls++
				if peer.VersionInformation == nil || peer.VersionInformation.Chosen != original {
					t.Fatal("wrong first-flight version")
				}
				sp.VersionInformation.Chosen = selected
				encoded, err := sp.Marshal(true)
				return selected, encoded, err
			})
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			if err := server.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := client.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			ck, _, err := packet.InitialKeys(original, []byte("original"))
			if err != nil {
				t.Fatal(err)
			}
			cn, sn := make(map[tls.QUICEncryptionLevel]uint64), make(map[tls.QUICEncryptionLevel]uint64)
			transfer(t, original, client, server, ck, ck, cn)
			if calls != 1 || server.Version() != selected {
				t.Fatal("server did not select exactly once")
			}
			if err := client.SetNegotiatedVersion(selected); err != nil {
				t.Fatal(err)
			}
			ck, sk, err := packet.InitialKeys(selected, []byte("original"))
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 10; i++ {
				n := transfer(t, selected, server, client, sk, sk, sn) + transfer(t, selected, client, server, ck, ck, cn)
				if n == 0 {
					break
				}
			}
			if !client.Complete() || !server.Complete() || len(client.ConnectionState().VerifiedChains) == 0 {
				t.Fatal("negotiated handshake incomplete")
			}
			for _, pair := range [][2]*TLS{{client, server}, {server, client}} {
				wk := pair[0].WriteKeys(tls.QUICEncryptionLevelApplication)
				rk := pair[1].ReadKeys(tls.QUICEncryptionLevelApplication)
				sealed, err := wk.Seal([]byte{0x43, 0, 0, 0, 7}, []byte("negotiated data"), 7)
				if err != nil {
					t.Fatal(err)
				}
				_, plain, pn, err := rk.Open(sealed, 1, 7)
				if err != nil || pn != 7 || string(plain) != "negotiated data" {
					t.Fatalf("application keys after switching: %v", err)
				}
			}
			if err := client.SetNegotiatedVersion(original); err == nil {
				t.Fatal("late version change accepted")
			}
			encoded, ok := client.PeerParameters()
			if !ok {
				t.Fatal("missing server parameters")
			}
			peer, err := wire.ParseTransportParameters(encoded, true)
			if err != nil || peer.VersionInformation.Chosen != selected {
				t.Fatalf("negotiated server parameters: %v", err)
			}
			if _, err := NewNegotiatingServer(original, sc, sb, nil); err == nil {
				t.Fatal("nil selector accepted")
			}
		})
	}
}

func TestInvalidTLSVersionSelection(t *testing.T) {
	for _, mode := range []string{"unoffered", "unknown", "wrong-client-chosen", "wrong-server-chosen", "malformed", "selector-error"} {
		t.Run(mode, func(t *testing.T) {
			cc, sc := configs(t)
			cp := wire.DefaultTransportParameters()
			cp.InitialSourceConnectionID = []byte("client00")
			cp.VersionInformation = &wire.VersionInformation{Chosen: wire.Version1, Available: []wire.Version{wire.Version1}}
			if mode == "wrong-client-chosen" {
				cp.VersionInformation.Chosen = wire.Version2
				cp.VersionInformation.Available = []wire.Version{wire.Version2}
			}
			cb, err := cp.Marshal(false)
			if err != nil {
				t.Fatal(err)
			}
			sp := wire.DefaultTransportParameters()
			sp.InitialSourceConnectionID = []byte("server00")
			sp.OriginalDestinationConnectionID = []byte("original")
			sp.VersionInformation = &wire.VersionInformation{Chosen: wire.Version1, Available: []wire.Version{wire.Version1}}
			sb, err := sp.Marshal(true)
			if err != nil {
				t.Fatal(err)
			}
			client, err := New(false, wire.Version1, cc, cb)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			server, err := NewNegotiatingServer(wire.Version1, sc, sb, func(wire.TransportParameters) (wire.Version, []byte, error) {
				switch mode {
				case "unoffered":
					return wire.Version2, sb, nil
				case "unknown":
					return 99, sb, nil
				case "wrong-server-chosen":
					sp.VersionInformation.Chosen = wire.Version2
					b, err := sp.Marshal(true)
					return wire.Version1, b, err
				case "malformed":
					return wire.Version1, []byte{0xff}, nil
				case "selector-error":
					return 0, nil, fmt.Errorf("policy rejected")
				}
				return wire.Version1, sb, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			if err := client.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := server.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := client.SetNegotiatedVersion(wire.Version2); err == nil {
				t.Fatal("unoffered client transition accepted")
			}
			rejected := false
			for _, f := range client.Drain() {
				if err := server.HandleCrypto(f.Level, f.Offset, f.Data); err != nil {
					rejected = true
					break
				}
			}
			if !rejected || len(server.Drain()) != 0 || server.ReadKeys(tls.QUICEncryptionLevelHandshake) != nil || server.WriteKeys(tls.QUICEncryptionLevelHandshake) != nil {
				t.Fatal("invalid negotiation emitted a flight or installed keys")
			}
			if server.Version() != wire.Version1 {
				t.Fatal("failed selection changed version")
			}
		})
	}
}
