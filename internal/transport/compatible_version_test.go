package transport

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

func compatiblePair(t testing.TB, original, selected wire.Version, keyLog ...io.Writer) (*Conn, *Conn) {
	t.Helper()
	ct, st := testTLS(t)
	if len(keyLog) != 0 {
		ct.KeyLogWriter = keyLog[0]
	}
	cfg := Config{Version: original, TLS: ct, LocalCID: []byte("client00"), OriginalDCID: []byte("original"), PeerCID: []byte("original"), CompatibleVersions: []wire.Version{selected, original}}
	cfg.Parameters = wire.DefaultTransportParameters()
	cfg.Parameters.VersionInformation = &wire.VersionInformation{Chosen: original, Available: []wire.Version{original, selected}}
	client, err := NewConn(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	cfg.Server, cfg.TLS = true, st
	cfg.LocalCID, cfg.PeerCID = []byte("server00"), []byte("client00")
	server, err := NewConn(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return client, server
}

func TestCompatibleEngineHandshake(t *testing.T) {
	for _, original := range []wire.Version{wire.Version1, wire.Version2} {
		selected := wire.Version1
		if original == selected {
			selected = wire.Version2
		}
		for _, loss := range []bool{false, true} {
			for _, reverse := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/loss=%t/reverse=%t", original, loss, reverse), func(t *testing.T) {
					client, server := compatiblePair(t, original, selected)
					drivePair(t, client, server, loss, reverse)
					for _, c := range []*Conn{client, server} {
						if c.config.Version != selected || !c.confirmed {
							t.Fatal("version switch incomplete")
						}
						if c.originalInitialRead != nil || c.initialRead != nil {
							t.Fatal("Initial keys survived confirmation")
						}
					}
					if client.peerParams.VersionInformation.Chosen != selected || server.peerParams.VersionInformation.Chosen != original {
						t.Fatal("wrong authenticated version information")
					}
				})
			}
		}
	}
}

func TestCompatibleInitialAuthenticationAndRetention(t *testing.T) {
	client, server := compatiblePair(t, wire.Version1, wire.Version2)
	now := time.Unix(100, 0)
	first := drain(t, client, now)
	for _, p := range first {
		if err := server.HandleDatagram(p, now); err != nil {
			t.Fatal(err)
		}
	}
	if server.originalInitialRead == nil || server.config.Version != wire.Version2 {
		t.Fatal("server did not retain original keys")
	}
	before := server.authenticatedPackets
	for _, p := range first {
		if err := server.HandleDatagram(p, now); err != nil {
			t.Fatal(err)
		}
	}
	if server.authenticatedPackets <= before {
		t.Fatal("original Initial retransmission was not authenticated")
	}
	flight := drain(t, server, now)
	for _, p := range flight {
		bad := append([]byte(nil), p...)
		bad[len(bad)-1] ^= 1
		if err := client.HandleDatagram(bad, now); err != nil {
			t.Fatal(err)
		}
	}
	if client.config.Version != wire.Version1 || client.versionChosen {
		t.Fatal("unauthenticated packet selected version")
	}
	for _, p := range flight {
		if err := client.HandleDatagram(p, now); err != nil {
			t.Fatal(err)
		}
	}
	if client.config.Version != wire.Version2 {
		t.Fatal("authenticated Initial did not select version")
	}
	drivePair(t, client, server, false, false)
}

func TestCompatibleOriginalCryptoLocksVersion(t *testing.T) {
	client, server := compatiblePair(t, wire.Version1, wire.Version2)
	server.config.CompatibleVersions = []wire.Version{wire.Version1, wire.Version2}
	now := time.Unix(100, 0)
	for _, p := range drain(t, client, now) {
		if err := server.HandleDatagram(p, now); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range drain(t, server, now) {
		h, err := server.parseHeader(p)
		if err != nil {
			t.Fatal(err)
		}
		if h.Type == wire.Initial {
			if err := client.HandleDatagram(p, now); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !client.versionChosen || client.config.Version != wire.Version1 {
		t.Fatal("original server CRYPTO did not lock version")
	}
	_, keys, err := packet.InitialKeys(wire.Version2, []byte("original"))
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 100)
	payload[0] = 1 // PING and padding.
	header, err := wire.AppendLongHeader(nil, wire.Version2, wire.Initial, []byte("client00"), []byte("server00"), nil, 7, 4, len(payload)+16)
	if err != nil {
		t.Fatal(err)
	}
	data, err := keys.Seal(header, payload, 7)
	if err != nil {
		t.Fatal(err)
	}
	before := client.authenticatedPackets
	if err := client.HandleDatagram(data, now); err != nil {
		t.Fatal(err)
	}
	if client.config.Version != wire.Version1 || client.authenticatedPackets != before {
		t.Fatal("late version switch was accepted")
	}
}
