package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/handshake"
	"github.com/getlantern/photon/internal/wire"
)

func TestVersionInformationHandshake(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := testPair(t, v)
		drivePair(t, client, server, false, false)
		for _, c := range []*Conn{client, server} {
			info := c.peerParams.VersionInformation
			if info == nil || info.Chosen != v || len(info.Available) != 1 || info.Available[0] != v {
				t.Fatal(info)
			}
			if err := c.validateVersionInformation(nil); err != nil {
				t.Fatal("legacy peer rejected", err)
			}
			var protocol *TransportError
			err := c.validateVersionInformation(&wire.VersionInformation{Chosen: wire.Version(99)})
			if !errors.As(err, &protocol) || protocol.Code != 0x11 {
				t.Fatal(err)
			}
		}
	}
}

func TestMismatchedVersionInformationClosesHandshake(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	// Inject a syntactically valid but inconsistent server transport parameter
	// through the independent TLS adapter. NewConn rejects this local config.
	params := server.config.Parameters
	params.VersionInformation = &wire.VersionInformation{Chosen: wire.Version2, Available: []wire.Version{wire.Version1}}
	encoded, err := params.Marshal(true)
	if err != nil {
		t.Fatal(err)
	}
	server.tls.Close()
	server.tls, err = handshake.New(true, wire.Version1, server.config.TLS, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.tls.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	for _, b := range drain(t, client, now) {
		if err := server.HandleDatagram(b, now); err != nil {
			t.Fatal(err)
		}
	}
	rejected := false
	for _, b := range drain(t, server, now) {
		err := client.HandleDatagram(b, now)
		if err != nil {
			var protocol *TransportError
			if !errors.As(err, &protocol) || protocol.Code != 0x11 || !client.Closing() {
				t.Fatal(err)
			}
			rejected = true
			break
		}
	}
	if !rejected {
		t.Fatal("mismatched authenticated version was accepted")
	}
}

func TestNegotiatedVersionDowngradeChecks(t *testing.T) {
	for _, test := range []struct {
		name       string
		version    wire.Version
		available  []wire.Version
		absent, ok bool
	}{
		{name: "v2 valid", version: wire.Version2, available: []wire.Version{wire.Version2}, ok: true},
		{name: "v1 valid", version: wire.Version1, available: []wire.Version{wire.Version1}, ok: true},
		{name: "v1 legacy", version: wire.Version1, absent: true, ok: true},
		{name: "v2 missing", version: wire.Version2, absent: true},
		{name: "empty", version: wire.Version2},
		{name: "original removed from VN", version: wire.Version2, available: []wire.Version{wire.Version1, wire.Version2}},
		{name: "chosen retiring", version: wire.Version2, available: []wire.Version{0x1a2a3a4a}, ok: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			original := wire.Version1
			if test.version == wire.Version1 {
				original = wire.Version2
			}
			c := &Conn{config: Config{Version: test.version, Negotiation: &VersionNegotiationState{Original: original, Preferences: []wire.Version{original, test.version}}}}
			var info *wire.VersionInformation
			if !test.absent {
				info = &wire.VersionInformation{Chosen: test.version, Available: test.available}
			}
			err := c.validateVersionInformation(info)
			if (err == nil) != test.ok {
				t.Fatal(err)
			}
			if err != nil {
				var protocol *TransportError
				if !errors.As(err, &protocol) || protocol.Code != 0x11 {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestNegotiatedAttemptIgnoresFurtherVN(t *testing.T) {
	c, _ := testPair(t, wire.Version2)
	c.config.Negotiation = &VersionNegotiationState{Original: wire.Version1, Preferences: []wire.Version{wire.Version1, wire.Version2}}
	now := time.Unix(100, 0)
	if _, err := c.NextDatagram(now); err != nil {
		t.Fatal(err)
	}
	packet, _ := wire.AppendVersionNegotiation(nil, 0, c.config.LocalCID, c.config.OriginalDCID, []wire.Version{wire.Version1})
	if err := c.HandleDatagram(packet, now); err != nil || c.closed {
		t.Fatal("second VN acted upon", err)
	}
}

// Returning to the initially attempted version cannot authenticate a forged VN.
// The decision to start the second connection must itself remain justified.
func TestCompatibleFallbackVersionDowngradeChecks(t *testing.T) {
	for _, first := range []wire.Version{wire.Version1, wire.Version2} {
		fallback := wire.Version1
		if fallback == first {
			fallback = wire.Version2
		}
		for _, available := range [][]wire.Version{{first, fallback}, {fallback}, {0x1a2a3a4a}, nil} {
			c := &Conn{originalVersion: fallback, config: Config{Version: first, Negotiation: &VersionNegotiationState{Original: first, Preferences: []wire.Version{first, fallback}}}}
			info := &wire.VersionInformation{Chosen: first, Available: available}
			var protocol *TransportError
			if err := c.validateVersionInformation(info); !errors.As(err, &protocol) || protocol.Code != 0x11 {
				t.Fatalf("switched fallback accepted: %v", err)
			}
		}
		c := &Conn{originalVersion: fallback, config: Config{Version: fallback, Negotiation: &VersionNegotiationState{Original: first, Preferences: []wire.Version{first, fallback}}}}
		if err := c.validateVersionInformation(&wire.VersionInformation{Chosen: fallback, Available: []wire.Version{fallback}}); err != nil {
			t.Fatal("valid unswitched fallback rejected", err)
		}
	}
}
