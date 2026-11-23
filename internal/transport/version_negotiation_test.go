package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func TestServerVersionNegotiation(t *testing.T)         { testServerVersionNegotiation(t, false) }
func TestServerVersionNegotiationDisabled(t *testing.T) { testServerVersionNegotiation(t, true) }

func testServerVersionNegotiation(t *testing.T, disabled bool) {
	_, tlsConfig := testTLS(t)
	codec, _ := token.NewRetryTokens([32]byte{1}, nil, time.Minute)
	peer := netip.MustParseAddrPort("192.0.2.1:443")
	for _, configured := range []wire.Version{0, wire.Version1, wire.Version2} {
		e, err := NewServerEndpoint(Config{TLS: tlsConfig, Version: configured}, codec, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		e.DisableVersionNegotiationPackets = disabled
		for _, length := range []int{0, 8, 255} {
			dcid, scid := bytes.Repeat([]byte{2}, length), bytes.Repeat([]byte{3}, length)
			b, _ := wire.AppendVersionNegotiation(nil, 0, dcid, scid, []wire.Version{wire.Version1})
			binary.BigEndian.PutUint32(b[1:5], 0x12345678)
			b = append(b, make([]byte, 1200-len(b))...)
			c, out, err := e.HandleDatagram(context.Background(), b, peer, time.Now())
			if disabled {
				if err != nil || c != nil || out != nil || len(e.conns) != 0 {
					t.Fatal("suppressed negotiation allocated or replied", c, out, err)
				}
				continue
			}
			if err != nil || c != nil || out == nil || out.Conn != nil || len(out.Data) > len(b) || len(e.conns) != 0 {
				t.Fatal(c, out, err)
			}
			h, err := wire.ParseHeader(out.Data, 0)
			if err != nil || !bytes.Equal(h.DCID, scid) || !bytes.Equal(h.SCID, dcid) {
				t.Fatal(h, err)
			}
			count := 1
			if configured == 0 {
				count = 2
			}
			if len(h.Versions) != count+1 {
				t.Fatal(h.Versions)
			}
			if configured != 0 && h.Versions[0] != configured {
				t.Fatal(h.Versions)
			}
			if _, reply, err := e.HandleDatagram(context.Background(), b[:1199], peer, time.Now()); err != nil || reply != nil {
				t.Fatal("undersized", reply, err)
			}
			// Even a large valid Version Negotiation packet cannot cause a reply loop.
			versions := make([]wire.Version, 300)
			for i := range versions {
				versions[i] = wire.Version1
			}
			vn, _ := wire.AppendVersionNegotiation(nil, 0, dcid, scid, versions)
			if _, reply, err := e.HandleDatagram(context.Background(), vn, peer, time.Now()); err != nil || reply != nil {
				t.Fatal("VN reply", reply, err)
			}
		}
	}
}

func TestClientVersionNegotiationValidation(t *testing.T) {
	for _, test := range []string{"valid", "chosen-listed", "wrong-source", "wrong-destination", "before-send", "after-retry", "after-authenticated"} {
		t.Run(test, func(t *testing.T) {
			c, unused := testPair(t, wire.Version1)
			unused.Close()
			now := time.Unix(100, 0)
			if test != "before-send" {
				if _, err := c.NextDatagram(now); err != nil {
					t.Fatal(err)
				}
			}
			dcid, scid := append([]byte(nil), c.config.LocalCID...), append([]byte(nil), c.config.OriginalDCID...)
			versions := []wire.Version{wire.Version2}
			switch test {
			case "chosen-listed":
				versions = append(versions, wire.Version1)
			case "wrong-source":
				scid[0] ^= 1
			case "wrong-destination":
				dcid[0] ^= 1
			case "after-retry":
				c.retryUsed = true
			case "after-authenticated":
				c.authenticatedPackets = 1
			}
			b, _ := wire.AppendVersionNegotiation(nil, 0, dcid, scid, versions)
			err := c.HandleDatagram(b, now)
			if test != "valid" {
				if err != nil || c.closed {
					t.Fatal("invalid VN changed state", err)
				}
				return
			}
			var rejected *VersionNegotiationError
			if !errors.As(err, &rejected) || !c.closed || c.Closing() || rejected.ChosenVersion != wire.Version1 || len(rejected.PeerVersions) != 1 || rejected.PeerVersions[0] != wire.Version2 {
				t.Fatal(err)
			}
		})
	}
}

func TestUnknownVersionOnKnownRoute(t *testing.T) {
	c, _ := testPair(t, wire.Version1)
	r, err := NewRouter(len(c.config.LocalCID), 1)
	if err != nil {
		t.Fatal(err)
	}
	peer := netip.MustParseAddrPort("192.0.2.1:443")
	if err := r.Add(c, peer); err != nil {
		t.Fatal(err)
	}
	b, _ := wire.AppendVersionNegotiation(nil, 0, c.config.LocalCID, []byte("source"), []wire.Version{wire.Version1})
	binary.BigEndian.PutUint32(b[1:5], 0x12345678)
	got, known := r.Route(b, peer)
	if got != c || !known {
		t.Fatal("lost existing route")
	}
	got, known = r.Route(b, netip.MustParseAddrPort("192.0.2.2:443"))
	if got != nil || !known {
		t.Fatal("wrong-address CID was allowed to reach admission")
	}
}
