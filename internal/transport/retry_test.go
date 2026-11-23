package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/getlantern/photon/internal/token"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

func retryPacket(t *testing.T, c *Conn, scid, token []byte) []byte {
	t.Helper()
	bits, _ := c.config.Version.TypeBits(wire.Retry)
	b := binary.BigEndian.AppendUint32([]byte{0xc0 | bits<<4}, uint32(c.config.Version))
	b = append(b, byte(len(c.config.LocalCID)))
	b = append(b, c.config.LocalCID...)
	b = append(b, byte(len(scid)))
	b = append(b, scid...)
	b = append(b, token...)
	tag, err := packet.RetryTag(c.config.Version, c.config.OriginalDCID, b)
	if err != nil {
		t.Fatal(err)
	}
	return append(b, tag[:]...)
}

func TestClientRetryRepacketizesClientHello(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, _ := testPair(t, v)
		now := time.Unix(200, 0)
		var original []byte
		for _, item := range c.spaces[initialSpace].queue {
			if f, ok := item.frame.(wire.CryptoFrame); ok {
				original = append(original, f.Data...)
			}
		}
		drain(t, c, now)
		next := c.spaces[initialSpace].nextPN
		token := bytes.Repeat([]byte{7}, 700)
		cid := []byte("retrycid")
		raw := retryPacket(t, c, cid, token)
		bad := bytes.Clone(raw)
		bad[len(bad)-1] ^= 1
		c.HandleDatagram(bad, now)
		if c.retryUsed {
			t.Fatal("invalid tag accepted")
		}
		if err := c.HandleDatagram(raw, now); err != nil {
			t.Fatal(err)
		}
		if !c.retryUsed || c.recovery.BytesInFlight != 0 || c.spaces[initialSpace].nextPN != next {
			t.Fatal("Retry did not reset recovery or preserved wrong PN")
		}
		c.HandleDatagram(retryPacket(t, c, []byte("othercid"), []byte{9}), now)
		if !bytes.Equal(c.peerCID, cid) {
			t.Fatal("second Retry accepted")
		}
		keys, _, _ := packet.InitialKeys(v, cid)
		received := make([]byte, len(original))
		total := 0
		for _, b := range drain(t, c, now) {
			h, err := wire.ParseHeader(b, len(c.config.LocalCID))
			if err != nil {
				t.Fatal(err)
			}
			if len(b) != 1200 || !bytes.Equal(h.Token, token) || !bytes.Equal(h.SCID, c.config.LocalCID) || !bytes.Equal(h.DCID, cid) {
				t.Fatal("Retry Initial fields")
			}
			_, plain, pn, err := keys.Open(b, h.PNOffset, next)
			if err != nil || pn < next {
				t.Fatal("keys/PN", pn, err)
			}
			f, _, err := wire.ConsumeFrame(plain)
			if err != nil {
				t.Fatal(err)
			}
			crypto := f.(wire.CryptoFrame)
			copy(received[int(crypto.Offset):], crypto.Data)
			total += len(crypto.Data)
		}
		if total != len(original) || !bytes.Equal(received, original) {
			t.Fatal("ClientHello changed")
		}
	}
}

func TestRetryAfterInitialIgnored(t *testing.T) {
	c, s := testPair(t, wire.Version1)
	now := time.Unix(200, 0)
	for _, b := range drain(t, c, now) {
		if err := s.HandleDatagram(b, now); err != nil {
			t.Fatal(err)
		}
	}
	for _, b := range drain(t, s, now) {
		if err := c.HandleDatagram(b, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.HandleDatagram(retryPacket(t, c, []byte("retrycid"), []byte{1}), now); err != nil || c.retryUsed {
		t.Fatal("late Retry accepted", err)
	}
}

func TestServerHandshakeAfterRetry(t *testing.T) { testServerHandshakeAfterRetry(t, false, false) }

func TestCompatibleServerHandshakeAfterRetry(t *testing.T) {
	for _, impaired := range []bool{false, true} {
		t.Run(fmt.Sprintf("impaired=%t", impaired), func(t *testing.T) { testServerHandshakeAfterRetry(t, true, impaired) })
	}
}

func testServerHandshakeAfterRetry(t *testing.T, compatible, impaired bool) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, cid := range [][]byte{[]byte("retrycid"), {}} {
			selected := v
			var client, unused *Conn
			if compatible {
				selected = wire.Version1
				if selected == v {
					selected = wire.Version2
				}
				client, unused = compatiblePair(t, v, selected)
			} else {
				client, unused = testPair(t, v)
			}
			unused.Close()
			serverTLS := unused.config.TLS
			// Keep the trusted certificate from the original pair, rather than creating
			// an unrelated identity for the replacement server.
			now := time.Unix(100, 0)
			drain(t, client, now)
			codec, _ := token.NewRetryTokens([32]byte{9}, nil, time.Minute)
			address := netip.MustParseAddrPort("192.0.2.1:1234")
			opaque, err := codec.Issue(address, v, client.config.OriginalDCID, cid, now)
			if err != nil {
				t.Fatal(err)
			}
			retry, err := packet.BuildRetry(v, client.config.OriginalDCID, client.config.LocalCID, cid, opaque)
			if err != nil {
				t.Fatal(err)
			}
			if compatible {
				wrong, err := packet.BuildRetry(selected, client.config.OriginalDCID, client.config.LocalCID, cid, opaque)
				if err != nil {
					t.Fatal(err)
				}
				if err := client.HandleDatagram(wrong, now); err != nil {
					t.Fatal(err)
				}
				if client.retryUsed || client.config.Version != v {
					t.Fatal("wrong-version Retry accepted")
				}
			}
			if err := client.HandleDatagram(retry, now); err != nil {
				t.Fatal(err)
			}
			postRetry := drain(t, client, now)
			header, err := wire.ParseHeader(postRetry[0], len(cid))
			if err != nil {
				t.Fatal(err)
			}
			if header.Version != v {
				t.Fatal("post-Retry Initial switched before server selection")
			}
			if compatible {
				if _, err := codec.Validate(header.Token, address, selected, header.DCID, now); err == nil {
					t.Fatal("Retry token accepted across versions")
				}
			}
			original, err := codec.Validate(header.Token, address, v, header.DCID, now)
			if err != nil {
				t.Fatal(err)
			}
			cfg := Config{Server: true, Version: v, TLS: serverTLS, LocalCID: []byte("server00"), PeerCID: client.config.LocalCID, OriginalDCID: original, RetrySourceCID: cid, Parameters: wire.DefaultTransportParameters()}
			if compatible {
				cfg.CompatibleVersions = []wire.Version{selected, v}
			}
			server, err := NewConn(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			if err := server.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if server.budget.Validated() {
				t.Fatal("routing context implicitly lifted amplification limit")
			}
			for _, b := range postRetry {
				if err := server.HandleDatagram(b, now); err != nil {
					t.Fatal(err)
				}
			}
			drivePair(t, client, server, impaired, impaired)
			if client.config.Version != selected || server.config.Version != selected {
				t.Fatal("wrong negotiated version after Retry")
			}
			if !client.confirmed || !server.confirmed {
				t.Fatal("Retry handshake incomplete")
			}
			if !bytes.Equal(client.peerParams.OriginalDestinationConnectionID, client.config.OriginalDCID) || client.peerParams.RetrySourceConnectionID == nil || !bytes.Equal(client.peerParams.RetrySourceConnectionID, cid) {
				t.Fatal("Retry CID binding")
			}
		}
	}
}
