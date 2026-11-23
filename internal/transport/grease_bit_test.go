package transport

import (
	"context"
	"github.com/getlantern/photon/internal/token"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestGreasedBitReceivePolicy(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, local := range []bool{false, true} {
			parameters := wire.DefaultTransportParameters()
			parameters.GreaseQUICBit = local
			client, server := testPair(t, version, parameters)
			drivePair(t, client, server, false, false)
			for _, receiver := range []*Conn{client, server} {
				sender := client
				if receiver == client {
					sender = server
				}
				for _, peer := range []bool{false, true} {
					receiver.peerParams.GreaseQUICBit = peer // Must not control receive permission.
					for _, tamper := range []bool{false, true} {
						pn := uint64(1000)
						if peer {
							pn += 10
						}
						if tamper {
							pn++
						}
						header, err := sender.makeHeader(applicationSpace, pn, 17)
						if err != nil {
							t.Fatal(err)
						}
						header[0] |= 0x40
						if !tamper {
							header[0] &= ^byte(0x40)
						}
						data, err := sender.writeKeys(applicationSpace).Seal(header, []byte{1}, pn)
						if err != nil {
							t.Fatal(err)
						}
						if tamper {
							data[0] &= ^byte(0x40)
						}
						before := receiver.authenticatedPackets
						if err := receiver.HandleDatagram(data, time.Unix(200, 0)); err != nil {
							t.Fatal(err)
						}
						accepted := receiver.authenticatedPackets > before
						if accepted != (local && !tamper) {
							t.Fatalf("v=%x local=%v peer=%v tamper=%v accepted=%v", version, local, peer, tamper, accepted)
						}
					}
				}
			}
		}
	}
}

func TestGreasedBitRouterIdentity(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	peer := netip.MustParseAddrPort("192.0.2.1:443")
	router, _ := NewRouter(8, 2)
	if err := router.Add(server, peer); err != nil {
		t.Fatal(err)
	}
	data := makePeerPacket(t, client, initialSpace, 0, wire.PingFrame{})
	data[0] &= ^byte(0x40) // Routing must not authenticate or rewrite this packet.
	if got, known := router.Route(data, peer); got != server || !known {
		t.Fatal("zero-bit CID not routed")
	}
	if got, known := router.Route(data, netip.MustParseAddrPort("192.0.2.2:443")); got != nil || !known {
		t.Fatal("routing relaxed address identity")
	}
	before := server.authenticatedPackets
	if err := server.HandleDatagram(data, time.Unix(100, 0)); err != nil || server.authenticatedPackets != before {
		t.Fatal("strict connection accepted routed packet", err)
	}
}

func TestGreasedBitInitialAdmission(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := testPair(t, version)
		now := time.Unix(100, 0)
		data := drain(t, client, now)[0]
		parsed, err := wire.ParseHeader(data, 8)
		if err != nil {
			t.Fatal(err)
		}
		keys := client.writeKeys(initialSpace)
		header, payload, pn, err := keys.Open(data[:parsed.PacketLen], parsed.PNOffset, 0)
		if err != nil {
			t.Fatal(err)
		}
		header[0] &= ^byte(0x40)
		greased, err := keys.Seal(header, payload, pn)
		if err != nil {
			t.Fatal(err)
		}
		tokens, _ := token.NewRetryTokens([32]byte{9}, nil, time.Minute)
		for _, allow := range []bool{false, true} {
			params := wire.DefaultTransportParameters()
			params.GreaseQUICBit = allow
			template := Config{Version: version, TLS: server.config.TLS, Parameters: params}
			conn, response, err := AdmitInitial(context.Background(), template, tokens, greased, netip.MustParseAddrPort("192.0.2.1:443"), now)
			if conn != nil {
				conn.Close()
				t.Fatal("admitted without token")
			}
			if err != nil || (len(response) > 0) != allow {
				t.Fatal("incorrect Initial admission policy", allow, err)
			}
			if len(response) > 0 && response[0]&0x40 == 0 {
				t.Fatal("Retry greased without peer permission")
			}
		}
	}
}

func TestGreasedBitSendPermission(t *testing.T) {
	c := &Conn{}
	for i := 0; i < 512; i++ {
		if c.greaseQUICBit(0xc3) != 0xc3 {
			t.Fatal("greased before peer parameters")
		}
	}
	c.peerParams = &wire.TransportParameters{}
	if c.greaseQUICBit(0x43) != 0x43 {
		t.Fatal("greased without peer permission")
	}
	c.peerParams.GreaseQUICBit = true
	for i := range c.quicBits {
		c.quicBits[i] = 0xaa
	}
	c.quicBitsLeft = 256
	for i := 0; i < 256; i++ {
		got := c.greaseQUICBit(0xe3)
		want := byte(0xe3)
		if i%2 != 0 {
			want &^= 0x40
		}
		if got != want {
			t.Fatalf("bit %d: %x != %x", i, got, want)
		}
	}
	// Refill the cryptographic cache, then remove permission while bits remain.
	_ = c.greaseQUICBit(0x43)
	if c.quicBitsLeft != 255 {
		t.Fatal("entropy cache not refilled")
	}
	c.peerParams.GreaseQUICBit = false
	if c.greaseQUICBit(0x43) != 0x43 || c.quicBitsLeft != 255 {
		t.Fatal("cached entropy bypassed permission")
	}
}
