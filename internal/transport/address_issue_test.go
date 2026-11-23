package transport

import (
	"bytes"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func TestAddressTokenReliableIssuance(t *testing.T) { testAddressTokenReliableIssuance(t, false) }
func TestCompatibleAddressTokenReliableIssuance(t *testing.T) {
	testAddressTokenReliableIssuance(t, true)
}

func testAddressTokenReliableIssuance(t *testing.T, compatible bool) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		selected := v
		var client, server *Conn
		if compatible {
			selected = wire.Version1
			if selected == v {
				selected = wire.Version2
			}
			client, server = compatiblePair(t, v, selected)
		} else {
			client, server = testPair(t, v)
		}
		codec, _ := token.NewAddressTokens([32]byte{4}, nil, time.Hour)
		server.config.AddressTokens = codec
		address := netip.MustParseAddrPort("192.0.2.1:1234")
		now := time.Unix(100, 0)
		if err := server.IssueAddressToken(address, now); !errors.Is(err, ErrHandshakeUnconfirmed) {
			t.Fatal("premature issuance", err)
		}
		drivePair(t, client, server, false, false)
		if err := client.IssueAddressToken(address, now); err == nil {
			t.Fatal("client issued token")
		}
		if err := server.IssueAddressToken(address, now); err != nil {
			t.Fatal(err)
		}
		space := &server.spaces[applicationSpace]
		item := space.queue[len(space.queue)-1]
		frame, ok := item.frame.(wire.NewTokenFrame)
		if !ok {
			t.Fatal("missing token frame")
		}
		original := bytes.Clone(frame.Token)
		if err := codec.Validate(original, address, selected, now); err != nil {
			t.Fatal(err)
		}
		if compatible {
			if err := codec.Validate(original, address, v, now); err == nil {
				t.Fatal("switched connection issued old-version address token")
			}
		}
		// Discard the original packet, then drive the real recovery timer.
		if len(drain(t, server, now)) == 0 {
			t.Fatal("no token packet")
		}
		at := server.RecoveryDeadline()
		if at.IsZero() {
			t.Fatal("token did not arm recovery")
		}
		if err := server.HandleTimeout(at); err != nil {
			t.Fatal(err)
		}
		retransmitted := drain(t, server, at)
		if len(retransmitted) == 0 {
			t.Fatal("no recovery packet")
		}
		if !bytes.Equal(item.frame.(wire.NewTokenFrame).Token, original) {
			t.Fatal("recovery replaced token")
		}
		for _, packet := range retransmitted {
			if err := client.HandleDatagram(packet, at); err != nil {
				t.Fatal(err)
			}
		}
		// Keep the simulated clock monotonic while processing acknowledgment.
		for step := 0; step < 20 && !item.acked; step++ {
			at = at.Add(10 * time.Millisecond)
			for _, pair := range [][2]*Conn{{client, server}, {server, client}} {
				if err := pair[0].HandleTimeout(at); err != nil {
					t.Fatal(err)
				}
				for _, packet := range drain(t, pair[0], at) {
					if err := pair[1].HandleDatagram(packet, at); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		if !item.acked {
			t.Fatal("token retransmission not acknowledged")
		}
		if err := server.IssueAddressToken(address, now); err != nil {
			t.Fatal(err)
		}
		next := space.queue[len(space.queue)-1].frame.(wire.NewTokenFrame).Token
		if bytes.Equal(next, original) {
			t.Fatal("fresh issuance reused token")
		}
		saved := server.queuedBytes
		server.queuedBytes = maxSendBytes
		if err := server.IssueAddressToken(address, now); !errors.Is(err, ErrSendBuffer) || server.err != nil {
			t.Fatal("optional issuance closed connection", err)
		}
		server.queuedBytes = saved
	}
}

func TestEndpointDefersAddressTokenWhenQueueFull(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	codec, _ := token.NewAddressTokens([32]byte{4}, nil, time.Hour)
	server.config.AddressTokens = codec
	retry, _ := token.NewRetryTokens([32]byte{5}, nil, time.Minute)
	endpoint, err := NewServerEndpoint(server.config, retry, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	address := netip.MustParseAddrPort("192.0.2.1:1234")
	if err := endpoint.router.Add(server, address); err != nil {
		t.Fatal(err)
	}
	endpoint.conns = append(endpoint.conns, server)
	now := time.Unix(100, 0)
	saved := server.queuedBytes
	server.queuedBytes = maxSendBytes
	if err := endpoint.syncAt(server, now); err != nil || server.addressTokenIssued || server.err != nil {
		t.Fatal("full queue issuance", err)
	}
	server.queuedBytes = saved
	if err := endpoint.syncAt(server, now); err != nil || !server.addressTokenIssued {
		t.Fatal("deferred issuance failed", err)
	}
	if !endpoint.announced[server] {
		t.Fatal("optional token blocked admission")
	}
}
