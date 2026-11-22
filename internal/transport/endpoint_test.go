package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func TestServerEndpointMultipleConnections(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		first, unused := testPair(t, v)
		unused.Close()
		codec, _ := token.NewRetryTokens([32]byte{5}, nil, time.Minute)
		addressTokens, _ := token.NewAddressTokens([32]byte{6}, nil, time.Hour)
		issued := make(map[*Conn][]byte)
		e, err := NewServerEndpoint(Config{Version: v, TLS: unused.config.TLS, Parameters: wire.DefaultTransportParameters(), AddressTokens: addressTokens}, codec, 2)
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		if _, _, err := e.Accept(); !errors.Is(err, ErrAcceptWouldBlock) {
			t.Fatal("accepted before handshake", err)
		}
		cfg := first.config
		cfg.LocalCID = []byte("client01")
		cfg.PeerCID = []byte("origin01")
		cfg.OriginalDCID = cfg.PeerCID
		second, err := NewConn(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer second.Close()
		if err := second.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		clients := map[netip.AddrPort]*Conn{netip.MustParseAddrPort("192.0.2.1:1000"): first, netip.MustParseAddrPort("192.0.2.2:1000"): second}
		now := time.Unix(100, 0)
		complete := false
		for step := 0; step < 200; step++ {
			for peer, c := range clients {
				for _, raw := range drain(t, c, now) {
					conn, response, err := e.HandleDatagram(context.Background(), raw, peer, now)
					if err != nil {
						t.Fatal(err)
					}
					if response != nil {
						if response.Peer != peer || response.Conn != nil {
							t.Fatal("bad Retry route")
						}
						if err := c.HandleDatagram(response.Data, now); err != nil {
							t.Fatal(err)
						}
					}
					if conn != nil {
						for _, item := range conn.spaces[applicationSpace].queue {
							if frame, ok := item.frame.(wire.NewTokenFrame); ok {
								if err := addressTokens.Validate(frame.Token, peer, v, now); err != nil {
									t.Fatal("issued token address/clock", err)
								}
								issued[conn] = append([]byte(nil), frame.Token...)
							}
						}
						count := len(e.conns)
						duplicate, reply, err := e.HandleDatagram(context.Background(), raw, peer, now)
						if err != nil || duplicate != conn || reply != nil || len(e.conns) != count {
							t.Fatal("duplicate readmitted", err)
						}
					}
				}
			}
			for sent := 0; sent < 100; sent++ {
				out, err := e.NextDatagram(now)
				if err != nil {
					t.Fatal(err)
				}
				if out == nil {
					break
				}
				c := clients[out.Peer]
				if c == nil || out.Conn == nil {
					t.Fatal("wrong output association")
				}
				if err := c.HandleDatagram(out.Data, now); err != nil {
					t.Fatal(err)
				}
			}
			complete = len(e.conns) == 2
			for _, c := range clients {
				complete = complete && c.confirmed && !c.HasOutstanding()
			}
			for _, c := range e.conns {
				complete = complete && c.confirmed && !c.HasOutstanding()
			}
			if complete {
				break
			}
			now = now.Add(10 * time.Millisecond)
			if err := e.HandleTimeout(now); err != nil {
				t.Fatal(err)
			}
			for _, c := range clients {
				if err := c.HandleTimeout(now); err != nil {
					t.Fatal(err)
				}
			}
		}
		if !complete {
			t.Fatal("shared endpoint did not converge")
		}
		if len(issued) != 2 {
			t.Fatal("missing automatic tokens", len(issued))
		}
		var previous []byte
		for conn, opaque := range issued {
			if bytes.Equal(previous, opaque) {
				t.Fatal("token reused across clients")
			}
			previous = opaque
			if !conn.addressTokenIssued {
				t.Fatal("issuance flag absent")
			}
			before := conn.queuedBytes
			if err := e.syncAt(conn, now); err != nil {
				t.Fatal(err)
			}
			if conn.queuedBytes != before {
				t.Fatal("duplicate automatic issuance")
			}
		}

		accepted := map[*Conn]bool{}
		for i := 0; i < 2; i++ {
			c, peer, err := e.Accept()
			if err != nil || c == nil || !c.confirmed || clients[peer] == nil || accepted[c] {
				t.Fatal("accept result", err)
			}
			accepted[c] = true
			if err := e.sync(c); err != nil {
				t.Fatal(err)
			}
		}
		if _, _, err := e.Accept(); !errors.Is(err, ErrAcceptWouldBlock) {
			t.Fatal("duplicate acceptance", err)
		}
		if _, _, err := e.HandleDatagram(context.Background(), []byte("unknown"), netip.MustParseAddrPort("192.0.2.3:1000"), now); !errors.Is(err, ErrRouterCapacity) {
			t.Fatal("capacity not enforced", err)
		}
		// Queued packets from both connections must get an output opportunity.
		for i, c := range e.conns {
			if err := c.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
				t.Fatal(i, err)
			}
		}
		seen := map[*Conn]bool{}
		for i := 0; i < 2; i++ {
			out, err := e.NextDatagram(now.Add(time.Second))
			if err != nil || out == nil {
				t.Fatal("scheduled output", err)
			}
			seen[out.Conn] = true
		}
		if len(seen) != 2 {
			t.Fatal("round-robin starvation")
		}
		e.Close()
		if len(e.router.entries) != 0 || len(e.conns) != 0 || !e.Deadline().IsZero() {
			t.Fatal("endpoint resource leak")
		}
		if _, err := e.NextDatagram(now); !errors.Is(err, ErrEndpointClosed) {
			t.Fatal(fmt.Sprint(err))
		}
	}
}

func TestEndpointTimeoutDoesNotStrandOtherConnections(t *testing.T) {
	_, first := testPair(t, wire.Version1)
	_, second := testPair(t, wire.Version1)
	codec, _ := token.NewRetryTokens([32]byte{7}, nil, time.Minute)
	e, _ := NewServerEndpoint(Config{Version: wire.Version1, TLS: first.config.TLS}, codec, 2)
	defer e.Close()
	second.config.LocalCID = []byte("server01")
	second.config.OriginalDCID = []byte("origin01")
	second.localIDs = newLocalIDs(second.config.LocalCID)
	now := time.Unix(100, 0)
	for i, c := range []*Conn{first, second} {
		c.config.HandshakeTimeout = time.Duration(i+1) * time.Second
		c.handshakeStarted = now
		if err := e.router.Add(c, netip.MustParseAddrPort(fmt.Sprintf("192.0.2.%d:1000", i+1))); err != nil {
			t.Fatal(err)
		}
		e.conns = append(e.conns, c)
	}
	if e.Deadline() != now.Add(time.Second) {
		t.Fatal("wrong earliest deadline")
	}
	if err := e.HandleTimeout(now.Add(time.Second)); !errors.Is(err, ErrHandshakeTimeout) {
		t.Fatal(err)
	}
	if len(e.conns) != 1 || e.conns[0] != second || second.closed {
		t.Fatal("timeout affected other connection")
	}
	if err := e.HandleTimeout(now.Add(2 * time.Second)); !errors.Is(err, ErrHandshakeTimeout) {
		t.Fatal(err)
	}
	if len(e.conns) != 0 || len(e.router.entries) != 0 {
		t.Fatal("timeout leaked routes")
	}
}

func TestEndpointDropsClosedUnacceptedConnections(t *testing.T) {
	for _, draining := range []bool{false, true} {
		client, c := testPair(t, wire.Version1)
		drivePair(t, client, c, false, false)
		codec, _ := token.NewRetryTokens([32]byte{8}, nil, time.Minute)
		e, _ := NewServerEndpoint(Config{Version: wire.Version1, TLS: c.config.TLS}, codec, 1)
		defer e.Close()
		e.router.Add(c, netip.MustParseAddrPort("192.0.2.1:1234"))
		e.conns = append(e.conns, c)
		if err := e.sync(c); err != nil {
			t.Fatal(err)
		}
		if e.ready.Len() != 1 {
			t.Fatal("confirmed connection not queued")
		}
		if draining {
			c.beginDraining(ErrPeerClosed, time.Unix(200, 0))
			e.sync(c)
		} else {
			c.Close()
		}
		if got, _, err := e.Accept(); got != nil || !errors.Is(err, ErrAcceptWouldBlock) {
			t.Fatal("accepted terminated connection", err)
		}
		if e.ready.Len() != 0 || len(e.readyNodes) != 0 {
			t.Fatal("queue retained dead connection")
		}
		if draining && len(e.router.entries) != 1 {
			t.Fatal("accept released draining route")
		}
		e.Close()
		if len(e.announced) != 0 {
			t.Fatal("announcement history leaked")
		}
		if _, _, err := e.Accept(); !errors.Is(err, ErrEndpointClosed) {
			t.Fatal(err)
		}
	}
}
