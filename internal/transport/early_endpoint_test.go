package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func TestEndpointEarlyRouting(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, acceptEarly := range []bool{false, true} {
			cc, sc := testTLS(t)
			cc.ClientSessionCache = tls.NewLRUClientSessionCache(4)
			c, first := earlyPair(t, v, cc, sc, true)
			// Obtain a datagram-capable ticket from the same TLS configuration.
			cfg := first.config
			cfg.Parameters.MaxDatagramFrameSize = 100
			first.Close()
			var err error
			first, err = NewConn(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { first.Close() })
			if err := first.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			drivePair(t, c, first, false, false)
			now := time.Unix(100, 0)
			peer := netip.MustParseAddrPort("127.0.0.1:24680")
			addresses, _ := token.NewAddressTokens([32]byte{9}, nil, time.Hour)
			retries, _ := token.NewRetryTokens([32]byte{8}, nil, time.Minute)
			cfg.AcceptEarly = acceptEarly
			cfg.AddressTokens = addresses
			endpoint, err := NewServerEndpoint(cfg, retries, 4)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { endpoint.Close() })
			clientConfig := c.config
			clientConfig.Parameters.InitialMaxStreamsUni = 2
			clientConfig.Parameters.InitialMaxStreamDataUni = 4096
			clientConfig.Parameters.MaxDatagramFrameSize = 100
			clientConfig.InitialToken, err = addresses.Issue(peer, v, now)
			if err != nil {
				t.Fatal(err)
			}
			c, err = NewConn(clientConfig)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { c.Close() })
			if err := c.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			id, err := c.OpenStream(false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.WriteStream(id, []byte("early routed stream"), true); err != nil {
				t.Fatal(err)
			}
			if err := c.SendDatagram([]byte("delayed datagram")); err != nil {
				t.Fatal(err)
			}
			var initials, early [][]byte
			for _, b := range drain(t, c, now) {
				h, err := wire.ParseHeader(b, 8)
				if err != nil {
					t.Fatal(err)
				}
				if h.Type == wire.ZeroRTT {
					early = append(early, b)
				} else {
					initials = append(initials, b)
				}
			}
			if len(early) != 2 {
				t.Fatal("missing early flight")
			}
			if admitted, reply, err := endpoint.HandleDatagram(context.Background(), early[0], peer, now); err != nil || admitted != nil || reply != nil || len(endpoint.conns) != 0 {
				t.Fatal("unknown early packet allocated connection", err)
			}
			var s *Conn
			for _, b := range initials {
				conn, reply, err := endpoint.HandleDatagram(context.Background(), b, peer, now)
				if err != nil || reply != nil {
					t.Fatal("token admission", err)
				}
				if conn != nil {
					s = conn
				}
			}
			if s == nil {
				t.Fatal("no admitted connection")
			}
			conn, reply, err := endpoint.HandleDatagram(context.Background(), early[0], peer, now)
			if err != nil || reply != nil || conn != s || s.complete {
				t.Fatal("early route", err)
			}
			if accepted, address, err := endpoint.Accept(); acceptEarly {
				if err != nil || accepted != s || address != peer {
					t.Fatal("early accept", err)
				}
				if _, err := s.OpenStream(true); err != nil {
					t.Fatal("early server stream", err)
				}
				if err := s.SendDatagram([]byte("early response")); err != nil {
					t.Fatal("early server datagram", err)
				}
			} else if !errors.Is(err, ErrAcceptWouldBlock) {
				t.Fatal("endpoint exposed unconfirmed connection", err)
			}
			got, err := s.AcceptStream(false)
			if err != nil || got != id {
				t.Fatal(got, err)
			}
			data := make([]byte, 32)
			n, err := s.ReadStream(id, data)
			if string(data[:n]) != "early routed stream" {
				t.Fatal(string(data[:n]), err)
			}
			for step := 0; step < 100 && !(c.confirmed && s.confirmed); step++ {
				for {
					out, err := endpoint.NextDatagram(now)
					if err != nil {
						t.Fatal(err)
					}
					if out == nil {
						break
					}
					if err := c.HandleDatagram(out.Data, now); err != nil {
						t.Fatal(err)
					}
				}
				for _, b := range drain(t, c, now) {
					if _, reply, err := endpoint.HandleDatagram(context.Background(), b, peer, now); err != nil || reply != nil {
						t.Fatal(err)
					}
				}
				now = now.Add(time.Millisecond)
				if err := endpoint.HandleTimeout(now); err != nil {
					t.Fatal(err)
				}
				if err := c.HandleTimeout(now); err != nil {
					t.Fatal(err)
				}
			}
			if !c.confirmed || !s.confirmed || !s.spaces[initialSpace].discarded {
				t.Fatal("handshake did not finish")
			}
			accepted, address, err := endpoint.Accept()
			if acceptEarly {
				if !errors.Is(err, ErrAcceptWouldBlock) {
					t.Fatal("connection accepted twice", err)
				}
			} else if err != nil || accepted != s || address != peer {
				t.Fatal("confirmed accept", err)
			}
			// This original-DCID alias must still route early data after Initial keys go.
			conn, reply, err = endpoint.HandleDatagram(context.Background(), early[1], peer, now)
			if err != nil || reply != nil || conn != s || len(endpoint.conns) != 1 {
				t.Fatal("delayed alias route", err)
			}
			datagram, err := s.ReceiveDatagram()
			if err != nil || string(datagram) != "delayed datagram" {
				t.Fatal(string(datagram), err)
			}
			other := netip.MustParseAddrPort("127.0.0.1:24681")
			conn, reply, err = endpoint.HandleDatagram(context.Background(), early[1], other, now)
			if err != nil || conn != nil || reply != nil || len(endpoint.conns) != 1 {
				t.Fatal("wrong address early route", err)
			}
		}
	}
}
