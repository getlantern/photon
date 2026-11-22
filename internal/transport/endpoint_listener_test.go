package transport

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func TestEndpointCloseListener(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, stage := range []string{"handshaking", "queued", "accepted"} {
			t.Run(fmt.Sprintf("%v/%s", v, stage), func(t *testing.T) {
				client, incoming := testPair(t, v)
				if stage != "handshaking" {
					drivePair(t, client, incoming, false, false)
				} else {
					for _, packet := range drain(t, client, time.Unix(100, 0)) {
						if err := incoming.HandleDatagram(packet, time.Unix(100, 0)); err != nil {
							t.Fatal(err)
						}
					}
				}
				tokens, err := token.NewRetryTokens([32]byte{17}, nil, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				e, err := NewServerEndpoint(Config{Version: v, TLS: incoming.config.TLS}, tokens, 4)
				if err != nil {
					t.Fatal(err)
				}
				defer e.Close()
				peer := netip.MustParseAddrPort("192.0.2.1:443")
				if err := e.router.Add(incoming, peer); err != nil {
					t.Fatal(err)
				}
				e.conns = append(e.conns, incoming)
				if err := e.sync(incoming); err != nil {
					t.Fatal(err)
				}
				if stage == "accepted" {
					if c, _, err := e.Accept(); err != nil || c != incoming {
						t.Fatal("accept", err)
					}
				}
				outgoing, _ := testPair(t, v)
				if err := e.AddClient(outgoing, netip.MustParseAddrPort("192.0.2.2:443")); err != nil {
					t.Fatal(err)
				}
				now := time.Unix(100, int64(time.Millisecond))
				e.CloseListener(now)
				e.CloseListener(now)
				if e.closed || outgoing.Err() != nil || outgoing.closed {
					t.Fatal("listener closure destroyed endpoint or outgoing connection")
				}
				if _, _, err := e.Accept(); !errors.Is(err, ErrListenerClosed) {
					t.Fatal("accept after close", err)
				}
				if e.ready.Len() != 0 || len(e.readyNodes) != 0 {
					t.Fatal("accept queue retained")
				}
				if stage == "accepted" {
					if incoming.Err() != nil || incoming.closed {
						t.Fatal("accepted connection closed")
					}
					if err := incoming.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
						t.Fatal(err)
					}
				} else {
					var closeErr *ConnectionTransportError
					if !errors.As(incoming.Err(), &closeErr) || closeErr.ErrorCode != 2 || !incoming.Closing() {
						t.Fatal("unaccepted connection not refused", incoming.Err())
					}
				}
				delivered := false
				for i := 0; i < 100; i++ {
					out, err := e.NextDatagram(now)
					if err != nil {
						t.Fatal(err)
					}
					if out == nil {
						break
					}
					if out.Conn != incoming {
						continue
					}
					err = client.HandleDatagram(out.Data, now)
					if stage == "accepted" {
						if err != nil {
							t.Fatal("accepted connection traffic", err)
						}
					} else {
						var closeErr *ConnectionTransportError
						if !errors.As(err, &closeErr) || closeErr.ErrorCode != 2 || !closeErr.Remote {
							t.Fatal("peer refusal", err)
						}
					}
					delivered = true
					break
				}
				if !delivered {
					t.Fatal("no protected traffic after listener close")
				}
				// Unknown Initials must neither get Retry responses nor allocate state.
				config := outgoing.config
				config.LocalCID = []byte("freshcid")
				config.PeerCID = []byte("freshdst")
				config.OriginalDCID = config.PeerCID
				fresh, err := NewConn(config)
				if err != nil {
					t.Fatal(err)
				}
				defer fresh.Close()
				if err := fresh.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				packets := drain(t, fresh, now)
				count := len(e.conns)
				for _, packet := range packets {
					c, out, err := e.HandleDatagram(context.Background(), packet, netip.MustParseAddrPort("192.0.2.3:443"), now)
					if c != nil || out != nil || err != nil || len(e.conns) != count {
						t.Fatal("admitted after listener close", err)
					}
				}
			})
		}
	}
}

func TestCloseListenerWakesAccept(t *testing.T) {
	s := testUDPServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	waiting, err := s.invoke(ctx, func(e *ServerEndpoint) error { _, _, err := e.Accept(); return err }, false)
	if !errors.Is(err, ErrAcceptWouldBlock) {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, _, err := s.Accept(ctx); result <- err }()
	if err := s.Do(ctx, func(e *ServerEndpoint) error { e.CloseListener(time.Now()); return nil }); err != nil {
		t.Fatal(err)
	}
	select {
	case <-waiting.changed:
	case <-ctx.Done():
		t.Fatal("listener close did not notify waiting accept")
	}
	if err := <-result; !errors.Is(err, ErrListenerClosed) {
		t.Fatal(err)
	}
	if err := s.Do(ctx, func(e *ServerEndpoint) error {
		if e.closed {
			return ErrEndpointClosed
		}
		return nil
	}); err != nil {
		t.Fatal("socket owner stopped", err)
	}
}

func TestCloseListenerRetainsDrainingRoute(t *testing.T) {
	_, c := testPair(t, wire.Version1)
	tokens, err := token.NewRetryTokens([32]byte{18}, nil, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	e, err := NewServerEndpoint(Config{Version: wire.Version1, TLS: c.config.TLS}, tokens, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := e.router.Add(c, netip.MustParseAddrPort("192.0.2.1:443")); err != nil {
		t.Fatal(err)
	}
	e.conns = append(e.conns, c)
	now := time.Unix(100, 0)
	c.beginDraining(ErrPeerClosed, now)
	e.CloseListener(now)
	if c.closed || !c.Draining() || len(e.router.entries) != 1 {
		t.Fatal("listener close retired draining route early")
	}
	e.HandleTimeout(now.Add(time.Minute))
	if !c.closed || len(e.router.entries) != 0 {
		t.Fatal("draining route not retired at deadline")
	}
}
