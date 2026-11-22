package quic

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/transport"
	"github.com/getlantern/photon/internal/wire"
)

func TestHandshakeStatePublication(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		ct, st := dialTLS(t)
		ct.ServerName = "127.0.0.1"
		cfg := transport.Config{Version: v, TLS: ct, LocalCID: []byte("client00"), PeerCID: []byte("original"), OriginalDCID: []byte("original"), Parameters: wire.DefaultTransportParameters()}
		client, err := transport.NewConn(cfg)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Server, cfg.TLS, cfg.LocalCID, cfg.PeerCID = true, st, []byte("server00"), []byte("client00")
		server, err := transport.NewConn(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range []*transport.Conn{client, server} {
			t.Cleanup(func() { c.Close() })
			if err := c.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		c := &Conn{raw: client}
		if err := c.observeHandshake(); err != nil {
			t.Fatal(err)
		}
		if c.ConnectionState().TLS.HandshakeComplete {
			t.Fatal("initial state complete")
		}
		select {
		case <-c.HandshakeComplete():
			t.Fatal("premature completion")
		default:
		}
		stop := make(chan struct{})
		var readers sync.WaitGroup
		var stopOnce sync.Once
		stopReaders := func() { stopOnce.Do(func() { close(stop) }); readers.Wait() }
		defer stopReaders()
		for i := 0; i < 4; i++ {
			readers.Add(1)
			go func() {
				defer readers.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					state := c.ConnectionState()
					if state.Version != v || state.Used0RTT {
						t.Error("inconsistent state snapshot")
						return
					}
					select {
					case <-c.HandshakeComplete():
						if !c.ConnectionState().TLS.HandshakeComplete {
							t.Error("completion preceded state publication")
						}
						return
					default:
					}
				}
			}()
		}
		now := time.Unix(100, 0)
		for step := 0; step < 100 && !(client.HandshakeConfirmed() && server.HandshakeConfirmed()); step++ {
			for _, pair := range [][2]*transport.Conn{{client, server}, {server, client}} {
				for {
					b, err := pair[0].NextDatagram(now)
					if err != nil {
						t.Fatal(err)
					}
					if b == nil {
						break
					}
					if err := pair[1].HandleDatagram(b, now); err != nil {
						t.Fatal(err)
					}
				}
			}
			now = now.Add(10 * time.Millisecond)
			if err := client.HandleTimeout(now); err != nil {
				t.Fatal(err)
			}
			if err := server.HandleTimeout(now); err != nil {
				t.Fatal(err)
			}
		}
		stopReaders()
		select {
		case <-c.HandshakeComplete():
		default:
			t.Fatal("completion missing")
		}
		state := c.ConnectionState()
		if !state.TLS.HandshakeComplete || state.Version != v {
			t.Fatal("final state", state.Version)
		}
		client.Close()
		if !c.ConnectionState().TLS.HandshakeComplete {
			t.Fatal("snapshot lost after shutdown")
		}
	}
}
