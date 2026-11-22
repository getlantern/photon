package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func TestStreamInitialCreditPromotion(t *testing.T) {
	for _, server := range []bool{false, true} {
		c := &Conn{config: Config{Server: server, Parameters: wire.DefaultTransportParameters()}}
		c.initStreams()
		if _, err := c.getStream(0); !errors.Is(err, stream.ErrWouldBlock) {
			t.Fatal("stream created without credit", err)
		}
		p := wire.DefaultTransportParameters().EarlyLimits()
		p.InitialMaxData = 100
		p.InitialMaxStreamDataBidiLocal = 3
		p.InitialMaxStreamDataBidiRemote = 5
		p.InitialMaxStreamDataUni = 7
		p.InitialMaxStreamsBidi, p.InitialMaxStreamsUni = 1, 1
		if err := c.applyPeerStreamLimits(p); err != nil {
			t.Fatal(err)
		}
		var used uint64
		for id := uint64(0); id < 4; id++ {
			s, err := c.getStream(id)
			if err != nil {
				t.Fatal(err)
			}
			local := (id&1 == 0) != server
			if id&2 != 0 && !local {
				if s.send != nil {
					t.Fatal("send side on remote uni stream")
				}
				continue
			}
			want := 3
			if local {
				want = 5
			}
			if id&2 != 0 {
				want = 7
			}
			_, n, err := s.send.Write(make([]byte, 20), false, 20)
			if err != nil || n != want {
				t.Fatal(server, id, n, want, err)
			}
			used += uint64(n)
		}
		if c.peerParams != nil {
			t.Fatal("remembered credit exposed as live parameters")
		}
		if c.streams.send.Used() != used {
			t.Fatal("connection credit not charged")
		}
		// MAX_STREAM_DATA already received must survive the initial-limit update.
		localBidi := uint64(0)
		if server {
			localBidi = 1
		}
		c.streams.objects[localBidi].send.UpdateLimit(50)
		p.InitialMaxData = 200
		p.InitialMaxStreamDataBidiLocal = 13
		p.InitialMaxStreamDataBidiRemote = 15
		p.InitialMaxStreamDataUni = 17
		p.InitialMaxStreamsBidi, p.InitialMaxStreamsUni = 2, 2
		if err := c.applyPeerStreamLimits(p); err != nil {
			t.Fatal(err)
		}
		if c.streams.send.Used() != used {
			t.Fatal("promotion returned used credit")
		}
		for id, s := range c.streams.objects {
			if s.send == nil {
				continue
			}
			want := 10
			if id == localBidi {
				want = 45
			}
			_, n, err := s.send.Write(make([]byte, 60), false, 60)
			if err != nil || n != want {
				t.Fatal("promoted stream", server, id, n, want, err)
			}
		}
		for _, uni := range []bool{false, true} {
			for i := 0; i < 2; i++ {
				if _, err := c.streams.ids.Open(uni); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := c.streams.ids.Open(uni); !errors.Is(err, stream.ErrWouldBlock) {
				t.Fatal("stream count limit", err)
			}
		}
		s, err := c.getStream(localBidi + 8)
		if err != nil {
			t.Fatal(err)
		}
		_, n, err := s.send.Write(make([]byte, 30), false, 30)
		if err != nil || n != 15 {
			t.Fatal("new stream initial credit", n, err)
		}
		before := c.streams.peerLimits
		p.InitialMaxStreamsBidi = 1<<60 + 1
		if err := c.applyPeerStreamLimits(p); !errors.Is(err, wire.ErrEarlyLimits) {
			t.Fatal("invalid limits", err)
		}
		if c.streams.peerLimits != before {
			t.Fatal("invalid limit partially installed")
		}
	}
}

// Reserve bytes against an actual resumed ticket, then promote that same
// stream through a real handshake advertising larger limits. The early send
// path must use only remembered credit until application keys become available.
func TestResumedStreamCreditHandshakePromotion(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		cc, sc := testTLS(t)
		cc.ClientSessionCache = tls.NewLRUClientSessionCache(4)
		firstClient, firstServer := earlyPair(t, v, cc, sc, true)
		drivePair(t, firstClient, firstServer, false, false)
		client, err := NewConn(firstClient.config)
		if err != nil {
			t.Fatal(err)
		}
		serverConfig := firstServer.config
		serverConfig.Parameters.InitialMaxData = 16384
		serverConfig.Parameters.InitialMaxStreamDataBidiRemote = 8192
		server, err := NewConn(serverConfig)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range []*Conn{client, server} {
			t.Cleanup(func() { c.Close() })
			if err := c.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		remembered, ok := client.tls.ResumedTransport()
		if !ok {
			t.Fatal("missing remembered credit")
		}
		if err := client.applyPeerStreamLimits(remembered.EarlyLimits); err != nil {
			t.Fatal(err)
		}
		id, err := client.streams.ids.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.getStream(id); err != nil {
			t.Fatal(err)
		}
		data := make([]byte, 8192)
		var sent int
		for sent < 4096 {
			n, err := client.WriteStream(id, data[sent:4096], false)
			if err != nil {
				t.Fatal(err)
			}
			sent += n
		}
		if _, err := client.WriteStream(id, data[:1], false); !errors.Is(err, stream.ErrWouldBlock) {
			t.Fatal("remembered stream credit exceeded", err)
		}
		if client.peerParams != nil {
			t.Fatal("remembered values became live parameters")
		}
		drivePair(t, client, server, false, false)
		if !client.tls.EarlyDataAccepted() {
			t.Fatal("early offer was not accepted")
		}
		// No server application reads or MAX_STREAM_DATA grant can mask the
		// transition: only the larger current transport parameter permits this.
		for sent < len(data) {
			n, err := client.WriteStream(id, data[sent:], true)
			if err != nil {
				t.Fatal("current stream credit unavailable", sent, err)
			}
			sent += n
		}
		if client.streams.send.Used() != 8192 {
			t.Fatal("credit was reset during promotion")
		}
	}
}
