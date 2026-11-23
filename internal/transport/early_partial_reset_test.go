package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// Configure actual asymmetric reception and negotiate it through TLS tickets.
func earlyPartialResetPair(t *testing.T, v wire.Version, cc, sc *tls.Config, support, accept bool) (*Conn, *Conn) {
	t.Helper()
	p := wire.DefaultTransportParameters()
	p.InitialMaxData = 8192
	p.InitialMaxStreamDataBidiRemote = 4096
	p.InitialMaxStreamDataUni = 4096
	p.InitialMaxStreamsBidi, p.InitialMaxStreamsUni = 2, 2
	cfg := Config{Version: v, TLS: cc, LocalCID: []byte("client00"), PeerCID: []byte("original"), OriginalDCID: []byte("original"), Parameters: p, enableEarlyData: true}
	c, err := NewConn(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	cfg.Server, cfg.TLS, cfg.LocalCID, cfg.PeerCID, cfg.enableEarlyData = true, sc, []byte("server00"), []byte("client00"), accept
	cfg.Parameters.ResetStreamAt = support
	s, err := NewConn(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	for _, conn := range []*Conn{c, s} {
		if err := conn.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	return c, s
}

func TestEngineEarlyPartialReset(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, uni := range []bool{false, true} {
			for _, mode := range []string{"accept", "reorder", "loss", "reject", "withdrawn", "unsupported", "reduce"} {
				t.Run(fmt.Sprintf("%x/uni=%v/%s", v, uni, mode), func(t *testing.T) {
					cc, sc := testTLS(t)
					cc.ClientSessionCache = tls.NewLRUClientSessionCache(4)
					c, s := earlyPartialResetPair(t, v, cc, sc, mode != "unsupported", true)
					drivePair(t, c, s, false, false)
					c, s = earlyPartialResetPair(t, v, cc, sc, mode != "withdrawn" && mode != "unsupported", mode != "reject")
					if !c.EarlyReady() || c.complete {
						t.Fatal("not sending early")
					}
					id, err := c.OpenStream(uni)
					if err != nil {
						t.Fatal(err)
					}
					data := bytes.Repeat([]byte("0123456789abcdef"), 256)
					for offset := 0; offset < len(data); {
						n, err := c.WriteStream(id, data[offset:], false)
						if err != nil || n == 0 {
							t.Fatal("write", n, err)
						}
						offset += n
					}
					before := c.queuedBytes
					err = c.CancelWriteAt(id, 7, 3000)
					if mode == "unsupported" {
						if !errors.Is(err, ErrUnsupportedFrame) || c.queuedBytes != before || c.streams.objects[id].partialReset != nil {
							t.Fatal("unsupported cancellation mutated state", err)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					reliable := 3000
					if mode == "reduce" {
						if err := c.CancelWriteAt(id, 99, 1500); err != nil {
							t.Fatal(err)
						}
						reliable = 1500
					}
					released := false
					var releaseErr, sendErr error
					if err := c.AutoReleaseStreamWithSendError(id, func(r, s error) { released = true; releaseErr = r; sendErr = s }); err != nil {
						t.Fatal(err)
					}
					now := time.Unix(100, 0)
					var initial, early [][]byte
					for _, b := range drain(t, c, now) {
						h, err := wire.ParseHeader(b, 8)
						if err != nil {
							t.Fatal(err)
						}
						switch h.Type {
						case wire.Initial:
							initial = append(initial, b)
						case wire.ZeroRTT:
							early = append(early, b)
						default:
							t.Fatal("unexpected first flight", h.Type)
						}
					}
					if len(early) < 2 {
						t.Fatal("missing early prefix and reset packets")
					}
					deliver := func(packets [][]byte) {
						for _, b := range packets {
							if err := s.HandleDatagram(b, now); err != nil {
								t.Fatal(err)
							}
						}
					}
					if mode == "reorder" {
						deliver(early)
						deliver(initial)
					} else {
						deliver(initial)
						if mode != "loss" {
							deliver(early)
						}
					}
					accepted := mode != "reject" && mode != "withdrawn"
					readPrefix := func() {
						t.Helper()
						got, err := s.AcceptStream(uni)
						if err != nil || got != id {
							t.Fatal("accept", got, err)
						}
						out := make([]byte, len(data))
						n, err := s.ReadStream(id, out)
						if n != reliable || !bytes.Equal(out[:n], data[:reliable]) {
							t.Fatal("prefix", n, err)
						}
						if err == nil {
							_, err = s.ReadStream(id, out)
						}
						var reset *StreamError
						if !errors.As(err, &reset) || reset.ErrorCode != 7 || !reset.Remote {
							t.Fatal("reset identity", err)
						}
					}
					if accepted && mode != "loss" {
						if s.complete {
							t.Fatal("delivery happened after handshake")
						}
						readPrefix()
						// Replaying protected early packets must not create another stream.
						deliver(early)
					}
					if !accepted {
						// Also discard a trimmed item still queued when rejection
						// arrives, alongside the already-transmitted generation.
						extra, err := c.OpenStream(uni)
						if err != nil {
							t.Fatal(err)
						}
						if _, err := c.WriteStream(extra, []byte("queued partial reset"), false); err != nil {
							t.Fatal(err)
						}
						if err := c.CancelWriteAt(extra, 8, 6); err != nil {
							t.Fatal(err)
						}
						for _, b := range drain(t, s, now) {
							if err := c.HandleDatagram(b, now); err != nil {
								t.Fatal(err)
							}
						}

					}
					drivePair(t, c, s, false, false)
					if accepted {
						if !c.tls.EarlyDataAccepted() || c.earlyRejected {
							t.Fatal("unexpected rejection")
						}
						if mode == "loss" {
							readPrefix()
						}
						if uni {
							var reset *StreamError
							if !released || releaseErr != nil || !errors.As(sendErr, &reset) || reset.ErrorCode != 7 {
								t.Fatal("sender release", released, releaseErr, sendErr)
							}
						} else if !c.streams.objects[id].send.Complete() {
							t.Fatal("sender not acknowledged")
						}
					} else {
						if !released || !errors.Is(releaseErr, ErrEarlyDataRejected) || !c.EarlyDataRejected() {
							t.Fatal("rejection lifecycle", released, releaseErr)
						}
						if len(c.streams.objects) != 0 || len(s.streams.objects) != 0 || c.streams.send.Used() != 0 {
							t.Fatal("rejected generation survived")
						}
						fresh, err := c.OpenStream(uni)
						if err != nil || fresh != id {
							t.Fatal("fresh ID", fresh, err)
						}
						if _, err := c.WriteStream(fresh, []byte("fresh"), true); err != nil {
							t.Fatal(err)
						}
						drivePair(t, c, s, false, false)
						// Replayed rejected ciphertext cannot reset the new stream
						// with the same numeric ID.
						deliver(early)
						got, err := s.AcceptStream(uni)
						if err != nil || got != fresh {
							t.Fatal("fresh accept", got, err)
						}
						out := make([]byte, 16)
						n, err := s.ReadStream(got, out)
						if string(out[:n]) != "fresh" {
							t.Fatal("old reset polluted fresh stream", n, err)
						}
					}
				})
			}
		}
	}
}
