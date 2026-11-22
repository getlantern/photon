package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func TestEngineEarlyDatagrams(t *testing.T) {
	for _, compatible := range []bool{false, true} {
		for _, v := range []wire.Version{wire.Version1, wire.Version2} {
			for _, mode := range []string{"accept", "loss", "reject", "reduced", "larger", "packet-boundary", "unsupported"} {
				t.Run(fmt.Sprintf("%x/%s/compatible=%v", v, mode, compatible), func(t *testing.T) {
					cc, sc := testTLS(t)
					cc.ClientSessionCache = tls.NewLRUClientSessionCache(4)
					firstLimit := uint64(100)
					if mode == "packet-boundary" {
						firstLimit = 65535
					}
					if mode == "unsupported" {
						firstLimit = 0
					}
					round := 0
					pair := func(limit uint64, accept bool) (*Conn, *Conn) {
						p := wire.DefaultTransportParameters()
						p.MaxDatagramFrameSize = limit
						cfg := Config{Version: v, TLS: cc, Parameters: p, LocalCID: bytes.Repeat([]byte{1}, 20), PeerCID: bytes.Repeat([]byte{3}, 20), OriginalDCID: bytes.Repeat([]byte{3}, 20), enableEarlyData: true}
						if compatible && round > 0 {
							selected := wire.Version1
							if selected == v {
								selected = wire.Version2
							}
							cfg.CompatibleVersions = []wire.Version{selected, v}
							cfg.Parameters.VersionInformation = &wire.VersionInformation{Chosen: v, Available: []wire.Version{v, selected}}
						}
						round++
						c, err := NewConn(cfg)
						if err != nil {
							t.Fatal(err)
						}
						cfg.Server, cfg.TLS, cfg.LocalCID, cfg.PeerCID, cfg.enableEarlyData = true, sc, bytes.Repeat([]byte{2}, 20), bytes.Repeat([]byte{1}, 20), accept
						s, err := NewConn(cfg)
						if err != nil {
							t.Fatal(err)
						}
						for _, conn := range []*Conn{c, s} {
							t.Cleanup(func() { conn.Close() })
							if err := conn.Start(context.Background()); err != nil {
								t.Fatal(err)
							}
						}
						return c, s
					}
					c, s := pair(firstLimit, true)
					if err := c.SendDatagram(nil); !errors.Is(err, stream.ErrWouldBlock) {
						t.Fatal("fresh connection sent early datagram", err)
					}
					drivePair(t, c, s, false, false)
					limit := firstLimit
					if mode == "reduced" {
						limit = 50
					}
					if mode == "larger" {
						limit = 200
					}
					c, s = pair(limit, mode != "reject")
					if mode == "unsupported" {
						if err := c.SendDatagram(nil); !errors.Is(err, ErrDatagramUnsupported) {
							t.Fatal(err)
						}
						return
					}
					size := 99
					if mode == "packet-boundary" {
						size = 1130
					}
					want := bytes.Repeat([]byte{7}, size)
					data := bytes.Clone(want)
					if err := c.SendDatagram(data); err != nil {
						t.Fatal(err)
					}
					data[0] = 8
					requireDatagramLimit(t, c.SendDatagram(make([]byte, size+1)), int64(size))
					if err := c.SendDatagram(nil); err != nil {
						t.Fatal(err)
					}
					now := time.Unix(100, 0)
					var early [][]byte
					for _, b := range drain(t, c, now) {
						h, err := wire.ParseHeader(b, 20)
						if err != nil {
							t.Fatal(err)
						}
						if h.Type == wire.ZeroRTT {
							early = append(early, b)
							if len(b) > 1200 {
								t.Fatal("oversized packet")
							}
							continue
						}
						if err := s.HandleDatagram(b, now); err != nil {
							t.Fatal(err)
						}
					}
					if len(early) != 2 || c.pendingDatagrams != 0 || len(c.spaces[applicationSpace].sent) != 0 {
						t.Fatal("unreliable payload retained")
					}
					if mode != "loss" {
						for _, b := range early {
							for i := 0; i < 2; i++ {
								if err := s.HandleDatagram(b, now); err != nil {
									t.Fatal(err)
								}
							}
						}
					}
					accepted := mode != "reject" && mode != "reduced"
					if accepted && mode != "loss" {
						got, err := s.ReceiveDatagram()
						if err != nil || !bytes.Equal(got, want) || s.complete {
							t.Fatal("early payload", len(got), err)
						}
						got, err = s.ReceiveDatagram()
						if err != nil || len(got) != 0 {
							t.Fatal("empty early datagram", got, err)
						}
						if _, err = s.ReceiveDatagram(); !errors.Is(err, stream.ErrWouldBlock) {
							t.Fatal("duplicate delivery", err)
						}
					}
					if !accepted {
						if err := c.SendDatagram([]byte("queued")); err != nil {
							t.Fatal(err)
						}
					}
					for _, b := range drain(t, s, now) {
						if err := c.HandleDatagram(b, now); err != nil {
							t.Fatal(err)
						}
					}
					drivePair(t, c, s, false, false)
					if c.pendingDatagrams != 0 {
						t.Fatal("pending datagram survived handshake")
					}
					if _, err := s.ReceiveDatagram(); !errors.Is(err, stream.ErrWouldBlock) {
						t.Fatal("lost or rejected datagram replayed", err)
					}
					if !accepted && !c.earlyRejected {
						t.Fatal("rejection missing")
					}
					if mode == "larger" {
						if err := c.SendDatagram(make([]byte, 199)); err != nil {
							t.Fatal("new datagram limit unavailable", err)
						}
						drivePair(t, c, s, false, false)
						got, err := s.ReceiveDatagram()
						if err != nil || len(got) != 199 {
							t.Fatal("new limit delivery", len(got), err)
						}
					}
				})
			}
		}
	}
}
