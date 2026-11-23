package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func byteBlockedEarlyPair(t *testing.T, v wire.Version, cc, sc *tls.Config, accept, promote bool) (*Conn, *Conn) {
	t.Helper()
	p := streamParameters()
	p.InitialMaxData = 2
	p.InitialMaxStreamDataBidiRemote = 1
	p.InitialMaxStreamDataUni = 1
	cfg := Config{Version: v, TLS: cc, LocalCID: []byte("client00"), PeerCID: []byte("original"), OriginalDCID: []byte("original"), Parameters: p, enableEarlyData: true}
	c, err := NewConn(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Server = true
	cfg.TLS = sc
	cfg.LocalCID = []byte("server00")
	cfg.PeerCID = []byte("client00")
	cfg.enableEarlyData = accept
	if promote {
		cfg.Parameters.InitialMaxData = 4
		cfg.Parameters.InitialMaxStreamDataBidiRemote = 2
		cfg.Parameters.InitialMaxStreamDataUni = 2
	}
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

func TestByteBlockedEarlyGeneration(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, uni := range []bool{false, true} {
			for _, mode := range []string{"accept", "loss", "reject", "promote"} {
				t.Run(fmt.Sprintf("%s/uni=%t/%s", vName(v), uni, mode), func(t *testing.T) {
					cc, sc := testTLS(t)
					cc.ClientSessionCache = tls.NewLRUClientSessionCache(4)
					c, s := byteBlockedEarlyPair(t, v, cc, sc, true, false)
					drivePair(t, c, s, false, false)
					c, s = byteBlockedEarlyPair(t, v, cc, sc, mode != "reject", mode == "promote")
					if !c.EarlyReady() || c.complete {
						t.Fatal("missing early generation")
					}
					var ids [2]uint64
					for i := range ids {
						id, err := c.OpenStream(uni)
						if err != nil {
							t.Fatal(err)
						}
						ids[i] = id
						if n, err := c.WriteStream(id, []byte{byte(i + 1)}, false); n != 1 || err != nil {
							t.Fatal(n, err)
						}
					}
					block := func(id uint64) {
						t.Helper()
						if n, err := c.WriteStream(id, []byte{9}, false); n != 0 || !errors.Is(err, stream.ErrWouldBlock) {
							t.Fatal(n, err)
						}
					}
					for _, id := range ids {
						block(id)
					}
					var notifications []*sendItem
					connectionFrames, streamFrames := 0, 0
					for _, item := range c.spaces[applicationSpace].queue {
						switch f := item.frame.(type) {
						case wire.DataBlockedFrame:
							if f.Limit != 2 {
								t.Fatal(f)
							}
							connectionFrames++
							notifications = append(notifications, item)
						case wire.StreamDataBlockedFrame:
							if f.Limit != 1 {
								t.Fatal(f)
							}
							streamFrames++
							notifications = append(notifications, item)
						}
					}
					if connectionFrames != 1 || streamFrames != 2 || c.streams.dataBlockedCount != 2 {
						t.Fatal("missing shared/stream notifications")
					}
					earlyPackets := 0
					now := time.Unix(100, 0)
					for _, packet := range drain(t, c, now) {
						h, err := wire.ParseHeader(packet, 8)
						if err != nil {
							t.Fatal(err)
						}
						if h.Type == wire.ZeroRTT {
							earlyPackets++
							if mode == "loss" || mode == "promote" {
								continue
							}
						}
						if err := s.HandleDatagram(packet, now); err != nil {
							t.Fatal(err)
						}
					}
					if earlyPackets == 0 {
						t.Fatal("no early traffic")
					}
					for _, item := range notifications {
						sent := false
						for _, mapped := range c.spaces[applicationSpace].sent {
							sent = sent || mapped == item
						}
						if !sent || item.queued {
							t.Fatal("notification did not leave in early flight")
						}
					}
					drivePair(t, c, s, false, false)
					if !c.ConnectionState().DidResume {
						t.Fatal("missing resumption")
					}
					for _, item := range notifications {
						if !item.acked {
							t.Fatal("early notification still live")
						}
					}
					if c.queuedBytes != 0 {
						t.Fatal("early output charge survived", c.queuedBytes)
					}
					if mode == "reject" {
						if !c.EarlyDataRejected() || c.streams.dataBlockedCount != 0 || c.streams.dataBlockedNotified || len(c.streams.objects) != 0 || len(s.streams.objects) != 0 || c.reservedResetBytes != 0 {
							t.Fatal("rejected blocked generation retained state")
						}
						for i, want := range ids {
							id, err := c.OpenStream(uni)
							if err != nil || id != want {
								t.Fatal("fresh generation ID", id, err)
							}
							if n, err := c.WriteStream(id, []byte{byte(i + 1)}, false); n != 1 || err != nil {
								t.Fatal(n, err)
							}
						}
						for _, id := range ids {
							block(id)
						}
						if c.streams.dataBlockedCount != 2 || !c.streams.dataBlockedNotified {
							t.Fatal("fresh generation failed to signal")
						}
						for _, id := range ids {
							if !c.streams.objects[id].blockedNotified {
								t.Fatal("fresh stream signal missing")
							}
						}
						return
					}
					if !c.tls.EarlyDataAccepted() {
						t.Fatal("early data unexpectedly rejected")
					}
					if mode == "promote" {
						if c.streams.dataBlockedCount != 0 || c.streams.dataBlockedNotified || c.streams.send.Limit() != 4 {
							t.Fatal("promoted connection limit retained blocked state")
						}
						for _, id := range ids {
							st := c.streams.objects[id]
							if st.connectionBlocked || st.blockedNotified || st.send.Limit() != 2 {
								t.Fatal("promoted stream limit retained blocked state")
							}
							if n, err := c.WriteStream(id, []byte{9}, false); n != 1 || err != nil {
								t.Fatal("promoted credit", n, err)
							}
						}
						for _, id := range ids {
							block(id)
						}
						if c.streams.dataBlockedCount != 2 || c.streams.send.Used() != 4 {
							t.Fatal("reblocking accounting")
						}
					} else {
						for _, id := range ids {
							block(id)
						}
						if c.queuedBytes != 0 || c.streams.dataBlockedCount != 2 {
							t.Fatal("ACKed signals duplicated")
						}
						// Read only after state checks: consumption must not mask promotion or
						// stale-signal errors by generating fresh credit grants.
						for i, want := range ids {
							id, err := s.AcceptStream(uni)
							if err != nil || id != want {
								t.Fatal(id, err)
							}
							data := make([]byte, 1)
							if n, err := s.ReadStream(id, data); n != 1 || err != nil || data[0] != byte(i+1) {
								t.Fatal(n, err, data)
							}
						}
					}
				})
			}
		}
	}
}
