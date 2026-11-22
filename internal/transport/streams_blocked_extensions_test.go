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

func TestStreamsBlockedEarlyLifecycle(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, uni := range []bool{false, true} {
			for _, mode := range []string{"accept", "loss", "reject", "promote"} {
				t.Run(fmt.Sprintf("%s/uni=%t/%s", vName(v), uni, mode), func(t *testing.T) {
					cc, sc := testTLS(t)
					cc.ClientSessionCache = tls.NewLRUClientSessionCache(4)
					c, s := earlyPartialResetPair(t, v, cc, sc, false, true)
					drivePair(t, c, s, false, false)
					if mode == "promote" {
						clientConfig, serverConfig := c.config, s.config
						serverConfig.Parameters.InitialMaxStreamsBidi = 3
						serverConfig.Parameters.InitialMaxStreamsUni = 3
						var err error
						c, err = NewConn(clientConfig)
						if err != nil {
							t.Fatal(err)
						}
						s, err = NewConn(serverConfig)
						if err != nil {
							t.Fatal(err)
						}
						for _, conn := range []*Conn{c, s} {
							t.Cleanup(func() { conn.Close() })
							if err := conn.Start(context.Background()); err != nil {
								t.Fatal(err)
							}
						}
					} else {
						c, s = earlyPartialResetPair(t, v, cc, sc, false, mode != "reject")
					}
					if !c.EarlyReady() || c.complete {
						t.Fatal("no resumed early keys")
					}
					exhaust := func() {
						t.Helper()
						for i := 0; i < 2; i++ {
							if _, err := c.OpenStream(uni); err != nil {
								t.Fatal(err)
							}
						}
					}
					block := func() {
						t.Helper()
						if _, err := c.OpenStream(uni); !errors.Is(err, stream.ErrWouldBlock) {
							t.Fatal(err)
						}
					}
					exhaust()
					block()
					space := &c.spaces[applicationSpace]
					if len(space.queue) != 1 {
						t.Fatal("missing early notification")
					}
					original := space.queue[0]
					now := time.Unix(100, 0)
					early := 0
					for _, packet := range drain(t, c, now) {
						h, err := wire.ParseHeader(packet, 8)
						if err != nil {
							t.Fatal(err)
						}
						if h.Type == wire.ZeroRTT {
							early++
							if mode == "loss" || mode == "promote" {
								continue
							}
						}
						if err := s.HandleDatagram(packet, now); err != nil {
							t.Fatal(err)
						}
					}
					if early != 1 {
						t.Fatal("expected isolated early blocked packet", early)
					}
					drivePair(t, c, s, false, false)
					if !c.ConnectionState().DidResume {
						t.Fatal("not a resumed TLS connection")
					}
					if !original.acked || c.queuedBytes != 0 {
						t.Fatal("early notification not retired")
					}
					if len(s.streams.objects) != 0 || s.streams.ids.Pending(uni) != 0 {
						t.Fatal("blocked signal opened peer streams")
					}
					if mode == "reject" {
						if !c.EarlyDataRejected() || len(c.streams.objects) != 0 {
							t.Fatal("rejected generation survived")
						}
						exhaust()
						block()
						if len(space.queue) != 1 || space.queue[0] == original {
							t.Fatal("rejection retained notification dedup state")
						}
					} else {
						if !c.tls.EarlyDataAccepted() || c.EarlyDataRejected() {
							t.Fatal("unexpected early rejection")
						}
						if mode != "promote" {
							block()
							if len(space.queue) != 0 {
								t.Fatal("ACKed notification duplicated")
							}
							packet := makePeerPacket(t, s, applicationSpace, 100, wire.MaxStreamsFrame{Unidirectional: uni, Maximum: 3})
							if err := c.HandleDatagram(packet, time.Unix(101, 0)); err != nil {
								t.Fatal(err)
							}
						} else if c.streams.ids.SendLimit(uni) != 3 {
							t.Fatal("accepted handshake did not promote stream limit")
						}
						if _, err := c.OpenStream(uni); err != nil {
							t.Fatal(err)
						}
						block()
						if len(space.queue) != 1 {
							t.Fatal("new blocking limit not signaled")
						}
						if frame, ok := space.queue[0].frame.(wire.StreamsBlockedFrame); !ok || frame.Limit != 3 {
							t.Fatal(space.queue[0].frame)
						}
					}
				})
			}
		}
	}
}

func TestStreamsBlockedMultipathRetirement(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, uni := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/uni=%t", vName(v), uni), func(t *testing.T) {
				c, peer := sendingPathPair(t, v)
				for _, conn := range []*Conn{c, peer} {
					app := conn.multipath.paths[1].application
					app.validation.state = PathValidated
					app.budget.Validate()
				}
				limit := c.streams.ids.SendLimit(uni)
				for i := uint64(0); i < limit; i++ {
					if _, err := c.OpenStream(uni); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := c.OpenStream(uni); !errors.Is(err, stream.ErrWouldBlock) {
					t.Fatal(err)
				}
				primary := c.multipath.paths[0].application.space
				if len(primary.queue) != 1 {
					t.Fatal("missing primary blocked signal")
				}
				original := primary.queue[0]
				now := time.Unix(101, 0)
				if packet, err := c.nextPacketOnPath(0, now); err != nil || packet == nil {
					t.Fatal(err)
				}
				if err := c.abandonFailedPath(0); err != nil {
					t.Fatal(err)
				}
				survivor := c.multipath.paths[1].application.space
				found := false
				for _, item := range survivor.queue {
					found = found || item == original
				}
				if !found || original.acked {
					t.Fatal("notification not evacuated")
				}
				before := c.queuedBytes
				if err := peer.queueFrameOnPath(1, wire.MaxStreamsFrame{Unidirectional: uni, Maximum: limit + 1}); err != nil {
					t.Fatal(err)
				}
				packet, err := peer.nextPacketOnPath(1, now)
				if err != nil || packet == nil {
					t.Fatal(err)
				}
				if err := c.handleMultipathPacket(packet, now); err != nil {
					t.Fatal(err)
				}
				if !original.acked || c.queuedBytes != before-32 {
					t.Fatal("stale evacuated notification not retired exactly once")
				}
				for _, state := range c.multipath.paths {
					for _, item := range state.application.space.queue {
						if item == original {
							t.Fatal("stale queue mapping")
						}
					}
					for _, item := range state.application.space.sent {
						if item == original {
							t.Fatal("stale sent mapping")
						}
					}
				}
				if _, err := c.OpenStream(uni); err != nil {
					t.Fatal("survivor credit", err)
				}
				if _, err := c.OpenStream(uni); !errors.Is(err, stream.ErrWouldBlock) {
					t.Fatal(err)
				}
				found = false
				for _, item := range survivor.queue {
					if frame, ok := item.frame.(wire.StreamsBlockedFrame); ok && frame.Limit == limit+1 {
						found = true
					}
				}
				if !found {
					t.Fatal("new notification not queued on survivor")
				}
			})
		}
	}
}
