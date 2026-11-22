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

func TestEngineEarlySend(t *testing.T) {
	for _, compatible := range []bool{false, true} {
		for _, v := range []wire.Version{wire.Version1, wire.Version2} {
			for _, mode := range []string{"accept", "reorder", "loss", "reject", "reduced"} {
				t.Run(fmt.Sprintf("%x/%s/compatible=%v", v, mode, compatible), func(t *testing.T) {
					cc, sc := testTLS(t)
					cc.ClientSessionCache = tls.NewLRUClientSessionCache(4)
					c, s := earlyPair(t, v, cc, sc, true)
					drivePair(t, c, s, false, false)
					c, s = earlyPairConfigured(t, v, cc, sc, mode != "reject", mode == "reduced", compatible)
					id, err := c.OpenStream(false)
					if err != nil {
						t.Fatal(err)
					}
					var rejected error
					if err := c.WatchReceiveClose(id, func(err error) { rejected = err }); err != nil {
						t.Fatal(err)
					}
					payload := make([]byte, 4096)
					for i := range payload {
						payload[i] = byte(i)
					}
					for sent := 0; sent < len(payload); {
						n, err := c.WriteStream(id, payload[sent:], true)
						if err != nil {
							t.Fatal(err)
						}
						sent += n
					}
					var released error
					if err := c.AutoReleaseStream(id, func(err error) { released = err }); err != nil {
						t.Fatal(err)
					}
					now := time.Unix(100, 0)
					flight := drain(t, c, now)
					var initial, early [][]byte
					for _, b := range flight {
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
						if len(b) > 1200 {
							t.Fatal("oversized early packet")
						}
					}
					if len(early) < 2 || c.earlySealed != uint64(len(early)) || c.sealed[applicationSpace] != 0 {
						t.Fatal("missing early packets or mixed key counters")
					}
					next := c.spaces[applicationSpace].nextPN
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
					accepted := mode != "reject" && mode != "reduced"
					if accepted && mode != "loss" {
						if s.complete {
							t.Fatal("early data arrived after completion")
						}
						got, err := s.AcceptStream(false)
						if err != nil || got != id {
							t.Fatal(got, err)
						}
						out := make([]byte, len(payload))
						n, err := s.ReadStream(id, out)
						if n != len(payload) || string(out) != string(payload) {
							t.Fatal("early stream payload", n, err)
						}
					}
					if !accepted {
						// Include both transmitted and not-yet-transmitted early stream state.
						extra, err := c.OpenStream(false)
						if err != nil {
							t.Fatal(err)
						}
						if _, err := c.WriteStream(extra, []byte("must not replay"), false); err != nil {
							t.Fatal(err)
						}
					}
					if !accepted {
						for _, b := range drain(t, s, now) {
							if err := c.HandleDatagram(b, now); err != nil {
								t.Fatal(err)
							}
						}
					}
					drivePair(t, c, s, false, false)
					if compatible && (c.Version() == v || s.Version() != c.Version()) {
						t.Fatal("compatible version not selected")
					}
					if c.spaces[applicationSpace].nextPN < next || c.earlySealed != uint64(len(early)) {
						t.Fatal("packet numbers reset or early send continued")
					}
					if accepted {
						if c.earlyRejected || !c.tls.EarlyDataAccepted() {
							t.Fatal("unexpected rejection")
						}
						if mode == "loss" {
							got, err := s.AcceptStream(false)
							if err != nil || got != id {
								t.Fatal(got, err)
							}
							out := make([]byte, len(payload))
							n, err := s.ReadStream(id, out)
							if n != len(payload) || string(out) != string(payload) {
								t.Fatal("lost early data not recovered in 1-RTT", n, err)
							}
						}
					} else {
						if !errors.Is(rejected, ErrEarlyDataRejected) || !errors.Is(released, ErrEarlyDataRejected) || !c.earlyRejected {
							t.Fatal("rejection not reported", rejected)
						}
						if len(c.streams.objects) != 0 || c.streams.send.Used() != 0 || len(s.streams.objects) != 0 {
							t.Fatal("rejected streams survived or replayed")
						}
						fresh, err := c.OpenStream(false)
						if err != nil || fresh != 0 {
							t.Fatal("fresh stream IDs", fresh, err)
						}
						if _, err := c.WriteStream(fresh, []byte("fresh"), true); err != nil {
							t.Fatal(err)
						}
						drivePair(t, c, s, false, false)
						got, err := s.AcceptStream(false)
						if err != nil || got != fresh {
							t.Fatal(got, err)
						}
						out := make([]byte, 16)
						n, err := s.ReadStream(got, out)
						if string(out[:n]) != "fresh" {
							t.Fatal("rejection polluted new stream", string(out[:n]), err)
						}
						forged := makePeerPacket(t, s, applicationSpace, s.spaces[applicationSpace].nextPN, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}})
						err = c.HandleDatagram(forged, now)
						var protocol *TransportError
						if !errors.As(err, &protocol) || protocol.Code != 0xa || !c.Closing() {
							t.Fatal("ACK of rejected early packet accepted", err)
						}
					}
				})
			}
		}
	}
}

func TestEngineEarlySendRetry(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		cc, sc := testTLS(t)
		cc.ClientSessionCache = tls.NewLRUClientSessionCache(4)
		c, s := earlyPair(t, v, cc, sc, true)
		drivePair(t, c, s, false, false)
		c, s = earlyPair(t, v, cc, sc, true)
		id, err := c.OpenStream(false)
		if err != nil {
			t.Fatal(err)
		}
		payload := bytes.Repeat([]byte{0x5a}, 4096)
		for sent := 0; sent < len(payload); {
			n, err := c.WriteStream(id, payload[sent:], true)
			if err != nil {
				t.Fatal(err)
			}
			sent += n
		}
		now := time.Unix(100, 0)
		drain(t, c, now) // Router requests Retry instead of creating this connection.
		next := c.spaces[applicationSpace].nextPN
		if next == 0 {
			t.Fatal("no early packets before Retry")
		}
		retryCID := bytes.Repeat([]byte{0x42}, 20)
		if err := c.HandleDatagram(retryPacket(t, c, retryCID, bytes.Repeat([]byte{0x21}, 700)), now); err != nil {
			t.Fatal(err)
		}
		if c.spaces[applicationSpace].nextPN != next || c.streams.send.Used() != 4096 || c.earlyRejected {
			t.Fatal("Retry reset early state")
		}
		cfg := s.config
		cfg.RetrySourceCID = retryCID
		s, err = NewConn(cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		if err := s.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		early := 0
		for _, b := range drain(t, c, now) {
			h, err := wire.ParseHeader(b, 8)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(h.DCID, retryCID) {
				t.Fatal("old destination after Retry")
			}
			if h.Type == wire.ZeroRTT {
				_, _, pn, err := c.tls.WriteKeys(tls.QUICEncryptionLevelEarly).Open(b, h.PNOffset, next)
				if err != nil || pn < next {
					t.Fatal("early nonce reused", pn, err)
				}
				early++
			}
			if err := s.HandleDatagram(b, now); err != nil {
				t.Fatal(err)
			}
		}
		if early == 0 {
			t.Fatal("Retry lost early retransmission")
		}
		got, err := s.AcceptStream(false)
		if err != nil || got != id {
			t.Fatal(got, err)
		}
		out := make([]byte, len(payload))
		n, err := s.ReadStream(got, out)
		if n != len(out) || !bytes.Equal(out, payload) {
			t.Fatal("early data after Retry", n, err)
		}
		drivePair(t, c, s, false, false)
		if !c.tls.EarlyDataAccepted() {
			t.Fatal("Retry rejected early data")
		}
	}
}
