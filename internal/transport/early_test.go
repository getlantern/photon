package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func earlyPair(t *testing.T, v wire.Version, cc, sc *tls.Config, accept bool, reduced ...bool) (*Conn, *Conn) {
	return earlyPairConfigured(t, v, cc, sc, accept, len(reduced) > 0 && reduced[0], false)
}
func earlyPairConfigured(t *testing.T, v wire.Version, cc, sc *tls.Config, accept, reduced, compatible bool) (*Conn, *Conn) {
	t.Helper()
	p := wire.DefaultTransportParameters()
	p.InitialMaxData = 8192
	p.InitialMaxStreamDataBidiRemote = 4096
	p.InitialMaxStreamsBidi = 2
	cfg := Config{Version: v, TLS: cc, LocalCID: []byte("client00"), PeerCID: []byte("original"), OriginalDCID: []byte("original"), Parameters: p, enableEarlyData: true}
	if compatible {
		selected := wire.Version1
		if v == selected {
			selected = wire.Version2
		}
		cfg.CompatibleVersions = []wire.Version{selected, v}
		cfg.Parameters.VersionInformation = &wire.VersionInformation{Chosen: v, Available: []wire.Version{v, selected}}
	}
	c, err := NewConn(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Server, cfg.TLS, cfg.LocalCID, cfg.PeerCID, cfg.enableEarlyData = true, sc, []byte("server00"), []byte("client00"), accept
	if reduced {
		cfg.Parameters.InitialMaxData = 4096
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

func sealEarlyTest(t *testing.T, c *Conn, keys *packet.Keys, pn uint64, f wire.Frame) []byte {
	t.Helper()
	payload, err := f.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	h, err := wire.AppendLongHeader(nil, c.config.Version, wire.ZeroRTT, []byte("original"), []byte("client00"), nil, pn, 4, len(payload)+16)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := keys.Seal(h, payload, pn)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestEngineEarlyReceive(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []string{"accept", "reject", "reduced", "forbidden"} {
			t.Run(fmt.Sprintf("%x/%s", v, mode), func(t *testing.T) {
				cc, sc := testTLS(t)
				cc.ClientSessionCache = tls.NewLRUClientSessionCache(4)
				c, s := earlyPair(t, v, cc, sc, true)
				drivePair(t, c, s, false, false)
				c, s = earlyPair(t, v, cc, sc, mode != "reject", mode == "reduced")
				keys := c.tls.WriteKeys(tls.QUICEncryptionLevelEarly)
				if keys == nil {
					t.Fatal("missing early keys")
				}
				var frame wire.Frame = wire.StreamFrame{StreamID: 0, Data: []byte("early payload"), Fin: true}
				if mode == "forbidden" {
					frame = wire.CryptoFrame{Data: []byte{1}}
				}
				raw := sealEarlyTest(t, c, keys, 0, frame)
				now := time.Unix(100, 0)
				if err := s.HandleDatagram(raw, now); err != nil {
					t.Fatal(err)
				}
				if mode != "reject" && len(s.deferred) != 1 {
					t.Fatal("early packet not deferred")
				}
				var receiveErr error
				for _, b := range drain(t, c, now) {
					receiveErr = s.HandleDatagram(b, now)
					if receiveErr != nil {
						break
					}
				}
				if mode == "forbidden" {
					if receiveErr == nil {
						t.Fatal("accepted CRYPTO in 0-RTT")
					}
					return
				}
				if receiveErr != nil {
					t.Fatal(receiveErr)
				}
				if len(s.deferred) != 0 {
					t.Fatal("deferred early packet retained")
				}
				if mode == "reject" || mode == "reduced" {
					if len(s.streams.objects) != 0 {
						t.Fatal("rejected data created streams")
					}
					if err := s.HandleDatagram(raw, now); err != nil {
						t.Fatal(err)
					}
					if len(s.deferred) != 0 {
						t.Fatal("known rejection buffered")
					}
					drivePair(t, c, s, false, false)
					if !c.tls.EarlyDataRejected() {
						t.Fatal("client did not observe rejection")
					}
					return
				}
				if s.complete {
					t.Fatal("early data delivered after handshake")
				}
				id, err := s.AcceptStream(false)
				if err != nil || id != 0 {
					t.Fatal(id, err)
				}
				buf := make([]byte, 32)
				n, err := s.ReadStream(id, buf)
				if string(buf[:n]) != "early payload" {
					t.Fatal(string(buf[:n]), err)
				}
				// A replay is acknowledged but never redelivers stream bytes.
				if err := s.HandleDatagram(raw, now); err != nil {
					t.Fatal(err)
				}
				if s.spaces[applicationSpace].received.Expected() != 1 {
					t.Fatal("early packet used wrong number space")
				}
				// Account for the injected packet before allowing the normal engine to send 1-RTT.
				if err := c.recovery.OnSent(recovery.Space(applicationSpace), recovery.Packet{Number: 0, SentAt: now, Bytes: len(raw), AckEliciting: true, InFlight: true}); err != nil {
					t.Fatal(err)
				}
				late := sealEarlyTest(t, c, keys, 1, wire.StreamFrame{StreamID: 4, Data: []byte("reordered"), Fin: true})
				if err := c.recovery.OnSent(recovery.Space(applicationSpace), recovery.Packet{Number: 1, SentAt: now, Bytes: len(late), AckEliciting: true, InFlight: true}); err != nil {
					t.Fatal(err)
				}
				c.spaces[applicationSpace].largestSent = 1
				c.spaces[applicationSpace].nextPN = 2
				c.spaces[applicationSpace].sentAny = true
				drivePair(t, c, s, false, false)
				if !c.tls.EarlyDataAccepted() || !s.tls.EarlyDataAccepted() {
					t.Fatal("TLS early acceptance missing")
				}
				if s.earlyReadExpiry.IsZero() {
					t.Fatal("no early retirement deadline")
				}
				expiry := s.earlyReadExpiry
				if !s.spaces[initialSpace].discarded {
					t.Fatal("Initial space still live")
				}
				if err := s.HandleDatagram(late, expiry.Add(-time.Nanosecond)); err != nil {
					t.Fatal(err)
				}
				id, err = s.AcceptStream(false)
				if err != nil || id != 4 {
					t.Fatal("late early stream", id, err)
				}
				n, err = s.ReadStream(id, buf)
				if string(buf[:n]) != "reordered" {
					t.Fatal("late early data", string(buf[:n]), err)
				}
				if deadline := s.RecoveryDeadline(); deadline.IsZero() || deadline.After(expiry) {
					t.Fatal("retirement omitted from deadline", deadline, expiry)
				}
				s.startEarlyKeyRetirement(expiry.Add(-time.Nanosecond))
				if s.earlyReadExpiry != expiry {
					t.Fatal("retirement extended")
				}
				if err := s.HandleTimeout(expiry); err != nil {
					t.Fatal(err)
				}
				if s.tls.ReadKeys(tls.QUICEncryptionLevelEarly) != nil {
					t.Fatal("early keys retained")
				}
				if err := s.HandleDatagram(raw, expiry); err != nil {
					t.Fatal(err)
				}
				if len(s.deferred) != 0 {
					t.Fatal("retired early packet buffered")
				}
			})
		}
	}
}
