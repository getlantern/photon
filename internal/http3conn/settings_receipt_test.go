package http3conn

import (
	"context"
	"testing"
	"time"

	h3 "github.com/getlantern/photon/internal/http3wire"
)

func TestLiveSettingsReceiptLifecycle(t *testing.T) {
	for _, mode := range []string{"empty", "unknown", "duplicate", "over-budget", "closed"} {
		t.Run(mode, func(t *testing.T) {
			transport := &dispatchTransport{streams: make(chan ReceiveStream, 1)}
			local := &LocalStreams{Control: &fakeStream{interrupt: make(chan struct{})}, Encoder: &fakeStream{interrupt: make(chan struct{})}, Decoder: &fakeStream{interrupt: make(chan struct{})}}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			cfg := earlySessionConfig(nil)
			cfg.MaxSettingsEntries = 1
			if mode == "duplicate" {
				cfg.MaxSettingsEntries = 2
			}
			s, err := StartSession(ctx, transport, local, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Stop(context.Canceled)
			if _, ok := s.PeerSettings(); ok {
				t.Fatal("premature receipt")
			}
			if mode == "closed" {
				s.Stop(context.Canceled)
			} else {
				var settings []h3.Setting
				if mode == "unknown" {
					settings = []h3.Setting{{ID: 0xface, Value: 1234}}
				}
				if mode == "over-budget" {
					settings = []h3.Setting{{ID: 0x21, Value: 0}, {ID: 0x22, Value: 0}}
				}
				data, err := h3.AppendSettings([]byte{0}, settings)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "duplicate" {
					data = []byte{0, 4, 4, 0x21, 0, 0x21, 1}
				}
				transport.streams <- heldSessionStream(3, data)
				if mode == "empty" || mode == "unknown" {
					select {
					case <-s.ReceivedSettings():
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					got, ok := s.PeerSettings()
					if !ok || len(got) != len(settings) {
						t.Fatal("missing live snapshot", got, ok)
					}
					if len(got) > 0 && got[0] != settings[0] {
						t.Fatal(got)
					}
					s.Stop(context.Canceled)
					if after, ok := s.PeerSettings(); !ok || len(after) != len(settings) {
						t.Fatal("shutdown lost snapshot")
					}
					return
				}
				select {
				case <-s.Done():
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if mode == "duplicate" {
				transport.check(t, h3.SettingsError)
			}
			if mode == "over-budget" {
				transport.check(t, h3.ExcessiveLoad)
			}
			select {
			case <-s.ReceivedSettings():
				t.Fatal("failed/closed session signaled receipt")
			default:
			}
			if _, ok := s.PeerSettings(); ok {
				t.Fatal("failed/closed session published settings")
			}
		})
	}
}
