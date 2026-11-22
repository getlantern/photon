package http3conn

import (
	"context"
	"errors"
	"testing"
	"time"

	h3 "github.com/getlantern/photon/internal/http3wire"
)

func TestSessionOwnsDatagramReceiver(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		name := "stop"
		if malformed {
			name = "malformed"
		}
		t.Run(name, func(t *testing.T) {
			transport := &dispatchTransport{streams: make(chan ReceiveStream)}
			d, _ := NewDatagramDispatch(1, 2, 16)
			request, _ := d.Register(0, true)
			source := datagramSource{input: make(chan []byte, 1)}
			local := &LocalStreams{Control: &fakeStream{interrupt: make(chan struct{})}, Encoder: &fakeStream{interrupt: make(chan struct{})}, Decoder: &fakeStream{interrupt: make(chan struct{})}}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			cfg := SessionConfig{PeerIsServer: true, LocalSettings: []h3.Setting{{ID: h3.H3Datagram, Value: 1}}, Decoder: decoderLimits(), Encoder: encoderLimits(), MaxActiveStreams: 4, MaxSettingsBytes: 1024, MaxSettingsEntries: 8, MaxEncoderInstruction: 1024, Control: func(context.Context, h3.ControlEvent) error { return nil }, Datagrams: &DatagramSessionConfig{Dispatch: d, Source: source, AbortRequest: func(context.Context, uint64, uint64) error { t.Error("unexpected request abort"); return nil }}}
			session, err := StartSession(ctx, transport, local, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Stop(context.Canceled)
			source.input <- []byte{0, 9}
			if data, err := request.ReceiveContext(ctx); err != nil || len(data) != 1 || data[0] != 9 {
				t.Fatal("session dispatch", data, err)
			}
			waiting := make(chan error, 1)
			go func() { _, err := request.ReceiveContext(ctx); waiting <- err }()
			if malformed {
				source.input <- nil
			} else {
				session.Stop(context.Canceled)
			}
			select {
			case <-session.Done():
			case <-ctx.Done():
				t.Fatal("session did not join receiver")
			}
			if err := <-waiting; !errors.Is(err, ErrDatagramRequestClosed) {
				t.Fatal("request waiter survived session", err)
			}
			if _, err := d.Register(4, true); !errors.Is(err, ErrDatagramRequestClosed) {
				t.Fatal("dispatcher survived", err)
			}
			if malformed {
				transport.check(t, h3.DatagramError)
			}
		})
	}
}
