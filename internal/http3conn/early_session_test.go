package http3conn

import (
	"context"
	"errors"
	"testing"
	"time"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

func earlySessionConfig(settings *[]h3.Setting) SessionConfig {
	return SessionConfig{EarlyPeerSettings: settings, PeerIsServer: true, Decoder: decoderLimits(), Encoder: encoderLimits(), MaxActiveStreams: 4, MaxSettingsBytes: 1024, MaxSettingsEntries: 8, MaxEncoderInstruction: 1024, Control: func(context.Context, h3.ControlEvent) error { return nil }}
}
func TestEarlySessionEncoderPublication(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		settings := []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 128}, {ID: h3.QPACKBlockedStreams, Value: 1}, {ID: h3.MaxFieldSectionSize, Value: 100}}
		transport := &dispatchTransport{streams: make(chan ReceiveStream, 2)}
		local := &LocalStreams{Control: &fakeStream{interrupt: make(chan struct{})}, Encoder: &fakeStream{interrupt: make(chan struct{})}, Decoder: &fakeStream{interrupt: make(chan struct{})}}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		session, err := StartSession(ctx, transport, local, earlySessionConfig(&settings))
		if err != nil {
			t.Fatal(err)
		}
		defer session.Stop(context.Canceled)
		encoder, err := session.WaitEncoder(ctx)
		if err != nil {
			t.Fatal("early encoder unavailable", err)
		}
		select {
		case <-session.ReceivedSettings():
			t.Fatal("remembered settings signaled live receipt")
		default:
		}
		if _, ok := session.PeerSettings(); ok {
			t.Fatal("remembered wire settings published")
		}
		if _, ready := session.Settings().Peer(); ready {
			t.Fatal("live SETTINGS falsely published")
		}
		if ok, err := encoder.Insert(fs("x-early", "value")); err != nil || !ok {
			t.Fatal(ok, err)
		}
		if _, err := encoder.Encode(0, []qpack.DecodedField{fs("x-early", "value")}); err != nil {
			t.Fatal(err)
		}
		feedback := heldSessionStream(3, []byte{3, 0x80})
		transport.streams <- feedback
		select {
		case <-feedback.held:
		case <-session.Done():
			t.Fatal("early feedback rejected")
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		// Mutate caller-owned remembered storage: the session must have copied it.
		settings[0].Value = 256
		live := []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 128}, {ID: h3.QPACKBlockedStreams, Value: 2}, {ID: h3.MaxFieldSectionSize, Value: 200}}
		if invalid {
			live[0].Value = 256
		}
		data, err := h3.AppendSettings([]byte{0}, live)
		if err != nil {
			t.Fatal(err)
		}
		control := heldSessionStream(7, data)
		transport.streams <- control
		if invalid {
			select {
			case <-session.Done():
			case <-ctx.Done():
				t.Fatal("invalid live settings not fatal")
			}
			transport.check(t, QPACKDecoderStreamError)
			select {
			case <-session.ReceivedSettings():
				t.Fatal("invalid settings published")
			default:
			}
		} else {
			select {
			case <-control.held:
			case <-session.Done():
				t.Fatal("valid live settings rejected")
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case <-session.ReceivedSettings():
			default:
				t.Fatal("live settings not signaled")
			}
			snapshot, ok := session.PeerSettings()
			if !ok || len(snapshot) != len(live) {
				t.Fatal("wire snapshot missing")
			}
			snapshot[0].Value = 999
			again, _ := session.PeerSettings()
			if again[0].Value != live[0].Value {
				t.Fatal("snapshot aliases storage")
			}
			promoted, err := session.WaitEncoder(ctx)
			if err != nil || promoted != encoder {
				t.Fatal("encoder replaced", err)
			}
			if encoder.table.KnownReceivedCount() != 1 {
				t.Fatal("feedback state reset")
			}
			if snapshot, ok := session.Settings().Peer(); !ok || snapshot.MaxFieldSectionSize != 200 {
				t.Fatal("live limits missing")
			}
		}
		session.Stop(context.Canceled)
	}
}
func TestEarlySessionInitializationCancellation(t *testing.T) {
	settings := []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 128}}
	transport := &dispatchTransport{streams: make(chan ReceiveStream)}
	encoder := &fakeStream{block: true, entered: make(chan struct{}), interrupt: make(chan struct{})}
	local := &LocalStreams{Control: &fakeStream{interrupt: make(chan struct{})}, Encoder: encoder, Decoder: &fakeStream{interrupt: make(chan struct{})}}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	result := make(chan error, 1)
	go func() { _, err := StartSession(ctx, transport, local, earlySessionConfig(&settings)); result <- err }()
	select {
	case <-encoder.entered:
	case <-time.After(time.Second):
		t.Fatal("initialization did not write capacity")
	}
	cause := errors.New("early setup canceled")
	cancel(cause)
	select {
	case err := <-result:
		if err != cause {
			t.Fatal("lost cancellation cause", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked early setup did not join")
	}
	transport.check(t, InternalError)
}

func TestEarlySessionDefaultAndRole(t *testing.T) {
	for _, server := range []bool{false, true} {
		settings := []h3.Setting{}
		cfg := earlySessionConfig(&settings)
		cfg.PeerIsServer = !server
		transport := &dispatchTransport{streams: make(chan ReceiveStream)}
		local := &LocalStreams{Control: &fakeStream{interrupt: make(chan struct{})}, Encoder: &fakeStream{interrupt: make(chan struct{})}, Decoder: &fakeStream{interrupt: make(chan struct{})}}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		session, err := StartSession(ctx, transport, local, cfg)
		if server {
			if err == nil {
				session.Stop(context.Canceled)
				t.Fatal("server accepted remembered peer settings")
			}
		} else {
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			if _, err := session.WaitEncoder(ctx); err != nil {
				t.Fatal("default early encoder unavailable", err)
			}
			if _, received := session.Settings().Peer(); received {
				t.Fatal("default early settings treated as received")
			}
			session.Stop(context.Canceled)
		}
		cancel()
	}
}
