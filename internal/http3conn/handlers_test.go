package http3conn

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

func TestCriticalHandlers(t *testing.T) {
	control, encoder, decoder := 0, 0, 0
	h := Handlers{PeerIsServer: true, MaxSettingsBytes: 128, MaxSettingsEntries: 8, MaxEncoderInstruction: 64,
		Control: func(_ context.Context, v http3wire.ControlEvent) error {
			control++
			if v.Type != http3wire.Settings {
				t.Fatal(v)
			}
			return nil
		},
		Encoder: func(_ context.Context, wire []byte) error {
			encoder++
			if !bytes.Equal(wire, []byte{0x20}) {
				t.Fatal(wire)
			}
			return nil
		},
		Decoder: func(_ context.Context, v qpack.DecoderInstruction) error {
			decoder++
			if v != (qpack.DecoderInstruction{Type: qpack.InsertCountIncrement, Value: 1}) {
				t.Fatal(v)
			}
			return nil
		}}
	handler, err := h.Handler()
	if err != nil {
		t.Fatal(err)
	}
	settings, _ := http3wire.AppendSettings(nil, nil)
	for _, c := range []struct {
		typ  uint64
		wire []byte
	}{{0, settings}, {2, []byte{0x20}}, {3, []byte{1}}} {
		err := handler(context.Background(), c.typ, &fakeReceive{Reader: bytes.NewReader(c.wire), id: 3})
		if c.typ == 0 {
			var protocol *http3wire.Error
			if !errors.As(err, &protocol) || protocol.Code != http3wire.ClosedCriticalStream {
				t.Fatal(err)
			}
		} else if err != io.EOF {
			t.Fatal(err)
		}
	}
	if control != 1 || encoder != 1 || decoder != 1 {
		t.Fatal(control, encoder, decoder)
	}
	for _, c := range []struct {
		typ  uint64
		wire []byte
		code uint64
	}{{3, []byte{0}, QPACKDecoderStreamError}, {2, append([]byte{0x3f}, bytes.Repeat([]byte{0xff}, 15)...), QPACKEncoderStreamError}, {2, []byte{0x41, 'a', 100}, http3wire.ExcessiveLoad}} {
		err := handler(context.Background(), c.typ, &fakeReceive{Reader: bytes.NewReader(c.wire), id: 3})
		var protocol *http3wire.Error
		if !errors.As(err, &protocol) || protocol.Code != c.code {
			t.Fatal(c, err)
		}
	}
	failure := errors.New("transport reset")
	if err := handler(context.Background(), 2, &fakeReceive{Reader: errorReader{failure}, id: 3}); err != failure {
		t.Fatal("source error changed", err)
	}
}
