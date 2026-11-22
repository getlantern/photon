package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	h3conn "github.com/getlantern/photon/internal/http3conn"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
	upstream "github.com/quic-go/quic-go"
)

func TestHTTP3LiveCriticalFailure(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, failure := range []string{"fin", "duplicate", "feedback", "invalid-settings"} {
			t.Run(fmt.Sprintf("%x/%s", uint32(version), failure), func(t *testing.T) {
				clientTLS, serverTLS := tlsConfigs(t)
				clientTLS.NextProtos = []string{"h3"}
				serverTLS.NextProtos = []string{"h3"}
				listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				peer, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "")
				conn, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.CloseWithError(0, "")
				encoder, err := h3conn.NewFieldEncoder(h3conn.SettingsSnapshot{MaxFieldSectionSize: 65536}, h3conn.EncoderLimits{MaxEncodedSection: 65536, MaxFieldSection: 65536, MaxFields: 64}, io.Discard)
				if err != nil {
					t.Fatal(err)
				}
				handlers := h3conn.Handlers{MaxSettingsBytes: 128, MaxSettingsEntries: 8, MaxEncoderInstruction: 128, Control: func(context.Context, h3.ControlEvent) error { return nil }, Encoder: func(context.Context, []byte) error { return nil }, Decoder: func(_ context.Context, instruction qpack.DecoderInstruction) error {
					return encoder.Feedback(instruction)
				}}
				handler, err := handlers.Handler()
				if err != nil {
					t.Fatal(err)
				}
				result := make(chan error, 1)
				done := make(chan struct{})
				go func() {
					defer close(done)
					result <- h3conn.RunIncoming(ctx, h3conn.QUICTransport{Conn: conn}, false, 8, handler)
				}()
				defer func() {
					cancel()
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Error("dispatcher did not stop")
					}
				}()
				control, err := peer.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				settingsWire := []byte{0, 4, 0}
				if failure == "invalid-settings" {
					settingsWire = []byte{0, 4, 2, 8, 2}
				}
				if _, err := control.Write(settingsWire); err != nil {
					t.Fatal(err)
				}
				code := h3.ClosedCriticalStream
				if failure == "duplicate" {
					code = h3.StreamCreationError
					second, err := peer.OpenUniStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := second.Write([]byte{0, 4, 0}); err != nil {
						t.Fatal(err)
					}
				} else if failure == "feedback" {
					code = h3conn.QPACKDecoderStreamError
					feedback, err := peer.OpenUniStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					// Type 3 followed by Insert Count Increment 1, impossible without an insertion.
					if _, err := feedback.Write([]byte{3, 1}); err != nil {
						t.Fatal(err)
					}
				} else if failure == "invalid-settings" {
					code = h3.SettingsError
				} else if err := control.Close(); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-result:
					var protocol *h3.Error
					if !errors.As(err, &protocol) || protocol.Code != code {
						t.Fatal("local error", err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				select {
				case <-peer.Context().Done():
					var app *upstream.ApplicationError
					if !errors.As(context.Cause(peer.Context()), &app) || uint64(app.ErrorCode) != code {
						t.Fatal("wire error", context.Cause(peer.Context()))
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			})
		}
	}
}
