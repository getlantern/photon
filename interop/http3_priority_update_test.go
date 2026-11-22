package interop

import (
	"context"
	"errors"
	"fmt"
	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	h3 "github.com/getlantern/photon/internal/http3wire"
	upstream "github.com/quic-go/quic-go"
	"net/http"
	"testing"
	"time"
)

func TestHTTP3PriorityUpdateErrorsIndependent(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, test := range []struct {
			name     string
			typ      uint64
			payloads [][]byte
			code     uint64
		}{
			{"wrong stream ID", h3.PriorityUpdateRequest, [][]byte{{2, 'i'}}, h3.IDError},
			{"unpromised push", h3.PriorityUpdatePush, [][]byte{{0, 'i'}}, h3.IDError},
			{"missing ID", h3.PriorityUpdateRequest, [][]byte{nil}, h3.FrameError},
			{"pending budget", h3.PriorityUpdateRequest, [][]byte{{4, 'i'}, {8, 'i'}}, h3.ExcessiveLoad},
		} {
			t.Run(fmt.Sprintf("%v/%s", version, test.name), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				ct.NextProtos = []string{"h3"}
				st.NextProtos = []string{"h3"}
				listener, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{version}})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				server := &ownh3.Server{MaxConcurrentRequests: 1, Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unexpected request") })}
				result := make(chan error, 1)
				go func() { result <- server.ServeListener(listener) }()
				defer func() {
					server.Close()
					listener.Close()
					select {
					case <-result:
					case <-time.After(time.Second):
						t.Error("server failed to stop")
					}
				}()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				client, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
				if err != nil {
					t.Fatal(err)
				}
				defer client.CloseWithError(0, "")
				control, err := client.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				data, _ := h3.AppendSettings([]byte{0}, nil)
				for _, payload := range test.payloads {
					data, err = h3.AppendHeader(data, h3.Header{Type: test.typ, Length: uint64(len(payload))})
					if err != nil {
						t.Fatal(err)
					}
					data = append(data, payload...)
				}
				if _, err := control.Write(data); err != nil {
					t.Fatal(err)
				}
				select {
				case <-client.Context().Done():
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				var app *upstream.ApplicationError
				if !errors.As(context.Cause(client.Context()), &app) || uint64(app.ErrorCode) != test.code || !app.Remote {
					t.Fatal(context.Cause(client.Context()))
				}
			})
		}
	}
}
