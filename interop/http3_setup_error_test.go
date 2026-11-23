package interop

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
)

func TestHTTP3SetupPeerCloseError(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, mode := range []string{"new", "dial", "early", "pool"} {
			t.Run(fmt.Sprintf("%x/%s", version, mode), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				ct.NextProtos = []string{"h3"}
				st.NextProtos = []string{"h3"}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				// With no critical-stream credit, HTTP setup cannot finish before closure.
				listener, err := upstream.ListenAddr("127.0.0.1:0", st, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, MaxIncomingUniStreams: -1})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				origin := "https://" + listener.Addr().String()
				result := make(chan error, 1)
				rawReady := make(chan struct{})
				go func() {
					cfg := &quic.Config{Versions: []quic.Version{version}}
					var client *ownh3.ClientConn
					var err error
					switch mode {
					case "new":
						var raw *quic.Conn
						raw, err = quic.DialAddr(ctx, listener.Addr().String(), ct, cfg)
						if err == nil {
							defer raw.CloseWithError(0, "")
							close(rawReady)
							client, err = ownh3.NewClientConn(ctx, raw, origin, nil)
						}
					case "dial":
						client, err = ownh3.Dial(ctx, origin, ct, cfg, nil)
					case "early":
						client, err = ownh3.DialEarly(ctx, origin, ct, cfg, nil)
					case "pool":
						transport := &ownh3.Transport{TLSClientConfig: ct, QUICConfig: cfg}
						defer transport.Close()
						req, _ := http.NewRequestWithContext(ctx, "GET", origin, nil)
						response, e := transport.RoundTrip(req)
						err = e
						if response != nil {
							response.Body.Close()
						}
					}
					if client != nil {
						client.Close()
					}
					result <- err
				}()
				joined := false
				defer func() {
					cancel()
					if !joined {
						select {
						case <-result:
						case <-time.After(time.Second):
							t.Error("setup did not join")
						}
					}
				}()
				peer, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "new" {
					select {
					case <-rawReady:
					case <-ctx.Done():
						t.Fatal("raw connection not ready", ctx.Err())
					}
				}
				if err = peer.CloseWithError(0x109, "setup settings failure"); err != nil {
					t.Fatal(err)
				}
				select {
				case err = <-result:
					joined = true
					var h *ownh3.Error
					var close *quic.PeerCloseError
					var app *quic.ApplicationError
					if !errors.As(err, &app) || !app.Remote || app.ErrorCode != 0x109 || !errors.As(err, &h) || !h.Remote || h.ErrorCode != ownh3.ErrCodeSettingsError || h.ErrorMessage != "setup settings failure" || !errors.As(err, &close) || !close.Application || close.Code != 0x109 {
						t.Fatal("setup cause", err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			})
		}
	}
}
