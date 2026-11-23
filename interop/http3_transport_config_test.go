package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

func TestHTTP3TransportProtocolOptions(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, mode := range []string{"default", "large-default", "limited", "decoded-limit", "explicit-datagrams", "automatic-datagrams"} {
			t.Run(fmt.Sprintf("%x/%s", v, mode), func(t *testing.T) {
				if v == quic.Version2 && mode == "automatic-datagrams" && os.Getenv("QUIC_INTEROP_VN_FAILURES") == "" {
					t.Skip("pinned peer omits version information after default-version negotiation; strict opt-in QUIC_INTEROP_VN_FAILURES")
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				ct, st := tlsConfigs(t)
				ct.NextProtos, st.NextProtos = []string{"h3"}, []string{"h3"}
				headerSize := 4096
				if mode == "decoded-limit" {
					headerSize = 100
				}
				if mode == "large-default" {
					headerSize = 70 << 10
				}
				observed := make(chan bool, 1)
				written := make(chan error, 1)
				server := &peerh3.Server{TLSConfig: st, EnableDatagrams: true, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					settings := w.(peerh3.Settingser)
					select {
					case <-settings.ReceivedSettings():
					case <-ctx.Done():
						return
					}
					observed <- settings.Settings().EnableDatagrams
					w.Header().Set("X-Large", strings.Repeat("x", headerSize))
					if mode == "decoded-limit" {
						for i := 0; i < 50; i++ {
							w.Header().Set(fmt.Sprintf("X-Extra-%d", i), "a")
						}
					}
					_, err := w.Write([]byte("configured transport"))
					written <- err
				})}
				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				served := make(chan error, 1)
				go func() { served <- server.Serve(socket) }()
				defer func() { cancel(); server.Close(); socket.Close(); <-served }()
				tr := &ownh3.Transport{TLSClientConfig: ct, QUICConfig: &quic.Config{Versions: []quic.Version{v}}}
				defer tr.Close()
				wantDatagrams := false
				switch mode {
				case "limited":
					tr.MaxResponseHeaderBytes = 512
				case "decoded-limit":
					tr.MaxResponseHeaderBytes = 1024
				case "explicit-datagrams":
					tr.EnableDatagrams = true
					tr.QUICConfig.EnableDatagrams = true
					wantDatagrams = true
				case "automatic-datagrams":
					tr.EnableDatagrams = true
					tr.QUICConfig = nil
					wantDatagrams = true
				}
				request, _ := http.NewRequestWithContext(ctx, "GET", "https://"+socket.LocalAddr().String()+"/", nil)
				request.Header.Set("X-Request", strings.Repeat("r", 1024))
				response, err := tr.RoundTrip(request)
				if mode == "limited" || mode == "decoded-limit" {
					if err == nil || response != nil {
						if response != nil {
							response.Body.Close()
						}
						t.Fatal("oversized response accepted", err)
					}
					var protocolError *ownh3.Error
					if !errors.As(err, &protocolError) || protocolError.ErrorCode != ownh3.ErrCodeExcessiveLoad {
						t.Fatalf("expected local header limit error, got %T: %v", err, err)
					}
					if mode == "decoded-limit" && protocolError.ErrorMessage == "encoded HEADERS exceeds local limit" {
						t.Fatal("fixture failed before decoded size enforcement")
					}

				} else {
					if err != nil {
						t.Fatal(err)
					}
					body, err := io.ReadAll(response.Body)
					response.Body.Close()
					if err != nil || string(body) != "configured transport" || len(response.Header.Get("X-Large")) != headerSize {
						t.Fatal("response limit/configuration failed", err)
					}
				}
				select {
				case got := <-observed:
					if got != wantDatagrams {
						t.Fatalf("peer HTTP DATAGRAM setting=%t, want %t", got, wantDatagrams)
					}
				case <-ctx.Done():
					t.Fatal("request did not reach peer", ctx.Err())
				}
				select {
				case err := <-written:
					if err != nil {
						t.Fatal("peer failed to write headers", err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			})
		}
	}
}
