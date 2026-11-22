package interop

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestHTTP3TransportCompression(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(version)), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			serverTLS.NextProtos = []string{"h3"}
			var compressed bytes.Buffer
			writer := gzip.NewWriter(&compressed)
			writer.Write([]byte("decoded response"))
			writer.Close()
			seen := make(chan string, 8)
			payloadReady := make(chan chan struct{}, 8)
			server := &peerh3.Server{TLSConfig: serverTLS, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen <- r.Header.Get("Accept-Encoding")
				w.Header().Set("Content-Encoding", "gzip")
				w.Header().Set("Content-Length", strconv.Itoa(compressed.Len()))
				w.Header().Set("Trailer", "X-Result")
				w.(http.Flusher).Flush()
				gate := make(chan struct{})
				payloadReady <- gate
				select {
				case <-gate:
				case <-r.Context().Done():
					return
				}
				w.Write(compressed.Bytes())
				w.Header().Set("X-Result", "complete")
			})}
			socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			served := make(chan struct{})
			go func() { defer close(served); server.Serve(socket) }()
			defer func() {
				server.Close()
				socket.Close()
				select {
				case <-served:
				case <-time.After(time.Second):
					t.Error("server did not join")
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for _, tc := range []struct {
				name, method, encoding, rangeValue, wantEncoding string
				disabled, decoded                                bool
			}{
				{name: "automatic", method: "GET", wantEncoding: "gzip", decoded: true},
				{name: "explicit", method: "GET", encoding: "gzip", wantEncoding: "gzip"},
				{name: "disabled", method: "GET", disabled: true},
				{name: "range", method: "GET", rangeValue: "bytes=0-9"},
				{name: "head", method: "HEAD"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					transport := &ownh3.Transport{DisableCompression: tc.disabled, TLSClientConfig: clientTLS, QUICConfig: &quic.Config{Versions: []quic.Version{version}}}
					defer transport.Close()
					request, _ := http.NewRequestWithContext(ctx, tc.method, "https://"+socket.LocalAddr().String(), nil)
					if tc.encoding != "" {
						request.Header.Set("Accept-Encoding", tc.encoding)
					}
					if tc.rangeValue != "" {
						request.Header.Set("Range", tc.rangeValue)
					}
					response, err := transport.RoundTrip(request)
					if err != nil {
						t.Fatal(err)
					}
					defer response.Body.Close()
					assertHTTP3BodySettings(t, ctx, response.Body, false, nil)
					close(<-payloadReady) // RoundTrip must return before compressed payload is available.
					payload, err := io.ReadAll(response.Body)
					if err != nil {
						t.Fatal(err)
					}
					if err := response.Body.Close(); err != nil {
						t.Fatal(err)
					}
					if got := <-seen; got != tc.wantEncoding {
						t.Fatal("negotiation", got)
					}
					if response.Request != request || request.Header.Get("Accept-Encoding") != tc.encoding {
						t.Fatal("request mutated")
					}
					if response.Uncompressed != tc.decoded {
						t.Fatal("Uncompressed", response.Uncompressed)
					}
					if tc.decoded {
						if string(payload) != "decoded response" || response.ContentLength != -1 || response.Header.Get("Content-Encoding") != "" || response.Header.Get("Content-Length") != "" {
							t.Fatal(string(payload), response)
						}
					} else if tc.method != "HEAD" && !bytes.Equal(payload, compressed.Bytes()) {
						t.Fatal("explicit encoding not preserved")
					}
					if tc.method != "HEAD" && response.Trailer.Get("X-Result") != "complete" {
						t.Fatal("trailer lost", response.Trailer)
					}
				})
			}
		})
	}
}
