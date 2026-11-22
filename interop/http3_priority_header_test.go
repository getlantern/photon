package interop

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

func TestHTTP3PriorityHeadersIndependent(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, datagrams := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/datagrams=%v", version, datagrams), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				ct.NextProtos = []string{"h3"}
				st.NextProtos = []string{"h3"}
				listener, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{version}, EnableDatagrams: datagrams})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				server := &ownh3.Server{EnableDatagrams: datagrams, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("priority response")) })}
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
				client := &peerh3.Transport{TLSClientConfig: ct, EnableDatagrams: datagrams, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, EnableDatagrams: datagrams}}
				defer client.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				for _, values := range [][]string{nil, {"u=0, i"}, {"u=7", "i=?0"}, {"u=0", "invalid="}, {"u=2, x=(1 2);y"}} {
					request, err := http.NewRequestWithContext(ctx, "GET", "https://"+listener.Addr().String()+"/", nil)
					if err != nil {
						t.Fatal(err)
					}
					request.Header["Priority"] = values
					response, err := client.RoundTrip(request)
					if err != nil {
						t.Fatal(err)
					}
					body, err := io.ReadAll(response.Body)
					response.Body.Close()
					if err != nil || string(body) != "priority response" {
						t.Fatal(string(body), err)
					}
				}
			})
		}
	}
}
