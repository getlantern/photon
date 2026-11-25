package interop

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

func TestHTTP3SettingsSnapshotUpstream(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, datagrams := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/datagrams=%v", version, datagrams), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				ct.NextProtos = []string{"h3"}
				st.NextProtos = []string{"h3"}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				extra := map[uint64]uint64{1: 0, 7: 0, 0x21: 99, 0xface: 123456}
				server := &peerh3.Server{TLSConfig: st, EnableDatagrams: datagrams, AdditionalSettings: extra, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}, Handler: http.NotFoundHandler()}
				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				served := make(chan error, 1)
				go func() { served <- server.Serve(socket) }()
				defer func() { server.Close(); socket.Close(); <-served }()
				client, err := ownh3.Dial(ctx, "https://"+socket.LocalAddr().String(), ct, &quic.Config{Versions: []quic.Version{version}}, &ownh3.ClientConfig{EnableDatagrams: datagrams})
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				select {
				case <-client.ReceivedSettings():
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				got := client.Settings()
				if got == nil || got.EnableDatagrams != datagrams || !got.EnableExtendedConnect || !maps.Equal(got.Other, extra) {
					t.Fatalf("settings: %+v", got)
				}

				peer, err := dialUpstreamAddr(t, ctx, socket.LocalAddr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, EnableDatagrams: datagrams})
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "done")
				transport := &peerh3.Transport{Dial: loopbackDial(t), EnableDatagrams: datagrams}
				defer transport.Close()
				reference := transport.NewClientConn(peer)
				select {
				case <-reference.ReceivedSettings():
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				want := reference.Settings()
				if got.EnableDatagrams != want.EnableDatagrams || got.EnableExtendedConnect != want.EnableExtendedConnect || !maps.Equal(got.Other, want.Other) {
					t.Fatalf("own=%+v reference=%+v", got, want)
				}
				got.Other[0x21] = 0
				got.EnableDatagrams = !datagrams
				if fresh := client.Settings(); fresh.EnableDatagrams != datagrams || !maps.Equal(fresh.Other, extra) {
					t.Fatal("caller mutated snapshot storage")
				}
				request, _ := http.NewRequestWithContext(ctx, "GET", "https://"+socket.LocalAddr().String(), nil)
				response, err := client.RoundTrip(request)
				if err != nil {
					t.Fatal(err)
				}
				bodySettings := assertHTTP3BodySettings(t, ctx, response.Body, datagrams, extra)
				if bodySettings.ReceivedSettings() != client.ReceivedSettings() {
					t.Fatal("body lost connection binding")
				}
				if _, err := io.Copy(io.Discard, response.Body); err != nil {
					t.Fatal(err)
				}
				if err := response.Body.Close(); err != nil {
					t.Fatal(err)
				}
				if err := client.Close(); err != nil {
					t.Fatal(err)
				}
				assertHTTP3BodySettings(t, ctx, response.Body, datagrams, extra)
				if after := client.Settings(); after == nil || !maps.Equal(after.Other, extra) {
					t.Fatal("settings lost after close")
				}
			})
		}
	}
}

func assertHTTP3BodySettings(t *testing.T, ctx context.Context, body io.ReadCloser, datagrams bool, extra map[uint64]uint64) ownh3.Settingser {
	t.Helper()
	provider, ok := body.(ownh3.Settingser)
	if !ok {
		t.Fatalf("body %T does not expose settings", body)
	}
	select {
	case <-provider.ReceivedSettings():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	got := provider.Settings()
	if got == nil || got.EnableDatagrams != datagrams || !got.EnableExtendedConnect {
		t.Fatalf("body settings %+v", got)
	}
	for id, want := range extra {
		if value, ok := got.Other[id]; !ok || value != want {
			t.Fatalf("setting %x=%d present=%v want=%d", id, value, ok, want)
		}
	}
	return provider
}

func TestHTTP3ResponseWriterSettingsUpstream(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, datagrams := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/datagrams=%v", version, datagrams), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				ct.NextProtos = []string{"h3"}
				st.NextProtos = []string{"h3"}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				listener, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{version}, EnableDatagrams: datagrams})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				extra := map[uint64]uint64{0xface: 0x1234}
				checked := make(chan error, 1)
				var handlerPhase atomic.Uint32 // 0: not entered; 1: waiting for SETTINGS; 2: flushing; 3: writing; 4: returned
				server := &ownh3.Server{EnableDatagrams: datagrams, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					handlerPhase.Store(1)
					defer handlerPhase.Store(4)
					provider, ok := w.(ownh3.Settingser)
					if !ok {
						checked <- fmt.Errorf("writer %T lacks settings", w)
						return
					}
					select {
					case <-provider.ReceivedSettings():
					case <-r.Context().Done():
						checked <- r.Context().Err()
						return
					}
					got := provider.Settings()
					if got == nil || got.EnableDatagrams != datagrams || got.EnableExtendedConnect || !maps.Equal(got.Other, extra) {
						checked <- fmt.Errorf("settings %+v", got)
						return
					}
					got.Other[0xface] = 0
					if provider.Settings().Other[0xface] != 0x1234 {
						checked <- fmt.Errorf("mutable settings storage")
						return
					}
					handlerPhase.Store(2)
					if err := http.NewResponseController(w).Flush(); err != nil {
						checked <- err
						return
					}
					handlerPhase.Store(3)
					_, err := io.WriteString(w, "settings visible")
					checked <- err
				})}
				served := make(chan error, 1)
				go func() { served <- server.Serve(ctx, listener) }()
				defer func() { server.Close(); listener.Close(); <-served }()
				transport := &peerh3.Transport{Dial: loopbackDial(t), TLSClientConfig: ct, EnableDatagrams: datagrams, AdditionalSettings: extra, QUICConfig: &upstream.Config{EnableDatagrams: datagrams, Versions: []upstream.Version{upstream.Version(version)}}}
				defer transport.Close()
				req, _ := http.NewRequestWithContext(ctx, "GET", "https://"+listener.Addr().String(), nil)
				response, err := transport.RoundTrip(req)
				if err != nil {
					select {
					case handlerErr := <-checked:
						t.Fatalf("RoundTrip: %v; handler phase=%d, result=%v", err, handlerPhase.Load(), handlerErr)
					default:
						t.Fatalf("RoundTrip: %v; handler phase=%d, no handler result", err, handlerPhase.Load())
					}
				}
				defer response.Body.Close()
				payload, err := io.ReadAll(response.Body)
				if err != nil || string(payload) != "settings visible" {
					t.Fatalf("response %q: %v", payload, err)
				}
				select {
				case err := <-checked:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			})
		}
	}
}
