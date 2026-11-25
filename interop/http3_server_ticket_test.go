package interop

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

// Keep one endpoint and address token so Retry cannot explain rejection. The
// early probe is a QUIC datagram, consumed before starting the resumed HTTP
// session: this isolates ticket acceptance from peer HTTP recovery behavior.
func TestHTTP3ServerTicketSettingsUpstream(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, policy := range []string{"same", "increase", "reduce", "missing", "remove-connect", "remove-datagrams", "extra-same", "extra-change", "extra-add", "extra-remove", "extra-new"} {
			t.Run(fmt.Sprintf("%x/%s", version, policy), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				ct, source := tlsConfigs(t)
				ct.NextProtos, source.NextProtos = []string{"h3"}, []string{"h3"}
				cache := &observedSessionCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), stored: make(chan bool, 8)}
				ct.ClientSessionCache = cache
				var handled atomic.Int32
				handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handled.Add(1); w.Write([]byte("settings ticket HTTP")) })
				seedServer := &ownh3.Server{Handler: handler, MaxHeaderBytes: 2048}
				currentServer := &ownh3.Server{Handler: handler, MaxHeaderBytes: 2048}
				if policy == "increase" {
					currentServer.MaxHeaderBytes = 4096
				}
				if policy == "reduce" {
					currentServer.MaxHeaderBytes = 1024
				}
				if policy == "remove-datagrams" {
					seedServer.EnableDatagrams = true
				}
				if policy == "remove-connect" {
					seedServer.ExtendedConnectHandlers = map[string]http.Handler{"test": handler}
				}
				switch policy {
				case "extra-same", "extra-change", "extra-add", "extra-remove":
					seedServer.AdditionalSettings = map[uint64]uint64{0xface: 7, 0x21: 9}
					currentServer.AdditionalSettings = map[uint64]uint64{0x21: 9, 0xface: 7}
					switch policy {
					case "extra-change":
						currentServer.AdditionalSettings[0xface] = 8
					case "extra-add":
						currentServer.AdditionalSettings[0x22] = 0
					case "extra-remove":
						delete(currentServer.AdditionalSettings, 0xface)
					}
				case "extra-new":
					currentServer.AdditionalSettings = map[uint64]uint64{0xface: 7}
				}
				seedTLS, err := seedServer.ConfigureTLSConfig(source)
				if err != nil {
					t.Fatal(err)
				}
				if policy == "missing" {
					seedTLS = source
				}
				currentTLS, err := currentServer.ConfigureTLSConfig(source)
				if err != nil {
					t.Fatal(err)
				}
				router := source.Clone()
				var hellos atomic.Int32
				router.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
					if hellos.Add(1) == 1 {
						return seedTLS, nil
					}
					return currentTLS, nil
				}
				listener, err := quic.ListenAddrEarly("127.0.0.1:0", router, &quic.Config{Versions: []quic.Version{version}, Allow0RTT: true, EnableDatagrams: true})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				tokens := &observedTokenStore{TokenStore: upstream.NewLRUTokenStore(4, 4), stored: make(chan struct{}, 4)}
				config := &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, EnableDatagrams: true, TokenStore: tokens}
				seed, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, config)
				if err != nil {
					t.Fatal(err)
				}
				defer seed.CloseWithError(0, "")
				seedPeer, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				seedDone := make(chan error, 1)
				go func() { seedDone <- seedServer.ServeQUICConn(ctx, seedPeer) }()
				defer func() { seedServer.Close(); <-seedDone }()
				h3Transport := &peerh3.Transport{Dial: loopbackDial(t)}
				defer h3Transport.Close()
				origin := "https://" + listener.Addr().String()
				request, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/seed", nil)
				response, err := h3Transport.NewClientConn(seed).RoundTrip(request)
				if err != nil {
					t.Fatal(err)
				}
				_, err = io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				waitEarlyTicket(t, ctx, cache)
				select {
				case <-tokens.stored:
				case <-ctx.Done():
					t.Fatal("no address token", ctx.Err())
				}
				seed.CloseWithError(0, "seed complete")

				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				gate := &earlyPacketGate{PacketConn: socket, seen: make(chan struct{}), done: make(chan struct{})}
				released := make(chan struct{})
				var once sync.Once
				release := func() { once.Do(func() { close(released) }) }
				defer release()
				flight := &heldServerFlight{earlyPacketGate: gate, release: released}
				transport := &upstream.Transport{Conn: flight}
				defer func() { gate.Close(); transport.Close() }()
				stop := context.AfterFunc(ctx, func() { gate.Close() })
				defer stop()
				client, err := transport.DialEarly(ctx, listener.Addr(), ct, config)
				if err != nil {
					t.Fatal(err)
				}
				defer client.CloseWithError(0, "")
				if err := client.SendDatagram([]byte("early policy probe")); err != nil {
					t.Fatal(err)
				}
				select {
				case <-gate.seen:
				case <-ctx.Done():
					t.Fatal("no actual early probe", ctx.Err())
				}
				peer, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "")
				if flight.retries.Load() != 0 {
					t.Fatal("Retry contaminated policy test")
				}
				accepted := policy == "same" || policy == "increase" || policy == "extra-same"
				if accepted {
					checkDatagram(t, ctx, peer.ReceiveDatagram, []byte("early policy probe"))
				}
				select {
				case <-peer.HandshakeComplete():
					t.Fatal("premature TLS completion")
				default:
				}
				release()
				waitEarlyHandshake(t, ctx, client.HandshakeComplete())
				waitEarlyHandshake(t, ctx, peer.HandshakeComplete())
				if !peer.ConnectionState().TLS.DidResume || !client.ConnectionState().TLS.DidResume {
					t.Fatal("ordinary resumption lost")
				}
				if peer.ConnectionState().Used0RTT != accepted || client.ConnectionState().Used0RTT != accepted {
					t.Fatal("HTTP policy acceptance mismatch", peer.ConnectionState().Used0RTT, client.ConnectionState().Used0RTT)
				}
				if _, err := client.NextConnection(ctx); err != nil {
					t.Fatal(err)
				}
				// A fresh datagram verifies the rejected early probe is not delivered late.
				if err := client.SendDatagram([]byte("fresh probe")); err != nil {
					t.Fatal(err)
				}
				checkDatagram(t, ctx, peer.ReceiveDatagram, []byte("fresh probe"))
				currentDone := make(chan error, 1)
				go func() { currentDone <- currentServer.ServeQUICConn(ctx, peer) }()
				defer func() { currentServer.Close(); <-currentDone }()
				request, _ = http.NewRequestWithContext(ctx, http.MethodGet, origin+"/fresh", nil)
				httpClient := h3Transport.NewClientConn(client)
				response, err = httpClient.RoundTrip(request)
				if err != nil {
					t.Fatal("HTTP after policy decision", err)
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || string(body) != "settings ticket HTTP" {
					t.Fatal(string(body), err)
				}
				select {
				case <-httpClient.ReceivedSettings():
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				received := httpClient.Settings()
				for _, id := range []uint64{0xface, 0x21, 0x22} {
					want, wantPresent := currentServer.AdditionalSettings[id]
					got, gotPresent := received.Other[id]
					if got != want || gotPresent != wantPresent {
						t.Fatalf("live setting %x=%d/%v want %d/%v", id, got, gotPresent, want, wantPresent)
					}
				}
				before := handled.Load()
				marked, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/forwarded", nil)
				marked.Header["Early-Data"] = []string{"0", "invalid"}
				rejected, err := httpClient.RoundTrip(marked)
				if err != nil {
					t.Fatal("forwarded early response", err)
				}
				_, err = io.Copy(io.Discard, rejected.Body)
				rejected.Body.Close()
				if err != nil || rejected.StatusCode != http.StatusTooEarly || handled.Load() != before {
					t.Fatal("forwarded marker reached resource", rejected.StatusCode, handled.Load(), before, err)
				}

			})
		}
	}
}
