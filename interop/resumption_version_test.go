package interop

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestServerTicketVersionUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, selected := range []bool{false, true} {
			for _, mode := range []string{"valid", "missing", "duplicate", "wrong-version"} {
				t.Run(fmt.Sprintf("%x/selected=%t/%s", v, selected, mode), func(t *testing.T) {
					clientTLS, serverTLS := tlsConfigs(t)
					cache := &sharedTicketCache{stored: make(chan struct{}, 1)}
					clientTLS.ClientSessionCache = cache
					var unwraps atomic.Int32
					serverTLS.WrapSession = func(cs tls.ConnectionState, ss *tls.SessionState) ([]byte, error) {
						// Rewrite the authenticated fixture's private metadata while retaining
						// the TLS ticket and PSK so an upstream client really offers resumption.
						switch mode {
						case "missing":
							ss.Extra = nil
						case "duplicate":
							ss.Extra = append(ss.Extra, bytes.Clone(ss.Extra[0]))
						case "wrong-version":
							const prefix = "getlantern/photon/early-limits\x00"
							if len(ss.Extra) != 1 || !bytes.HasPrefix(ss.Extra[0], []byte(prefix)) {
								return nil, fmt.Errorf("missing fixture record")
							}
							other := quic.Version1
							if other == v {
								other = quic.Version2
							}
							binary.BigEndian.PutUint32(ss.Extra[0][len(prefix):], uint32(other))
						}
						return serverTLS.EncryptTicket(cs, ss)
					}
					serverTLS.UnwrapSession = func(ticket []byte, cs tls.ConnectionState) (*tls.SessionState, error) {
						unwraps.Add(1)
						return serverTLS.DecryptTicket(ticket, cs)
					}
					base := serverTLS
					if selected {
						base = serverTLS.Clone()
						base.WrapSession = nil
						base.UnwrapSession = nil
						base.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) { return serverTLS, nil }
					}
					listener, err := quic.ListenAddr("127.0.0.1:0", base, &quic.Config{Versions: []quic.Version{v}, EnableDatagrams: true})
					if err != nil {
						t.Fatal(err)
					}
					defer listener.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					for round := 0; round < 2; round++ {
						c, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, EnableDatagrams: true})
						if err != nil {
							t.Fatal(round, err)
						}
						defer c.CloseWithError(0, "")
						peer, err := listener.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
						defer peer.CloseWithError(0, "")
						want := round == 1 && mode == "valid"
						if c.ConnectionState().TLS.DidResume != want || peer.ConnectionState().TLS.DidResume != want {
							t.Fatal("incorrect ticket decision", round, want)
						}
						if err := c.SendDatagram([]byte("version-bound")); err != nil {
							t.Fatal(err)
						}
						payload, err := peer.ReceiveDatagram(ctx)
						if err != nil || string(payload) != "version-bound" {
							t.Fatal("payload", string(payload), err)
						}
						if round == 0 {
							select {
							case <-cache.stored:
							case <-ctx.Done():
								t.Fatal(ctx.Err())
							}
							cache.freeze()
						}
						c.CloseWithError(0, "")
						peer.CloseWithError(0, "")
					}
					if unwraps.Load() != 1 {
						t.Fatal("did not exercise offered ticket", unwraps.Load())
					}
				})
			}
		}
	}
}
