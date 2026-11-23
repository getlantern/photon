package interop

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestServerResumptionTicketPolicy(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, custom := range []bool{false, true} {
			for _, selected := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/custom=%t/selected=%t", v, custom, selected), func(t *testing.T) {
					clientTLS, serverTLS := tlsConfigs(t)
					cache := &sharedTicketCache{stored: make(chan struct{}, 1)}
					clientTLS.ClientSessionCache = cache
					var oldKey, newKey [32]byte
					oldKey[0] = 1
					newKey[0] = 2 // Deterministic test-only keys.
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					for stage := 0; stage < 4; stage++ {
						deadline, _ := ctx.Deadline()
						t.Logf("ticket policy stage %d, remaining %s", stage, time.Until(deadline))
						func() {
							cfg := serverTLS.Clone()
							keys := [][32]byte{oldKey}
							if stage == 1 {
								keys = [][32]byte{newKey, oldKey}
							}
							if stage == 2 {
								keys = [][32]byte{newKey}
							}
							cfg.SetSessionTicketKeys(keys)
							// Restore the valid old key in the disabled stage: rejection
							// must come from policy, not an undecryptable ticket.
							cfg.SessionTicketsDisabled = stage == 3
							var wraps, unwraps, restored atomic.Int32
							if custom {
								cfg.WrapSession = func(cs tls.ConnectionState, ss *tls.SessionState) ([]byte, error) {
									wraps.Add(1)
									ss.Extra = append(ss.Extra, []byte("caller ticket state"))
									return cfg.EncryptTicket(cs, ss)
								}
								cfg.UnwrapSession = func(identity []byte, cs tls.ConnectionState) (*tls.SessionState, error) {
									unwraps.Add(1)
									ss, err := cfg.DecryptTicket(identity, cs)
									if ss != nil {
										for _, extra := range ss.Extra {
											if string(extra) == "caller ticket state" {
												restored.Add(1)
											}
										}
									}
									return ss, err
								}
							}
							listenTLS := cfg
							var selections, baseWraps, baseUnwraps atomic.Int32
							if selected {
								listenTLS = serverTLS.Clone()
								// Deliberately conflicting base policy must not override the selected
								// configuration, including selected disabled-ticket policy.
								listenTLS.SessionTicketsDisabled = !cfg.SessionTicketsDisabled
								var unrelated [32]byte
								unrelated[0] = 99
								listenTLS.SetSessionTicketKeys([][32]byte{unrelated})
								listenTLS.WrapSession = func(cs tls.ConnectionState, ss *tls.SessionState) ([]byte, error) {
									baseWraps.Add(1)
									return listenTLS.EncryptTicket(cs, ss)
								}
								listenTLS.UnwrapSession = func(identity []byte, cs tls.ConnectionState) (*tls.SessionState, error) {
									baseUnwraps.Add(1)
									return listenTLS.DecryptTicket(identity, cs)
								}
								listenTLS.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
									selections.Add(1)
									return cfg, nil
								}
							}
							l, err := quic.ListenAddr("127.0.0.1:0", listenTLS, &quic.Config{Versions: []quic.Version{v}})
							if err != nil {
								t.Fatal(err)
							}
							defer l.Close()
							c, err := dialUpstreamAddr(t, ctx, l.Addr().String(), clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}})
							if err != nil {
								t.Fatalf("stage %d dial: %v", stage, err)
							}
							defer c.CloseWithError(0, "")
							peer, err := l.Accept(ctx)
							if err != nil {
								t.Fatal(err)
							}
							want := stage == 1
							if c.ConnectionState().TLS.DidResume != want || peer.ConnectionState().TLS.DidResume != want {
								t.Fatalf("stage %d resume %t/%t", stage, c.ConnectionState().TLS.DidResume, peer.ConnectionState().TLS.DidResume)
							}
							tx, err := c.OpenUniStreamSync(ctx)
							if err != nil {
								t.Fatal(err)
							}
							tx.SetWriteDeadline(time.Now().Add(time.Second))
							if _, err := tx.Write([]byte("ticket policy")); err != nil {
								t.Fatal(err)
							}
							if err := tx.Close(); err != nil {
								t.Fatal(err)
							}
							rx, err := peer.AcceptUniStream(ctx)
							if err != nil {
								t.Fatal(err)
							}
							rx.SetReadDeadline(time.Now().Add(time.Second))
							if b, err := io.ReadAll(rx); err != nil || string(b) != "ticket policy" {
								t.Fatalf("%q: %v", b, err)
							}
							if stage == 0 {
								select {
								case <-cache.stored:
								case <-ctx.Done():
									t.Fatal("no issued ticket", ctx.Err())
								}
								if cache.freeze() == nil {
									t.Fatal("missing frozen ticket")
								}
							}
							if selected && (selections.Load() != 1 || baseWraps.Load() != 0 || baseUnwraps.Load() != 0) {
								t.Fatal("selected configuration did not own ticket policy", selections.Load(), baseWraps.Load(), baseUnwraps.Load())
							}
							if custom {
								wantWrap := int32(1)
								if stage == 3 {
									wantWrap = 0
								}
								if wraps.Load() != wantWrap {
									t.Fatalf("stage %d wrap count %d", stage, wraps.Load())
								}
								wantUnwrap := int32(0)
								if stage == 1 || stage == 2 {
									wantUnwrap = 1
								}
								if unwraps.Load() != wantUnwrap {
									t.Fatalf("stage %d unwrap count %d", stage, unwraps.Load())
								}
								wantRestore := int32(0)
								if stage == 1 {
									wantRestore = 1
								}
								if restored.Load() != wantRestore {
									t.Fatal("caller state restoration", stage, restored.Load())
								}
							}
						}()
					}
				})
			}
		}
	}
}
