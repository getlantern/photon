package quic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"
)

func TestStreamCreditRelease(t *testing.T) {
	type handle interface {
		ReleaseStreamCredit()
		Release(context.Context) error
	}
	for _, version := range []Version{Version1, Version2} {
		for _, mode := range []string{"client", "server", "selected"} {
			for _, uni := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/%s/uni=%t", version, mode, uni), func(t *testing.T) {
					clientTLS, serverTLS := dialTLS(t)
					required := &Config{Versions: []Version{version}, MaxIncomingStreams: 1, MaxIncomingUniStreams: 1, RequireStreamCreditRelease: true}
					automatic := &Config{Versions: []Version{version}, MaxIncomingStreams: 1, MaxIncomingUniStreams: 1}
					clientConfig, serverConfig := required, automatic
					switch mode {
					case "server":
						clientConfig, serverConfig = automatic, required
					case "selected":
						clientConfig = automatic
						serverConfig = &Config{Versions: []Version{version}, GetConfigForClient: func(*ClientInfo) (*Config, error) { return required, nil }}
					}
					l, err := ListenAddr("127.0.0.1:0", serverTLS, serverConfig)
					if err != nil {
						t.Fatal(err)
					}
					defer l.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					client, err := DialAddr(ctx, l.Addr().String(), clientTLS, clientConfig)
					if err != nil {
						t.Fatal(err)
					}
					defer client.server.Close()
					server, err := l.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					opener, acceptor := server, client
					if mode != "client" {
						opener, acceptor = client, server
					}
					if !acceptor.ConnectionState().RequireStreamCreditRelease || opener.ConnectionState().RequireStreamCreditRelease {
						t.Fatal("connection state", acceptor.ConnectionState().RequireStreamCreditRelease, opener.ConnectionState().RequireStreamCreditRelease)
					}
					type opened struct {
						from   *Conn
						send   *SendStream
						stream *Stream
					}
					open := func(from *Conn, uni bool) opened {
						t.Helper()
						o := opened{from: from}
						var err error
						if uni {
							o.send, err = from.OpenUniStreamSync(ctx)
						} else {
							o.stream, err = from.OpenStreamSync(ctx)
							if err == nil {
								o.send = o.stream.SendStream
							}
						}
						if err != nil {
							t.Fatal("open", err)
						}
						return o
					}
					// Credit already in flight would arrive well within the wait.
					assertWithheld := func(from *Conn, uni bool) {
						t.Helper()
						blocked, stop := context.WithTimeout(ctx, 50*time.Millisecond)
						defer stop()
						var err error
						if uni {
							_, err = from.OpenUniStreamSync(blocked)
						} else {
							_, err = from.OpenStreamSync(blocked)
						}
						if !errors.Is(err, context.DeadlineExceeded) {
							t.Fatalf("stream credit was returned: %v", err)
						}
						if uni {
							_, err = from.OpenUniStream()
						} else {
							_, err = from.OpenStream()
						}
						if !errors.Is(err, &StreamLimitReachedError{}) {
							t.Fatalf("limit error: %v", err)
						}
					}
					// finish completes both directions and waits for the accepting
					// side to reclaim the stream, the point where credit can return.
					finish := func(o opened, to *Conn, uni, consentFirst bool) handle {
						t.Helper()
						o.send.SetWriteDeadline(time.Now().Add(5 * time.Second))
						if _, err := o.send.Write([]byte("request")); err != nil {
							t.Fatal(err)
						}
						if err := o.send.Close(); err != nil {
							t.Fatal(err)
						}
						var h handle
						var receive *ReceiveStream
						var accepted *Stream
						var err error
						if uni {
							receive, err = to.AcceptUniStream(ctx)
							h = receive
						} else {
							accepted, err = to.AcceptStream(ctx)
							if err == nil {
								receive, h = accepted.ReceiveStream, accepted
							}
						}
						if err != nil {
							t.Fatal("accept", err)
						}
						if consentFirst {
							h.ReleaseStreamCredit()
							h.ReleaseStreamCredit()
						}
						receive.SetReadDeadline(time.Now().Add(5 * time.Second))
						if b, err := io.ReadAll(receive); err != nil || string(b) != "request" {
							t.Fatalf("request %q: %v", b, err)
						}
						if !uni {
							if consentFirst {
								// Half-closed is not complete: the reply is still unsent.
								assertWithheld(o.from, uni)
							}
							if err := accepted.Close(); err != nil {
								t.Fatal(err)
							}
							o.stream.SetReadDeadline(time.Now().Add(5 * time.Second))
							if b, err := io.ReadAll(o.stream); err != nil || len(b) != 0 {
								t.Fatalf("reply %q: %v", b, err)
							}
						}
						if err := h.Release(ctx); err != nil {
							t.Fatal("release", err)
						}
						return h
					}

					first := open(opener, uni)
					h := finish(first, acceptor, uni, false)
					assertWithheld(opener, uni)
					if !uni {
						// The opener holds no credit for its own stream to release.
						first.stream.ReleaseStreamCredit()
						assertWithheld(opener, uni)
					}
					for range 3 {
						h.ReleaseStreamCredit()
					}
					second := open(opener, uni)
					assertWithheld(opener, uni) // repeated calls returned one credit

					finish(second, acceptor, uni, true)
					third := open(opener, uni)
					assertWithheld(opener, uni)

					h = finish(third, acceptor, uni, false)
					assertWithheld(opener, uni)
					h.ReleaseStreamCredit()
					open(opener, uni)

					// The other direction did not opt in: cleanup alone returns
					// credit, and ReleaseStreamCredit adds none.
					reverse := open(acceptor, uni)
					h = finish(reverse, opener, uni, false)
					h.ReleaseStreamCredit()
					open(acceptor, uni)
					h.ReleaseStreamCredit()
					assertWithheld(acceptor, uni)
				})
			}
		}
	}
}

func TestConfigCloneRequireStreamCreditRelease(t *testing.T) {
	original := &Config{RequireStreamCreditRelease: true}
	cloned := original.Clone()
	if !cloned.RequireStreamCreditRelease {
		t.Fatal("clone dropped RequireStreamCreditRelease")
	}
	cloned.RequireStreamCreditRelease = false
	if !original.RequireStreamCreditRelease || (&Config{}).Clone().RequireStreamCreditRelease {
		t.Fatal("clone shares or invents RequireStreamCreditRelease")
	}
}
