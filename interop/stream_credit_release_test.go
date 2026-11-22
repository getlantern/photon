package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

// creditStream and creditConn let one script drive either implementation.
type creditStream struct {
	send          io.WriteCloser // nil on an accepted unidirectional stream
	receive       io.Reader      // nil on an opened unidirectional stream
	releaseCredit func()
}

type creditConn struct {
	open   func(ctx context.Context, uni bool) (creditStream, error)
	accept func(ctx context.Context, uni bool) (creditStream, error)
}

func independentCreditConn(c *quic.Conn) creditConn {
	return creditConn{
		open: func(ctx context.Context, uni bool) (creditStream, error) {
			if uni {
				s, err := c.OpenUniStreamSync(ctx)
				return creditStream{send: s}, err
			}
			s, err := c.OpenStreamSync(ctx)
			if err != nil {
				return creditStream{}, err
			}
			return creditStream{send: s, receive: s, releaseCredit: s.ReleaseStreamCredit}, nil
		},
		accept: func(ctx context.Context, uni bool) (creditStream, error) {
			if uni {
				s, err := c.AcceptUniStream(ctx)
				if err != nil {
					return creditStream{}, err
				}
				return creditStream{receive: s, releaseCredit: s.ReleaseStreamCredit}, nil
			}
			s, err := c.AcceptStream(ctx)
			if err != nil {
				return creditStream{}, err
			}
			return creditStream{send: s, receive: s, releaseCredit: s.ReleaseStreamCredit}, nil
		},
	}
}

func upstreamCreditConn(c *upstream.Conn) creditConn {
	return creditConn{
		open: func(ctx context.Context, uni bool) (creditStream, error) {
			if uni {
				s, err := c.OpenUniStreamSync(ctx)
				return creditStream{send: s}, err
			}
			s, err := c.OpenStreamSync(ctx)
			if err != nil {
				return creditStream{}, err
			}
			return creditStream{send: s, receive: s, releaseCredit: s.ReleaseStreamCredit}, nil
		},
		accept: func(ctx context.Context, uni bool) (creditStream, error) {
			if uni {
				s, err := c.AcceptUniStream(ctx)
				if err != nil {
					return creditStream{}, err
				}
				return creditStream{receive: s, releaseCredit: s.ReleaseStreamCredit}, nil
			}
			s, err := c.AcceptStream(ctx)
			if err != nil {
				return creditStream{}, err
			}
			return creditStream{send: s, receive: s, releaseCredit: s.ReleaseStreamCredit}, nil
		},
	}
}

// Each implementation withholds credit from the other, in both TLS roles, and
// the same script must observe the same grants.
func TestStreamCreditReleaseUpstream(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, independentAccepts := range []bool{true, false} {
			for _, independentServer := range []bool{true, false} {
				for _, uni := range []bool{false, true} {
					t.Run(fmt.Sprintf("%x/independentAccepts=%t/independentServer=%t/uni=%t", version, independentAccepts, independentServer, uni), func(t *testing.T) {
						clientTLS, serverTLS := tlsConfigs(t)
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						own := &quic.Config{Versions: []quic.Version{version}, MaxIncomingStreams: 1, MaxIncomingUniStreams: 1, RequireStreamCreditRelease: independentAccepts}
						peer := &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, MaxIncomingStreams: 1, MaxIncomingUniStreams: 1, RequireStreamCreditRelease: !independentAccepts}
						var ownConn *quic.Conn
						var peerConn *upstream.Conn
						if independentServer {
							l, err := quic.ListenAddr("127.0.0.1:0", serverTLS, own)
							if err != nil {
								t.Fatal(err)
							}
							defer l.Close()
							peerConn, err = dialUpstreamAddr(t, ctx, l.Addr().String(), clientTLS, peer)
							if err != nil {
								t.Fatal(err)
							}
							defer peerConn.CloseWithError(0, "")
							if ownConn, err = l.Accept(ctx); err != nil {
								t.Fatal(err)
							}
						} else {
							l, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, peer)
							if err != nil {
								t.Fatal(err)
							}
							defer l.Close()
							if ownConn, err = quic.DialAddr(ctx, l.Addr().String(), clientTLS, own); err != nil {
								t.Fatal(err)
							}
							defer ownConn.CloseWithError(0, "")
							if peerConn, err = l.Accept(ctx); err != nil {
								t.Fatal(err)
							}
							defer peerConn.CloseWithError(0, "")
						}
						if ownConn.ConnectionState().RequireStreamCreditRelease != independentAccepts || peerConn.ConnectionState().RequireStreamCreditRelease == independentAccepts {
							t.Fatal("connection state", ownConn.ConnectionState().RequireStreamCreditRelease, peerConn.ConnectionState().RequireStreamCreditRelease)
						}
						opener, acceptor := independentCreditConn(ownConn), upstreamCreditConn(peerConn)
						if independentAccepts {
							opener, acceptor = acceptor, opener
						}
						// Credit already in flight would arrive well within the wait.
						assertWithheld := func(from creditConn) {
							t.Helper()
							blocked, stop := context.WithTimeout(ctx, 100*time.Millisecond)
							defer stop()
							if _, err := from.open(blocked, uni); !errors.Is(err, context.DeadlineExceeded) {
								t.Fatalf("stream credit was returned: %v", err)
							}
						}
						open := func(from creditConn) creditStream {
							t.Helper()
							s, err := from.open(ctx, uni)
							if err != nil {
								t.Fatal("open", err)
							}
							return s
						}
						finish := func(o creditStream, to creditConn, consentFirst bool) creditStream {
							t.Helper()
							if _, err := o.send.Write([]byte("request")); err != nil {
								t.Fatal(err)
							}
							if err := o.send.Close(); err != nil {
								t.Fatal(err)
							}
							a, err := to.accept(ctx, uni)
							if err != nil {
								t.Fatal("accept", err)
							}
							if consentFirst {
								a.releaseCredit()
								a.releaseCredit()
							}
							if b, err := io.ReadAll(a.receive); err != nil || string(b) != "request" {
								t.Fatalf("request %q: %v", b, err)
							}
							if !uni {
								if err := a.send.Close(); err != nil {
									t.Fatal(err)
								}
								if b, err := io.ReadAll(o.receive); err != nil || len(b) != 0 {
									t.Fatalf("reply %q: %v", b, err)
								}
							}
							return a
						}

						first := open(opener)
						accepted := finish(first, acceptor, false)
						assertWithheld(opener)
						if !uni {
							first.releaseCredit() // the opener holds no credit for its own stream
							assertWithheld(opener)
						}
						for range 3 {
							accepted.releaseCredit()
						}
						second := open(opener)
						assertWithheld(opener) // repeated calls returned one credit

						finish(second, acceptor, true)
						third := open(opener)
						assertWithheld(opener)

						accepted = finish(third, acceptor, false)
						assertWithheld(opener)
						accepted.releaseCredit()
						open(opener)

						// The other direction did not opt in, so completion alone
						// returns credit and ReleaseStreamCredit adds none.
						reverse := open(acceptor)
						accepted = finish(reverse, opener, false)
						accepted.releaseCredit()
						open(acceptor)
						accepted.releaseCredit()
						assertWithheld(acceptor)
					})
				}
			}
		}
	}
}
