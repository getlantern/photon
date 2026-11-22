package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

type transportContextKey struct{}

func TestTransportConnContext(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		for _, retry := range []bool{false, true} {
			for _, closeMode := range []string{"local", "peer", "transport"} {
				t.Run(fmt.Sprintf("%v/%v/%s", version, retry, closeMode), func(t *testing.T) {
					ct, st := dialTLS(t)
					socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
					if err != nil {
						t.Fatal(err)
					}
					defer socket.Close()
					inputs := make(chan context.Context, 4)
					infos := make(chan ClientInfo, 4)
					tlsValues := make(chan any, 4)
					st.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
						tlsValues <- hello.Context().Value(transportContextKey{})
						return nil, nil
					}
					tr := &Transport{Conn: socket, VerifySourceAddress: func(net.Addr) bool { return retry }, ConnContext: func(ctx context.Context, info *ClientInfo) (context.Context, error) {
						inputs <- ctx
						infos <- *info
						return context.WithValue(ctx, transportContextKey{}, "incoming"), nil
					}}
					defer tr.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					cfg := &Config{Versions: []Version{version}}
					for generation := 0; generation < 2; generation++ {
						listener, err := tr.Listen(st, cfg)
						if err != nil {
							t.Fatal(err)
						}
						client, err := DialAddr(ctx, listener.Addr().String(), ct, cfg)
						if err != nil {
							t.Fatal(err)
						}
						defer client.server.Close()
						server, err := listener.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
						var input context.Context
						select {
						case input = <-inputs:
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
						info := <-infos
						if info.AddrVerified != retry || info.RemoteAddr.(*net.UDPAddr).Port != client.LocalAddr().(*net.UDPAddr).Port {
							t.Fatal(info)
						}
						if got := <-tlsValues; got != "incoming" {
							t.Fatal("TLS context", got)
						}
						if server.Context().Value(transportContextKey{}) != "incoming" {
							t.Fatal("connection value lost")
						}
						stream, err := server.OpenUniStreamSync(ctx)
						if err != nil {
							t.Fatal(err)
						}
						if stream.Context().Value(transportContextKey{}) != "incoming" {
							t.Fatal("stream value lost")
						}
						if err := listener.Close(); err != nil {
							t.Fatal(err)
						}
						if input.Err() != nil || server.Context().Err() != nil {
							t.Fatal("listener close canceled accepted connection")
						}
						mode := closeMode
						if generation == 0 {
							mode = "local"
						}
						switch mode {
						case "local":
							err = server.CloseWithError(73, "done")
						case "peer":
							err = client.CloseWithError(73, "done")
						case "transport":
							err = tr.Close()
						}
						if err != nil {
							t.Fatal(err)
						}
						for _, lifetime := range []context.Context{input, server.Context(), stream.Context()} {
							select {
							case <-lifetime.Done():
							case <-ctx.Done():
								t.Fatal(ctx.Err())
							}
							if mode != "transport" {
								var app *ApplicationError
								if !errors.As(context.Cause(lifetime), &app) || app.ErrorCode != 73 {
									t.Fatal("close cause", context.Cause(lifetime))
								}
							}
						}
						select {
						case <-inputs:
							t.Fatal("duplicate callback")
						default:
						}
					}
				})
			}
		}
	}
}

func TestTransportConnContextFailure(t *testing.T) {
	for _, mode := range []string{"reject", "nil", "tls", "generator", "config"} {
		t.Run(mode, func(t *testing.T) {
			ct, st := dialTLS(t)
			socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			inputs := make(chan context.Context, 8)
			secret := errors.New("private admission detail")
			tr := &Transport{Conn: socket, ConnContext: func(ctx context.Context, _ *ClientInfo) (context.Context, error) {
				inputs <- ctx
				if mode == "reject" {
					return nil, secret
				}
				if mode == "nil" {
					return nil, nil
				}
				return context.WithValue(ctx, transportContextKey{}, true), nil
			}}
			if mode == "tls" {
				st.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) { return nil, secret }
			}
			if mode == "generator" {
				tr.ConnectionIDGenerator = &contextFailGenerator{err: secret}
			}
			defer tr.Close()
			var cfg *Config
			if mode == "config" {
				cfg = &Config{GetConfigForClient: func(*ClientInfo) (*Config, error) { return nil, secret }}
			}
			listener, err := tr.Listen(st, cfg)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			client, err := DialAddr(ctx, listener.Addr().String(), ct, nil)
			if client != nil {
				client.server.Close()
				t.Fatal("unexpected admission")
			}
			if err == nil || strings.Contains(err.Error(), secret.Error()) {
				t.Fatal("unexpected peer error", err)
			}
			var input context.Context
			select {
			case input = <-inputs:
			default:
				t.Fatal("no callback")
			}
			select {
			case <-input.Done():
			case <-time.After(time.Second):
				t.Fatal("admission context leaked")
			}
			if (mode == "reject" || mode == "generator" || mode == "config") && !errors.Is(context.Cause(input), secret) {
				t.Fatal(context.Cause(input))
			}
		})
	}
}

type contextFailGenerator struct{ err error }

func (*contextFailGenerator) ConnectionIDLen() int { return 8 }
func (g *contextFailGenerator) GenerateConnectionID() (ConnectionID, error) {
	return ConnectionID{}, g.err
}

func TestTransportConnContextNotDialed(t *testing.T) {
	ct, st := dialTLS(t)
	listener, err := ListenAddr("127.0.0.1:0", st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	tr := &Transport{Conn: socket, ConnContext: func(context.Context, *ClientInfo) (context.Context, error) {
		return nil, errors.New("outgoing callback invoked")
	}}
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := tr.Dial(ctx, listener.Addr(), ct, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := listener.Accept(ctx); err != nil {
		t.Fatal(err)
	}
}
