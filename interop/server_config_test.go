package interop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestServerConfigSelectionIndependentPeer(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, mode := range []string{"selected", "defaults", "rejected", "invalid"} {
			t.Run(fmt.Sprintf("%x/%s", version, mode), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				calls := make(chan quic.ClientInfo, 8)
				listener, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{
					Versions: []quic.Version{version}, EnableDatagrams: true, MaxIncomingStreams: -1,
					GetConfigForClient: func(info *quic.ClientInfo) (*quic.Config, error) {
						select {
						case calls <- *info:
						default:
						}
						switch mode {
						case "defaults":
							return nil, nil
						case "rejected":
							return nil, errors.New("private policy detail")
						case "invalid":
							return &quic.Config{InitialStreamReceiveWindow: 17 << 20}, nil
						default:
							return &quic.Config{EnableDatagrams: true, MaxIncomingStreams: 1,
								GetConfigForClient: func(*quic.ClientInfo) (*quic.Config, error) { return nil, errors.New("nested selector invoked") },
							}, nil
						}
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				client, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, EnableDatagrams: true})
				if client != nil {
					defer client.CloseWithError(0, "")
				}
				if mode == "rejected" || mode == "invalid" {
					var failure *upstream.TransportError
					if !errors.As(err, &failure) || !failure.Remote || failure.ErrorCode != upstream.ConnectionRefused || failure.ErrorMessage != "" {
						t.Fatal("peer refusal", err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					server, err := listener.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					wantDatagrams := mode == "selected"
					if server.DatagramsEnabled() != wantDatagrams || client.ConnectionState().SupportsDatagrams.Remote != wantDatagrams {
						t.Fatal("selected datagram settings did not reach wire and accepted handle")
					}
					if wantDatagrams {
						payload := []byte("selected datagram policy")
						if err := client.SendDatagram(payload); err != nil {
							t.Fatal(err)
						}
						got, err := server.ReceiveDatagram(ctx)
						if err != nil || !bytes.Equal(got, payload) {
							t.Fatal("selected DATAGRAM delivery", got, err)
						}
					} else {
						if _, err := server.ReceiveDatagram(ctx); !errors.Is(err, quic.ErrDatagramsDisabled) {
							t.Fatal("default receive policy", err)
						}
					}
					if _, err := client.OpenStream(); err != nil {
						t.Fatal("selected initial stream limit", err)
					}
					_, err = client.OpenStream()
					if (err != nil) != (mode == "selected") {
						t.Fatal("selected/default stream limit", err)
					}
					info := <-calls
					if !info.AddrVerified || info.RemoteAddr.(*net.UDPAddr).Port != client.LocalAddr().(*net.UDPAddr).Port || !info.RemoteAddr.(*net.UDPAddr).IP.IsLoopback() {
						t.Fatal("client information", info, client.LocalAddr())
					}
				}
				if mode == "rejected" || mode == "invalid" {
					select {
					case info := <-calls:
						if !info.AddrVerified {
							t.Fatal("unverified selector")
						}
					default:
						t.Fatal("selector not invoked")
					}
				}
				if mode == "selected" || mode == "defaults" {
					select {
					case <-calls:
						t.Fatal("admitted connection selected more than once")
					default:
					}
				}
			})
		}
	}
}
