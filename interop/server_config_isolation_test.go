package interop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

// Keep all accepted connections alive while selecting different settings and
// refusing another source on the same UDP listener. Exercise earlier handles
// after later selections to catch listener-global policy leakage.
func TestServerConfigIsolationIndependentPeer(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ct, st := tlsConfigs(t)
			policies := make(map[int]int)
			peers := make([]*upstream.Transport, 4)
			for i := range peers {
				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer socket.Close()
				policies[socket.LocalAddr().(*net.UDPAddr).Port] = i
				peers[i] = &upstream.Transport{Conn: socket}
				defer peers[i].Close()
			}
			listener, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{
				Versions: []quic.Version{version}, MaxIncomingStreams: -1,
				GetConfigForClient: func(info *quic.ClientInfo) (*quic.Config, error) {
					index, ok := policies[info.RemoteAddr.(*net.UDPAddr).Port]
					if !ok || !info.AddrVerified {
						return nil, errors.New("unexpected source")
					}
					if index == 3 {
						return nil, errors.New("source denied")
					}
					return &quic.Config{EnableDatagrams: index != 1, MaxIncomingStreams: int64(index + 1)}, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			clients := make([]*upstream.Conn, 3)
			servers := make([]*quic.Conn, 3)
			for i, peer := range peers {
				client, err := peer.Dial(ctx, listener.Addr(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, EnableDatagrams: true})
				if i == 3 {
					if client != nil {
						client.CloseWithError(0, "")
						t.Fatal("denied source admitted")
					}
					var closeErr *upstream.TransportError
					if !errors.As(err, &closeErr) || closeErr.ErrorCode != upstream.ConnectionRefused {
						t.Fatal("refused source", err)
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				defer client.CloseWithError(0, "")
				clients[i] = client
				servers[i], err = listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
			}
			for i, client := range clients {
				server := servers[i]
				enabled := i != 1
				if server.DatagramsEnabled() != enabled || client.ConnectionState().SupportsDatagrams.Remote != enabled {
					t.Fatal("another selection changed datagram policy", i)
				}
				payload := []byte(fmt.Sprintf("connection %d retains its policy", i))
				if enabled {
					if err := client.SendDatagram(payload); err != nil {
						t.Fatal(err)
					}
					got, err := server.ReceiveDatagram(ctx)
					if err != nil || !bytes.Equal(got, payload) {
						t.Fatal("isolated datagram delivery", i, got, err)
					}
				} else if _, err := server.ReceiveDatagram(ctx); !errors.Is(err, quic.ErrDatagramsDisabled) {
					t.Fatal("disabled policy changed", err)
				}
				first, err := client.OpenStream()
				if err != nil {
					t.Fatal(err)
				}
				for j := 1; j < i+1; j++ {
					if _, err := client.OpenStream(); err != nil {
						t.Fatal("selected stream limit reduced", i, err)
					}
				}
				if _, err := client.OpenStream(); err == nil {
					t.Fatal("selected stream limit increased", i)
				}
				if _, err := first.Write(payload); err != nil {
					t.Fatal(err)
				}
				if err := first.Close(); err != nil {
					t.Fatal(err)
				}
				received, err := server.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				deadline, _ := ctx.Deadline()
				received.SetReadDeadline(deadline)
				got, err := io.ReadAll(received)
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatal("refusal disrupted existing stream", i, got, err)
				}
			}
		})
	}
}
