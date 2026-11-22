package interop

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	independent "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestUpstreamClientNATRebinding(t *testing.T) {
	for _, version := range []upstream.Version{upstream.Version1, upstream.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(version)), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			listener, err := independent.ListenAddr("127.0.0.1:0", serverTLS, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			open := func() *net.UDPConn {
				t.Helper()
				s, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { s.Close() })
				return s
			}
			front, oldBack, newBack := open(), open(), open()
			var active atomic.Pointer[net.UDPConn]
			var clientAddress atomic.Pointer[net.UDPAddr]
			active.Store(oldBack)
			var workers sync.WaitGroup
			workers.Add(3)
			go func() {
				defer workers.Done()
				buffer := make([]byte, 65535)
				for {
					n, from, err := front.ReadFromUDP(buffer)
					if err != nil {
						return
					}
					clientAddress.Store(from)
					if _, err := active.Load().WriteToUDP(buffer[:n], listener.Addr().(*net.UDPAddr)); err != nil {
						return
					}
				}
			}()
			for _, back := range []*net.UDPConn{oldBack, newBack} {
				go func() {
					defer workers.Done()
					buffer := make([]byte, 65535)
					for {
						n, _, err := back.ReadFromUDP(buffer)
						if err != nil {
							return
						}
						if address := clientAddress.Load(); address != nil {
							if _, err := front.WriteToUDP(buffer[:n], address); err != nil {
								return
							}
						}
					}
				}()
			}
			defer func() { front.Close(); oldBack.Close(); newBack.Close(); workers.Wait() }()
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			clientSocket := open()
			transport := &upstream.Transport{Conn: clientSocket, ConnectionIDLength: 8}
			defer transport.Close()
			client, err := transport.Dial(ctx, front.LocalAddr(), clientTLS, &upstream.Config{Versions: []upstream.Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseWithError(0, "")
			server, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if server.ConnectionState().SupportsMultipath {
				t.Fatal("ordinary connection negotiated multipath")
			}
			for i, back := range []*net.UDPConn{oldBack, newBack, oldBack} {
				active.Store(back)
				send, err := client.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				send.SetWriteDeadline(time.Now().Add(5 * time.Second))
				payload := fmt.Sprintf("upstream rebinding %d", i)
				if _, err := send.Write([]byte(payload)); err != nil {
					t.Fatal(err)
				}
				if err := send.Close(); err != nil {
					t.Fatal(err)
				}
				receive, err := server.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				receive.SetReadDeadline(time.Now().Add(5 * time.Second))
				data, err := io.ReadAll(receive)
				if err != nil || string(data) != payload {
					t.Fatalf("request %q: %v", data, err)
				}
				if server.RemoteAddr().String() != back.LocalAddr().String() {
					t.Fatal("server did not commit rebound address")
				}
				reply, err := server.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				reply.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if _, err := reply.Write(data); err != nil {
					t.Fatal(err)
				}
				if err := reply.Close(); err != nil {
					t.Fatal(err)
				}
				incoming, err := client.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				incoming.SetReadDeadline(time.Now().Add(5 * time.Second))
				echoed, err := io.ReadAll(incoming)
				if err != nil || string(echoed) != payload {
					t.Fatalf("reply %q: %v", echoed, err)
				}
			}
		})
	}
}
