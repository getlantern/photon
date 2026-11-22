package quic

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// relayPath supplies a distinct remote tuple for one path and drops
// selected client datagrams only after validation. Each relay counts only its
// own path, independently of the other relay.
func relayPath(t *testing.T, remote *net.UDPAddr, cleared ...*atomic.Int64) (*net.UDPAddr, *atomic.Bool, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	var drop atomic.Bool
	var forwarded, dropped atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		var client *net.UDPAddr
		buffer := make([]byte, 65535)
		count := 0
		for {
			n, from, err := socket.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			target := remote
			if from.AddrPort() == remote.AddrPort() {
				if client == nil {
					continue
				}
				target = client
			} else {
				client = from
				if len(cleared) > 0 && n > 0 && buffer[0]&0xc0 == 0 {
					cleared[0].Add(1)
				}
				count++
				if drop.Load() && count%13 == 0 {
					dropped.Add(1)
					continue
				}
				forwarded.Add(int64(n))
			}
			if _, err := socket.WriteToUDP(buffer[:n], target); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { socket.Close(); <-done })
	return socket.LocalAddr().(*net.UDPAddr), &drop, &forwarded, &dropped
}

func TestPublicMultipathLargeTransfer(t *testing.T) {
	for _, grease := range []bool{false, true} {
		t.Run(fmt.Sprintf("grease=%v", grease), func(t *testing.T) { testPublicMultipathLargeTransfer(t, grease) })
	}
}
func testPublicMultipathLargeTransfer(t *testing.T, grease bool) {
	for _, version := range []Version{Version1, Version2} {
		for _, loss := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/loss=%v", version, loss), func(t *testing.T) {
				clientTLS, serverTLS := dialTLS(t)
				listener, err := ListenAddr("127.0.0.1:0", serverTLS, &Config{EnableMultipath: true, EnableQUICBitGreasing: grease})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				var primaryClear, secondaryClear atomic.Int64
				primary, _, primaryBytes, _ := relayPath(t, listener.Addr().(*net.UDPAddr), &primaryClear)
				relay, drop, forwarded, dropped := relayPath(t, listener.Addr().(*net.UDPAddr), &secondaryClear)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				bind := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}
				client, err := DialAddr(ctx, primary.String(), clientTLS, &Config{Versions: []Version{version}, EnableMultipath: true, EnableQUICBitGreasing: grease, LocalAddr: bind})
				if err != nil {
					t.Fatal(err)
				}
				defer client.server.Close()
				server, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				path, err := client.OpenPath(ctx, 1, bind, relay)
				if err != nil {
					t.Fatal(err)
				}

				defer path.Close()
				local, remote, found := client.PathAddresses(1)
				if !found || local.String() != path.LocalAddr().String() || remote.String() != path.RemoteAddr().String() {
					t.Fatal("client path snapshot differs from handle")
				}
				serverLocal, serverRemote, found := server.PathAddresses(1)
				if !found || serverLocal.String() != listener.Addr().String() || serverRemote.String() != relay.String() {
					t.Fatal("server did not report accepted path binding")
				}
				remote.(*net.UDPAddr).Port = 1
				_, fresh, _ := client.PathAddresses(1)
				if fresh.String() != path.RemoteAddr().String() {
					t.Fatal("path address copy aliases snapshot")
				}
				if _, _, found := client.PathAddresses(99); found {
					t.Fatal("unknown path reported")
				}

				before := forwarded.Load()
				primaryBefore := primaryBytes.Load()
				drop.Store(loss)
				payload := make([]byte, 256*1024)
				for i := range payload {
					payload[i] = byte(i*37 + i/251)
				}
				send, err := client.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				send.SetWriteDeadline(time.Now().Add(10 * time.Second))
				result := make(chan error, 1)
				go func() {
					_, err := send.Write(payload)
					if err == nil {
						err = send.Close()
					}
					result <- err
				}()
				receive, err := server.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				receive.SetReadDeadline(time.Now().Add(10 * time.Second))
				data, err := io.ReadAll(receive)
				if err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-result:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if !bytes.Equal(data, payload) {
					t.Fatalf("payload mismatch: got %d bytes", len(data))
				}
				// Validation/control alone cannot account for this much client traffic.
				// Do not require equal shares: the scheduler can move backlog away
				// from a congested or lossy path.
				if n := forwarded.Load() - before; n < int64(len(payload)/16) {
					t.Fatalf("secondary path carried only %d bytes", n)
				}
				if n := primaryBytes.Load() - primaryBefore; n < int64(len(payload)/16) {
					t.Fatalf("primary path carried only %d bytes", n)
				}
				if grease && (primaryClear.Load() == 0 || secondaryClear.Load() == 0) {
					t.Fatal("missing cleared-bit traffic on a path", primaryClear.Load(), secondaryClear.Load())
				}
				if !grease && (primaryClear.Load() != 0 || secondaryClear.Load() != 0) {
					t.Fatal("greasing without support")
				}
				if loss && dropped.Load() == 0 {
					t.Fatal("loss scenario dropped no packets")
				}
			})
		}
	}
}
