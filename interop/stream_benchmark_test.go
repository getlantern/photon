package interop

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

type benchmarkStream interface {
	io.ReadWriteCloser
	SetDeadline(time.Time) error
}

// BenchmarkServerStreamEcho compares public server APIs against the same
// upstream client. Setup/handshake are excluded; each operation opens a stream
// and exchanges a 1 MiB request plus identical response concurrently.
func BenchmarkServerStreamEcho(b *testing.B) {
	for _, independent := range []bool{true, false} {
		name := "upstream"
		if independent {
			name = "independent"
		}
		b.Run(name, func(b *testing.B) {
			clientTLS, serverTLS := tlsConfigs(b)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var addr string
			var closeListener func()
			var acceptConnection func() (func(context.Context) (benchmarkStream, error), func(), error)
			if independent {
				l, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{Versions: []quic.Version{quic.Version1}, InitialStreamReceiveWindow: 65536, MaxStreamReceiveWindow: 65536, InitialConnectionReceiveWindow: 1 << 20, MaxConnectionReceiveWindow: 1 << 20, MaxIncomingStreams: 16, MaxIncomingUniStreams: 16})
				if err != nil {
					b.Fatal(err)
				}
				addr = l.Addr().String()
				closeListener = func() { l.Close() }
				acceptConnection = func() (func(context.Context) (benchmarkStream, error), func(), error) {
					c, err := l.Accept(ctx)
					if err != nil {
						return nil, nil, err
					}
					return func(ctx context.Context) (benchmarkStream, error) { return c.AcceptStream(ctx) }, func() { c.CloseWithError(0, "") }, nil
				}
			} else {
				// Match receive windows and explicitly pin the same stream-count policy
				// on both servers, independently of changing public defaults.
				l, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version1}, InitialStreamReceiveWindow: 65536, MaxStreamReceiveWindow: 65536, InitialConnectionReceiveWindow: 1 << 20, MaxConnectionReceiveWindow: 1 << 20, MaxIncomingStreams: 16, MaxIncomingUniStreams: 16})
				if err != nil {
					b.Fatal(err)
				}
				addr = l.Addr().String()
				closeListener = func() { l.Close() }
				acceptConnection = func() (func(context.Context) (benchmarkStream, error), func(), error) {
					c, err := l.Accept(ctx)
					if err != nil {
						return nil, nil, err
					}
					return func(ctx context.Context) (benchmarkStream, error) { return c.AcceptStream(ctx) }, func() { c.CloseWithError(0, "") }, nil
				}
			}
			defer closeListener()
			dialCtx, stop := context.WithTimeout(ctx, 5*time.Second)
			peer, err := dialUpstreamAddr(b, dialCtx, addr, clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version1}})
			stop()
			if err != nil {
				b.Fatal(err)
			}
			defer peer.CloseWithError(0, "")
			accept, closeServer, err := acceptConnection()
			if err != nil {
				b.Fatal(err)
			}
			defer closeServer()
			serverDone := make(chan error, 1)
			ready := make(chan struct{})
			go func() {
				buf := make([]byte, 32*1024)
				close(ready)
				for i := 0; i < b.N; i++ {
					s, err := accept(ctx)
					if err != nil {
						serverDone <- err
						return
					}
					s.SetDeadline(time.Now().Add(30 * time.Second))
					if _, err := io.CopyBuffer(s, s, buf); err != nil {
						serverDone <- err
						return
					}
					if err := s.Close(); err != nil {
						serverDone <- err
						return
					}
				}
				serverDone <- nil
			}()
			// Every iteration checks the full echo, not just the number of bytes read.
			payload := bytes.Repeat([]byte("0123456789abcdef"), 65536)
			response := make([]byte, len(payload))
			sent := make(chan error, 1)
			b.SetBytes(int64(2 * len(payload)))
			b.ReportAllocs()
			<-ready
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s, err := peer.OpenStreamSync(ctx)
				if err != nil {
					b.Fatal(err)
				}
				s.SetDeadline(time.Now().Add(30 * time.Second))
				go func() {
					_, err := s.Write(payload)
					if err == nil {
						err = s.Close()
					}
					sent <- err
				}()
				if _, err := io.ReadFull(s, response); err != nil {
					b.Fatal(err)
				}
				var tail [1]byte
				if n, err := s.Read(tail[:]); n != 0 || err != io.EOF {
					b.Fatal("echo FIN", n, err)
				}
				if err := <-sent; err != nil {
					b.Fatal(err)
				}
				if !bytes.Equal(payload, response) {
					b.Fatal(fmt.Sprintf("echo mismatch at iteration %d", i))
				}
			}
			b.StopTimer()
			if err := <-serverDone; err != nil {
				b.Fatal(err)
			}
		})
	}
}
