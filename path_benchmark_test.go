package quic

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// BenchmarkPublicPathTransfer measures completed one-way 256 KiB transfers.
// Both endpoints are this implementation; it is not an upstream comparison.
func BenchmarkPublicPathTransfer(b *testing.B) {
	for _, version := range []Version{Version1, Version2} {
		for _, paths := range []int{0, 1, 2} {
			b.Run(fmt.Sprintf("v%x/paths=%d", version, paths), func(b *testing.B) {
				benchmarkPublicTransfer(b, version, paths, false)
			})
		}
	}
}

// BenchmarkPublicMTUTransfer compares short post-handshake transfers. It does
// not wait for discovery convergence, so it is not a steady-state MTU benchmark.
func BenchmarkPublicMTUTransfer(b *testing.B) {
	for _, v := range []Version{Version1, Version2} {
		for _, disabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("v%x/disabled=%t", v, disabled), func(b *testing.B) {
				benchmarkPublicTransfer(b, v, 0, disabled)
			})
		}
	}
}

func benchmarkPublicTransfer(b *testing.B, version Version, paths int, disableMTU bool) {
	clientTLS, serverTLS := dialTLS(b)
	cfg := &Config{Versions: []Version{version}, EnableMultipath: paths > 0, DisablePathMTUDiscovery: disableMTU, LocalAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}}
	l, err := ListenAddr("127.0.0.1:0", serverTLS, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handshake, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	client, err := DialAddr(handshake, l.Addr().String(), clientTLS, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer client.server.Close()
	server, err := l.Accept(handshake)
	if err != nil {
		b.Fatal(err)
	}
	if paths == 2 {
		if _, err := client.OpenPath(handshake, 1, cfg.LocalAddr, l.Addr().(*net.UDPAddr)); err != nil {
			b.Fatal(err)
		}
	}
	payload := make([]byte, 256*1024)
	for i := range payload {
		payload[i] = byte(i*37 + i/251)
	}
	completed := make(chan error, 1)
	done := make(chan struct{})
	ready := make(chan struct{})
	go func() {
		defer close(done)
		buffer := make([]byte, len(payload))
		close(ready)
		for {
			receive, err := server.AcceptUniStream(ctx)
			if err != nil {
				return
			}
			err = receive.SetReadDeadline(time.Now().Add(10 * time.Second))
			if err == nil {
				_, err = io.ReadFull(receive, buffer)
			}
			if err == nil && !bytes.Equal(buffer, payload) {
				err = fmt.Errorf("payload mismatch")
			}
			if err == nil {
				var end [1]byte
				n, final := receive.Read(end[:])
				if n != 0 || final != io.EOF {
					err = fmt.Errorf("expected FIN: n=%d err=%v", n, final)
				}
			}
			completed <- err
			if err != nil {
				return
			}
		}
	}()
	defer func() { cancel(); l.Close(); <-done }()
	<-ready
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		send, err := client.OpenUniStreamSync(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := send.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
			b.Fatal(err)
		}
		if n, err := send.Write(payload); err != nil || n != len(payload) {
			b.Fatal(n, err)
		}
		if err := send.Close(); err != nil {
			b.Fatal(err)
		}
		select {
		case err := <-completed:
			if err != nil {
				b.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			b.Fatal("receiver completion timeout")
		}
	}
	b.StopTimer()
}
