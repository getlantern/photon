package quic

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

func TestPublicStreamPriorityConcurrent(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ct, st := dialTLS(t)
			listener, err := ListenAddr("127.0.0.1:0", st, &Config{Versions: []Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := DialAddr(ctx, listener.Addr().String(), ct, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer client.server.Close()
			server, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			stream, err := client.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("priority"), 8192)
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					stream.SetPriority(int8(i-25), i%2 == 0)
				}
			}()
			go func() {
				defer wg.Done()
				stream.SetWriteDeadline(time.Now().Add(3 * time.Second))
				if _, err := stream.Write(payload); err != nil {
					t.Error(err)
				}
				if err := stream.Close(); err != nil {
					t.Error(err)
				}
			}()
			received, err := server.AcceptStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			received.SetReadDeadline(time.Now().Add(3 * time.Second))
			got, err := io.ReadAll(received)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatal(len(got), err)
			}
			wg.Wait()
			// Late updates to a completed or canceled stream are harmless.
			stream.SetPriority(0, false)
			uni, err := server.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			uni.CancelWrite(3)
			uni.SetPriority(7, true)
		})
	}
}
