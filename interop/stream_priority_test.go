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

func TestStreamPriorityIndependent(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			listener, err := upstream.ListenAddr("127.0.0.1:0", st, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := quic.DialAddr(ctx, listener.Addr().String(), ct, &quic.Config{Versions: []quic.Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseWithError(0, "")
			server, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			streams := make([]*quic.SendStream, 3)
			payload := bytes.Repeat([]byte("priority transfer"), 2048)
			results := make(chan error, 3)
			for i := range streams {
				streams[i], err = client.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				streams[i].SetPriority(int8(7-i*3), i != 1)
				go func(s *quic.SendStream) {
					s.SetWriteDeadline(time.Now().Add(3 * time.Second))
					_, err := s.Write(payload)
					if err == nil {
						err = s.Close()
					}
					results <- err
				}(streams[i])
			}
			for i := 0; i < 3; i++ {
				received, err := server.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				received.SetReadDeadline(time.Now().Add(3 * time.Second))
				got, err := io.ReadAll(received)
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatal(len(got), err)
				}
			}
			for i := 0; i < 3; i++ {
				if err := <-results; err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
