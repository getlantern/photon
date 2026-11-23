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

func TestConnectionStatisticsIndependentTraffic(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, ownServer := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/server=%t", version, ownServer), func(t *testing.T) {
				clientTLS, serverTLS := tlsConfigs(t)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var own *quic.Conn
				var peer *upstream.Conn
				if ownServer {
					listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{Versions: []quic.Version{version}})
					if err != nil {
						t.Fatal(err)
					}
					defer listener.Close()
					peer, err = dialUpstreamAddr(t, ctx, listener.Addr().String(), clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
					if err != nil {
						t.Fatal(err)
					}
					own, err = listener.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
				} else {
					listener, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
					if err != nil {
						t.Fatal(err)
					}
					defer listener.Close()
					own, err = quic.DialAddr(ctx, listener.Addr().String(), clientTLS, &quic.Config{Versions: []quic.Version{version}})
					if err != nil {
						t.Fatal(err)
					}
					peer, err = listener.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
				}
				defer own.CloseWithError(0, "")
				defer peer.CloseWithError(0, "")
				before := own.ConnectionStats()
				payload := bytes.Repeat([]byte("stats"), 6000)
				out, err := own.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				out.SetWriteDeadline(time.Now().Add(4 * time.Second))
				if _, err := out.Write(payload); err != nil {
					t.Fatal(err)
				}
				if err := out.Close(); err != nil {
					t.Fatal(err)
				}
				in, err := peer.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				in.SetReadDeadline(time.Now().Add(4 * time.Second))
				got, err := io.ReadAll(in)
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatal("peer receive", len(got), err)
				}
				peerOut, err := peer.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				peerOut.SetWriteDeadline(time.Now().Add(4 * time.Second))
				if _, err := peerOut.Write(payload); err != nil {
					t.Fatal(err)
				}
				if err := peerOut.Close(); err != nil {
					t.Fatal(err)
				}
				ownIn, err := own.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				ownIn.SetReadDeadline(time.Now().Add(4 * time.Second))
				got, err = io.ReadAll(ownIn)
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatal("own receive", len(got), err)
				}
				after := own.ConnectionStats()
				if after.BytesSent < before.BytesSent+uint64(len(payload)) || after.BytesReceived < before.BytesReceived+uint64(len(payload)) || after.PacketsSent <= before.PacketsSent || after.PacketsReceived <= before.PacketsReceived {
					t.Fatal("traffic absent from counters", before, after)
				}
				if after.MinRTT <= 0 || after.LatestRTT < after.MinRTT || after.SmoothedRTT <= 0 || after.MeanDeviation < 0 {
					t.Fatal("invalid RTT snapshot", after)
				}
				if err := peer.CloseWithError(42, "statistics complete"); err != nil {
					t.Fatal(err)
				}
				select {
				case <-own.Context().Done():
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				closed := own.ConnectionStats()
				if closed.BytesSent < after.BytesSent || closed.BytesReceived < after.BytesReceived || closed.PacketsSent < after.PacketsSent || closed.PacketsReceived < after.PacketsReceived {
					t.Fatal("close lost counters", after, closed)
				}
			})
		}
	}
}
