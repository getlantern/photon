package quic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/transport"
	"github.com/getlantern/photon/internal/wire"
)

func TestInitialPacketSizeConfiguration(t *testing.T) {
	for _, tc := range []struct{ input, want uint16 }{{0, 1280}, {1, 1200}, {1199, 1200}, {1200, 1200}, {1280, 1280}, {1452, 1452}, {65535, 1452}} {
		input := Config{InitialPacketSize: tc.input}
		cfg, _, err := prepareConfig(&input)
		if err != nil || cfg.InitialPacketSize != tc.want || input.InitialPacketSize != tc.input {
			t.Fatal(tc, cfg.InitialPacketSize, err)
		}
	}
}

func TestDialInitialPacketSizeOnWire(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		for _, tc := range []struct{ input, want uint16 }{{0, 1280}, {1, 1200}, {1452, 1452}, {65535, 1452}} {
			t.Run(fmt.Sprintf("%x/%d", version, tc.input), func(t *testing.T) {
				sink, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				defer sink.Close()
				ct, _ := dialTLS(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					c, err := DialAddr(ctx, sink.LocalAddr().String(), ct, &Config{Versions: []Version{version}, InitialPacketSize: tc.input})
					if c != nil {
						c.CloseWithError(0, "")
					}
					done <- err
				}()
				sink.SetReadDeadline(time.Now().Add(3 * time.Second))
				buf := make([]byte, 2048)
				n, _, readErr := sink.ReadFromUDP(buf)
				cancel()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Fatal("dial cancellation", err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("dial did not stop")
				}
				if readErr != nil {
					t.Fatal(readErr)
				}
				h, err := wire.ParseHeader(buf[:n], 8)
				if err != nil || h.Type != wire.Initial || h.Version != version || n != int(tc.want) {
					t.Fatal("Initial wire size", n, tc.want, h.Type, err)
				}
			})
		}
	}
}

func TestPublicConfiguredDatagramSize(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		for _, size := range []uint16{1200, 1280, 1452} {
			t.Run(fmt.Sprintf("%x/%d", version, size), func(t *testing.T) {
				ct, st := dialTLS(t)
				cfg := &Config{Versions: []Version{version}, InitialPacketSize: size, DisablePathMTUDiscovery: true, EnableDatagrams: true}
				listener, err := ListenAddr("127.0.0.1:0", st, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				client, err := DialAddr(ctx, listener.Addr().String(), ct, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer client.CloseWithError(0, "")
				server, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, pair := range [][2]*Conn{{client, server}, {server, client}} {
					payload := bytes.Repeat([]byte{81}, int(size)-42)
					if err := pair[0].SendDatagram(payload); err != nil {
						t.Fatal("send at configured size", err)
					}
					got, err := pair[1].ReceiveDatagram(ctx)
					if err != nil || !bytes.Equal(got, payload) {
						t.Fatal("receive at configured size", len(got), err)
					}
					if err := pair[0].SendDatagram(make([]byte, len(payload)+1)); !errors.Is(err, transport.ErrDatagramTooLarge) {
						t.Fatal("disabled discovery exceeded size", err)
					}
				}
			})
		}
	}
}
