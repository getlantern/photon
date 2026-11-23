package quic

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestTransportDisableVersionNegotiationPackets(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		for _, disabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/disabled=%v", version, disabled), func(t *testing.T) {
				ct, st := dialTLS(t)
				socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				defer socket.Close()
				tr := &Transport{Conn: socket, DisableVersionNegotiationPackets: disabled}
				defer tr.Close()
				cfg := &Config{Versions: []Version{version}}
				listener, err := tr.Listen(st, cfg)
				if err != nil {
					t.Fatal(err)
				}
				probe, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				defer probe.Close()
				dcid, scid := []byte("unknown0"), []byte("probe000")
				packet, err := wire.AppendVersionNegotiation(nil, 0x80, dcid, scid, []wire.Version{version})
				if err != nil {
					t.Fatal(err)
				}
				binary.BigEndian.PutUint32(packet[1:5], 0x12345678)
				packet = append(packet, make([]byte, 1200-len(packet))...)
				check := func() {
					t.Helper()
					if _, err := probe.WriteToUDP(packet, socket.LocalAddr().(*net.UDPAddr)); err != nil {
						t.Fatal(err)
					}
					timeout := time.Second
					if disabled {
						timeout = 40 * time.Millisecond
					}
					probe.SetReadDeadline(time.Now().Add(timeout))
					var reply [1500]byte
					n, _, err := probe.ReadFromUDP(reply[:])
					if disabled {
						network, ok := err.(net.Error)
						if !ok || !network.Timeout() {
							t.Fatal("unsupported version was not silent", n, err)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					header, err := wire.ParseHeader(reply[:n], 0)
					if err != nil || header.Version != 0 || !bytes.Equal(header.DCID, scid) || !bytes.Equal(header.SCID, dcid) {
						t.Fatal("negotiation response", header, err)
					}
				}
				check()
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				client, err := DialAddr(ctx, listener.Addr().String(), ct, cfg)
				if err != nil {
					t.Fatal("supported version blocked", err)
				}
				defer client.server.Close()
				accepted, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				listener.Close()
				listener, err = tr.Listen(st, cfg)
				if err != nil {
					t.Fatal(err)
				}
				check() // Endpoint policy survives listener replacement.
				tx, err := client.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				tx.SetWriteDeadline(time.Now().Add(time.Second))
				if _, err := tx.Write([]byte("existing connection")); err != nil {
					t.Fatal(err)
				}
				tx.Close()
				rx, err := accepted.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				rx.SetReadDeadline(time.Now().Add(time.Second))
				got, err := io.ReadAll(rx)
				if err != nil || string(got) != "existing connection" {
					t.Fatal(string(got), err)
				}
			})
		}
	}
}
