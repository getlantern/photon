package quic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestTransportRawDatagrams(t *testing.T) {
	for _, kind := range []string{"udp", "plain", "oob"} {
		t.Run(kind, func(t *testing.T) {
			socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			tr := &Transport{Conn: transportTestWrapSocket(socket, kind)}
			defer tr.Close()
			peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			peer.SetDeadline(time.Now().Add(2 * time.Second))
			sent := []byte{0xff, 1, 2, 3}
			if n, err := tr.WriteTo(sent, peer.LocalAddr()); err != nil || n != len(sent) {
				t.Fatal(n, err)
			}
			var buffer [128]byte
			n, source, err := peer.ReadFromUDP(buffer[:])
			if err != nil || !bytes.Equal(buffer[:n], sent) || source.String() != socket.LocalAddr().String() {
				t.Fatal("raw write", n, source, err)
			}
			// First use arms capture. Its canceled wait leaves reception enabled.
			arm, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
			_, _, err = tr.ReadNonQUICPacket(arm, buffer[:])
			stop()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("empty raw read", err)
			}
			for _, data := range [][]byte{{0x40}, {0x80}, {0xc0}, {}, {0x3f, 4, 5, 6}} {
				if _, err := peer.WriteToUDP(data, socket.LocalAddr().(*net.UDPAddr)); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			n, addr, err := tr.ReadNonQUICPacket(ctx, buffer[:2])
			if err != nil || n != 2 || !bytes.Equal(buffer[:n], []byte{0x3f, 4}) || addr.String() != peer.LocalAddr().String() {
				t.Fatal("raw classification/truncation", n, addr, err)
			}
			payload := []byte{0, 7, 8, 9}
			if _, err := peer.WriteToUDP(payload, socket.LocalAddr().(*net.UDPAddr)); err != nil {
				t.Fatal(err)
			}
			n, _, err = tr.ReadNonQUICPacket(ctx, buffer[:])
			if err != nil || !bytes.Equal(buffer[:n], payload) {
				t.Fatal("raw packet boundary", n, err)
			}
			blocked := make(chan error, 1)
			go func() { _, _, err := tr.ReadNonQUICPacket(ctx, buffer[:]); blocked <- err }()
			tr.Close()
			if err := <-blocked; !errors.Is(err, ErrTransportClosed) {
				t.Fatal("raw read after close", err)
			}
			if _, err := tr.WriteTo(payload, peer.LocalAddr()); !errors.Is(err, ErrTransportClosed) {
				t.Fatal("raw write after close", err)
			}
		})
	}
}

type observeGreasedPacketConn struct {
	net.PacketConn
	cleared atomic.Int64
}

func (c *observeGreasedPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(b)
	if err == nil && n > 0 && b[0]&0xc0 == 0 {
		c.cleared.Add(1)
	}
	return n, addr, err
}

func TestTransportRawReadPreservesGreasedQUIC(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ct, st := dialTLS(t)
			makeSocket := func() *net.UDPConn {
				s, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { s.Close() })
				return s
			}
			observed := &observeGreasedPacketConn{PacketConn: makeSocket()}
			server := &Transport{Conn: observed}
			client := &Transport{Conn: makeSocket()}
			defer server.Close()
			defer client.Close()
			cfg := &Config{Versions: []Version{version}, EnableQUICBitGreasing: true}
			listener, err := server.Listen(st, cfg)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			sender, err := client.Dial(ctx, listener.Addr(), ct, cfg)
			if err != nil {
				t.Fatal(err)
			}
			receiver, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var raw [2048]byte
			arm, stop := context.WithTimeout(ctx, 20*time.Millisecond)
			_, _, err = server.ReadNonQUICPacket(arm, raw[:])
			stop()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("greased"), 9000)
			tx, err := sender.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			tx.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if _, err := tx.Write(payload); err != nil {
				t.Fatal(err)
			}
			if err := tx.Close(); err != nil {
				t.Fatal(err)
			}
			rx, err := receiver.AcceptUniStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			rx.SetReadDeadline(time.Now().Add(2 * time.Second))
			got, err := io.ReadAll(rx)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatal("greased transfer", len(got), err)
			}
			if observed.cleared.Load() == 0 {
				t.Fatal("fixture did not receive greased short headers")
			}
			marker := []byte{0, 1, 2, 3}
			if _, err := client.WriteTo(marker, listener.Addr()); err != nil {
				t.Fatal(err)
			}
			n, _, err := server.ReadNonQUICPacket(ctx, raw[:])
			if err != nil || !bytes.Equal(raw[:n], marker) {
				t.Fatal("QUIC packet leaked to raw reader", n, err)
			}
		})
	}
}
