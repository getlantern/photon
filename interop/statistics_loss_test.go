package interop

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

type statisticsACKTrace struct{ *handshakeTrace }

func (s *statisticsACKTrace) AddProducer() qlogwriter.Recorder { return s }
func (s *statisticsACKTrace) RecordEvent(event qlogwriter.Event) {
	s.handshakeTrace.RecordEvent(event)
	switch e := event.(type) {
	case qlog.PacketSent:
		for _, frame := range e.Frames {
			if ack, ok := frame.Frame.(*qlog.AckFrame); ok {
				s.packet(fmt.Sprintf("ACK ranges=%v", ack.AckRanges), netip.AddrPort{}, nil)
			}
		}
	case qlog.PacketReceived:
		s.packet(fmt.Sprintf("received header=%v", e.Header), netip.AddrPort{}, nil)
	}
}

func TestStatisticsIndependentLateACK(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, ownServer := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/server=%t", version, ownServer), func(t *testing.T) {
				clientTLS, serverTLS := tlsConfigs(t)
				trace := &statisticsACKTrace{newHandshakeTrace(t)}
				peerConfig := &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, DisablePathMTUDiscovery: true, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }}
				ownConfig := &quic.Config{Versions: []quic.Version{version}, DisablePathMTUDiscovery: true}
				var ownListener *quic.Listener
				var peerListener *upstream.Listener
				var serverAddress net.Addr
				var err error
				if ownServer {
					ownListener, err = quic.ListenAddr("127.0.0.1:0", serverTLS, ownConfig)
					if err != nil {
						t.Fatal(err)
					}
					defer ownListener.Close()
					serverAddress = ownListener.Addr()
				} else {
					peerListener, err = upstream.ListenAddr("127.0.0.1:0", serverTLS, peerConfig)
					if err != nil {
						t.Fatal(err)
					}
					defer peerListener.Close()
					serverAddress = peerListener.Addr()
				}
				socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				remote := serverAddress.(*net.UDPAddr)
				var armed atomic.Bool
				type heldPacket struct {
					data   []byte
					target *net.UDPAddr
				}
				held := make(chan heldPacket, 1)
				done := make(chan struct{})
				go func() {
					defer close(done)
					buffer := make([]byte, 65535)
					var client *net.UDPAddr
					for {
						n, from, err := socket.ReadFromUDP(buffer)
						if err != nil {
							return
						}
						target := remote
						fromServer := from.AddrPort() == remote.AddrPort()
						if fromServer {
							if client == nil {
								continue
							}
							target = client
						} else {
							client = from
						}
						// Withhold a full-sized data packet from our endpoint only.
						if fromServer == ownServer && n > 1000 && buffer[0]&0x80 == 0 && armed.CompareAndSwap(true, false) {
							held <- heldPacket{bytes.Clone(buffer[:n]), target}
							continue
						}
						if _, err := socket.WriteToUDP(buffer[:n], target); err != nil {
							return
						}
					}
				}()
				defer func() { socket.Close(); <-done }()
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				var own *quic.Conn
				var peer *upstream.Conn
				if ownServer {
					peer, err = dialUpstreamAddr(t, ctx, socket.LocalAddr().String(), clientTLS, peerConfig)
					if err != nil {
						t.Fatal(err)
					}
					own, err = ownListener.Accept(ctx)
				} else {
					own, err = quic.DialAddr(ctx, socket.LocalAddr().String(), clientTLS, ownConfig)
					if err != nil {
						t.Fatal(err)
					}
					peer, err = peerListener.Accept(ctx)
				}
				if err != nil {
					t.Fatal(err)
				}
				defer own.CloseWithError(0, "")
				defer peer.CloseWithError(0, "")
				// Complete an acknowledged warm-up stream before arming the relay,
				// separating the withheld data from handshake/control exchanges.
				warm, err := own.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := warm.Write([]byte("warm")); err != nil {
					t.Fatal(err)
				}
				if err := warm.Close(); err != nil {
					t.Fatal(err)
				}
				warmIn, err := peer.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				warmIn.SetReadDeadline(time.Now().Add(6 * time.Second))
				if got, err := io.ReadAll(warmIn); err != nil || string(got) != "warm" {
					t.Fatal("warm-up", err)
				}
				if err := warm.WaitAcknowledged(ctx); err != nil {
					t.Fatal(err)
				}
				before := own.ConnectionStats()
				armed.Store(true)
				tx, err := own.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				tx.SetWriteDeadline(time.Now().Add(6 * time.Second))
				payload := bytes.Repeat([]byte("loss-stats"), 3000)
				if _, err := tx.Write(payload); err != nil {
					t.Fatal(err)
				}
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				var delayed heldPacket
				select {
				case delayed = <-held:
				case <-ctx.Done():
					t.Fatal("no packet withheld", ctx.Err())
				}
				rx, err := peer.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				// Do not consume EOF until after releasing the delayed packet. Doing
				// so can elicit MAX_STREAMS and an ACK-of-ACK that lets the peer retire
				// the original packet number from its receive history before release.
				if err := tx.WaitAcknowledged(ctx); err != nil {
					t.Fatal(err)
				}
				// Successful retransmission need not erase the original packet loss.
				lost := own.ConnectionStats()
				if lost.PacketsLost <= before.PacketsLost || lost.BytesLost < before.BytesLost+uint64(len(delayed.data)) {
					t.Fatal("withheld packet not counted lost", before, lost, len(delayed.data))
				}
				if _, err := socket.WriteToUDP(delayed.data, delayed.target); err != nil {
					t.Fatal(err)
				}
				tick := time.NewTicker(time.Millisecond)
				defer tick.Stop()
				for {
					s := own.ConnectionStats()
					if s.PacketsLost == lost.PacketsLost-1 && s.BytesLost == lost.BytesLost-uint64(len(delayed.data)) {
						break
					}
					select {
					case <-tick.C:
					case <-ctx.Done():
						t.Fatal("late peer ACK did not correct exact loss", lost, s, len(delayed.data), ctx.Err())
					}
				}
				rx.SetReadDeadline(time.Now().Add(6 * time.Second))
				got, err := io.ReadAll(rx)
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatal("retransmission delivery", len(got), err)
				}

			})
		}
	}
}
