package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

// Deliver EncryptedExtensions separately from Finished in real protected
// handshake packets. CID issuance must respect remembered limits while early
// keys remain the only available application write keys.
func TestEarlyCIDLimitPromotion(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(version), func(t *testing.T) {
			cc, sc := testTLS(t)
			cc.ClientSessionCache = tls.NewLRUClientSessionCache(4)
			seedClient, seedServer := earlyPair(t, version, cc, sc, true)
			drivePair(t, seedClient, seedServer, false, false)
			client, err := NewConn(seedClient.config)
			if err != nil {
				t.Fatal(err)
			}
			config := seedServer.config
			config.Parameters.ActiveConnectionIDLimit = 4
			config.Parameters.InitialMaxData = 16384
			config.Parameters.InitialMaxStreamDataBidiRemote = 8192
			config.Parameters.InitialMaxStreamsBidi = 4
			config.Parameters.MaxDatagramFrameSize = 1200
			server, err := NewConn(config)
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range []*Conn{client, server} {
				t.Cleanup(func() { c.Close() })
				if err := c.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if !client.sendingEarly() || client.streams.peerLimits.ActiveConnectionIDLimit != 2 {
				t.Fatal("fixture lacks remembered early CID limit")
			}
			now := time.Unix(100, 0)
			// Let the resumed ClientHello generate the real server flight.
			for _, raw := range drain(t, client, now) {
				if err := server.HandleDatagram(raw, now); err != nil {
					t.Fatal(err)
				}
			}
			split := false
			for _, item := range server.spaces[handshakeSpace].queue {
				frame, ok := item.frame.(wire.CryptoFrame)
				if !ok || frame.Offset != 0 || len(frame.Data) < 4 || frame.Data[0] != 8 {
					continue
				}
				end := 4 + int(frame.Data[1])<<16 + int(frame.Data[2])<<8 + int(frame.Data[3])
				if end >= len(frame.Data) {
					t.Fatal("fixture needs combined EncryptedExtensions/Finished")
				}
				tail := wire.CryptoFrame{Offset: uint64(end), Data: frame.Data[end:]}
				frame.Data = frame.Data[:end]
				server.queuedBytes -= item.bytes - end
				item.bytes, item.frame = end, frame
				if err := server.enqueue(handshakeSpace, tail, len(tail.Data)); err != nil {
					t.Fatal(err)
				}
				split = true
				break
			}
			if !split {
				t.Fatal("server handshake flight was not split")
			}
			for attempt := 0; client.peerParams == nil && attempt < 16; attempt++ {
				raw, err := server.NextDatagram(now)
				if err != nil {
					t.Fatal(err)
				}
				if raw != nil {
					if err := client.HandleDatagram(raw, now); err != nil {
						t.Fatal(err)
					}
				}
				now = now.Add(time.Millisecond)
			}
			if client.peerParams == nil || client.peerParams.ActiveConnectionIDLimit != 4 || !client.sendingEarly() || client.tls.Complete() {
				t.Fatal("fixture did not expose fresh parameters before Finished")
			}

			if len(client.localIDs.active) != 2 {
				t.Fatal("new CID limit entered early output", len(client.localIDs.active))
			}
			for _, item := range client.spaces[applicationSpace].queue {
				if frame, ok := item.frame.(wire.NewConnectionIDFrame); ok && frame.Sequence >= 2 {
					t.Fatal("excess early CID queued", frame.Sequence)
				}
			}
			// Fresh stream and DATAGRAM limits are visible, but no Finished has
			// been processed. Consume the old stream/connection credit exactly.
			var ids []uint64
			for range 2 {
				id, err := client.OpenStream(false)
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
				remaining := 4096
				for remaining > 0 {
					n, err := client.WriteStream(id, make([]byte, remaining), false)
					if err != nil || n == 0 {
						t.Fatal("remembered credit unavailable", n, err)
					}
					remaining -= n
				}
				if _, err := client.WriteStream(id, []byte{1}, false); !errors.Is(err, stream.ErrWouldBlock) {
					t.Fatal("fresh stream credit leaked into early output", err)
				}
			}
			if _, err := client.OpenStream(false); !errors.Is(err, stream.ErrWouldBlock) {
				t.Fatal("fresh stream count leaked into early output", err)
			}
			if client.streams.send.Used() != 8192 || client.streams.send.Limit() != 8192 {
				t.Fatal("fresh connection credit leaked into early output")
			}
			if err := client.SendDatagram([]byte("new datagram capability")); !errors.Is(err, ErrDatagramUnsupported) {
				t.Fatal("fresh datagram support leaked into early output", err)
			}

			earlyPackets := 0
			for _, raw := range drain(t, client, now) {
				header, err := server.parseHeader(raw)
				if err != nil {
					t.Fatal(err)
				}
				if header.Type == wire.ZeroRTT {
					earlyPackets++
				}
				if err := server.HandleDatagram(raw, now); err != nil {
					t.Fatal(err)
				}
			}
			if earlyPackets == 0 || len(server.peerIDs.active) != 2 {
				t.Fatal("remembered CID limit not exercised on early wire", earlyPackets, len(server.peerIDs.active))
			}
			// Deliver the remaining Finished and complete accepted TLS resumption.
			drivePair(t, client, server, false, false)
			if !client.tls.EarlyDataAccepted() || len(client.localIDs.active) != 4 {
				t.Fatal("current CID limit not promoted", len(client.localIDs.active))
			}
			// No server reads have granted MAX_DATA or MAX_STREAM_DATA: these
			// operations depend on promotion of the new handshake parameters.
			if n, err := client.WriteStream(ids[0], []byte{1}, false); err != nil || n != 1 {
				t.Fatal("new stream credit not promoted", n, err)
			}
			if _, err := client.OpenStream(false); err != nil {
				t.Fatal("new stream count not promoted", err)
			}
			if client.streams.send.Used() != 8193 || client.streams.send.Limit() != 16384 {
				t.Fatal("connection promotion reset or lost credit")
			}
			if err := client.SendDatagram([]byte("new datagram capability")); err != nil {
				t.Fatal("new datagram support not promoted", err)
			}
			drivePair(t, client, server, false, false)
			if payload, err := server.ReceiveDatagram(); err != nil || string(payload) != "new datagram capability" {
				t.Fatal("promoted datagram not delivered", string(payload), err)
			}

		})
	}
}
