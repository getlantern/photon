package transport

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestPacketAndRetransmissionOwnPayload(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := testPair(t, v, streamParameters())
		drivePair(t, client, server, false, false)
		first := bytes.Repeat([]byte{0x31}, 700)
		second := bytes.Repeat([]byte{0x72}, 600)
		for _, payload := range [][]byte{first, second} {
			id, err := client.OpenStream(true)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := client.WriteStream(id, payload, true); err != nil || n != len(payload) {
				t.Fatal(n, err)
			}
		}
		packets := drain(t, client, time.Unix(1000, 0))
		for i := range client.payloadScratch {
			client.payloadScratch[i] = 0xee
		}
		// Both protected output and retained STREAM payloads must be independent of
		// the overwritten scratch, including when another packet was serialized.
		streams := 0
		for _, item := range client.spaces[applicationSpace].sent {
			if frame, ok := item.frame.(wire.StreamFrame); ok {
				streams++
				expected := first
				if frame.StreamID == 6 {
					expected = second
				}
				if !bytes.Equal(frame.Data, expected) {
					t.Fatal("retransmission borrowed serialization buffer")
				}
			}
		}
		if streams != 2 {
			t.Fatal("expected two retained frames", streams)
		}
		for _, packet := range packets {
			if err := server.HandleDatagram(packet, time.Unix(1000, 0)); err != nil {
				t.Fatal(err)
			}
		}
		for _, expected := range [][]byte{first, second} {
			id, err := server.AcceptStream(true)
			if err != nil {
				t.Fatal(err)
			}
			data := make([]byte, 800)
			n, err := server.ReadStream(id, data)
			if err != io.EOF || !bytes.Equal(data[:n], expected) {
				t.Fatal("protected output borrowed scratch", n, err)
			}
		}
		client.Close()
		if client.payloadScratch != [defaultPathMTUCeiling]byte{} {
			t.Fatal("scratch retained on close")
		}
	}
}
