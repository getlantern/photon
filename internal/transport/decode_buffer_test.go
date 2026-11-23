package transport

import (
	"bytes"
	"io"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestDecodeStorageDoesNotEscapeReceive(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(v), func(t *testing.T) {
			p := streamParameters()
			p.MaxDatagramFrameSize = 65535
			client, server := testPair(t, v, p)
			drivePair(t, client, server, false, false)
			id, err := client.OpenStream(false)
			if err != nil {
				t.Fatal(err)
			}
			want := bytes.Repeat([]byte("retained stream"), 100)
			writeAll(t, client, id, want)
			drivePair(t, client, server, true, true)
			if err := client.SendDatagram([]byte("retained datagram")); err != nil {
				t.Fatal(err)
			}
			drivePair(t, client, server, true, false)
			if len(server.decodeScratch) == 0 {
				t.Fatal("decode storage not used")
			}
			for i := range server.decodeScratch {
				server.decodeScratch[i] = 0xff
			}
			got := make([]byte, len(want)+1)
			n, err := server.ReadStream(id, got)
			if err != io.EOF || !bytes.Equal(got[:n], want) {
				t.Fatal("stream aliases decode storage", n, err)
			}
			datagram, err := server.ReceiveDatagram()
			if err != nil || string(datagram) != "retained datagram" {
				t.Fatal("datagram aliases decode storage", err)
			}
			saved := server.decodeScratch
			server.releaseTransportBuffers()
			if server.decodeScratch != nil || !bytes.Equal(saved, make([]byte, len(saved))) {
				t.Fatal("cleanup retained plaintext storage")
			}
		})
	}
}
