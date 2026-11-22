package transport

import (
	"github.com/getlantern/photon/internal/wire"
	"testing"
)

func TestPortableStreamPayloadFitsWorstHeader(t *testing.T) {
	f := wire.StreamFrame{StreamID: wire.MaxVarint, Offset: wire.MaxVarint - uint64(portableStreamPayload), Data: make([]byte, portableStreamPayload), Fin: true}
	encoded, err := f.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := 1 + 20 + 4 + 16 + len(encoded); got != 1200 {
		t.Fatal("worst-case packet size", got)
	}
}

func TestWriteStreamUsesPortablePayload(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		c, s := testPair(t, version, streamParameters())
		drivePair(t, c, s, false, false)
		id, err := c.OpenStream(false)
		if err != nil {
			t.Fatal(err)
		}
		n, err := c.WriteStream(id, make([]byte, portableStreamPayload+1), true)
		if err != nil || n != portableStreamPayload || c.streams.objects[id].finQueued {
			t.Fatal("partial frame", n, err)
		}
		n, err = c.WriteStream(id, []byte{1}, true)
		if err != nil || n != 1 || !c.streams.objects[id].finQueued {
			t.Fatal("final frame", n, err)
		}
	}
}
