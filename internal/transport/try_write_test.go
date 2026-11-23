package transport

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func TestTryWriteAllAtomicAdmission(t *testing.T) {
	data := bytes.Repeat([]byte{93}, 3*portableStreamPayload+17)
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []string{"accept", "stream", "connection", "queue"} {
			t.Run(fmt.Sprintf("%x/%s", v, mode), func(t *testing.T) {
				p := streamParameters()
				p.InitialMaxData = uint64(len(data))
				p.InitialMaxStreamDataUni = uint64(len(data))
				if mode == "stream" {
					p.InitialMaxStreamDataUni--
				}
				if mode == "connection" {
					p.InitialMaxData--
				}
				c, s := testPair(t, v, p)
				drivePair(t, c, s, false, false)
				id, err := c.OpenStream(true)
				if err != nil {
					t.Fatal(err)
				}
				before := c.queuedBytes
				if mode == "queue" {
					c.queuedBytes = maxSendBytes - (len(data) + 4*32 - 1)
				}
				charged := c.queuedBytes
				err = c.TryWriteStreamAll(id, data)
				if mode != "accept" {
					if !errors.Is(err, stream.ErrWouldBlock) || c.queuedBytes != charged || c.streams.send.Used() != 0 || c.streams.objects[id].send.Offset() != 0 || len(c.spaces[applicationSpace].queue) != 0 {
						t.Fatal("non-atomic rejection", err)
					}
					c.queuedBytes = before
					// Model a larger grant on both sides before delivering the retry.
					if err := s.streams.receive.SetLimit(uint64(len(data))); err != nil {
						t.Fatal(err)
					}
					s.config.Parameters.InitialMaxStreamDataUni = uint64(len(data))
					if err := c.updateConnectionSendLimit(uint64(len(data))); err != nil {
						t.Fatal(err)
					}
					if err := c.updateStreamSendLimit(id, c.streams.objects[id], uint64(len(data))); err != nil {
						t.Fatal(err)
					}
					err = c.TryWriteStreamAll(id, data)
				}
				if err != nil || c.queuedBytes != before+len(data)+4*32 || c.streams.send.Used() != uint64(len(data)) {
					t.Fatal("whole-slice admission", err)
				}
				// Caller ownership ends at return; retained frames must own their bytes.
				sent := bytes.Clone(data)
				for i := range data {
					data[i] ^= 1
				}
				drivePair(t, c, s, true, true)
				got := make([]byte, len(sent))
				if n, err := s.ReadStream(id, got); n != len(sent) || err != nil || !bytes.Equal(got, sent) {
					t.Fatal("recovered data", n, err)
				}
			})
		}
	}
}
