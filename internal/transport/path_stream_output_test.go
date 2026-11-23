package transport

import (
	"io"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestStreamWriteAfterPrimaryPathDisposal(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, version)
		now := time.Unix(101, 0)
		app := client.multipath.paths[1].application
		app.budget.Validate()
		app.validation.state = PathValidated
		if err := client.abandonFailedPath(0); err != nil {
			t.Fatal(err)
		}
		if _, err := client.multipath.receiveAbandon(wire.PathAbandonFrame{PathID: 0}, now, time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if err := client.expireMultipathPaths(now.Add(3 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		if client.multipath.paths[0] != nil {
			t.Fatal("primary retained")
		}
		id, err := client.OpenStream(false)
		if err != nil {
			t.Fatal(err)
		}
		if n, err := client.WriteStream(id, []byte("after primary"), true); err != nil || n != 13 {
			t.Fatal(n, err)
		}
		for i := 0; i < 8; i++ {
			path, b, err := client.nextMultipathPacket(now.Add(time.Duration(i+4) * time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			if path != 1 {
				t.Fatal("wrong surviving path")
			}
			if err := server.handleMultipathPacket(b, now); err != nil {
				t.Fatal(err)
			}
		}
		stream, err := server.AcceptStream(false)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]byte, 32)
		n, err := server.ReadStream(stream, out)
		if err != io.EOF || string(out[:n]) != "after primary" {
			t.Fatal(n, err, string(out[:n]))
		}
	}
}

func TestMultipathCancelRetiresSharedStreamFrames(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	id, err := c.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteStream(id, []byte("cancel"), false); err != nil {
		t.Fatal(err)
	}
	primary := &c.spaces[applicationSpace]
	item := primary.queue[len(primary.queue)-1]
	// Model a cross-path copy retained for a late ACK and an active queue.
	secondary := c.multipath.paths[1].application.space
	secondary.sent[0] = item
	before := c.queuedBytes
	if err := c.CancelWrite(id, 7); err != nil {
		t.Fatal(err)
	}
	if !item.acked || len(secondary.sent) != 0 || c.queuedBytes != before-item.bytes+32 {
		t.Fatal("shared item not retired exactly once")
	}
	for _, state := range c.multipath.paths {
		for _, item := range state.application.space.queue {
			if f, ok := item.frame.(wire.StreamFrame); ok && f.StreamID == id {
				t.Fatal("canceled stream data still queued")
			}
		}
	}
}
