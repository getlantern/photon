package transport

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathStreamDistribution(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		c, peer := sendingPathPair(t, version)
		for _, conn := range []*Conn{c, peer} {
			app := conn.multipath.paths[1].application
			app.validation.state = PathValidated
			app.budget.Validate()
		}
		id, err := c.OpenStream(false)
		if err != nil {
			t.Fatal(err)
		}
		var want []byte
		for i := 0; i < 4; i++ {
			data := bytes.Repeat([]byte{byte('a' + i)}, 512)
			n, err := c.WriteStream(id, data, i == 3)
			if err != nil || n != len(data) {
				t.Fatal(n, err)
			}
			want = append(want, data...)
		}
		for _, path := range []uint64{0, 1} {
			count := 0
			for _, item := range c.multipath.paths[path].application.space.queue {
				if _, ok := item.frame.(wire.StreamFrame); ok {
					count++
				}
			}
			if count != 2 {
				t.Fatalf("path %d has %d stream frames", path, count)
			}
		}
		now := time.Unix(101, 0)
		sent := map[uint64]int{}
		for i := 0; i < 20; i++ {
			path, packet, err := c.nextMultipathPacket(now.Add(time.Duration(i) * time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			if packet == nil {
				continue
			}
			sent[path]++
			if err := peer.handleMultipathPacket(packet, now.Add(time.Duration(i)*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
		}
		if sent[0] == 0 || sent[1] == 0 {
			t.Fatal("did not transmit on both paths", sent)
		}
		received, err := peer.AcceptStream(false)
		if err != nil {
			t.Fatal(err)
		}
		data := make([]byte, len(want)+1)
		n, err := peer.ReadStream(received, data)
		if err != io.EOF || !bytes.Equal(data[:n], want) {
			t.Fatalf("reassembly n=%d err=%v", n, err)
		}
	}
}

func TestMultipathEnqueueEligibility(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	p := c.multipath
	// An unvalidated secondary cannot take application data.
	if id, ok := c.applicationEnqueuePath(); !ok || id != 0 {
		t.Fatal(id, ok)
	}
	p.paths[1].application.validation.state = PathValidated
	p.lastEnqueued, p.haveEnqueued = 0, true
	if id, ok := c.applicationEnqueuePath(); !ok || id != 1 {
		t.Fatal(id, ok)
	}
	p.paths[1].backup = true
	if id, ok := c.applicationEnqueuePath(); !ok || id != 0 {
		t.Fatal("backup chosen over available", id, ok)
	}
	if _, err := p.beginAbandon(0); err != nil {
		t.Fatal(err)
	}
	if id, ok := c.applicationEnqueuePath(); !ok || id != 1 {
		t.Fatal("backup unavailable after primary abandonment", id, ok)
	}
}
