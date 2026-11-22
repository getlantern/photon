package transport

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// The deterministic link drops oversized datagrams, without altering recovery
// state or injecting ACK/loss metadata. It is not a kernel/network-namespace test.
func TestEncryptedMTUBlackholeRecovery(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			c, peer := sendingPathPair(t, version)
			for _, conn := range []*Conn{c, peer} {
				a := conn.multipath.paths[1].application
				a.validation.state = PathValidated
				a.budget.Validate()
			}
			c.multipath.paths[0].backup = true
			a := c.multipath.paths[1].application
			if err := c.startPathMTUSearch(1, 4800); err != nil {
				t.Fatal(err)
			}
			now := time.Unix(101, 0)
			linkLimit := 3000
			dropped := 0
			deliver := func(from, to *Conn) []byte {
				t.Helper()
				b, err := from.nextPacketOnPath(1, now)
				if err != nil {
					t.Fatal(err)
				}
				if len(b) > linkLimit {
					dropped++
					return b
				}
				if b != nil {
					if err := to.handleMultipathPacket(b, now); err != nil {
						t.Fatal(err)
					}
				}
				return b
			}
			if b := deliver(c, peer); len(b) != 3000 {
				t.Fatal("discovery probe", len(b))
			}
			now = now.Add(time.Millisecond)
			if b := deliver(peer, c); b == nil || a.mtu.confirmed != 3000 {
				t.Fatal("confirmed size missing")
			}
			linkLimit = 1200
			id, err := c.OpenStream(false)
			if err != nil {
				t.Fatal(err)
			}
			payload := make([]byte, 7500)
			for i := range payload {
				payload[i] = byte(i * 37)
			}
			for i := 0; i < 3; i++ {
				if n, err := c.WriteStream(id, payload[i*2500:(i+1)*2500], i == 2); err != nil || n != 2500 {
					t.Fatal("large admission", n, err)
				}
				now = now.Add(time.Millisecond)
				if b := deliver(c, peer); len(b) <= 1200 {
					t.Fatal("expected large dropped data", len(b))
				}
			}
			if dropped != 3 || a.mtu.confirmed != 3000 {
				t.Fatal("link loss bypassed recovery")
			}
			window := a.congestion.Window()
			now = a.timer.Deadline()
			if now.IsZero() {
				t.Fatal("no PTO after silent loss")
			}
			if err := c.expireMultipathRecovery(now); err != nil {
				t.Fatal(err)
			}
			if b := deliver(c, peer); b == nil || len(b) > 1200 {
				t.Fatal("PTO did not test smaller reachability")
			}
			now = now.Add(time.Millisecond)
			if b := deliver(peer, c); b == nil {
				t.Fatal("missing PTO ACK")
			}
			if a.mtu.confirmed != 1200 || a.mtu.upper != 1200 || a.mtuRaiseDeadline().IsZero() || a.congestion.Window() >= window {
				t.Fatal("real loss feedback did not trigger conservative fallback")
			}
			// The loss callback queued original items; ordinary packet construction
			// now splits them using capacity reserved before their first transmission.
			for i := 0; i < 300 && c.StreamAcknowledged(id) != nil; i++ {
				now = now.Add(time.Millisecond)
				if b := deliver(c, peer); b != nil && len(b) > 1200 {
					t.Fatal("fallback retransmission still too large")
				}
				now = now.Add(time.Millisecond)
				deliver(peer, c)
			}
			if err := c.StreamAcknowledged(id); err != nil {
				t.Fatal("transfer did not complete", err)
			}
			if c.queuedBytes != 0 || dropped != 3 {
				t.Fatal("memory leak or continued oversized transmission", c.queuedBytes, dropped)
			}
			got := make([]byte, len(payload))
			n, err := peer.ReadStream(id, got)
			if n != len(payload) || !bytes.Equal(got, payload) {
				t.Fatal("reassembly", n, err)
			}
			var end [1]byte
			if n, err := peer.ReadStream(id, end[:]); n != 0 || !errors.Is(err, io.EOF) {
				t.Fatal("missing reassembled FIN", n, err)
			}
		})
	}
}
