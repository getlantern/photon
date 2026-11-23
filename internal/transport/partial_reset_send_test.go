package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

// These engine tests negotiate receive support through TLS.
func partialResetSendPair(t *testing.T, version wire.Version, fin bool) (*Conn, *Conn, uint64) {
	t.Helper()
	p := streamParameters()
	p.ResetStreamAt = true
	c, s := testPair(t, version, p)
	drivePair(t, c, s, false, false)
	id, err := c.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := c.WriteStream(id, []byte("abcdefgh"), false); n != 8 || err != nil {
		t.Fatal(n, err)
	}
	if n, err := c.WriteStream(id, []byte("ijklmnop"), fin); n != 8 || err != nil {
		t.Fatal(n, err)
	}
	return c, s, id
}

func acknowledgePartialTestItem(t *testing.T, c *Conn, item *sendItem) {
	t.Helper()
	if item.acked {
		return
	}
	if err := acknowledgeSendItem(item, func(root *sendItem) error {
		if err := c.ackStreamFrame(root.frame); err != nil {
			return err
		}
		c.queuedBytes -= root.bytes
		c.recycleStreamItem(root)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPartialResetSendACKOrders(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, resetFirst := range []bool{false, true} {
			c, _, id := partialResetSendPair(t, v, true)
			if err := c.CancelWriteAt(id, 7, 12); err != nil {
				t.Fatal(err)
			}
			state := c.streams.objects[id]
			var resetError *StreamError
			if err := c.StreamAcknowledged(id); !errors.As(err, &resetError) || resetError.ErrorCode != 7 || resetError.Remote {
				t.Fatal("FIN hid partial reset", err)
			}
			released := 0
			if err := c.AutoReleaseStream(id, func(err error) {
				if err != nil {
					t.Fatal(err)
				}
				released++
			}); err != nil {
				t.Fatal(err)
			}
			var data []*sendItem
			var reset *sendItem
			for _, item := range c.spaces[applicationSpace].queue {
				switch item.frame.(type) {
				case wire.StreamFrame:
					data = append(data, item)
				case wire.ResetStreamAtFrame:
					reset = item
				}
			}
			if len(data) != 2 || reset == nil || state.outstandingData != 2 {
				t.Fatal("partial queue")
			}
			if resetFirst {
				acknowledgePartialTestItem(t, c, reset)
			}
			// A higher-offset ACK cannot cover the first write's missing bytes.
			acknowledgePartialTestItem(t, c, data[1])
			if state.send.Complete() || released != 0 {
				t.Fatal("prefix hole retired stream")
			}
			acknowledgePartialTestItem(t, c, data[0])
			if !resetFirst {
				if state.send.Complete() || released != 0 {
					t.Fatal("old FIN flag bypassed reset ACK")
				}
				acknowledgePartialTestItem(t, c, reset)
			}
			if !state.send.Complete() || released != 1 || c.streams.objects[id] != nil || c.queuedBytes != 0 {
				t.Fatal("incomplete retirement", released, c.queuedBytes)
			}
		}
	}
}

func TestPartialResetSendReductionAndStop(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, _, id := partialResetSendPair(t, v, false)
		if err := c.CancelWriteAt(id, 7, 12); err != nil {
			t.Fatal(err)
		}
		state := c.streams.objects[id]
		var stale *sendItem
		for _, item := range c.spaces[applicationSpace].queue {
			if _, ok := item.frame.(wire.ResetStreamAtFrame); ok {
				stale = item
			}
		}
		before := c.queuedBytes
		if err := c.CancelWriteAt(id, 99, 12); err != nil || c.queuedBytes != before {
			t.Fatal("duplicate reset", err)
		}
		if err := c.CancelWriteAt(id, 99, 4); err != nil {
			t.Fatal(err)
		}
		if !stale.acked || state.outstandingData != 1 || state.partialReset.ErrorCode != 7 || state.partialReset.ReliableSize != 4 {
			t.Fatal("reduction/supersession")
		}
		acknowledgePartialTestItem(t, c, stale)
		if state.send.Complete() {
			t.Fatal("stale ACK completed reset")
		}
		if err := c.handleStreamFrame(wire.StopSendingFrame{StreamID: id, ErrorCode: 55}); err != nil {
			t.Fatal(err)
		}
		if state.partialReset != nil || state.outstandingData != 0 {
			t.Fatal("STOP retained prefix")
		}
		var reset *sendItem
		for _, item := range c.spaces[applicationSpace].queue {
			if item.acked {
				continue
			}
			f, ok := item.frame.(wire.ResetStreamFrame)
			if !ok || f.StreamID != id || f.ErrorCode != 7 || f.FinalSize != 16 {
				t.Fatalf("ordinary fallback: %#v", item.frame)
			}
			reset = item
		}
		if reset == nil || state.send.Complete() {
			t.Fatal("ordinary reset not pending")
		}
		acknowledgePartialTestItem(t, c, reset)
		if !state.send.Complete() {
			t.Fatal("ordinary reset ACK")
		}
	}
}

func TestPartialResetSendAdmission(t *testing.T) {
	c, _, id := partialResetSendPair(t, wire.Version1, false)
	c.peerParams.ResetStreamAt = false
	if err := c.CancelWriteAt(id, 7, 4); !errors.Is(err, ErrUnsupportedFrame) {
		t.Fatal(err)
	}
	c.peerParams.ResetStreamAt = true
	state := c.streams.objects[id]
	before := c.queuedBytes
	c.queuedBytes = maxSendBytes
	if err := c.CancelWriteAt(id, 7, 4); !errors.Is(err, ErrSendBuffer) {
		t.Fatal(err)
	}
	if state.partialReset != nil || state.resetQueued || state.outstandingData != 2 || c.queuedBytes != maxSendBytes {
		t.Fatal("failed admission mutated state")
	}
	c.queuedBytes = before
	if n, err := c.WriteStream(id, []byte("q"), false); n != 1 || err != nil {
		t.Fatal("failed reset closed writing", n, err)
	}
	if err := c.CancelWriteAt(id, 7, 18); !errors.Is(err, stream.ErrFinalSize) {
		t.Fatal(err)
	}
	if err := c.CancelWriteAt(id, 7, 0); err != nil {
		t.Fatal(err)
	}
	if state.outstandingData != 0 || state.send.Complete() {
		t.Fatal("zero prefix requires reset ACK")
	}
}

func TestPartialResetProtectedTransfer(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(v), func(t *testing.T) {
			c, s, id := partialResetSendPair(t, v, true)
			if err := c.CancelWriteAt(id, 7, 12); err != nil {
				t.Fatal(err)
			}
			drivePair(t, c, s, true, true)
			if !c.streams.objects[id].send.Complete() {
				t.Fatal("reset/prefix not ACKed")
			}
			if got, err := s.AcceptStream(true); got != id || err != nil {
				t.Fatal(got, err)
			}
			b := make([]byte, 32)
			n, err := s.ReadStream(id, b)
			var reset *StreamError
			if n != 12 || string(b[:n]) != "abcdefghijkl" || !errors.As(err, &reset) || reset.ErrorCode != 7 || !reset.Remote {
				t.Fatal(n, string(b[:n]), err)
			}
			if c.streams.send.Used() != 16 || s.streams.receive.Charged() != 16 || s.streams.receive.Consumed() != 16 {
				t.Fatal("partial reset changed unique-byte credit")
			}
		})
	}
}

func TestPartialResetAfterPrimaryPathDisposal(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(version), func(t *testing.T) {
			c, s := sendingPathPair(t, version)
			c.peerParams.ResetStreamAt = true
			s.config.Parameters.ResetStreamAt = true
			now := time.Unix(101, 0)
			for _, conn := range []*Conn{c, s} {
				app := conn.multipath.paths[1].application
				app.budget.Validate()
				app.validation.state = PathValidated
			}
			if err := c.abandonFailedPath(0); err != nil {
				t.Fatal(err)
			}
			if _, err := c.multipath.receiveAbandon(wire.PathAbandonFrame{PathID: 0}, now, time.Millisecond); err != nil {
				t.Fatal(err)
			}
			if err := c.expireMultipathPaths(now.Add(3 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			id, err := c.OpenStream(true)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := c.WriteStream(id, []byte("abcdefghijklmnop"), true); n != 16 || err != nil {
				t.Fatal(n, err)
			}
			if err := c.CancelWriteAt(id, 7, 8); err != nil {
				t.Fatal(err)
			}
			for round := 0; round < 60; round++ {
				now = now.Add(time.Millisecond)
				for _, pair := range [][2]*Conn{{c, s}, {s, c}} {
					for i := 0; i < 20; i++ {
						b, err := pair[0].nextPacketOnPath(1, now)
						if err != nil {
							t.Fatal(err)
						}
						if b == nil {
							break
						}
						if err := pair[1].handleMultipathPacket(b, now); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			if !c.streams.objects[id].send.Complete() {
				t.Fatal("path ACK did not retire partial reset")
			}
			if got, err := s.AcceptStream(true); got != id || err != nil {
				t.Fatal(got, err)
			}
			b := make([]byte, 32)
			n, err := s.ReadStream(id, b)
			var reset *StreamError
			if n != 8 || string(b[:n]) != "abcdefgh" || !errors.As(err, &reset) || reset.ErrorCode != 7 {
				t.Fatal(n, err)
			}
		})
	}
}

func TestPartialResetAcknowledgedPrefixReduction(t *testing.T) {
	c, s, id := partialResetSendPair(t, wire.Version1, false)
	drivePair(t, c, s, false, false)
	state := c.streams.objects[id]
	if state.outstandingData != 0 || state.send.Complete() {
		t.Fatal("open acknowledged stream")
	}
	if err := c.CancelWriteAt(id, 7, 12); err != nil {
		t.Fatal(err)
	}
	var old *sendItem
	for _, item := range c.spaces[applicationSpace].queue {
		if _, ok := item.frame.(wire.ResetStreamAtFrame); ok {
			old = item
		}
	}
	if old == nil {
		t.Fatal("missing reset")
	}
	// All prefix data is already acknowledged; the superseded control's own
	// charge must fund the replacement even if unrelated streams fill the queue.
	c.queuedBytes = maxSendBytes
	if err := c.CancelWriteAt(id, 99, 0); err != nil {
		t.Fatal(err)
	}
	if c.queuedBytes != maxSendBytes || !old.acked || state.send.Complete() {
		t.Fatal("control supersession accounting")
	}
	var current *sendItem
	for _, item := range c.spaces[applicationSpace].queue {
		if f, ok := item.frame.(wire.ResetStreamAtFrame); ok && !item.acked {
			if f.ErrorCode != 7 || f.ReliableSize != 0 {
				t.Fatal(f)
			}
			current = item
		}
	}
	if current == nil {
		t.Fatal("missing replacement")
	}
	acknowledgePartialTestItem(t, c, old)
	if state.send.Complete() {
		t.Fatal("old reset acknowledged new boundary")
	}
	acknowledgePartialTestItem(t, c, current)
	if !state.send.Complete() || c.queuedBytes != maxSendBytes-32 {
		t.Fatal("replacement ACK")
	}
}
