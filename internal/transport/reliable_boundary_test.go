package transport

import (
	"crypto/tls"
	"errors"
	"testing"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func TestReliableBoundaryFullQueue(t *testing.T) {
	c, _, id := partialResetSendPair(t, wire.Version1, false)
	s := c.streams.objects[id]
	if !s.resetReserved || c.reservedResetBytes != 32 {
		t.Fatal("missing reservation")
	}
	if err := c.SetReliableBoundary(id); err != nil {
		t.Fatal(err)
	}
	if s.reliableBoundary != 16 {
		t.Fatal("boundary")
	}
	// Keep the prefix intact at the full admission budget. The reserved control
	// must fund cancellation even though no suffix can be discarded for space.
	pending := c.queuedBytes
	c.queuedBytes = maxSendBytes - c.reservedResetBytes
	if _, err := c.WriteStream(id, []byte("blocked"), false); !errors.Is(err, stream.ErrWouldBlock) {
		t.Fatal("write overcommitted reservation", err)
	}
	if err := c.CancelWriteReliable(id, 7); err != nil {
		t.Fatal(err)
	}
	if c.queuedBytes != maxSendBytes || c.reservedResetBytes != 0 || s.resetReserved || s.partialReset == nil || s.partialReset.ReliableSize != 16 {
		t.Fatal("reserved cancellation")
	}
	if err := c.SetReliableBoundary(id); err != nil {
		t.Fatal(err)
	}
	if err := c.CancelWriteReliable(id, 99); err != nil {
		t.Fatal(err)
	}
	if s.partialReset.ErrorCode != 7 || s.partialReset.ReliableSize != 16 || c.queuedBytes != maxSendBytes {
		t.Fatal("repeat cancellation changed reset")
	}
	c.queuedBytes = pending + 32
	for _, item := range c.spaces[applicationSpace].queue {
		acknowledgePartialTestItem(t, c, item)
	}
	if c.queuedBytes != 0 || !s.send.Complete() {
		t.Fatal("retirement", c.queuedBytes)
	}
}

func TestReliableBoundaryReservationLifecycle(t *testing.T) {
	for _, mode := range []string{"fin", "ordinary", "stop", "partial"} {
		t.Run(mode, func(t *testing.T) {
			c, s, id := partialResetSendPair(t, wire.Version2, false)
			if err := c.SetReliableBoundary(id); err != nil {
				t.Fatal(err)
			}
			if _, err := c.WriteStream(id, []byte("more"), false); err != nil {
				t.Fatal(err)
			}
			if err := c.SetReliableBoundary(id); err != nil {
				t.Fatal(err)
			}
			if c.streams.objects[id].reliableBoundary != 20 || c.reservedResetBytes != 32 {
				t.Fatal("boundary advance duplicated reservation")
			}
			switch mode {
			case "fin":
				if _, err := c.WriteStream(id, nil, true); err != nil {
					t.Fatal(err)
				}
			case "ordinary":
				if err := c.CancelWrite(id, 7); err != nil {
					t.Fatal(err)
				}
			case "stop":
				if err := c.cancelWrite(id, 7, true); err != nil {
					t.Fatal(err)
				}
			case "partial":
				if err := c.CancelWriteReliable(id, 7); err != nil {
					t.Fatal(err)
				}
			}
			drivePair(t, c, s, false, false)
			if c.reservedResetBytes != 0 || c.queuedBytes != 0 {
				t.Fatal("reservation leaked", c.reservedResetBytes, c.queuedBytes)
			}
		})
	}
}

func TestReliableBoundaryNewlyEnabledAfterEarlyWrite(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		cc, sc := testTLS(t)
		cc.ClientSessionCache = tls.NewLRUClientSessionCache(4)
		c, s := earlyPartialResetPair(t, v, cc, sc, false, true)
		drivePair(t, c, s, false, false)
		c, s = earlyPartialResetPair(t, v, cc, sc, true, true)
		id, err := c.OpenStream(true)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.WriteStream(id, []byte("early"), false); err != nil {
			t.Fatal(err)
		}
		if c.peerResetCapability() || c.reservedResetBytes != 32 {
			t.Fatal("early reservation or remembered capability")
		}
		drivePair(t, c, s, false, false)
		if !c.peerResetCapability() || !c.EarlyDataAccepted() {
			t.Fatal("new capability not installed")
		}
		if err := c.SetReliableBoundary(id); err != nil {
			t.Fatal(err)
		}
		c.queuedBytes = maxSendBytes - 32
		if err := c.CancelWriteReliable(id, 7); err != nil {
			t.Fatal("full queue cancellation", err)
		}
		if c.reservedResetBytes != 0 || c.queuedBytes != maxSendBytes || c.streams.objects[id].partialReset == nil {
			t.Fatal("reservation not consumed")
		}
	}
}

func TestReliableBoundaryServerBeforeHandshakeCompletion(t *testing.T) {
	c, s := testPair(t, wire.Version1, streamParameters())
	drivePair(t, c, s, false, false)
	// Synthetic ready-server state: test the same readiness gate used by an
	// early listener, without enabling public receive advertisement.
	s.complete = false
	s.peerParams.ResetStreamAt = true
	id, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteStream(id, []byte("server prefix"), false); err != nil {
		t.Fatal(err)
	}
	if err := s.SetReliableBoundary(id); err != nil {
		t.Fatal(err)
	}
	if err := s.CancelWriteReliable(id, 7); err != nil {
		t.Fatal(err)
	}
	if s.streams.objects[id].partialReset == nil {
		t.Fatal("early server abandoned reliable boundary")
	}
}
