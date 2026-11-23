package transport

import (
	"errors"
	"testing"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func TestStreamAcknowledgedOpenAndReset(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(version), func(t *testing.T) {
			client, server := testPair(t, version, streamParameters())
			drivePair(t, client, server, false, false)
			id, err := client.OpenStream(true)
			if err != nil {
				t.Fatal(err)
			}
			if err := client.StreamAcknowledged(id); err != nil {
				t.Fatal(err)
			}
			if _, err := client.WriteStream(id, []byte("first"), false); err != nil {
				t.Fatal(err)
			}
			if err := client.StreamAcknowledged(id); !errors.Is(err, stream.ErrWouldBlock) {
				t.Fatal("queued data acknowledged early", err)
			}
			drivePair(t, client, server, false, false)
			if err := client.StreamAcknowledged(id); err != nil {
				t.Fatal(err)
			}
			if _, err := client.WriteStream(id, []byte("second"), false); err != nil {
				t.Fatal("barrier closed send side", err)
			}
			if err := client.StreamAcknowledged(id); !errors.Is(err, stream.ErrWouldBlock) {
				t.Fatal(err)
			}
			if err := client.CancelWrite(id, 123); err != nil {
				t.Fatal(err)
			}
			var reset *StreamError
			if err := client.StreamAcknowledged(id); !errors.As(err, &reset) || reset.ErrorCode != 123 {
				t.Fatal("discarded bytes treated as delivered", err)
			}
		})
	}
}

func TestStreamAcknowledgedResetAfterFIN(t *testing.T) {
	for _, remote := range []bool{false, true} {
		c, s := testPair(t, wire.Version1, streamParameters())
		drivePair(t, c, s, false, false)
		id, err := c.OpenStream(true)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.WriteStream(id, []byte("discarded"), true); err != nil {
			t.Fatal(err)
		}
		if remote {
			err = c.handleStreamFrame(wire.StopSendingFrame{StreamID: id, ErrorCode: 42})
		} else {
			err = c.CancelWrite(id, 42)
		}
		if err != nil {
			t.Fatal(err)
		}
		var reset *StreamError
		if err := c.StreamAcknowledged(id); !errors.As(err, &reset) || reset.ErrorCode != 42 || reset.Remote != remote {
			t.Fatal("FIN hid ordinary reset", err)
		}
	}
}
