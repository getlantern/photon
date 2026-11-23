package transport

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func TestEngineDatagrams(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(v), func(t *testing.T) {
			p := wire.DefaultTransportParameters() // No stream flow credit.
			p.MaxDatagramFrameSize = 65535
			client, server := testPair(t, v, p)
			drivePair(t, client, server, false, false)
			data := []byte("message")
			if err := client.SendDatagram(data); err != nil {
				t.Fatal(err)
			}
			data[0] = 'X'
			if err := client.SendDatagram(nil); err != nil {
				t.Fatal(err)
			}
			drivePair(t, client, server, true, false) // Includes packet duplication.
			got, err := server.ReceiveDatagram()
			if err != nil || len(got) != 0 {
				t.Fatal(got, err)
			} // Reversed order.
			got, err = server.ReceiveDatagram()
			if err != nil || string(got) != "message" {
				t.Fatal(string(got), err)
			}
			if _, err := server.ReceiveDatagram(); !errors.Is(err, stream.ErrWouldBlock) {
				t.Fatal(err)
			}
			if client.pendingDatagrams != 0 || client.HasOutstanding() {
				t.Fatal("retained sent payload")
			}
			if err := server.SendDatagram(bytes.Repeat([]byte{7}, 1158)); err != nil {
				t.Fatal(err)
			}
			if err := server.SendDatagram(make([]byte, 1159)); !errors.Is(err, ErrDatagramTooLarge) {
				t.Fatal(err)
			}
			drivePair(t, client, server, false, false)
			got, err = client.ReceiveDatagram()
			if err != nil || len(got) != 1158 {
				t.Fatal(len(got), err)
			}
		})
	}
}

func TestDatagramLossDoesNotRetransmit(t *testing.T) {
	p := wire.DefaultTransportParameters()
	p.MaxDatagramFrameSize = 100
	c, s := testPair(t, wire.Version1, p)
	drivePair(t, c, s, false, false)
	now := time.Unix(200, 0)
	if err := c.SendDatagram([]byte("lost")); err != nil {
		t.Fatal(err)
	}
	b, err := c.NextDatagram(now)
	if err != nil || len(b) == 0 {
		t.Fatal(err)
	}
	if c.recovery.BytesInFlight != uint64(len(b)) || c.HasOutstanding() {
		t.Fatal("flight or payload accounting")
	}
	deadline := c.RecoveryDeadline()
	if !deadline.After(now) {
		t.Fatal("missing PTO")
	}
	if err := c.HandleTimeout(deadline); err != nil {
		t.Fatal(err)
	}
	b, err = c.NextDatagram(deadline)
	if err != nil || len(b) == 0 {
		t.Fatal("missing probe", err)
	}
	if err := s.HandleDatagram(b, deadline); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReceiveDatagram(); !errors.Is(err, stream.ErrWouldBlock) {
		t.Fatal("retransmitted datagram", err)
	}
}

func TestDatagramLimits(t *testing.T) {
	p := wire.DefaultTransportParameters()
	p.MaxDatagramFrameSize = 2
	c, s := testPair(t, wire.Version1, p)
	drivePair(t, c, s, false, false)
	if err := c.SendDatagram([]byte{1}); err != nil {
		t.Fatal(err)
	}
	requireDatagramLimit(t, c.SendDatagram([]byte{1, 2}), 1)
	for i := 1; i < maxBufferedDatagrams; i++ {
		if err := c.SendDatagram(nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.SendDatagram(nil); !errors.Is(err, stream.ErrWouldBlock) || c.err != nil {
		t.Fatal(err, c.err)
	}
	c.peerParams.MaxDatagramFrameSize = 0
	if err := c.SendDatagram(nil); !errors.Is(err, ErrDatagramUnsupported) {
		t.Fatal(err)
	}
	// Inject a forbidden frame through the protected packet path.
	c.spaces[applicationSpace].queue = nil
	c.queuedBytes = 0
	c.enqueue(applicationSpace, wire.DatagramFrame{Data: []byte{1, 2}}, 3)
	b, err := c.NextDatagram(time.Unix(200, 0))
	if err != nil {
		t.Fatal(err)
	}
	err = s.HandleDatagram(b, time.Unix(200, 0))
	var te *TransportError
	if !errors.As(err, &te) || te.Code != 0xa {
		t.Fatal(err)
	}
}

func TestDatagramReceiveBuffer(t *testing.T) {
	c, s := testPair(t, wire.Version1)
	drivePair(t, c, s, false, false)
	data := bytes.Repeat([]byte{1}, 1024)
	for i := 0; i < maxBufferedDatagrams+1; i++ {
		c.receiveDatagram(data)
	}
	data[0] = 2
	if len(c.datagrams.queue) != maxBufferedDatagrams || c.datagrams.bytes != maxDatagramBytes {
		t.Fatal("buffer limit")
	}
	got, err := c.ReceiveDatagram()
	if err != nil || got[0] != 1 {
		t.Fatal("ownership", err)
	}
	c.receiveDatagram(make([]byte, 1025))
	if len(c.datagrams.queue) != maxBufferedDatagrams-1 {
		t.Fatal("byte limit")
	}
	c.Close()
	if c.datagrams.bytes != 0 || c.datagrams.queue != nil {
		t.Fatal("close retained data")
	}
}
