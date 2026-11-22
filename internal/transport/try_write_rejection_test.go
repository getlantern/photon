package transport

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestTryWriteAllEarlyRejection(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, uni := range []bool{false, true} {
			for _, drop := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/uni=%t/drop=%t", v, uni, drop), func(t *testing.T) {
					cc, sc := testTLS(t)
					cc.ClientSessionCache = tls.NewLRUClientSessionCache(4)
					c, s := earlyPartialResetPair(t, v, cc, sc, true, true)
					drivePair(t, c, s, false, false)
					c, s = earlyPartialResetPair(t, v, cc, sc, true, false)
					if !c.EarlyReady() || c.complete {
						t.Fatal("no early generation")
					}
					id, err := c.OpenStream(uni)
					if err != nil {
						t.Fatal(err)
					}
					old := bytes.Repeat([]byte("old"), 800)
					if err := c.TryWriteStreamAll(id, old); err != nil {
						t.Fatal(err)
					}
					if _, err := c.WriteStream(id, nil, true); err != nil {
						t.Fatal(err)
					}
					items := append([]*sendItem(nil), c.spaces[applicationSpace].queue...)
					if len(items) < 3 || c.reservedResetBytes != partialResetControlCharge {
						t.Fatal("missing multi-frame early state")
					}
					earlyPackets := 0
					now := time.Unix(100, 0)
					for _, packet := range drain(t, c, now) {
						h, err := wire.ParseHeader(packet, 8)
						if err != nil {
							t.Fatal(err)
						}
						if h.Type == wire.ZeroRTT {
							earlyPackets++
							if drop {
								continue
							}
						}
						if err := s.HandleDatagram(packet, now); err != nil {
							t.Fatal(err)
						}
					}
					if earlyPackets == 0 {
						t.Fatal("no early packets")
					}
					for _, item := range items {
						found := false
						for _, mapped := range c.spaces[applicationSpace].sent {
							found = found || mapped == item
						}
						if !found || item.queued {
							t.Fatal("early frame did not leave")
						}
					}
					drivePair(t, c, s, true, false)
					if !c.ConnectionState().DidResume || !c.EarlyDataRejected() {
						t.Fatal("missing resumed rejection")
					}
					if len(c.streams.objects) != 0 || len(s.streams.objects) != 0 || c.streams.send.Used() != 0 || c.reservedResetBytes != 0 || c.queuedBytes != 0 {
						t.Fatal("rejected generation retained state")
					}
					for _, item := range items {
						if !item.acked {
							t.Fatal("old item still live")
						}
						for _, mapped := range c.spaces[applicationSpace].sent {
							if mapped == item {
								t.Fatal("old sent mapping survived")
							}
						}
					}
					fresh, err := c.OpenStream(uni)
					if err != nil || fresh != id {
						t.Fatal("fresh ID", fresh, err)
					}
					want := []byte("explicit fresh atomic write")
					if err := c.TryWriteStreamAll(fresh, want); err != nil {
						t.Fatal(err)
					}
					if _, err := c.WriteStream(fresh, nil, true); err != nil {
						t.Fatal(err)
					}
					drivePair(t, c, s, true, true)
					got := make([]byte, len(want)+1)
					if n, err := s.ReadStream(fresh, got); n != len(want) || !errors.Is(err, io.EOF) || !bytes.Equal(got[:n], want) {
						t.Fatal("fresh delivery", n, err)
					}
					if c.reservedResetBytes != 0 || c.queuedBytes != 0 {
						t.Fatal("fresh generation charge survived ACK")
					}
				})
			}
		}
	}
}
