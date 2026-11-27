package transport

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func TestTryWriteAllResetReservation(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, uni := range []bool{false, true} {
			for _, mode := range []string{"established", "early", "multipath"} {
				t.Run(fmt.Sprintf("%x/uni=%t/%s", v, uni, mode), func(t *testing.T) {
					var c, s *Conn
					if mode == "multipath" {
						p := streamParameters()
						p.ResetStreamAt = true
						c, s = sendingPathPair(t, v, p)
						for _, conn := range []*Conn{c, s} {
							app := conn.multipath.paths[1].application
							app.validation.state = PathValidated
							app.budget.Validate()
						}
						if err := c.abandonFailedPath(0); err != nil {
							t.Fatal(err)
						}
					} else {
						cc, sc := testTLS(t)
						cc.ClientSessionCache = tls.NewLRUClientSessionCache(4)
						c, s = earlyPartialResetPair(t, v, cc, sc, true, true)
						drivePair(t, c, s, false, false)
						if mode == "early" {
							c, s = earlyPartialResetPair(t, v, cc, sc, true, true)
							if !c.EarlyReady() || c.complete {
								t.Fatal("missing early keys")
							}
						}
					}
					id, err := c.OpenStream(uni)
					if err != nil {
						t.Fatal(err)
					}
					chunk := portableStreamPayload
					if mode == "early" {
						chunk = portableEarlyStreamPayload
					}
					data := bytes.Repeat([]byte{37}, 2*chunk+1)
					frameCharge := len(data) + 3*32
					need := frameCharge + partialResetControlCharge
					originalCharge := c.queuedBytes
					c.queuedBytes = c.streamSendLimit() - need + 1
					before := c.queuedBytes
					if err := c.TryWriteStreamAll(id, data); !errors.Is(err, stream.ErrWouldBlock) {
						t.Fatal("missing reservation rejection", err)
					}
					st := c.streams.objects[id]
					if c.queuedBytes != before || c.reservedResetBytes != 0 || st.resetReserved || st.send.Offset() != 0 || c.streams.send.Used() != 0 || st.outstandingData != 0 {
						t.Fatal("reservation rejection mutated state")
					}
					c.queuedBytes--
					if err := c.TryWriteStreamAll(id, data); err != nil {
						t.Fatal("exact budget", err)
					}
					if c.availableStreamBytes() != 0 || c.reservedResetBytes != partialResetControlCharge || !st.resetReserved || st.outstandingData != 3 {
						t.Fatal("reservation accounting")
					}
					// Existing reservations must not be charged a second time on this stream.
					c.queuedBytes -= 33
					if err := c.TryWriteStreamAll(id, []byte{38}); err != nil {
						t.Fatal("existing reservation", err)
					}
					if c.availableStreamBytes() != 0 || c.reservedResetBytes != partialResetControlCharge || st.outstandingData != 4 {
						t.Fatal("duplicated reservation")
					}
					// Remove only the synthetic unrelated charge before packet processing.
					c.queuedBytes = originalCharge + frameCharge + 33
					if _, err := c.WriteStream(id, nil, true); err != nil {
						t.Fatal(err)
					}
					if mode == "multipath" {
						now := time.Unix(101, 0)
						for i := 0; i < 20; i++ {
							for _, pair := range [][2]*Conn{{c, s}, {s, c}} {
								for {
									packet, err := pair[0].nextPacketOnPath(1, now)
									if err != nil {
										t.Fatal(err)
									}
									if packet == nil {
										break
									}
									if err := pair[1].handleMultipathPacket(packet, now); err != nil {
										t.Fatal(err)
									}
								}
							}
							now = now.Add(10 * time.Millisecond)
						}
					} else {
						drivePair(t, c, s, true, true)
					}
					if mode == "early" && (!c.ConnectionState().DidResume || !c.tls.EarlyDataAccepted()) {
						t.Fatal("missing accepted resumption")
					}
					want := append(bytes.Clone(data), 38)
					got := make([]byte, len(want))
					n, err := s.ReadStream(id, got)
					if n != len(want) || !errors.Is(err, io.EOF) || !bytes.Equal(got, want) {
						t.Fatal("reserved atomic delivery", n, err)
					}
					if c.reservedResetBytes != 0 {
						t.Fatal("FIN ACK retained reservation")
					}
				})
			}
		}
	}
}
