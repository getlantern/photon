package transport

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"syscall"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestBaseLargeWriteFallback(t *testing.T) {
	for _, borrowed := range []bool{false, true} {
		t.Run(fmt.Sprintf("borrowed=%t", borrowed), func(t *testing.T) { testBaseLargeWriteFallback(t, borrowed) })
	}
}

func testBaseLargeWriteFallback(t *testing.T, borrowed bool) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, datagram := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/datagram=%t", v, datagram), func(t *testing.T) {
				p := streamParameters()
				p.MaxDatagramFrameSize = 65535
				c, peer := testPair(t, v, p)
				c.config.InitialPacketSize = 1452
				// A rejected write mustn't cut the window, which only NewReno
				// would otherwise do.
				useNewReno(t, c)
				drivePair(t, c, peer, false, false)
				e, err := NewClientEndpoint(c, netip.MustParseAddrPort("127.0.0.1:443"))
				if err != nil {
					t.Fatal(err)
				}
				defer e.Close()
				var descriptor Outbound
				var metadata outboundBaseLarge
				next := e.NextDatagram
				if borrowed {
					next = func(at time.Time) (*Outbound, error) { return e.nextDatagramInto(at, &descriptor, nil, &metadata) }
				}
				payload := bytes.Repeat([]byte{43}, 1252)
				var id uint64
				if datagram {
					err = c.SendDatagram(payload)
					if err == nil {
						err = c.SendDatagram(payload)
					} // remains queued when the first write fails
				} else {
					id, err = c.OpenStream(false)
					if err != nil {
						t.Fatal(err)
					}
					var n int
					n, err = c.WriteStream(id, payload, true)
					if n != len(payload) {
						t.Fatal("admission", n)
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				now := time.Unix(101, 0)
				idle, window := c.idle, c.congestion.Window()
				// An explicit check queued behind data must not deadlock fallback.
				if err := c.ValidatePath(); err != nil {
					t.Fatal(err)
				}
				out, err := next(now)
				if err != nil || out == nil || out.baseLarge == nil || len(out.Data) <= 1200 {
					t.Fatal("base large metadata", err)
				}
				if borrowed && out.baseLarge != &metadata {
					t.Fatal("metadata not borrowed")
				}
				pn, sealed := out.baseLarge.number, c.sealed[applicationSpace]
				e.HandleWriteError(out, syscall.EMSGSIZE, now)
				if c.closed || !c.baseMTUNeedsValidation || c.basePacketPayloadLimit() != 1200 || c.recovery.BytesInFlight != 0 || c.congestion.Window() != window || c.idle != idle {
					t.Fatal("fallback accounting")
				}
				if c.spaces[applicationSpace].nextPN != pn+1 || c.sealed[applicationSpace] != sealed {
					t.Fatal("send counters rewound")
				}
				if _, err := c.recovery.OnACK(recovery.Application, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: pn, Largest: pn}}}, now, 0, 0, true); !errors.Is(err, recovery.ErrACK) {
					t.Fatal("rejected packet ACK", err)
				}
				if datagram {
					if c.pendingDatagrams != 1 || c.queuedBytes != len(payload)+1 {
						t.Fatal("failed message released the wrong queue accounting")
					}
					if err := c.SendDatagram([]byte("after fallback")); err != nil {
						t.Fatal(err)
					}
				}
				out, err = e.NextDatagram(now.Add(time.Millisecond))
				if err != nil || out == nil || len(out.Data) != 1200 || out.baseLarge != nil || c.baseMTUChallenge != nil || c.validation.state != PathValidating {
					t.Fatal("baseline challenge", err)
				}
				if blocked, err := e.NextDatagram(now.Add(1500 * time.Microsecond)); err != nil || blocked != nil {
					t.Fatal("application data bypassed pending baseline validation", err)
				}
				if err := peer.HandleDatagram(out.Data, now.Add(time.Millisecond)); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 30; i++ {
					now = now.Add(time.Millisecond)
					b, err := peer.NextDatagram(now)
					if err != nil {
						t.Fatal(err)
					}
					if b != nil {
						if err := c.HandleDatagram(b, now); err != nil {
							t.Fatal(err)
						}
					}
					out, err = next(now)
					if err != nil {
						t.Fatal(err)
					}
					if out != nil {
						if len(out.Data) > 1200 {
							t.Fatal("fallback exceeded baseline")
						}
						if err := peer.HandleDatagram(out.Data, now); err != nil {
							t.Fatal(err)
						}
					}
				}
				if c.baseMTUNeedsValidation || c.validation.state != PathValidated {
					t.Fatal("validation did not finish")
				}
				if datagram {
					got, err := peer.ReceiveDatagram()
					if err != nil || string(got) != "after fallback" || len(peer.datagrams.queue) != 0 {
						t.Fatal("datagram fallback delivery", len(got), err)
					}
				} else {
					got := make([]byte, len(payload))
					n, err := peer.ReadStream(id, got)
					if n != len(payload) || !bytes.Equal(got, payload) || (err != nil && err != io.EOF) {
						t.Fatal("stream fallback delivery", n, err)
					}
				}
			})
		}
	}
}

func TestBaseMTUChallengeBypassesFullDataQueue(t *testing.T) {
	c, peer := testPair(t, wire.Version1, streamParameters())
	c.config.InitialPacketSize = 1452
	drivePair(t, c, peer, false, false)
	e, err := NewClientEndpoint(c, netip.MustParseAddrPort("127.0.0.1:443"))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	id, err := c.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := c.WriteStream(id, make([]byte, 1252), false); n != 1252 || err != nil {
		t.Fatal(n, err)
	}
	now := time.Unix(101, 0)
	out, err := e.NextDatagram(now)
	if err != nil || out == nil {
		t.Fatal(err)
	}
	c.queuedBytes = maxSendBytes
	e.HandleWriteError(out, syscall.EMSGSIZE, now)
	out, err = e.NextDatagram(now.Add(time.Millisecond))
	if err != nil || out == nil || len(out.Data) != 1200 || c.queuedBytes != maxSendBytes || c.closed {
		t.Fatal("full data queue blocked baseline probe", err)
	}
}

func TestBaseMTUValidationFailure(t *testing.T) {
	c, peer := testPair(t, wire.Version1, streamParameters())
	c.config.InitialPacketSize = 1452
	drivePair(t, c, peer, false, false)
	e, err := NewClientEndpoint(c, netip.MustParseAddrPort("127.0.0.1:443"))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	id, err := c.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteStream(id, make([]byte, 1252), false); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(101, 0)
	out, err := e.NextDatagram(now)
	if err != nil || out == nil {
		t.Fatal(err)
	}
	e.HandleWriteError(out, syscall.EMSGSIZE, now)
	out, err = e.NextDatagram(now.Add(time.Millisecond))
	if err != nil || out == nil {
		t.Fatal(err)
	}
	expiry := c.validation.expires
	if expiry.IsZero() {
		t.Fatal("baseline validation has no deadline")
	}
	_, err = c.NextDatagram(expiry)
	var failure *TransportError
	if !errors.As(err, &failure) || failure.Code != 0x10 || c.baseMTUChallenge != nil {
		t.Fatal("unusable baseline not failed", err)
	}
}
