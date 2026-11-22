package transport

import (
	"errors"
	"github.com/getlantern/photon/internal/wire"
	"net/netip"
	"testing"
	"time"
)

func TestMigrationResponseQueue(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, peer := sendingPathPair(t, v)
		now := time.Unix(101, 0)
		tuple := pathTuple{netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.2:443")}
		candidate, err := newUncreditedMigrationCandidate(tuple, now, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		for i := byte(0); i < 8; i++ {
			if err := candidate.queueResponse(wire.PathResponseFrame{Data: [8]byte{i}}); err != nil {
				t.Fatal(err)
			}
		}
		head, tail := candidate.responsePending, candidate.responseTail
		if err := candidate.queueResponse(wire.PathResponseFrame{}); !errors.Is(err, ErrSendBuffer) || candidate.responsePending != head || candidate.responseTail != tail {
			t.Fatal("overflow changed response queue")
		}
		a := c.multipath.paths[1].application
		if b, _, err := c.sealMigrationProbe(1, candidate, now, true); err != nil || b != nil || candidate.responsePending != head || a.space.nextPN != 0 {
			t.Fatal("blocked response consumed", err)
		}
		candidate.budget.Validate()
		for i := byte(0); i < 8; i++ {
			b, got, err := c.sealMigrationProbe(1, candidate, now, true)
			if err != nil || len(b) != 1200 || got != tuple {
				t.Fatal("response send", len(b), err)
			}
			decoded, err := peer.decodeMultipathPacket(b, now)
			if err != nil || decoded == nil {
				t.Fatal(err)
			}
			f, ok := decoded.frames[0].(wire.PathResponseFrame)
			if !ok || f.Data != ([8]byte{i}) {
				t.Fatal("token order")
			}
			now = now.Add(time.Millisecond)
		}
		if candidate.responsePending != nil || candidate.responseTail.count != 0 {
			t.Fatal("responses retained")
		}
		if err := candidate.queueResponse(wire.PathResponseFrame{}); err != nil {
			t.Fatal(err)
		}
		if !candidate.expire(candidate.validation.expires) || candidate.responsePending != nil || candidate.responseTail.count != 0 {
			t.Fatal("expiration retained queue")
		}
	}
}
