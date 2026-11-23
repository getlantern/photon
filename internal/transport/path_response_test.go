package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestPathChallengeResponse(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, limited := range []bool{false, true} {
			client, server := testPair(t, v)
			drivePair(t, client, server, false, false)
			now := time.Unix(200, 0)
			token := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
			raw := makePeerPacket(t, client, applicationSpace, 1000, wire.PathChallengeFrame{Data: token})
			if limited {
				server.budget = Amplification{}
			}
			if err := server.HandleDatagram(raw, now); err != nil {
				t.Fatal(err)
			}
			// Replaying the same protected packet must not create another response.
			if err := server.HandleDatagram(raw, now); err != nil {
				t.Fatal(err)
			}
			responses := 0
			for _, b := range drain(t, server, now) {
				h, err := wire.ParseHeader(b, len(client.config.LocalCID))
				if err != nil {
					t.Fatal(err)
				}
				_, plain, _, err := client.readKeys(applicationSpace).Open(b, h.PNOffset, client.spaces[applicationSpace].received.Expected())
				if err != nil {
					t.Fatal(err)
				}
				f, _, err := wire.ConsumeFrame(plain)
				if err != nil {
					t.Fatal(err)
				}
				if response, ok := f.(wire.PathResponseFrame); ok {
					responses++
					if response.Data != token {
						t.Fatal("wrong echo")
					}
					if (!limited && len(b) != 1200) || (limited && len(b) >= 1200) {
						t.Fatal("padding or amplification", len(b))
					}
				}
			}
			if responses != 1 || server.HasOutstanding() {
				t.Fatal("response count or reliability", responses)
			}
			if limited && server.budget.Validated() {
				t.Fatal("challenge validated address")
			}
			// A PTO may send a PING; the original response must not be retransmitted.
			at := server.RecoveryDeadline()
			if err := server.HandleTimeout(at); err != nil {
				t.Fatal(err)
			}
			for _, item := range server.spaces[applicationSpace].queue {
				if _, ok := item.frame.(wire.PathResponseFrame); ok {
					t.Fatal("response retransmitted")
				}
			}
		}
	}
}

func TestUnsolicitedPathResponseDoesNotValidate(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	server.budget = Amplification{}
	raw := makePeerPacket(t, client, applicationSpace, 1000, wire.PathResponseFrame{Data: [8]byte{9}})
	if err := server.HandleDatagram(raw, time.Unix(200, 0)); err != nil {
		t.Fatal(err)
	}
	if server.budget.Validated() {
		t.Fatal("unsolicited response validated address")
	}
}
