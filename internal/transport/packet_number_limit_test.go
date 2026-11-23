package transport

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestPacketNumberExhaustionSilent(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []string{"initial", "application", "close", "multipath", "multipath-close", "migration", "local-migration"} {
			t.Run(fmt.Sprintf("%x/%s", v, mode), func(t *testing.T) {
				var c, peer *Conn
				if mode == "multipath" || mode == "multipath-close" || mode == "migration" || mode == "local-migration" {
					c, peer = sendingPathPair(t, v)
				} else {
					c, peer = testPair(t, v)
					if mode != "initial" {
						drivePair(t, c, peer, false, false)
					}
				}
				now := time.Unix(200, 0)
				var data []byte
				var err error
				switch mode {
				case "initial":
					c.spaces[initialSpace].nextPN = wire.MaxVarint
					data, err = c.NextDatagram(now)
				case "application":
					c.spaces[applicationSpace].nextPN = wire.MaxVarint
					c.enqueue(applicationSpace, wire.PingFrame{}, 1)
					data, err = c.NextDatagram(now)
				case "close":
					c.spaces[applicationSpace].nextPN = wire.MaxVarint
					err = c.CloseTransportError(1, 0, "", now)
				case "multipath":
					app := c.multipath.paths[1].application
					app.space.nextPN = wire.MaxVarint
					app.budget.Validate()
					if err := c.validateMultipathPath(1); err != nil {
						t.Fatal(err)
					}
					data, err = c.nextPacketOnPath(1, now)
				case "migration", "local-migration":
					c.multipath.paths[1].application.space.nextPN = wire.MaxVarint
					candidate := &migrationCandidate{responsePending: &wire.PathResponseFrame{}}
					candidate.budget.Validate()
					data, _, err = c.sealMigrationProbe(1, candidate, now, mode == "local-migration")
				case "multipath-close":
					for _, state := range c.multipath.paths {
						if state.application != nil {
							state.application.space.nextPN = wire.MaxVarint
						}
					}
					err = c.CloseTransportError(1, 0, "", now)
				}
				if !errors.Is(err, wire.ErrPacketNumber) || len(data) != 0 || !c.closed || c.Closing() {
					t.Fatalf("exhaustion sent or retained connection: bytes=%d closed=%v error=%v", len(data), c.closed, err)
				}
				if b, _ := c.NextDatagram(now.Add(time.Second)); len(b) != 0 {
					t.Fatal("output after exhaustion")
				}
				if err := c.CloseTransportError(1, 0, "", now); !errors.Is(err, wire.ErrPacketNumber) {
					t.Fatal("terminal error lost", err)
				}
			})
		}
	}
}

func TestPacketNumberCloseBeforeLimit(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, peer := testPair(t, v)
		drivePair(t, c, peer, false, false)
		c.spaces[applicationSpace].nextPN = wire.MaxVarint - 1
		// Close packet serialization does not require fabricating 2^62 prior
		// recovery records to advance that separate contiguous-send ledger.
		frame := wire.ConnectionCloseFrame{ErrorCode: 1}
		if b, err := c.sealClosePacket(frame, applicationSpace); err != nil || len(b) == 0 {
			t.Fatal("last permitted packet", err)
		}
		if b, err := c.sealClosePacket(frame, applicationSpace); !errors.Is(err, wire.ErrPacketNumber) || len(b) != 0 || !c.closed {
			t.Fatal("limit not enforced", err)
		}
	}
}
