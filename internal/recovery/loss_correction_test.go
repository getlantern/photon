package recovery

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestLateACKLossCorrections(t *testing.T) {
	for _, space := range []Space{Initial, Handshake, Application} {
		for _, horizon := range []string{"retained", "discarded", "evicted"} {
			t.Run(fmt.Sprintf("%d/%s", space, horizon), func(t *testing.T) {
				r := tracker(t)
				r.history.capacity = 6
				start := time.Unix(100, 0)
				for pn := uint64(0); pn < 4; pn++ {
					if err := r.OnSent(space, Packet{Number: pn, SentAt: start, Bytes: int(100 + pn), AckEliciting: true, InFlight: true}); err != nil {
						t.Fatal(err)
					}
				}
				now := start.Add(time.Millisecond)
				events, err := r.OnACK(space, ack(3, 3), now, 0, 0, true)
				if err != nil || len(events.Lost) != 1 || events.Lost[0].Number != 0 || events.RecoveredPackets != 0 || events.RecoveredBytes != 0 {
					t.Fatal("initial loss", events, err)
				}
				// Reject the entire ACK before correcting any valid subrange.
				if events, err := r.OnACK(space, ack(0, 4), now, 0, 0, true); !errors.Is(err, ErrACK) || events.RecoveredPackets != 0 || events.RecoveredBytes != 0 {
					t.Fatal("invalid ACK changed correction state", events, err)
				}
				switch horizon {
				case "discarded":
					r.Discard(space)
				case "evicted":
					for pn := uint64(4); pn < 10; pn++ {
						sent(t, r, space, pn, now, true, true)
					}
				}
				flight := r.BytesInFlight
				for repeat := 0; repeat < 2; repeat++ {
					events, err := r.OnACK(space, ack(0, 0), now, 0, 0, true)
					var packets, bytes uint64
					if horizon == "retained" && repeat == 0 {
						packets, bytes = 1, 100
					}
					if err != nil || events.RecoveredPackets != packets || events.RecoveredBytes != bytes || len(events.Acked) != 0 || len(events.Lost) != 0 || r.BytesInFlight != flight {
						t.Fatal("late ACK accounting", repeat, events, err, flight, r.BytesInFlight)
					}
				}
			})
		}
	}
}

func TestLossCorrectionsSeparatePacketSpaces(t *testing.T) {
	r := tracker(t)
	start := time.Unix(100, 0)
	for _, space := range []Space{Initial, Handshake, Application} {
		for pn := uint64(0); pn < 4; pn++ {
			sent(t, r, space, pn, start, true, true)
		}
		if _, err := r.OnACK(space, ack(3, 3), start.Add(time.Millisecond), 0, 0, true); err != nil {
			t.Fatal(err)
		}
	}
	for _, space := range []Space{Application, Initial, Handshake} {
		events, err := r.OnACK(space, ack(0, 0), start.Add(time.Millisecond), 0, 0, true)
		if err != nil || events.RecoveredPackets != 1 || events.RecoveredBytes != 1200 {
			t.Fatal(space, events, err)
		}
	}
}

func TestLossCorrectionsCumulativeACKBeyondHistory(t *testing.T) {
	r := tracker(t)
	r.history.capacity = 6
	start := time.Unix(100, 0)
	for pn := uint64(0); pn < 8; pn++ {
		if err := r.OnSent(Application, Packet{Number: pn, SentAt: start, Bytes: int(100 + pn), AckEliciting: true, InFlight: true}); err != nil {
			t.Fatal(err)
		}
	}
	now := start.Add(time.Millisecond)
	events, err := r.OnACK(Application, ack(7, 7), now, 0, 0, true)
	if err != nil || len(events.Lost) != 5 {
		t.Fatal(events, err)
	}
	// The ACK range exceeds retained history, exercising the bounded map scan.
	// Lost packets 0 and 1 were evicted; 2, 3 and 4 remain correctable.
	events, err = r.OnACK(Application, ack(0, 6), now, 0, 0, true)
	if err != nil || events.RecoveredPackets != 3 || events.RecoveredBytes != 309 || len(events.Acked) != 2 || r.BytesInFlight != 0 {
		t.Fatal(events, err, r.BytesInFlight)
	}
	events, err = r.OnACK(Application, ack(0, 7), now, 0, 0, true)
	if err != nil || events.RecoveredPackets != 0 || events.RecoveredBytes != 0 {
		t.Fatal("duplicate cumulative correction", events, err)
	}
}
