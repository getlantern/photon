package recovery

import (
	"testing"
	"time"
)

func TestMTUProbeRecoveryAccounting(t *testing.T) {
	now := time.Unix(1, 0)
	for _, lost := range []bool{false, true} {
		r, _ := NewSent(8)
		c, _ := NewCongestion(1200)
		probe := Packet{Number: 0, SentAt: now, Bytes: 1400, AckEliciting: true, InFlight: true, MTUProbe: true}
		if err := r.OnSent(Application, probe); err != nil {
			t.Fatal(err)
		}
		if r.BytesInFlight != 1400 || c.CanSend(c.Window(), 1400, true, false) {
			t.Fatal("MTU probe bypassed flight budget")
		}
		before := c.Window()
		if lost {
			// A later ACK makes the unacknowledged probe time-threshold lost.
			later := Packet{Number: 1, SentAt: now.Add(time.Second), Bytes: 40, AckEliciting: true, InFlight: true}
			if err := r.OnSent(Application, later); err != nil {
				t.Fatal(err)
			}
			events, err := r.OnACK(Application, ack(1, 1), now.Add(2*time.Second), 0, 0, true)
			if err != nil || len(events.Lost) != 1 || !events.Lost[0].MTUProbe {
				t.Fatal(events, err)
			}
			c.OnLoss(events.Lost, now.Add(2*time.Second))
			if c.Window() != before {
				t.Fatal("size probe reduced congestion window")
			}
			// Ordinary loss in the same batch must still reduce the window.
			c.OnLoss(append(events.Lost, later), now.Add(2*time.Second))
			if c.Window() != before/2 {
				t.Fatal("MTU probe hid ordinary loss")
			}
		} else {
			events, err := r.OnACK(Application, ack(0, 0), now.Add(time.Millisecond), 0, 0, true)
			if err != nil || len(events.Acked) != 1 || !events.Acked[0].MTUProbe {
				t.Fatal(events, err)
			}
			c.OnACK(ACKEvent{Acked: events.Acked})
			if c.Window() != before+1400 {
				t.Fatal("successful probe lost ACK accounting")
			}
		}
		if r.BytesInFlight != 0 {
			t.Fatal("probe flight accounting leaked", r.BytesInFlight)
		}
	}
}

func TestMTUProbeCannotAnchorPersistentCongestion(t *testing.T) {
	for _, ordinary := range []bool{false, true} {
		h := congestionHistory{capacity: 8}
		start := time.Unix(1, 0)
		var packets []Packet
		for i := 0; i < 3; i++ {
			p := Packet{Number: uint64(i), SentAt: start.Add(time.Duration(i) * time.Second), AckEliciting: true, MTUProbe: !ordinary || i == 1}
			packets = append(packets, p)
			h.sent(Application, p, true)
		}
		h.lost(Application, packets)
		if got := h.detect(time.Second); got != ordinary {
			t.Fatal("persistent congestion evidence", ordinary, got)
		}
	}
	// An acknowledged size probe still breaks the loss interval.
	h := congestionHistory{capacity: 8}
	for i := 0; i < 3; i++ {
		p := Packet{Number: uint64(i), SentAt: time.Unix(int64(i+1), 0), AckEliciting: true, MTUProbe: i == 1}
		h.sent(Application, p, true)
		h.lost(Application, []Packet{p})
	}
	h.ack(Application, ack(1, 1))
	if h.detect(time.Second) {
		t.Fatal("probe ACK did not break loss interval")
	}
}

func TestMTUProbeInvalidMetadata(t *testing.T) {
	for _, tc := range []struct {
		space     Space
		size      int
		eliciting bool
	}{{Initial, 1400, true}, {Handshake, 1400, true}, {Application, 1199, true}, {Application, 1400, false}} {
		r, _ := NewSent(2)
		if err := r.OnSent(tc.space, Packet{Bytes: tc.size, AckEliciting: tc.eliciting, InFlight: true, MTUProbe: true}); err == nil {
			t.Fatal("invalid probe accepted", tc)
		}
		if r.BytesInFlight != 0 {
			t.Fatal("invalid probe mutated flight")
		}
	}
}
