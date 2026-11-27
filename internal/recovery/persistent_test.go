package recovery

import (
	"testing"
	"time"
)

func TestPersistentCongestionHistory(t *testing.T) {
	for _, tc := range []struct {
		name    string
		middle  uint8
		sampled bool
		want    bool
	}{
		{"loss across spaces", 1, true, true},
		{"ack across spaces", 2, true, false},
		{"unresolved barrier", 0, true, false},
		{"no prior RTT", 1, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := congestionHistory{capacity: 10}
			start := time.Unix(1, 0)
			for i := 0; i < 3; i++ {
				h.sent(Space(i), Packet{Number: 0, SentAt: start.Add(time.Duration(i) * time.Second), AckEliciting: true}, tc.sampled)
			}
			h.lost(Initial, []Packet{{Number: 0}})
			if h.detect(time.Second) {
				t.Fatal("one lost packet")
			}
			if tc.middle == 1 {
				h.lost(Handshake, []Packet{{Number: 0}})
			}
			if tc.middle == 2 {
				h.ack(Handshake, ack(0, 0), []Packet{{Number: 0}})
			}
			h.lost(Application, []Packet{{Number: 0}})
			if got := h.detect(time.Second); got != tc.want {
				t.Fatal(got, tc.want)
			}
			if tc.want && h.detect(time.Second) {
				t.Fatal("reported interval reused")
			}
		})
	}
}

func TestPersistentLateACKAndDiscard(t *testing.T) {
	for _, discard := range []bool{false, true} {
		h := congestionHistory{capacity: 10}
		start := time.Unix(1, 0)
		for i := 0; i < 3; i++ {
			h.sent(Space(i), Packet{Number: 0, SentAt: start.Add(time.Duration(i) * time.Second), AckEliciting: true}, true)
			h.lost(Space(i), []Packet{{Number: 0}})
		}
		if discard {
			h.discard(Handshake)
		} else {
			h.ack(Handshake, ack(0, 0), []Packet{{Number: 0}})
		}
		if h.detect(time.Second) {
			t.Fatal("ACK or key discard did not split interval")
		}
	}
}

func TestPersistentDurationAndEndpoints(t *testing.T) {
	for _, eliciting := range []bool{false, true} {
		h := congestionHistory{capacity: 10}
		start := time.Unix(1, 0)
		for i := 0; i < 2; i++ {
			h.sent(Application, Packet{Number: uint64(i), SentAt: start.Add(time.Duration(i) * time.Second), AckEliciting: eliciting}, true)
		}
		h.lost(Application, []Packet{{Number: 0}, {Number: 1}})
		if h.detect(time.Second) {
			t.Fatal("duration must strictly exceed threshold")
		}
		if got := h.detect(time.Second - time.Nanosecond); got != eliciting {
			t.Fatal("requires ack-eliciting endpoints", got)
		}
	}
	h := congestionHistory{capacity: 2}
	start := time.Unix(1, 0)
	for i := 0; i < 3; i++ {
		h.sent(Application, Packet{Number: uint64(i), SentAt: start.Add(time.Duration(i) * time.Second), AckEliciting: true}, true)
	}
	h.lost(Application, []Packet{{Number: 0}, {Number: 1}, {Number: 2}})
	if h.detect(time.Second) {
		t.Fatal("evicted history used as evidence")
	}
	if len(h.index) != 2 {
		t.Fatal("history not bounded")
	}
}

func TestPersistentCongestionFromACK(t *testing.T) {
	r, _ := NewSent(20)
	r.RTT.Update(10*time.Millisecond, 0, 0, true)
	start := time.Unix(1, 0)
	for i, offset := range []time.Duration{0, time.Second, 2 * time.Second, 3 * time.Second} {
		if err := r.OnSent(Application, Packet{Number: uint64(i), SentAt: start.Add(offset), Bytes: 1200, AckEliciting: true, InFlight: true}); err != nil {
			t.Fatal(err)
		}
	}
	ev, err := r.OnACK(Application, ack(3, 3), start.Add(3*time.Second+10*time.Millisecond), 0, 25*time.Millisecond, true)
	if err != nil || len(ev.Lost) != 3 {
		t.Fatal(ev, err)
	}
	if !r.PersistentCongestion(25 * time.Millisecond) {
		t.Fatal("missing persistent congestion")
	}
	c, _ := NewCongestion(1200)
	c.OnLoss(LossEvent{Lost: ev.Lost, Now: start.Add(4 * time.Second)})
	c.OnPersistentCongestion()
	if c.Window() != 2400 {
		t.Fatal(c.Window())
	}
	// Persistent congestion ends the recovery epoch (RFC 9002 Appendix B.8).
	c.OnACK(ACKEvent{Acked: ev.Acked})
	if c.Window() != 3600 {
		t.Fatal(c.Window())
	}
}

func TestPersistentRequiresPriorSample(t *testing.T) {
	r, _ := NewSent(20)
	start := time.Unix(1, 0)
	for i := 0; i < 4; i++ {
		r.OnSent(Application, Packet{Number: uint64(i), SentAt: start.Add(time.Duration(i) * time.Second), Bytes: 1200, AckEliciting: true, InFlight: true})
	}
	r.OnACK(Application, ack(3, 3), start.Add(3*time.Second+time.Millisecond), 0, 0, true)
	if r.PersistentCongestion(0) {
		t.Fatal("first RTT sample enabled old packets")
	}
}
