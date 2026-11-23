package recovery

import (
	"testing"
	"time"
)

func TestAbandonSpace(t *testing.T) {
	r, _ := NewSent(4)
	now := time.Unix(100, 0)
	for _, s := range []Space{Initial, Application} {
		for i := uint64(0); i < 2; i++ {
			if err := r.OnSent(s, Packet{Number: i, SentAt: now, Bytes: 1200, InFlight: true, AckEliciting: true}); err != nil {
				t.Fatal(err)
			}
		}
	}
	lost := r.Abandon(Application)
	if len(lost) != 2 || lost[0].Number != 0 || lost[1].Number != 1 || r.BytesInFlight != 2400 {
		t.Fatal(lost, r.BytesInFlight)
	}
	if len(r.Abandon(Application)) != 0 {
		t.Fatal("lost twice")
	}
	if err := r.OnSent(Application, Packet{Number: 0}); err == nil {
		t.Fatal("abandonment reset packet numbers")
	}
	if err := r.OnSent(Application, Packet{Number: 2}); err != nil {
		t.Fatal("packet-number continuity lost", err)
	}
	if len(r.Abandon(Space(99))) != 0 {
		t.Fatal("invalid space returned packets")
	}
	if got := r.Abandon(Initial); len(got) != 2 || r.BytesInFlight != 0 {
		t.Fatal("other space damaged", got)
	}
}
