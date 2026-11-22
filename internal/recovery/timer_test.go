package recovery

import (
	"testing"
	"time"
)

func TestPTOBackoffAndProgress(t *testing.T) {
	r := tracker(t)
	start := time.Unix(1000, 0)
	var timer Timer
	state := TimerState{PeerValidated: true}
	sent(t, r, Initial, 0, start, true, true)
	timer.Arm(r, start, state)
	due := start.Add(999 * time.Millisecond)
	if timer.Deadline() != due {
		t.Fatal(timer.Deadline())
	}
	if _, ok := timer.Expire(r, due.Add(-time.Nanosecond)); ok {
		t.Fatal("early timeout")
	}
	ev, ok := timer.Expire(r, due)
	if !ok || !ev.Probe || ev.Space != Initial || len(ev.Lost) != 0 || r.BytesInFlight != 1200 || timer.Backoff() != 1 {
		t.Fatal(ev, ok, timer.Backoff())
	}
	if _, ok := timer.Expire(r, due); ok {
		t.Fatal("timer fired twice")
	}
	sent(t, r, Initial, 1, due, true, true)
	timer.Arm(r, due, state)
	if timer.Deadline() != due.Add(1998*time.Millisecond) {
		t.Fatal("no exponential backoff", timer.Deadline())
	}
	timer.Progress(Events{}, true)
	if timer.Backoff() != 1 {
		t.Fatal("duplicate ACK resets PTO")
	}
	acked, err := r.OnACK(Initial, ack(1, 1), due.Add(100*time.Millisecond), 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	timer.Progress(acked, false)
	if timer.Backoff() != 1 {
		t.Fatal("unvalidated peer resets PTO")
	}
	timer.Progress(acked, true)
	if timer.Backoff() != 0 {
		t.Fatal("progress did not reset PTO")
	}
}

func TestPTOSpaceSelection(t *testing.T) {
	start := time.Unix(1000, 0)
	r := tracker(t)
	var timer Timer
	sent(t, r, Application, 0, start, true, true)
	state := TimerState{PeerValidated: true, MaxACKDelay: 25 * time.Millisecond}
	timer.Arm(r, start, state)
	if !timer.Deadline().IsZero() {
		t.Fatal("application PTO before confirmation")
	}
	state.Confirmed = true
	timer.Arm(r, start, state)
	if timer.Deadline() != start.Add(1024*time.Millisecond) {
		t.Fatal("missing max ACK delay")
	}
	sent(t, r, Handshake, 0, start.Add(10*time.Millisecond), true, true)
	timer.Arm(r, start, state)
	if timer.space != Handshake || timer.Deadline() != start.Add(1009*time.Millisecond) {
		t.Fatal("wrong space", timer)
	}
	// The last ack-eliciting send remains the PTO origin even when that packet
	// is acknowledged and an older transmission remains outstanding.
	sent(t, r, Handshake, 1, start.Add(20*time.Millisecond), true, true)
	r.remove(&r.spaces[Handshake], r.spaces[Handshake].packets[1])
	timer.Arm(r, start, state)
	if timer.Deadline() != start.Add(1019*time.Millisecond) {
		t.Fatal("forgot latest send time", timer.Deadline())
	}
}

func TestAntiDeadlockAndAmplification(t *testing.T) {
	start := time.Unix(1000, 0)
	r := tracker(t)
	var timer Timer
	state := TimerState{}
	timer.Arm(r, start, state)
	if timer.space != Initial || timer.Deadline() != start.Add(999*time.Millisecond) {
		t.Fatal("missing Initial anti-deadlock timer")
	}
	timer.Arm(r, start.Add(500*time.Millisecond), state)
	if timer.Deadline() != start.Add(999*time.Millisecond) {
		t.Fatal("rearm postponed anti-deadlock timer")
	}
	state.HandshakeKeys = true
	timer.Arm(r, start, state)
	if timer.space != Handshake {
		t.Fatal("anti-deadlock did not use Handshake")
	}
	state.PeerValidated = true
	timer.Arm(r, start, state)
	if !timer.Deadline().IsZero() {
		t.Fatal("idle validated connection armed")
	}
	sent(t, r, Handshake, 0, start, true, true)
	state.AmplificationBlocked = true
	timer.Arm(r, start, state)
	if !timer.Deadline().IsZero() {
		t.Fatal("amplification-blocked server armed")
	}
	state.AmplificationBlocked = false
	timer.Arm(r, start.Add(2*time.Second), state)
	if ev, ok := timer.Expire(r, start.Add(2*time.Second)); !ok || !ev.Probe {
		t.Fatal("restored credit failed to fire overdue PTO")
	}
	timer.DiscardKeys()
	r.Discard(Handshake)
	timer.Arm(r, start, state)
	if timer.Backoff() != 0 || !timer.Deadline().IsZero() {
		t.Fatal("key discard retained timer")
	}
}

func TestLossTimerPrecedesPTO(t *testing.T) {
	start := time.Unix(1000, 0)
	r := tracker(t)
	var timer Timer
	sent(t, r, Initial, 0, start, true, true)
	sent(t, r, Initial, 1, start, true, true)
	r.OnACK(Initial, ack(1, 1), start.Add(100*time.Millisecond), 0, 0, false)
	timer.Arm(r, start, TimerState{AmplificationBlocked: true})
	due := start.Add(112500 * time.Microsecond)
	if timer.Deadline() != due {
		t.Fatal("loss timer suppressed by amplification block")
	}
	ev, ok := timer.Expire(r, due)
	if !ok || ev.Probe || len(ev.Lost) != 1 || ev.Lost[0].Number != 0 || timer.Backoff() != 0 {
		t.Fatal("loss treated as PTO", ev)
	}
}
