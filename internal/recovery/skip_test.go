package recovery

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func skipSender(t *testing.T) (*Sent, func(uint64), time.Time) {
	t.Helper()
	r, err := NewSent(64)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	send := func(n uint64) {
		t.Helper()
		if err := r.OnSent(Application, Packet{Number: n, SentAt: now, Bytes: 1200, AckEliciting: true, InFlight: true}); err != nil {
			t.Fatal(err)
		}
	}
	return r, send, now
}

func ranges(rs ...[2]uint64) wire.AckFrame {
	var f wire.AckFrame
	for _, r := range rs {
		f.Ranges = append(f.Ranges, wire.AckRange{Smallest: r[0], Largest: r[1]})
	}
	return f
}

// An ACK claiming a skipped number acknowledges a packet the peer never
// received, and fails; ACKs around the gap don't.
func TestSkippedPacketACKFails(t *testing.T) {
	r, send, now := skipSender(t)
	send(0)
	send(1)
	if pn, err := r.Skip(Application); err != nil || pn != 2 {
		t.Fatal("skip", pn, err)
	}
	send(3)
	if err := r.OnSent(Application, Packet{Number: 2, SentAt: now, Bytes: 1200, AckEliciting: true, InFlight: true}); err == nil {
		t.Fatal("sent a skipped number")
	}
	if _, err := r.OnACK(Application, ranges([2]uint64{0, 3}), now, 0, 0, true); !errors.Is(err, ErrACK) {
		t.Fatal("ACK across the skipped number accepted", err)
	}
	if _, err := r.OnACK(Application, ranges([2]uint64{2, 2}), now, 0, 0, true); !errors.Is(err, ErrACK) {
		t.Fatal("ACK of the skipped number accepted", err)
	}
	events, err := r.OnACK(Application, ranges([2]uint64{3, 3}, [2]uint64{0, 1}), now, 0, 0, true)
	if err != nil || len(events.Acked) != 3 {
		t.Fatal("ACK around the gap", events.Acked, err)
	}
	if _, err := r.Skip(Initial); err == nil {
		t.Fatal("skipped outside the application space")
	}
}

// Only the latest maxSkipped numbers are remembered, and they survive the
// resets that keep packet-number continuity.
func TestSkippedNumbersBoundedAndKept(t *testing.T) {
	r, send, _ := skipSender(t)
	var want []uint64
	for pn := uint64(0); pn < 2*(maxSkipped+1); pn += 2 {
		send(pn)
		if _, err := r.Skip(Application); err != nil {
			t.Fatal(err)
		}
		want = append(want, pn+1)
	}
	want = want[len(want)-maxSkipped:]
	if got := r.Skipped(Application); !slices.Equal(got, want) {
		t.Fatal("remembered", got, "want", want)
	}
	r.ResetForPathChange()
	r.ResetForRetry()
	if got := r.Skipped(Application); !slices.Equal(got, want) {
		t.Fatal("reset forgot skipped numbers", got)
	}
}

// A skipped number between a packet and the largest acknowledged one is not
// a packet sent after it, so it doesn't count toward packet-threshold loss.
func TestPacketThresholdExcludesSkipped(t *testing.T) {
	r, send, now := skipSender(t)
	send(0)
	if _, err := r.Skip(Application); err != nil {
		t.Fatal(err)
	}
	send(2)
	send(3)
	events, err := r.OnACK(Application, ranges([2]uint64{3, 3}), now, 0, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(events.Lost) != 0 {
		t.Fatal("lost with two packets sent after it", events.Lost)
	}
	send(4)
	events, err = r.OnACK(Application, ranges([2]uint64{4, 4}), now, 0, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(events.Lost) != 1 || events.Lost[0].Number != 0 {
		t.Fatal("not lost with three packets sent after it", events.Lost)
	}
}
