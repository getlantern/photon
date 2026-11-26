package recovery

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// rateFlight sends n in-flight packets of size bytes, gap apart from start,
// numbered from the space's next packet number.
func rateFlight(t *testing.T, r *Sent, start time.Time, n, size int, gap time.Duration) (first, last uint64) {
	t.Helper()
	first = r.spaces[Application].next
	for i := range n {
		p := Packet{Number: first + uint64(i), SentAt: start.Add(time.Duration(i) * gap), Bytes: size, AckEliciting: true, InFlight: true}
		if err := r.OnSent(Application, p); err != nil {
			t.Fatal(err)
		}
	}
	return first, first + uint64(n) - 1
}

func rateACK(t *testing.T, r *Sent, smallest, largest uint64, at time.Time) Events {
	t.Helper()
	events, err := r.OnACK(Application, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: smallest, Largest: largest}}}, at, 0, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	return events
}

// A flight's acknowledgment measures the data it delivered over the longer of
// its send and acknowledgment intervals, and a flight started after the path
// went idle measures from its own start.
func TestDeliveryRateSample(t *testing.T) {
	r, _ := NewSent(64)
	start := time.Unix(100, 0)
	first, last := rateFlight(t, r, start, 10, 1200, time.Millisecond)
	rs := rateACK(t, r, first, last, start.Add(50*time.Millisecond)).Rate
	if !rs.Valid || rs.Delivered != 12000 || rs.Interval != 50*time.Millisecond || rs.Rate != 240000 {
		t.Fatalf("first flight %+v, want 12000 bytes over 50ms, 240000 B/s", rs)
	}
	if rs.RTT != 41*time.Millisecond || rs.InFlight != 12000 || rs.AppLimited || rs.TotalDelivered != 12000 {
		t.Fatalf("first flight %+v", rs)
	}
	// The path is idle again, so the next flight starts its own interval.
	next := start.Add(60 * time.Millisecond)
	first, last = rateFlight(t, r, next, 5, 1200, 2*time.Millisecond)
	rs = rateACK(t, r, first, last, next.Add(48*time.Millisecond)).Rate
	if !rs.Valid || rs.Delivered != 6000 || rs.Interval != 48*time.Millisecond || rs.Rate != 125000 || rs.TotalDelivered != 18000 {
		t.Fatalf("second flight %+v, want 6000 bytes over 48ms", rs)
	}
}

// Packets sent after the sender ran out of data are flagged application
// limited until the flight in progress then has been delivered.
func TestDeliveryRateAppLimited(t *testing.T) {
	r, _ := NewSent(64)
	start := time.Unix(100, 0)
	first, last := rateFlight(t, r, start, 4, 1200, time.Millisecond)
	r.MarkAppLimited()
	limitedFirst, limitedLast := rateFlight(t, r, start.Add(10*time.Millisecond), 2, 1200, time.Millisecond)
	if rs := rateACK(t, r, first, last, start.Add(50*time.Millisecond)).Rate; rs.AppLimited {
		t.Fatal("packets sent before the mark sampled as app limited")
	}
	if rs := rateACK(t, r, limitedFirst, limitedLast, start.Add(60*time.Millisecond)).Rate; !rs.AppLimited {
		t.Fatal("packets sent after the mark not sampled as app limited")
	}
	// The marked flight is now delivered, so later packets are not limited.
	after := start.Add(70 * time.Millisecond)
	first, last = rateFlight(t, r, after, 2, 1200, time.Millisecond)
	if rs := rateACK(t, r, first, last, after.Add(50*time.Millisecond)).Rate; rs.AppLimited {
		t.Fatal("app-limited phase outlived the flight it marked")
	}
}

// A sample whose interval is shorter than the minimum RTT, as ACK
// compression can produce, is not valid.
func TestDeliveryRateRejectsShortInterval(t *testing.T) {
	now := time.Unix(100, 0)
	d := deliveryRate{delivered: 2400, deliveredAt: now}
	p := Packet{SentAt: now.Add(-10 * time.Millisecond), Bytes: 1200, InFlight: true,
		Delivery: DeliveryState{Delivered: 1200, DeliveredAt: now.Add(-10 * time.Millisecond), FirstSentAt: now.Add(-15 * time.Millisecond)}}
	s := rateSampler{newest: p, have: true, sendElapsed: 5 * time.Millisecond}
	if rs := d.sample(&s, now, 40*time.Millisecond, true); rs.Valid || rs.Interval != 10*time.Millisecond {
		t.Fatalf("sample over %v, under the 40ms minimum RTT: %+v", rs.Interval, rs)
	}
	if rs := d.sample(&s, now, 0, false); rs.Valid {
		t.Fatal("sample valid with no RTT measured", rs)
	}
	if rs := d.sample(&s, now, 10*time.Millisecond, true); !rs.Valid || rs.Rate != 120000 {
		t.Fatalf("sample over the minimum RTT: %+v", rs)
	}
}

// Acknowledging only ACK-only packets delivers nothing and measures no rate.
func TestDeliveryRateIgnoresACKOnly(t *testing.T) {
	r, _ := NewSent(64)
	start := time.Unix(100, 0)
	ackOnly := r.spaces[Application].next
	if err := r.OnSent(Application, Packet{Number: ackOnly, SentAt: start, Bytes: 50}); err != nil {
		t.Fatal(err)
	}
	if rs := rateACK(t, r, ackOnly, ackOnly, start.Add(50*time.Millisecond)).Rate; rs.Valid || r.rate.delivered != 0 {
		t.Fatal("an ACK-only packet counted as delivered", rs)
	}
}

// An ACK that also declares packets lost reports, in its sample, the bytes it
// newly acknowledged and newly declared lost, and the loss since its newest
// packet was sent.
func TestDeliveryRateCountsLoss(t *testing.T) {
	r, _ := NewSent(64)
	start := time.Unix(100, 0)
	first, last := rateFlight(t, r, start, 5, 1200, time.Millisecond)
	// Packet first is never acknowledged; acknowledging the three after it
	// declares it lost by packet threshold. last is not acknowledged yet.
	events := rateACK(t, r, first+1, last-1, start.Add(50*time.Millisecond))
	rs := events.Rate
	if len(events.Lost) != 1 || rs.NewlyLost != 1200 || rs.NewlyAcked != 3600 || r.LostBytes() != 1200 {
		t.Fatalf("lost %d packets, sample %+v, total lost %d", len(events.Lost), rs, r.LostBytes())
	}
	// The newest acknowledged packet was sent before the loss was declared.
	if rs.Lost != 1200 || rs.PriorDelivered != 0 {
		t.Fatalf("sample %+v, want 1200 lost since the newest packet was sent", rs)
	}
	// A packet sent after the loss doesn't count it.
	next := start.Add(60 * time.Millisecond)
	later, _ := rateFlight(t, r, next, 1, 1200, 0)
	if rs := rateACK(t, r, last, later, next.Add(50*time.Millisecond)).Rate; rs.Lost != 0 || rs.PriorDelivered != 3600 {
		t.Fatalf("sample %+v, want no loss since it was sent", rs)
	}
}
