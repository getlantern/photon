package transport

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestReceivedRanges(t *testing.T) {
	r, err := NewReceived(8)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(100, 0)
	if r.Expected() != 0 {
		t.Fatal("initial expected number")
	}
	for i, pn := range []uint64{4, 2, 3, 0, 8, 7, 6} {
		got, err := r.Record(pn, start.Add(time.Duration(i)*time.Millisecond))
		if err != nil || got != NewPacket {
			t.Fatal(got, err)
		}
	}
	if r.Expected() != 9 {
		t.Fatal("expected number")
	}
	ack, err := r.Ack(start.Add(20*time.Millisecond), 3)
	if err != nil {
		t.Fatal(err)
	}
	// Largest packet 8 arrived at 4ms; later lower packets must not reset its clock.
	if ack.Delay != 2000 || !reflect.DeepEqual(ack.Ranges, []wire.AckRange{{Smallest: 6, Largest: 8}, {Smallest: 2, Largest: 4}, {Smallest: 0, Largest: 0}}) {
		t.Fatalf("%+v", ack)
	}
	for _, pn := range []uint64{4, 2, 8, 0} {
		if got, _ := r.Record(pn, start.Add(time.Second)); got != DuplicatePacket {
			t.Fatal("duplicate", pn)
		}
	}
	if got, _ := r.Record(5, start.Add(time.Second)); got != NewPacket {
		t.Fatal("gap fill")
	}
	merged, _ := r.Ack(start.Add(20*time.Millisecond), 3)
	if !reflect.DeepEqual(merged.Ranges, []wire.AckRange{{Smallest: 2, Largest: 8}, {Smallest: 0, Largest: 0}}) {
		t.Fatalf("%+v", merged)
	}
	if len(ack.Ranges) != 3 || ack.Ranges[0].Smallest != 6 {
		t.Fatal("queued ACK snapshot mutated")
	}
	if _, err := merged.Append(nil); err != nil {
		t.Fatal("invalid ACK ranges", err)
	}
	early, _ := r.Ack(start.Add(-time.Second), 3)
	if early.Delay != 0 {
		t.Fatal("negative delay wrapped")
	}
}

func TestReceivedEviction(t *testing.T) {
	r, _ := NewReceived(2)
	now := time.Unix(100, 0)
	for _, pn := range []uint64{0, 2, 4} {
		if _, err := r.Record(pn, now); err != nil {
			t.Fatal(err)
		}
	}
	for _, pn := range []uint64{0, 1} {
		if got, _ := r.Record(pn, now); got != ForgottenPacket {
			t.Fatal("processed packet below retained floor", pn)
		}
	}
	if got, _ := r.Record(2, now); got != DuplicatePacket {
		t.Fatal("lost duplicate history")
	}
	if got, _ := r.Record(3, now); got != NewPacket {
		t.Fatal("gap fill")
	}
	ack, _ := r.Ack(now, 0)
	if !reflect.DeepEqual(ack.Ranges, []wire.AckRange{{Smallest: 2, Largest: 4}}) {
		t.Fatalf("%+v", ack)
	}
	if got, _ := r.Record(1, now); got != ForgottenPacket {
		t.Fatal("floor regressed after merge")
	}
	// A low range evicted immediately cannot be acknowledged, so discard its
	// packet without processing. No replay can be reclassified as new.
	r, _ = NewReceived(1)
	r.Record(100, now)
	if got, _ := r.Record(0, now); got != ForgottenPacket {
		t.Fatal("processed immediately evicted packet")
	}
	if got, _ := r.Record(0, now); got != ForgottenPacket {
		t.Fatal("replay after immediate eviction")
	}
}

func TestReceivedBoundaries(t *testing.T) {
	for _, n := range []int{0, -1, 4097} {
		if _, err := NewReceived(n); err == nil {
			t.Fatal("invalid capacity")
		}
	}
	r, _ := NewReceived(1)
	if _, err := r.Ack(time.Time{}, 3); err == nil {
		t.Fatal("empty ACK")
	}
	r.Record(wire.MaxVarint, time.Time{})
	if r.Expected() != wire.MaxVarint+1 {
		t.Fatal("packet number boundary")
	}
	if _, err := r.Record(wire.MaxVarint+1, time.Time{}); !errors.Is(err, wire.ErrPacketNumber) {
		t.Fatal(err)
	}
	if _, err := r.Ack(time.Time{}, 21); err == nil {
		t.Fatal("bad exponent")
	}
}

func TestAmplification(t *testing.T) {
	var a Amplification
	if a.CanSend(1) || a.Validated() {
		t.Fatal("initial budget")
	}
	if err := a.Received(1200); err != nil {
		t.Fatal(err)
	}
	if !a.CanSend(3600) || a.CanSend(3601) {
		t.Fatal("three times limit")
	}
	if err := a.Sent(2400); err != nil {
		t.Fatal(err)
	}
	if err := a.Sent(1201); !errors.Is(err, ErrAmplification) {
		t.Fatal(err)
	}
	if err := a.Sent(1200); err != nil {
		t.Fatal("failed send consumed budget", err)
	}
	if a.CanSend(1) {
		t.Fatal("spent credit")
	}
	a.Received(1)
	if !a.CanSend(3) || a.CanSend(4) {
		t.Fatal("incremental receive")
	}
	if err := a.Received(-1); err == nil {
		t.Fatal("negative receive")
	}
	if a.CanSend(-1) {
		t.Fatal("negative send")
	}
	a.Validate()
	if !a.Validated() || !a.CanSend(1000000) {
		t.Fatal("validated path still limited")
	}
	if err := a.Sent(1000000); err != nil {
		t.Fatal(err)
	}
}

func TestAmplificationSaturation(t *testing.T) {
	a := Amplification{credit: ^uint64(0) - 2}
	a.Received(1)
	if a.credit != ^uint64(0) {
		t.Fatal("credit overflow")
	}
	if err := a.Sent(1); err != nil || a.credit != ^uint64(0)-1 {
		t.Fatal("saturated debit", err)
	}
}

func FuzzReceived(f *testing.F) {
	f.Add([]byte{0, 2, 4, 3, 2, 1}, uint8(2))
	f.Fuzz(func(t *testing.T, input []byte, capacity uint8) {
		r, _ := NewReceived(int(capacity) + 1)
		seen := make(map[uint64]bool)
		for _, v := range input {
			pn := uint64(v)
			got, err := r.Record(pn, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if got == NewPacket && seen[pn] {
				t.Fatal("duplicate classified as new")
			}
			seen[pn] = true
			ack, err := r.Ack(time.Time{}, 3)
			if err != nil {
				t.Fatal(err)
			}
			if len(ack.Ranges) > int(capacity)+1 {
				t.Fatal("unbounded ACK ranges")
			}
			if _, err := ack.Append(nil); err != nil {
				t.Fatal("invalid ranges", err)
			}
			for _, a := range ack.Ranges {
				for n := a.Smallest; n <= a.Largest; n++ {
					if !seen[n] {
						t.Fatal("acknowledged unreceived packet")
					}
				}
			}
		}
	})
}
