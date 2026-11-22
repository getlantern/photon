package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestReceiveWindowConsumptionPolicy(t *testing.T) {
	at := time.Unix(100, 0)
	for _, test := range []struct {
		name          string
		delay         time.Duration
		maximum, want uint64
	}{
		{"fast", time.Millisecond, 4096, 2048},
		{"boundary", 20 * time.Millisecond, 4096, 2048},
		{"slow", 21 * time.Millisecond, 4096, 1024},
		{"capped", time.Millisecond, 1500, 1500},
		{"fixed", time.Millisecond, 1024, 1024},
	} {
		t.Run(test.name, func(t *testing.T) {
			var sample receiveWindowSample
			if got := sample.consume(4096, 1024, test.maximum, 10*time.Millisecond, at); got != 1024 {
				t.Fatal("first read grew window", got)
			}
			if got := sample.consume(511, 1024, test.maximum, 10*time.Millisecond, at.Add(test.delay)); got != 1024 {
				t.Fatal("subthreshold growth", got)
			}
			if got := sample.consume(1, 1024, test.maximum, 10*time.Millisecond, at.Add(test.delay)); got != test.want {
				t.Fatal("growth decision", got, test.want)
			}
		})
	}
	var sample receiveWindowSample
	sample.consume(1, 1024, 4096, time.Millisecond, at)
	if got := sample.consume(1000, 1024, 4096, 0, at.Add(time.Millisecond)); got != 1024 || !sample.start.IsZero() {
		t.Fatal("unmeasured RTT grew window")
	}
	sample.consume(1, 1024, 4096, time.Millisecond, at)
	if got := sample.consume(1000, 1024, 4096, time.Millisecond, at.Add(-time.Millisecond)); got != 1024 || sample.consumed != 0 {
		t.Fatal("backward clock grew window")
	}
	sample = receiveWindowSample{start: at}
	if got := sample.consume(1<<30, wire.MaxVarint-1, wire.MaxVarint, time.Duration(1<<63-1), at.Add(time.Second)); got != wire.MaxVarint-1 {
		t.Fatal("huge window grew below threshold")
	}
	sample = receiveWindowSample{start: at}
	if got := sample.consume(10, 3, 5, time.Duration(1<<63-1), at.Add(time.Second)); got != 5 {
		t.Fatal("odd window or RTT overflow", got)
	}
}

func FuzzReceiveWindowConsumption(f *testing.F) {
	f.Add(uint64(1024), uint64(4096), []byte{0, 255, 255, 255, 1})
	f.Fuzz(func(t *testing.T, initial, maximum uint64, actions []byte) {
		initial = 1 + initial%wire.MaxVarint
		maximum = max(initial, maximum%wire.MaxVarint)
		if len(actions) > 512 {
			actions = actions[:512]
		}
		current := initial
		now := time.Unix(100, 0)
		var sample receiveWindowSample
		for _, a := range actions {
			now = now.Add(time.Duration(int(a)-32) * time.Millisecond)
			rtt := time.Duration(a%10) * time.Millisecond
			got := sample.consume(int(a)*1024, current, maximum, rtt, now)
			if got < current || got > maximum || got-current > current {
				t.Fatal("unbounded growth", current, maximum, got)
			}
			if rtt == 0 && got != current {
				t.Fatal("unmeasured growth")
			}
			current = got
		}
	})
}
