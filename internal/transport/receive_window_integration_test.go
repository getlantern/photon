package transport

import (
	"bytes"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestReceiveWindowGrowthOnRead(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, slow := range []bool{false, true} {
			p := streamParameters()
			p.InitialMaxData = 3072
			p.InitialMaxStreamDataBidiRemote = 2048
			client, server := testPair(t, version, p)
			server.config.MaxStreamReceiveWindow = 4096
			server.config.MaxConnectionReceiveWindow = 6144
			drivePair(t, client, server, false, false)
			server.recovery.RTT.Sampled = true
			server.recovery.RTT.Smoothed = 20 * time.Millisecond
			id, err := client.OpenStream(false)
			if err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("grow"), 512)
			sent := 0
			for sent < len(payload) {
				n, err := client.WriteStream(id, payload[sent:], false)
				if err != nil || n == 0 {
					t.Fatal(n, err)
				}
				sent += n
			}
			drivePair(t, client, server, false, false)
			now := time.Unix(100, 0)
			delay := time.Millisecond
			if slow {
				delay = time.Second
			}
			var got []byte
			for _, size := range []int{1, 1024, 512} {
				buffer := make([]byte, size)
				n, err := server.ReadStreamAt(id, buffer, now)
				if err != nil || n != size {
					t.Fatal(n, err)
				}
				got = append(got, buffer...)
				now = now.Add(delay)
			}
			wantStream, wantConnection := uint64(4096), uint64(6144)
			if slow {
				wantStream, wantConnection = 2048, 3072
			}
			state := server.streams.objects[id]
			if state.window != wantStream || server.streams.window != wantConnection {
				t.Fatal("growth decision", slow, state.window, server.streams.window)
			}
			tail := make([]byte, 511)
			n, err := server.ReadStreamAt(id, tail, now)
			if err != nil || n != len(tail) {
				t.Fatal(n, err)
			}
			got = append(got, tail...)
			if !bytes.Equal(got, payload) {
				t.Fatal("growth corrupted unread data")
			}
			drivePair(t, client, server, false, false)
			if !slow && (client.streams.send.Limit() <= uint64(len(payload))+p.InitialMaxData) {
				t.Fatal("grown connection credit not advertised")
			}
			// Fill beyond the old stream window using only the newly advertised credit.
			if !slow {
				extra := bytes.Repeat([]byte{7}, 3000)
				sent = 0
				for sent < len(extra) {
					n, err := client.WriteStream(id, extra[sent:], false)
					if err != nil || n == 0 {
						t.Fatal("grown stream credit unavailable", n, err)
					}
					sent += n
				}
			}
		}
	}
}

func TestConnectionWindowPolicyDenialAndRetry(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := testPair(t, version, streamParameters())
		drivePair(t, client, server, false, false)
		current := server.streams.window
		server.config.MaxConnectionReceiveWindow = current * 2
		server.recovery.RTT.Sampled = true
		server.recovery.RTT.Smoothed = time.Millisecond
		state, err := server.getStream(0)
		if err != nil {
			t.Fatal(err)
		}
		calls := 0
		server.SetConnectionWindowPolicy(func(delta uint64) bool {
			calls++
			if delta != current || server.streams.window != current {
				t.Fatal("policy invoked after commit or with wrong delta", delta)
			}
			return calls > 1
		})
		at := time.Unix(100, 0)
		for i, n := range []int{1, int(current / 2), int(current / 2)} {
			if err := server.growReceiveWindows(state, n, at.Add(time.Duration(i)*time.Microsecond)); err != nil {
				t.Fatal(err)
			}
			want := current
			if i == 2 {
				want *= 2
			}
			if server.streams.window != want {
				t.Fatal("denied growth committed or retry failed", i, server.streams.window)
			}
		}
		if calls != 2 {
			t.Fatal("policy call count", calls)
		}
		if err := server.growReceiveWindows(state, int(current), at.Add(3*time.Microsecond)); err != nil || calls != 2 {
			t.Fatal("policy invoked at maximum", calls, err)
		}
		server.releaseTransportBuffers()
		if server.allowConnectionWindowIncrease != nil {
			t.Fatal("cleanup retained public callback")
		}
	}
}
