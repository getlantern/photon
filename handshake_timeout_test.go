package quic

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestHandshakeTimeoutConfiguration(t *testing.T) {
	for _, tc := range []struct{ idle, absolute, wantIdle, wantAbsolute time.Duration }{
		{0, 0, 5 * time.Second, 10 * time.Second},
		{time.Second, 0, time.Second, 2 * time.Second},
		{time.Second, 10 * time.Second, time.Second, 10 * time.Second},
		{time.Duration(1<<63 - 1), 0, time.Duration(1<<63 - 1), time.Duration(1<<63 - 1)},
	} {
		input := Config{HandshakeIdleTimeout: tc.idle, HandshakeTimeout: tc.absolute}
		cfg, _, err := prepareConfig(&input)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.HandshakeIdleTimeout != tc.wantIdle || cfg.HandshakeTimeout != tc.wantAbsolute {
			t.Fatal(tc, cfg.HandshakeIdleTimeout, cfg.HandshakeTimeout)
		}
		if input.HandshakeIdleTimeout != tc.idle || input.HandshakeTimeout != tc.absolute {
			t.Fatal("changed caller config")
		}
	}
	if _, _, err := prepareConfig(&Config{HandshakeIdleTimeout: -time.Second}); err == nil {
		t.Fatal("negative idle accepted")
	}
}

// A bound UDP socket receives Initial packets but never answers. This exercises
// the public adapter and real timer without relying on an unreachable-port ICMP.
func TestDialHandshakeIdleTimeout(t *testing.T) {
	for _, absolute := range []bool{false, true} {
		t.Run(fmt.Sprintf("absolute=%t", absolute), func(t *testing.T) {
			sink, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer sink.Close()
			ct, _ := dialTLS(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			cfg := &Config{HandshakeIdleTimeout: 50 * time.Millisecond, HandshakeTimeout: 10 * time.Second}
			if absolute {
				cfg.HandshakeIdleTimeout, cfg.HandshakeTimeout = 10*time.Second, 50*time.Millisecond
			}
			conn, err := DialAddr(ctx, sink.LocalAddr().String(), ct, cfg)
			if conn != nil {
				conn.CloseWithError(0, "")
				t.Fatal("silent peer completed handshake")
			}
			assertTransportTimeout(t, err, absolute)
			if ctx.Err() != nil {
				t.Fatal("context rather than handshake timer expired", ctx.Err())
			}
		})
	}
}
