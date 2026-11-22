package transport

import (
	"bytes"
	"testing"
	"time"
)

func TestAddressTokenReceiptBounded(t *testing.T) {
	now := time.Now()
	var got [][]byte
	c := &Conn{complete: true, config: Config{OnToken: func(b []byte, at time.Time) {
		if !at.Equal(now) {
			t.Fatal("receipt time")
		}
		got = append(got, b)
	}}}
	b := []byte("first")
	c.receiveAddressToken(b, now)
	b[0] = 'X'
	c.receiveAddressToken([]byte("first"), now)
	if len(got) != 1 || string(got[0]) != "first" {
		t.Fatal("ownership or duplicate suppression")
	}
	c.receiveAddressToken(make([]byte, 513), now)
	c.receiveAddressToken(nil, now)
	for i := 0; i < 100; i++ {
		c.receiveAddressToken([]byte{byte(i)}, now)
	}
	if len(got) != 64 || len(c.receivedTokens) != 64 {
		t.Fatal("unbounded receipt history")
	}
	c.receiveAddressToken([]byte("first"), now)
	if len(got) != 64 {
		t.Fatal("duplicate restored")
	}
	for _, server := range []bool{false, true} {
		c := &Conn{complete: server, config: Config{Server: server, OnToken: func([]byte, time.Time) { t.Fatal("unauthenticated or server token") }}}
		c.receiveAddressToken(bytes.Repeat([]byte{1}, 54), now)
	}
}
