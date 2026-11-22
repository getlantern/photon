package wire

import (
	"bytes"
	"testing"
)

func TestCryptoFrame(t *testing.T) {
	f := CryptoFrame{Offset: 63, Data: []byte("hello")}
	b, err := f.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, []byte{6, 63, 5, 'h', 'e', 'l', 'l', 'o'}) {
		t.Fatalf("%x", b)
	}
	got, n, err := ConsumeCryptoFrame(append(b, 0))
	if err != nil || n != len(b) || got.Offset != f.Offset || !bytes.Equal(got.Data, f.Data) {
		t.Fatalf("%+v %d %v", got, n, err)
	}
	for i := 0; i < len(b); i++ {
		if _, _, err := ConsumeCryptoFrame(b[:i]); err == nil {
			t.Fatal("truncated CRYPTO")
		}
	}
	if _, _, err := ConsumeCryptoFrame([]byte{0x40, 6, 0, 0}); err == nil {
		t.Fatal("non-minimal frame type")
	}
	if _, err := (CryptoFrame{Offset: MaxVarint, Data: []byte{1}}).Append(nil); err == nil {
		t.Fatal("offset overflow")
	}
	overflow := []byte{6}
	overflow, _ = AppendVarint(overflow, MaxVarint)
	overflow = append(overflow, 1, 0)
	if _, _, err := ConsumeCryptoFrame(overflow); err == nil {
		t.Fatal("accepted offset overflow")
	}
}

func FuzzCryptoFrame(f *testing.F) {
	f.Add([]byte{6, 0, 1, 42})
	f.Add([]byte{6, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		frame, n, err := ConsumeCryptoFrame(b)
		if err != nil {
			return
		}
		if n > len(b) || n < 3 {
			t.Fatal("invalid bounds")
		}
		encoded, err := frame.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		again, used, err := ConsumeCryptoFrame(encoded)
		if err != nil || used != len(encoded) || again.Offset != frame.Offset || !bytes.Equal(again.Data, frame.Data) {
			t.Fatal("round trip")
		}
	})
}
