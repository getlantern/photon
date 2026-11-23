package stream

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestPeekWrappedTerminalPrefix(t *testing.T) {
	for _, partial := range []bool{false, true} {
		f, _ := NewFlow(64)
		r, _ := NewReceive(f, 8, 8)
		if err := r.Push(0, []byte("abcdef"), false); err != nil {
			t.Fatal(err)
		}
		if n, err := r.Read(make([]byte, 5)); n != 5 || err != nil {
			t.Fatal(n, err)
		}
		if err := r.SetLimit(13); err != nil {
			t.Fatal(err)
		}
		if partial {
			if err := r.ResetAt(13, 9, 7); err != nil {
				t.Fatal(err)
			}
		}
		if err := r.Push(7, []byte("hi"), !partial); err != nil {
			t.Fatal(err)
		}
		dst := bytes.Repeat([]byte{99}, 5)
		if n, err := r.Peek(dst); n != 0 || !errors.Is(err, ErrWouldBlock) || !bytes.Equal(dst, bytes.Repeat([]byte{99}, 5)) {
			t.Fatal("gap peek", n, err, dst)
		}
		if err := r.Push(6, []byte("g"), false); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			if n, err := r.Peek(dst[:4]); n != 4 || err != nil || string(dst[:4]) != "fghi" {
				t.Fatal("exact prefix", n, err, dst)
			}
			n, err := r.Peek(dst)
			if n != 4 || string(dst[:4]) != "fghi" {
				t.Fatal(n, err, dst)
			}
			if partial {
				var reset *ResetError
				if !errors.As(err, &reset) || reset.Code != 7 {
					t.Fatal(err)
				}
			} else if !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			if r.Offset() != 5 || f.Consumed() != 5 || r.buffered != 4 || r.Drained() {
				t.Fatal("peek consumed state")
			}
		}
		n, err := r.Read(dst)
		if n != 4 || err == nil || string(dst[:4]) != "fghi" || !r.Drained() {
			t.Fatal("read after peek", n, err, dst)
		}
		if n, err := r.Peek(nil); n != 0 || err != nil {
			t.Fatal("empty terminal peek", n, err)
		}
	}
}
