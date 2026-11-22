package http3conn

import (
	"errors"
	"strings"
	"testing"

	"github.com/getlantern/photon/internal/qpack"
)

func lengthFields(values ...string) []qpack.DecodedField {
	var out []qpack.DecodedField
	for _, v := range values {
		out = append(out, fs("content-length", v))
	}
	return out
}
func TestParseContentLength(t *testing.T) {
	for _, c := range []struct {
		values []string
		want   string
	}{
		{nil, ""}, {[]string{"0"}, "0"}, {[]string{"00042"}, "42"}, {[]string{"42, 042", "0042"}, "42"}, {[]string{strings.Repeat("9", 1000)}, strings.Repeat("9", 1000)},
	} {
		got, err := ParseContentLength(lengthFields(c.values...))
		if err != nil || got != c.want {
			t.Fatalf("got %q, %v", got, err)
		}
	}
	for _, v := range [][]string{{""}, {"42,"}, {",42"}, {"42,,42"}, {"42", "43"}, {"-1"}, {"+1"}, {"1e2"}, {"1 2"}, {"１"}, {"1\r"}} {
		got, err := ParseContentLength(lengthFields(v...))
		var msg *MessageError
		if got != "" || !errors.As(err, &msg) {
			t.Fatalf("accepted %q: %q, %v", v, got, err)
		}
	}
}
func TestBodyLength(t *testing.T) {
	makeBody := func(t *testing.T, kind FieldContext, method string, status int, values ...string) *BodyLength {
		t.Helper()
		b, err := NewBodyLength(lengthFields(values...), ControlData{Method: method, Status: status}, kind, method)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	t.Run("exact", func(t *testing.T) {
		b := makeBody(t, RequestFields, "POST", 0, "5, 5")
		for _, n := range []uint64{0, 2, 3} {
			if err := b.Add(n); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Finish(); err != nil {
			t.Fatal(err)
		}
		if err := b.Finish(); err != nil {
			t.Fatal(err)
		}
		if b.Add(0) == nil {
			t.Fatal("add after finish")
		}
	})
	t.Run("overrun sticky", func(t *testing.T) {
		b := makeBody(t, RequestFields, "POST", 0, "4")
		b.Add(3)
		err := b.Add(2)
		if err == nil || b.received != 3 || b.Add(0) != err || b.Finish() != err {
			t.Fatal("overrun not sticky/atomic")
		}
	})
	t.Run("underrun sticky", func(t *testing.T) {
		b := makeBody(t, ResponseFields, "GET", 200, "4")
		b.Add(3)
		err := b.Finish()
		if err == nil || b.Add(1) != err || b.Finish() != err {
			t.Fatal("underrun not sticky")
		}
	})
	t.Run("unknown", func(t *testing.T) {
		b := makeBody(t, RequestFields, "POST", 0)
		if b.Add(10) != nil || b.Finish() != nil {
			t.Fatal("unknown length")
		}
	})
	t.Run("huge metadata", func(t *testing.T) {
		for _, c := range []struct {
			method string
			status int
		}{{"HEAD", 200}, {"GET", 304}} {
			b := makeBody(t, ResponseFields, c.method, c.status, strings.Repeat("9", 1000))
			if b.Finish() != nil {
				t.Fatal("metadata treated as content")
			}
			b = makeBody(t, ResponseFields, c.method, c.status, "12")
			if b.Add(1) == nil {
				t.Fatal("body permitted")
			}
		}
	})
	t.Run("huge actual", func(t *testing.T) {
		b := makeBody(t, ResponseFields, "GET", 200, strings.Repeat("9", 1000))
		if b.Add(100) != nil || b.Finish() == nil {
			t.Fatal("huge content mismatch")
		}
	})
	t.Run("overflow", func(t *testing.T) {
		b := makeBody(t, RequestFields, "POST", 0)
		if b.Add(^uint64(0)) != nil || b.Add(1) == nil {
			t.Fatal("overflow unchecked")
		}
	})
	t.Run("bodyless", func(t *testing.T) {
		for _, status := range []int{103, 204, 205, 304} {
			b := makeBody(t, ResponseFields, "GET", status)
			if b.Add(0) != nil || b.Add(1) == nil {
				t.Fatalf("status %d", status)
			}
		}
	})
	t.Run("forbidden lengths", func(t *testing.T) {
		for _, status := range []int{103, 204, 205} {
			v := "0"
			if status == 205 {
				v = "1"
			}
			if _, err := NewBodyLength(lengthFields(v), ControlData{Status: status}, ResponseFields, "GET"); err == nil {
				t.Fatalf("status %d", status)
			}
		}
	})
	t.Run("connect", func(t *testing.T) {
		b := makeBody(t, ResponseFields, "CONNECT", 200, "garbage", "conflict")
		if b.Add(123) != nil || b.Finish() != nil {
			t.Fatal("tunnel length applied")
		}
		b = makeBody(t, RequestFields, "CONNECT", 0)
		if b.Add(123) != nil || b.Finish() != nil {
			t.Fatal("request tunnel")
		}
		b = makeBody(t, ResponseFields, "CONNECT", 403, "2")
		if b.Add(3) == nil {
			t.Fatal("failed CONNECT treated as tunnel")
		}
	})
}
func FuzzContentLength(f *testing.F) {
	f.Add("42, 042", "42")
	f.Add("18446744073709551616", "")
	f.Add("0", "0")
	f.Fuzz(func(t *testing.T, a, b string) {
		got, err := ParseContentLength(lengthFields(a, b))
		if err != nil {
			if got != "" {
				t.Fatal("partial parse")
			}
			return
		}
		if got == "" {
			t.Fatal("missing length")
		}
		again, err := ParseContentLength(lengthFields(got))
		if err != nil || again != got {
			t.Fatal("not canonical")
		}
		for i := range got {
			if got[i] < '0' || got[i] > '9' {
				t.Fatal("not decimal")
			}
		}
	})
}
