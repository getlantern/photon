package http3conn

import (
	"errors"
	"testing"

	"github.com/getlantern/photon/internal/qpack"
)

func requestControl(method, scheme, authority, path string) []qpack.DecodedField {
	return []qpack.DecodedField{fs(":method", method), fs(":scheme", scheme), fs(":authority", authority), fs(":path", path)}
}
func TestControlData(t *testing.T) {
	cases := []struct {
		name     string
		fields   []qpack.DecodedField
		kind     FieldContext
		extended bool
		want     ControlData
	}{
		{"get", requestControl("GET", "https", "example.org", "/a?b=c"), RequestFields, false, ControlData{Method: "GET", Scheme: "https", Authority: "example.org", Path: "/a?b=c"}},
		{"options", requestControl("OPTIONS", "http", "example.org", "*"), RequestFields, false, ControlData{Method: "OPTIONS", Scheme: "http", Authority: "example.org", Path: "*"}},
		{"host fallback", []qpack.DecodedField{fs(":method", "custom"), fs(":scheme", "HTTPS"), fs(":path", "/"), fs("host", "example.org")}, RequestFields, false, ControlData{Method: "custom", Scheme: "HTTPS", Authority: "example.org", Path: "/"}},
		{"connect", []qpack.DecodedField{fs(":method", "CONNECT"), fs(":authority", "[::1]:443")}, RequestFields, false, ControlData{Method: "CONNECT", Authority: "[::1]:443"}},
		{"extended", append(requestControl("CONNECT", "https", "example.org", "/chat"), fs(":protocol", "websocket")), RequestFields, true, ControlData{Method: "CONNECT", Scheme: "https", Authority: "example.org", Path: "/chat", Protocol: "websocket"}},
		{"other scheme", []qpack.DecodedField{fs(":method", "GET"), fs(":scheme", "custom+v1"), fs(":path", "")}, RequestFields, false, ControlData{Method: "GET", Scheme: "custom+v1"}},
		{"informational", []qpack.DecodedField{fs(":status", "103")}, ResponseFields, false, ControlData{Status: 103}},
		{"unregistered status", []qpack.DecodedField{fs(":status", "599")}, ResponseFields, false, ControlData{Status: 599}},
		{"trailers", []qpack.DecodedField{fs("x-checksum", "done")}, TrailerFields, false, ControlData{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ValidateControlData(c.fields, c.kind, c.extended)
			if err != nil || got != c.want {
				t.Fatalf("got %+v, %v; want %+v", got, err, c.want)
			}
		})
	}
}
func TestControlDataMalformed(t *testing.T) {
	check := func(t *testing.T, f []qpack.DecodedField, k FieldContext, ext bool) {
		t.Helper()
		got, err := ValidateControlData(f, k, ext)
		var msg *MessageError
		if !errors.As(err, &msg) || got != (ControlData{}) {
			t.Fatalf("got %+v, %v", got, err)
		}
	}
	for _, name := range []string{":method", ":scheme", ":path"} {
		t.Run("missing "+name, func(t *testing.T) {
			f := requestControl("GET", "https", "example.org", "/")
			for i := range f {
				if f[i].Name == name {
					f = append(f[:i], f[i+1:]...)
					break
				}
			}
			check(t, f, RequestFields, false)
		})
	}
	for _, c := range []struct{ name, value string }{
		{":method", ""}, {":method", "GET /"}, {":method", "GÉT"}, {":scheme", ""}, {":scheme", "1https"}, {":scheme", "https:"}, {":authority", ""}, {":authority", "user@example.org"}, {":path", ""}, {":path", "*"}, {":path", "relative"}, {":path", "/fragment#x"}, {":path", "/a b"},
	} {
		t.Run(c.name+"="+c.value, func(t *testing.T) {
			f := requestControl("GET", "https", "example.org", "/")
			for i := range f {
				if f[i].Name == c.name {
					f[i].Value = c.value
				}
			}
			check(t, f, RequestFields, false)
		})
	}
	for _, status := range []string{"", "99", "099", "600", "200 OK", "2a0", "101", "0200"} {
		t.Run("status "+status, func(t *testing.T) { check(t, []qpack.DecodedField{fs(":status", status)}, ResponseFields, false) })
	}
	check(t, nil, ResponseFields, false)
	for _, authority := range []string{"example.org", "example.org:", ":443", "example.org:+443", "example.org:65536", "example.org:0", "user@example.org:443"} {
		check(t, []qpack.DecodedField{fs(":method", "CONNECT"), fs(":authority", authority)}, RequestFields, false)
	}
	check(t, append(requestControl("GET", "https", "example.org", "/"), fs("host", "different.org")), RequestFields, false)
	check(t, append(requestControl("GET", "https", "example.org", "/"), fs("host", "example.org"), fs("host", "example.org")), RequestFields, false)
	check(t, requestControl("CONNECT", "https", "example.org:443", "/"), RequestFields, false)
	ext := append(requestControl("CONNECT", "https", "example.org", "/"), fs(":protocol", "websocket"))
	check(t, ext, RequestFields, false)
	ext[0].Value = "GET"
	check(t, ext, RequestFields, true)
	ext[0].Value = "CONNECT"
	ext[4].Value = ""
	check(t, ext, RequestFields, true)
}
func FuzzControlData(f *testing.F) {
	f.Add("GET", "https", "example.org", "/", "", "200")
	f.Add("CONNECT", "https", "[::1]:443", "/chat", "websocket", "103")
	f.Fuzz(func(t *testing.T, method, scheme, authority, path, protocol, status string) {
		fields := requestControl(method, scheme, authority, path)
		if protocol != "" {
			fields = append(fields, fs(":protocol", protocol))
		}
		for _, kind := range []FieldContext{RequestFields, ResponseFields} {
			input := fields
			if kind == ResponseFields {
				input = []qpack.DecodedField{fs(":status", status)}
			}
			got, err := ValidateControlData(input, kind, true)
			if err != nil {
				var msg *MessageError
				if !errors.As(err, &msg) || got != (ControlData{}) {
					t.Fatalf("partial result or wrong error: %+v, %v", got, err)
				}
			} else if err := ValidateFieldStructure(input, kind, true); err != nil {
				t.Fatal(err)
			}
		}
	})
}
