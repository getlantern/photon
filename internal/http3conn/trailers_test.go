package http3conn

import (
	"errors"
	"reflect"
	"testing"

	"github.com/getlantern/photon/internal/qpack"
)

func TestResponseTrailerStatus(t *testing.T) {
	for _, status := range []string{"204", "304"} {
		for _, trailers := range [][]qpack.DecodedField{nil, {fs("x-checksum", "done")}} {
			m := newResponse(t)
			if _, _, err := m.Headers(responseFields(status)); err != nil {
				t.Fatal(err)
			}
			_, _, err := m.Headers(trailers)
			var msg *MessageError
			if !errors.As(err, &msg) || m.Finish() != err || m.Data(0) != err {
				t.Fatalf("status %s: %v", status, err)
			}
		}
	}
	for _, status := range []string{"200", "204", "304"} {
		m := newResponse(t)
		if _, _, err := m.Headers(responseFields(status)); err != nil {
			t.Fatal(err)
		}
		if m.Finish() != nil {
			t.Fatal("headers-only response", status)
		}
	}
	for _, status := range []string{"200", "205"} {
		m := newResponse(t)
		m.Headers(responseFields(status))
		if _, _, err := m.Headers([]qpack.DecodedField{fs("x-checksum", "done")}); err != nil {
			t.Fatal("permitted trailers", status, err)
		}
		if m.Finish() != nil {
			t.Fatal("finish")
		}
	}
	// Successful CONNECT semantics take priority over the ordinary 204 semantics.
	m, _ := NewMessage(MessageConfig{Kind: ResponseFields, RequestMethod: "CONNECT"})
	m.Headers(responseFields("204"))
	if m.Data(1) != nil || m.Finish() != nil {
		t.Fatal("CONNECT 204 tunnel")
	}
}
func TestOutgoingTrailerPolicy(t *testing.T) {
	fields := []qpack.DecodedField{fs("x-checksum", "first"), fs("x-checksum", "second")}
	before := append([]qpack.DecodedField(nil), fields...)
	var names []string
	policy := func(name string) bool { names = append(names, name); return name == "x-checksum" }
	if err := ValidateOutgoingTrailers(fields, policy); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fields, before) || !reflect.DeepEqual(names, []string{"x-checksum", "x-checksum"}) {
		t.Fatal("mutation or missed field")
	}
	if ValidateOutgoingTrailers(nil, nil) == nil {
		t.Fatal("implicit permission")
	}
	if err := ValidateOutgoingTrailers(nil, policy); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"content-length", "host", "trailer", "connection", ":status", "X-Test"} {
		if err := ValidateOutgoingTrailers([]qpack.DecodedField{fs(name, "value")}, func(string) bool { return true }); err == nil {
			t.Fatal("policy bypassed core rules", name)
		}
	}
	var msg *MessageError
	if err := ValidateOutgoingTrailers([]qpack.DecodedField{fs("unknown", "value")}, policy); !errors.As(err, &msg) {
		t.Fatal("unknown field accepted", err)
	}
}
