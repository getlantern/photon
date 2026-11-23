package http3conn

import (
	"errors"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
	"testing"
)

func responseFields(status string) []qpack.DecodedField {
	return []qpack.DecodedField{fs(":status", status)}
}
func newResponse(t *testing.T) *Message {
	t.Helper()
	m, e := NewMessage(MessageConfig{Kind: ResponseFields, RequestMethod: "GET"})
	if e != nil {
		t.Fatal(e)
	}
	return m
}
func TestMessageLifecycle(t *testing.T) {
	m := newResponse(t)
	for _, status := range []string{"100", "103", "103"} {
		kind, control, err := m.Headers(responseFields(status))
		if err != nil || kind != InformationalSection || control.Status >= 200 {
			t.Fatal(kind, control, err)
		}
		if err := m.OtherFrame(0x21); err != nil {
			t.Fatal(err)
		}
	}
	fields := append(responseFields("200"), fs("content-length", "3"))
	if kind, _, err := m.Headers(fields); err != nil || kind != InitialSection {
		t.Fatal(kind, err)
	}
	if m.Data(1) != nil || m.OtherFrame(h3.PushPromise) != nil || m.Data(2) != nil {
		t.Fatal("valid content sequence")
	}
	if kind, _, err := m.Headers([]qpack.DecodedField{fs("x-checksum", "ok")}); err != nil || kind != TrailerSection {
		t.Fatal(kind, err)
	}
	if m.OtherFrame(h3.PushPromise) != nil || m.OtherFrame(0x21) != nil || m.Finish() != nil || m.Finish() != nil {
		t.Fatal("valid trailing sequence")
	}
}
func TestMessageOrderingErrors(t *testing.T) {
	for _, stage := range []string{"before headers", "after informational", "after trailers", "after FIN"} {
		t.Run(stage, func(t *testing.T) {
			m := newResponse(t)
			switch stage {
			case "after informational":
				m.Headers(responseFields("103"))
			case "after trailers":
				m.Headers(responseFields("200"))
				m.Headers(nil)
			case "after FIN":
				m.Headers(responseFields("200"))
				m.Finish()
			}
			err := m.Data(0)
			var frame *h3.Error
			if !errors.As(err, &frame) || frame.Code != h3.FrameUnexpected || m.Finish() != err {
				t.Fatal("wrong error scope or not sticky", err)
			}
		})
	}
	for _, typ := range []uint64{h3.Settings, h3.CancelPush, h3.Goaway, h3.MaxPushID, 2, 6, 8, 9} {
		m := newResponse(t)
		var frame *h3.Error
		if err := m.OtherFrame(typ); !errors.As(err, &frame) || frame.Code != h3.FrameUnexpected {
			t.Fatal(typ, err)
		}
	}
	m := newResponse(t)
	m.Headers(responseFields("200"))
	m.Headers(nil)
	_, _, err := m.Headers(nil)
	var frame *h3.Error
	if !errors.As(err, &frame) {
		t.Fatal(err)
	}
	for _, info := range []bool{false, true} {
		m := newResponse(t)
		if info {
			m.Headers(responseFields("103"))
		}
		var msg *MessageError
		if err := m.Finish(); !errors.As(err, &msg) {
			t.Fatal("missing final headers", err)
		}
	}
}
func TestMessageSemantics(t *testing.T) {
	for _, name := range []string{"content-length", "host", "trailer", ":status"} {
		m := newResponse(t)
		m.Headers(responseFields("200"))
		_, _, err := m.Headers([]qpack.DecodedField{fs(name, "1")})
		var msg *MessageError
		if !errors.As(err, &msg) {
			t.Fatal(name, err)
		}
	}
	m := newResponse(t)
	m.Headers(append(responseFields("200"), fs("content-length", "5")))
	m.Data(4)
	_, _, err := m.Headers(nil)
	var msg *MessageError
	if !errors.As(err, &msg) || m.Data(1) != err {
		t.Fatal("trailer underrun", err)
	}
	m, _ = NewMessage(MessageConfig{Kind: RequestFields})
	if _, _, err := m.Headers(requestControl("POST", "https", "example.org", "/")); err != nil {
		t.Fatal(err)
	}
	if m.Data(3) != nil || m.Finish() != nil {
		t.Fatal("request")
	}
	for _, c := range []MessageConfig{{Kind: RequestFields}, {Kind: ResponseFields, RequestMethod: "GET", Push: true}} {
		m, _ := NewMessage(c)
		var frame *h3.Error
		if err := m.OtherFrame(h3.PushPromise); !errors.As(err, &frame) {
			t.Fatal("forbidden push", err)
		}
	}
}
func TestMessageConnect(t *testing.T) {
	m, _ := NewMessage(MessageConfig{Kind: ResponseFields, RequestMethod: "CONNECT"})
	m.Headers(responseFields("200"))
	if m.Data(42) != nil {
		t.Fatal("tunnel bytes")
	}
	_, _, err := m.Headers(nil)
	var frame *h3.Error
	if !errors.As(err, &frame) {
		t.Fatal("tunnel trailers", err)
	}
	m, _ = NewMessage(MessageConfig{Kind: RequestFields})
	m.Headers([]qpack.DecodedField{fs(":method", "CONNECT"), fs(":authority", "example.org:443")})
	if m.EstablishTunnel() != nil || m.Data(42) != nil || m.Finish() != nil {
		t.Fatal("request tunnel")
	}
	m, _ = NewMessage(MessageConfig{Kind: ResponseFields, RequestMethod: "CONNECT"})
	m.Headers(responseFields("403"))
	if _, _, err := m.Headers(nil); err != nil {
		t.Fatal("failed CONNECT trailers", err)
	}
}
func FuzzMessageSequence(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4})
	f.Add([]byte{1, 3, 2})
	f.Add([]byte{0, 0, 4})
	f.Fuzz(func(t *testing.T, steps []byte) {
		m := newResponse(t)
		var failure error
		for _, step := range steps {
			var err error
			switch step % 6 {
			case 0:
				_, _, err = m.Headers(responseFields("103"))
			case 1:
				_, _, err = m.Headers(responseFields("200"))
			case 2:
				err = m.Data(1)
			case 3:
				_, _, err = m.Headers(nil)
			case 4:
				err = m.Finish()
			case 5:
				err = m.OtherFrame(0x21)
			}
			if failure != nil && err != failure {
				t.Fatal("lost terminal error")
			}
			if err != nil {
				failure = err
			}
		}
	})
}
