package http3conn

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/getlantern/photon/internal/qpack"
	"io"
	"testing"

	h3 "github.com/getlantern/photon/internal/http3wire"
)

type messageSink struct {
	bytes.Buffer
	calls, failCall, closes int
	closeErr                error
}

func (s *messageSink) Write(b []byte) (int, error) {
	s.calls++
	if s.calls == s.failCall {
		return s.Buffer.Write(b[:len(b)-1])
	}
	return s.Buffer.Write(b)
}
func (s *messageSink) Close() error { s.closes++; return s.closeErr }
func responseWriter(t *testing.T, sink *messageSink, policy func(string) bool) *MessageWriter {
	t.Helper()
	encoder, err := NewFieldEncoder(SettingsSnapshot{MaxFieldSectionSize: 4096}, encoderLimits(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewMessageWriter(sink, encoder, 0, MessageConfig{Kind: ResponseFields, RequestMethod: "GET"}, policy)
	if err != nil {
		t.Fatal(err)
	}
	return writer
}
func TestMessageWriterRoundTrip(t *testing.T) {
	sink := new(messageSink)
	writer := responseWriter(t, sink, func(name string) bool { return name == "x-checksum" })
	if err := writer.Headers(responseFields("103")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Headers(append(responseFields("200"), fs("content-length", "3"))); err != nil {
		t.Fatal(err)
	}
	if n, err := writer.Write([]byte("abc")); n != 3 || err != nil {
		t.Fatal(n, err)
	}
	if err := writer.Headers([]qpack.DecodedField{fs("x-checksum", "ok")}); err != nil {
		t.Fatal(err)
	}
	if writer.Close() != nil || writer.Close() != nil || sink.closes != 1 {
		t.Fatal("FIN count", sink.closes)
	}
	if _, err := writer.Write(nil); err != io.ErrClosedPipe {
		t.Fatal(err)
	}
	reader := responseReader(t, sink.Bytes())
	ctx := context.Background()
	for _, want := range []SectionKind{InformationalSection, InitialSection} {
		event, err := reader.Next(ctx)
		if err != nil || event.Section != want {
			t.Fatal(event, err)
		}
	}
	event, err := reader.Next(ctx)
	if err != nil || event.Header.Type != h3.Data {
		t.Fatal(event, err)
	}
	body, err := io.ReadAll(reader)
	if err != nil || string(body) != "abc" {
		t.Fatal(string(body), err)
	}
	event, err = reader.Next(ctx)
	if err != nil || event.Section != TrailerSection || event.Fields[0].Value != "ok" {
		t.Fatal(event, err)
	}
	if _, err = reader.Next(ctx); err != io.EOF {
		t.Fatal(err)
	}
}
func TestMessageWriterRejectsBeforeTransmission(t *testing.T) {
	for _, mode := range []string{"early DATA", "overflow", "underflow", "trailer policy"} {
		t.Run(mode, func(t *testing.T) {
			sink := new(messageSink)
			writer := responseWriter(t, sink, nil)
			if mode != "early DATA" {
				if err := writer.Headers(append(responseFields("200"), fs("content-length", "0"))); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "underflow" {
				sink = new(messageSink)
				writer = responseWriter(t, sink, nil)
				if err := writer.Headers(append(responseFields("200"), fs("content-length", "1"))); err != nil {
					t.Fatal(err)
				}
			}
			before := sink.Len()
			var err error
			switch mode {
			case "early DATA", "overflow":
				_, err = writer.Write([]byte("x"))
			case "underflow":
				err = writer.Close()
			case "trailer policy":
				err = writer.Headers([]qpack.DecodedField{fs("x-checksum", "ok")})
			}
			if err == nil || sink.Len() != before || sink.closes != 0 {
				t.Fatal(err, sink.Len(), before, sink.closes)
			}
			if writer.Close() != err {
				t.Fatal("lost failure")
			}
			if _, again := writer.Write(nil); again != err {
				t.Fatal(again)
			}
		})
	}
}
func TestMessageWriterWriteFailures(t *testing.T) {
	for _, failCall := range []int{1, 2, 3, 4} {
		t.Run(fmt.Sprint(failCall), func(t *testing.T) {
			sink := &messageSink{failCall: failCall}
			writer := responseWriter(t, sink, nil)
			err := writer.Headers(responseFields("200"))
			n := 0
			if err == nil {
				n, err = writer.Write([]byte("abc"))
			}
			if !errors.Is(err, io.ErrShortWrite) {
				t.Fatal(err)
			}
			if failCall == 4 && n != 2 {
				t.Fatal("payload progress", n)
			}
			if failCall == 3 && n != 0 {
				t.Fatal("frame header counted as payload", n)
			}
			calls := sink.calls
			if writer.Close() != err || writer.Headers(responseFields("200")) != err {
				t.Fatal("failure not sticky")
			}
			if _, again := writer.Write([]byte("retry")); again != err {
				t.Fatal(again)
			}
			if sink.calls != calls || sink.closes != 0 {
				t.Fatal("retried failed write or sent FIN")
			}
		})
	}
	cause := errors.New("FIN failed")
	sink := &messageSink{closeErr: cause}
	writer := responseWriter(t, sink, nil)
	if err := writer.Headers(responseFields("200")); err != nil {
		t.Fatal(err)
	}
	if writer.Close() != cause || writer.Close() != cause || sink.closes != 1 {
		t.Fatal("FIN retry", sink.closes)
	}
}

func TestMessageWriterCONNECTContentLength(t *testing.T) {
	for _, status := range []string{"200", "201", "299"} {
		t.Run(status, func(t *testing.T) {
			for _, length := range []string{"0", "2", "invalid"} {
				sink := new(messageSink)
				encoder, err := NewFieldEncoder(SettingsSnapshot{MaxFieldSectionSize: 4096}, encoderLimits(), io.Discard)
				if err != nil {
					t.Fatal(err)
				}
				writer, err := NewMessageWriter(sink, encoder, 0, MessageConfig{Kind: ResponseFields, RequestMethod: "CONNECT"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				err = writer.Headers(append(responseFields(status), fs("content-length", length)))
				var message *MessageError
				if !errors.As(err, &message) || sink.Len() != 0 || sink.closes != 0 {
					t.Fatal(err, sink.Len())
				}
				if writer.Close() != err {
					t.Fatal("lost error")
				}
				// Receiving this field must still ignore it for successful CONNECT.
				incoming, err := NewMessage(MessageConfig{Kind: ResponseFields, RequestMethod: "CONNECT"})
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err = incoming.Headers(append(responseFields(status), fs("content-length", length))); err != nil {
					t.Fatal("changed receive tolerance", err)
				}
				if err = incoming.Data(7); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestMessageWriterNormalizesContentLength(t *testing.T) {
	sink := new(messageSink)
	writer := responseWriter(t, sink, nil)
	fields := append(responseFields("200"), fs("content-length", "003, 3"), fs("x-test", "kept"), fs("content-length", "3"))
	fields[len(fields)-1].NeverIndex = true
	if err := writer.Headers(fields); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if fields[1].Value != "003, 3" || fields[1].NeverIndex || len(fields) != 4 {
		t.Fatal("mutated caller fields")
	}
	reader := responseReader(t, sink.Bytes())
	event, err := reader.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, field := range event.Fields {
		if field.Name == "content-length" {
			count++
			if field.Value != "3" || !field.NeverIndex {
				t.Fatal(field)
			}
		}
	}
	if count != 1 || len(event.Fields) != 3 || event.Fields[2].Name != "x-test" {
		t.Fatal(event.Fields)
	}
}
