package http3conn

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"testing"

	"github.com/getlantern/photon/internal/qpack"
)

func TestEarlyDataRequestNormalization(t *testing.T) {
	for _, values := range [][]string{{"1"}, {""}, {"0"}, {"false"}, {"1, 1"}, {"1", "0"}} {
		fields := requestFields("/")
		for _, value := range values {
			fields = append(fields, fs("early-data", value))
		}
		if err := ValidateFieldStructure(fields, RequestFields, false); err != nil {
			t.Fatal("validly framed request rejected", values, err)
		}
		request, _, err := ReadHTTPRequest(context.Background(), requestReader(t, readerHeaders(t, fields...), false), BodyConfig{Close: func(bool) error { return nil }})
		if err != nil {
			t.Fatal(values, err)
		}
		if got := request.Header.Values("Early-Data"); !reflect.DeepEqual(got, []string{"1"}) {
			t.Fatal("received replay bit", values, got)
		}
		request.Body.Close()
		outbound, _ := http.NewRequest(http.MethodGet, "https://example.com/", nil)
		outbound.Header["eArLy-DaTa"] = append([]string(nil), values...)
		encoded, err := HTTPRequestHeaders(outbound)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, field := range encoded {
			if field.Name == "early-data" {
				count++
				if field.Value != "1" {
					t.Fatal(field)
				}
			}
		}
		if count != 1 || !reflect.DeepEqual(outbound.Header["eArLy-DaTa"], values) {
			t.Fatal("outbound ownership/cardinality", count, outbound.Header)
		}
	}
}

func TestEarlyDataWireWriterNormalization(t *testing.T) {
	fields := append(requestFields("/"), fs("early-data", "0"), fs("x-between", "kept"), fs("early-data", "1"))
	fields[len(fields)-1].NeverIndex = true
	original := append([]qpack.DecodedField(nil), fields...)
	encoder, err := NewFieldEncoder(SettingsSnapshot{MaxFieldSectionSize: 4096}, encoderLimits(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	sink := new(messageSink)
	writer, err := NewMessageWriter(sink, encoder, 0, MessageConfig{Kind: RequestFields}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Headers(fields); err != nil {
		t.Fatal(err)
	}
	event, err := requestReader(t, sink.Bytes(), false).Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, field := range event.Fields {
		if field.Name == "early-data" {
			count++
			if field.Value != "1" || !field.NeverIndex {
				t.Fatal("lost early marker or indexing policy", field)
			}
		}
	}
	if count != 1 || !reflect.DeepEqual(fields, original) {
		t.Fatal("wire cardinality or caller mutation", count, fields)
	}
}

func TestEarlyDataForbiddenContexts(t *testing.T) {
	for _, kind := range []FieldContext{ResponseFields, TrailerFields} {
		fields := []qpack.DecodedField{fs("early-data", "1")}
		if kind == ResponseFields {
			fields = append(responseFields("200"), fields...)
		}
		var malformed *MessageError
		if err := ValidateFieldStructure(fields, kind, false); !errors.As(err, &malformed) {
			t.Fatal("forbidden field accepted", kind, err)
		}
	}
	for _, status := range []string{"103", "200"} {
		sink := new(messageSink)
		writer := responseWriter(t, sink, func(string) bool { return true })
		if err := writer.Headers(append(responseFields(status), fs("early-data", "1"))); err == nil || sink.Len() != 0 {
			t.Fatal("forbidden response emitted", status, err, sink.Len())
		}
	}
	if err := ValidateOutgoingTrailers([]qpack.DecodedField{fs("early-data", "1")}, func(string) bool { return true }); err == nil {
		t.Fatal("trailer permission bypassed prohibition")
	}
	if _, err := announcedTrailers([]string{"Early-Data"}); err == nil {
		t.Fatal("forbidden trailer announced")
	}
	request, _ := http.NewRequest(http.MethodGet, "https://example.com/", nil)
	request.Trailer = http.Header{"Early-Data": []string{"1"}}
	if _, err := HTTPRequestHeaders(request); err == nil {
		t.Fatal("outbound early-data trailer declaration accepted")
	}
}
