package http3conn

import (
	"errors"
	"reflect"
	"testing"

	"github.com/getlantern/photon/internal/qpack"
)

func fs(name, value string) qpack.DecodedField {
	return qpack.DecodedField{Field: qpack.Field{Name: name, Value: value}}
}
func TestFieldStructure(t *testing.T) {
	for _, c := range []struct {
		kind   FieldContext
		fields []qpack.DecodedField
	}{
		{RequestFields, []qpack.DecodedField{fs(":method", "GET"), fs(":scheme", "https"), fs(":path", "/"), fs("te", "trailers"), fs("cookie", "a=b"), fs("cookie", "c=d")}},
		{ResponseFields, []qpack.DecodedField{fs(":status", "200"), fs("x-value", "a\tb\x80")}},
		{TrailerFields, []qpack.DecodedField{fs("x-trailer", "complete")}},
	} {
		before := append([]qpack.DecodedField(nil), c.fields...)
		if err := ValidateFieldStructure(c.fields, c.kind, false); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, c.fields) {
			t.Fatal("mutated fields")
		}
	}
	for _, c := range []struct {
		kind   FieldContext
		fields []qpack.DecodedField
	}{
		{RequestFields, []qpack.DecodedField{fs("X-Name", "value")}},
		{RequestFields, []qpack.DecodedField{fs("bad name", "value")}},
		{RequestFields, []qpack.DecodedField{fs("x", "a\r\nb")}},
		{RequestFields, []qpack.DecodedField{fs("x", " value")}},
		{RequestFields, []qpack.DecodedField{fs("x", "value\t")}},
		{RequestFields, []qpack.DecodedField{fs("x", "value"), fs(":path", "/")}},
		{RequestFields, []qpack.DecodedField{fs(":method", "GET"), fs(":method", "POST")}},
		{RequestFields, []qpack.DecodedField{fs(":status", "200")}},
		{ResponseFields, []qpack.DecodedField{fs(":path", "/")}},
		{TrailerFields, []qpack.DecodedField{fs(":status", "200")}},
		{RequestFields, []qpack.DecodedField{fs(":protocol", "websocket")}},
		{RequestFields, []qpack.DecodedField{fs("connection", "close")}},
		{ResponseFields, []qpack.DecodedField{fs("te", "trailers")}},
		{RequestFields, []qpack.DecodedField{fs("te", "gzip, trailers")}},
	} {
		var malformed *MessageError
		if err := ValidateFieldStructure(c.fields, c.kind, false); !errors.As(err, &malformed) {
			t.Fatal(c, err)
		}
	}
	if err := ValidateFieldStructure([]qpack.DecodedField{fs(":protocol", "websocket")}, RequestFields, true); err != nil {
		t.Fatal(err)
	}
}
func TestFieldOctetBoundaries(t *testing.T) {
	for b := 0; b < 256; b++ {
		value := "a" + string([]byte{byte(b)}) + "b"
		valid := b == 9 || b >= 32 && b != 127
		if got := validFieldValue(value); got != valid {
			t.Fatalf("value byte %d: %t", b, got)
		}
	}
}
func FuzzFieldStructure(f *testing.F) {
	f.Add("x-name", "value", uint8(0))
	f.Fuzz(func(t *testing.T, name, value string, kind uint8) {
		fields := []qpack.DecodedField{fs(name, value)}
		err := ValidateFieldStructure(fields, FieldContext(kind%3), false)
		if fields[0].Name != name || fields[0].Value != value {
			t.Fatal("mutated")
		}
		if err == nil && name != "" && name[0] != ':' {
			for i := range name {
				if !lowerToken(name[i]) {
					t.Fatal("accepted name")
				}
			}
			if !validFieldValue(value) {
				t.Fatal("accepted value")
			}
		}
	})
}
