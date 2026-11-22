package interop

import (
	local "github.com/getlantern/photon/internal/qpack"
	peer "github.com/quic-go/qpack"
	"io"
	"testing"
)

func TestQPACKSelectedFieldsIndependentDecoder(t *testing.T) {
	var fields []local.DecodedField
	for id := uint64(0); id < local.StaticTableLen; id++ {
		field, _ := local.Static(id)
		fields = append(fields, local.DecodedField{Field: field}, local.DecodedField{Field: field, NeverIndex: true}, local.DecodedField{Field: local.Field{Name: field.Name, Value: "custom-value"}})
	}
	fields = append(fields, local.DecodedField{Field: local.Field{Name: "custom-key", Value: "custom-value"}})
	encoder, _ := local.NewEncoderTable(0, 0, 0, 0)
	wire, err := encoder.EncodeFields(4, fields, 65536, 65536, 512)
	if err != nil {
		t.Fatal(err)
	}
	next := peer.NewDecoder().Decode(wire)
	for i, want := range fields {
		got, err := next()
		if err != nil || got.Name != want.Name || got.Value != want.Value {
			t.Fatalf("field %d: %+v %v", i, got, err)
		}
	}
	if _, err := next(); err != io.EOF {
		t.Fatal(err)
	}
}
