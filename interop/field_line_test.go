package interop

import (
	local "github.com/getlantern/photon/internal/qpack"
	peer "github.com/quic-go/qpack"
	"io"
	"testing"
)

func TestQPACKFieldLinesIndependentDecoder(t *testing.T) {
	check := func(v local.FieldLine, want local.Field) {
		t.Helper()
		encoder, err := local.NewEncoderTable(0, 0, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := encoder.EncodeSection(4, 0, []local.FieldLine{v}, 1024, 1024, 1)
		if err != nil {
			t.Fatal(err)
		}
		next := peer.NewDecoder().Decode(wire)
		got, err := next()
		if err != nil || got.Name != want.Name || got.Value != want.Value {
			t.Fatalf("%+v: %+v %v want %+v", v, got, err, want)
		}
		if _, err := next(); err != io.EOF {
			t.Fatal("trailing field", err)
		}
	}
	for id := uint64(0); id < local.StaticTableLen; id++ {
		field, _ := local.Static(id)
		check(local.FieldLine{Type: local.IndexedField, Static: true, Index: id}, field)
		for _, never := range []bool{false, true} {
			for _, h := range []bool{false, true} {
				check(local.FieldLine{Type: local.LiteralNameReference, Static: true, Index: id, NeverIndex: never, Value: "custom-value", HuffmanValue: h}, local.Field{Name: field.Name, Value: "custom-value"})
			}
		}
	}
	for _, never := range []bool{false, true} {
		for _, hn := range []bool{false, true} {
			for _, hv := range []bool{false, true} {
				check(local.FieldLine{Type: local.LiteralName, Name: "custom-key", Value: "custom-value", NeverIndex: never, HuffmanName: hn, HuffmanValue: hv}, local.Field{Name: "custom-key", Value: "custom-value"})
			}
		}
	}
}
