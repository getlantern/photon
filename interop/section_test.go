package interop

import (
	"bytes"
	"errors"
	local "github.com/getlantern/photon/internal/qpack"
	peer "github.com/quic-go/qpack"
	"testing"
)

func TestQPACKSectionIndependentEncoder(t *testing.T) {
	var fields []peer.HeaderField
	for index := uint64(0); index < local.StaticTableLen; index++ {
		field, _ := local.Static(index)
		fields = append(fields, peer.HeaderField{Name: field.Name, Value: field.Value}, peer.HeaderField{Name: field.Name, Value: "custom-value"})
	}
	fields = append(fields, peer.HeaderField{Name: "custom-key", Value: "custom-value"}, peer.HeaderField{Name: "custom-key", Value: "second"})
	checkPeerSection(t, fields)
}
func checkPeerSection(t *testing.T, fields []peer.HeaderField) {
	t.Helper()
	var buf bytes.Buffer
	encoder := peer.NewEncoder(&buf)
	var size uint64
	for _, field := range fields {
		if err := encoder.WriteField(field); err != nil {
			t.Fatal(err)
		}
		size += uint64(len(field.Name) + len(field.Value) + 32)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	table, _ := local.NewDecoderTable(0)
	p, got, err := table.DecodeSection(buf.Bytes(), size, len(fields))
	if err != nil || p != (local.SectionPrefix{}) || len(got) != len(fields) {
		t.Fatal(p, len(got), err)
	}
	for i, field := range fields {
		if got[i].Name != field.Name || got[i].Value != field.Value || got[i].NeverIndex {
			t.Fatalf("field %d: %+v != %+v", i, got[i], field)
		}
	}
	if _, got, err := table.DecodeSection(buf.Bytes(), size-1, len(fields)); !errors.Is(err, local.ErrSectionLimit) || got != nil {
		t.Fatal("size limit", got, err)
	}
}
func FuzzQPACKSectionIndependentEncoder(f *testing.F) {
	f.Add("custom-key", "custom-value")
	f.Add(":path", "/example")
	f.Fuzz(func(t *testing.T, name, value string) {
		if len(name)+len(value) > 32768 {
			t.Skip()
		}
		checkPeerSection(t, []peer.HeaderField{{Name: name, Value: value}, {Name: name, Value: value}})
	})
}
