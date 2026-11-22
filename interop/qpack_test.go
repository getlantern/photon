package interop

import (
	"fmt"
	"io"
	"testing"

	local "github.com/getlantern/photon/internal/qpack"
	peer "github.com/quic-go/qpack"
)

// Exercise every static reference through an independent field-section decoder.
// This is table compatibility evidence, not a complete QPACK endpoint test.
func TestQPACKStaticTableIndependentDecoder(t *testing.T) {
	for index := uint64(0); index < local.StaticTableLen; index++ {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			want, err := local.Static(index)
			if err != nil {
				t.Fatal(err)
			}
			// Zero Required Insert Count and Delta Base, then indexed static field.
			block, err := local.AppendInteger([]byte{0, 0}, 0xc0, 6, index)
			if err != nil {
				t.Fatal(err)
			}
			next := peer.NewDecoder().Decode(block)
			field, err := next()
			if err != nil {
				t.Fatal(err)
			}
			if field.Name != want.Name || field.Value != want.Value {
				t.Fatalf("index %d: peer decoded %+v; want %+v", index, field, want)
			}
			if _, err := next(); err != io.EOF {
				t.Fatalf("trailing field: %v", err)
			}
		})
	}
	block, err := local.AppendInteger([]byte{0, 0}, 0xc0, 6, local.StaticTableLen)
	if err != nil {
		t.Fatal(err)
	}
	next := peer.NewDecoder().Decode(block)
	if field, err := next(); err == nil {
		t.Fatalf("peer accepted index beyond static table: %+v", field)
	}
}
