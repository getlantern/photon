package wire

import (
	"bytes"
	"errors"
	"testing"
)

func TestMultipathParameter(t *testing.T) {
	for _, server := range []bool{false, true} {
		for _, value := range []uint64{0, 1, 63, 64, 1<<32 - 1} {
			p := DefaultTransportParameters()
			p.InitialSourceConnectionID = []byte{1}
			if server {
				p.OriginalDestinationConnectionID = []byte{2}
			}
			p.InitialMaxPathID = &value
			encoded, err := p.Marshal(server)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ParseTransportParameters(encoded, server)
			if err != nil || got.InitialMaxPathID == nil || *got.InitialMaxPathID != value {
				t.Fatalf("value %d: %#v %v", value, got.InitialMaxPathID, err)
			}
			clear(encoded)
			if *got.InitialMaxPathID != value {
				t.Fatal("parameter borrows input")
			}
			p.InitialMaxPathID = nil
			encoded, _ = p.Marshal(server)
			got, err = ParseTransportParameters(encoded, server)
			if err != nil || got.InitialMaxPathID != nil {
				t.Fatal("absence became advertisement", err)
			}
		}
	}
	p := DefaultTransportParameters()
	p.InitialSourceConnectionID = []byte{1}
	zero := uint64(0)
	p.InitialMaxPathID = &zero
	b, err := p.Marshal(false)
	if err != nil || !bytes.Equal(b, []byte{15, 1, 1, 0x3e, 1, 0}) {
		t.Fatalf("zero encoding %x %v", b, err)
	}
	// Nonminimal integer values are allowed; unlike frame types, TP IDs also
	// need not be minimally encoded.
	got, err := ParseTransportParameters([]byte{15, 1, 1, 0x40, 0x3e, 2, 0x40, 0}, false)
	if err != nil || got.InitialMaxPathID == nil || *got.InitialMaxPathID != 0 {
		t.Fatal("nonminimal parameter", err)
	}
}

func TestMultipathParameterRejectsInvalid(t *testing.T) {
	for _, s := range []string{"3e00", "3e0140", "3e020000", "3e01003e0101", "3e08c000000100000000"} {
		b := append([]byte{15, 1, 1}, decodeHex(t, s)...)
		_, err := ParseTransportParameters(b, false)
		var pe *ParameterError
		if !errors.As(err, &pe) || pe.ID != 0x3e {
			t.Fatalf("%s: %v", s, err)
		}
	}
	for _, v := range []uint64{1 << 32, MaxVarint, ^uint64(0)} {
		p := DefaultTransportParameters()
		p.InitialSourceConnectionID = []byte{1}
		p.InitialMaxPathID = &v
		if _, err := p.Marshal(false); !errors.Is(err, ErrTransportParameter) {
			t.Fatalf("accepted %d: %v", v, err)
		}
	}
}
