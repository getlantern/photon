package wire

import (
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

func TestVersionInformationParameter(t *testing.T) {
	for _, server := range []bool{false, true} {
		p := DefaultTransportParameters()
		p.InitialSourceConnectionID = []byte{}
		if server {
			p.OriginalDestinationConnectionID = []byte{}
		}
		p.VersionInformation = &VersionInformation{Chosen: Version2, Available: []Version{Version2, Version1, 0x1a2a3a4a}}
		encoded, err := p.Marshal(server)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := ParseTransportParameters(encoded, server)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(parsed.VersionInformation, p.VersionInformation) {
			t.Fatal(parsed.VersionInformation)
		}
		clear(encoded)
		if parsed.VersionInformation.Chosen != Version2 || parsed.VersionInformation.Available[1] != Version1 {
			t.Fatal("borrowed input")
		}
	}
}
func TestVersionInformationValidation(t *testing.T) {
	values := [][]byte{nil, {0, 0, 0}, {0, 0, 0, 1, 0}, {0, 0, 0, 0}, {0, 0, 0, 1, 0, 0, 0, 0}}
	for _, server := range []bool{false, true} {
		base := []byte{15, 0}
		if server {
			base = append(base, 0, 0)
		}
		for _, value := range values {
			b := appendParameter(append([]byte(nil), base...), 0x11, value)
			if _, err := ParseTransportParameters(b, server); !errors.Is(err, ErrTransportParameter) {
				t.Fatal(server, value, err)
			}
		}
		// Only servers may send an empty available list or omit their chosen version.
		for _, value := range [][]byte{{0, 0, 0, 1}, {0, 0, 0, 1, 0x6b, 0x33, 0x43, 0xcf}} {
			_, err := ParseTransportParameters(appendParameter(append([]byte(nil), base...), 0x11, value), server)
			if server && err != nil || !server && !errors.Is(err, ErrTransportParameter) {
				t.Fatal(server, value, err)
			}
		}
		v := binary.BigEndian.AppendUint32(nil, uint32(Version1))
		v = binary.BigEndian.AppendUint32(v, uint32(Version1))
		b := appendParameter(appendParameter(append([]byte(nil), base...), 0x11, v), 0x11, v)
		if _, err := ParseTransportParameters(b, server); !errors.Is(err, ErrTransportParameter) {
			t.Fatal("duplicate accepted", err)
		}
	}
}
