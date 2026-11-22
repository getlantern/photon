package wire

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestGreasedQUICBitHeaderPolicy(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		for _, typ := range []PacketType{Initial, Handshake, ZeroRTT, OneRTT, Retry} {
			var data []byte
			if typ == OneRTT {
				data = []byte{0x40, 1, 2, 3, 4}
			} else if typ == Retry {
				bits, _ := v.TypeBits(typ)
				data = []byte{0xc0 | bits<<4, byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v), 0, 0}
				data = append(data, bytes.Repeat([]byte{1}, 17)...)
			} else {
				var err error
				data, err = AppendLongHeader(nil, v, typ, nil, nil, nil, 0, 1, 16)
				if err != nil {
					t.Fatal(err)
				}
				data = append(data, make([]byte, 16)...)
			}
			expected, err := ParseHeader(data, 0)
			if err != nil {
				t.Fatal(v, typ, err)
			}
			data[0] &= ^byte(0x40)
			original := append([]byte(nil), data...)
			if _, err := ParseHeader(data, 0); !errors.Is(err, ErrHeader) {
				t.Fatal("strict parser accepted zero bit", v, typ, err)
			}
			got, err := ParseHeaderWithQUICBit(data, 0, true)
			if err != nil || !reflect.DeepEqual(expected, got) || !bytes.Equal(data, original) {
				t.Fatal("permissive parsing changed semantics/input", v, typ, got, err)
			}
			if _, err := ParseHeaderWithQUICBit(data, -1, true); typ == OneRTT && err == nil {
				t.Fatal("relaxed CID limits")
			}
		}
	}
}

func FuzzGreasedQUICBitHeader(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3}, uint8(0))
	f.Add([]byte{0x80, 0, 0, 0, 1, 0, 0, 0, 1, 0}, uint8(0))
	f.Add([]byte{0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, uint8(8))
	f.Fuzz(func(t *testing.T, data []byte, cid uint8) {
		if len(data) == 0 {
			return
		}
		strict := append([]byte(nil), data...)
		strict[0] |= 0x40
		greased := append([]byte(nil), data...)
		greased[0] &= ^byte(0x40)
		original := append([]byte(nil), greased...)
		length := int(cid%23) - 1
		expected, err1 := ParseHeader(strict, length)
		got, err2 := ParseHeaderWithQUICBit(greased, length, true)
		if !reflect.DeepEqual(expected, got) || !reflect.DeepEqual(err1, err2) || !bytes.Equal(original, greased) {
			t.Fatal("greasing changed non-bit semantics", expected, got, err1, err2)
		}
	})
}
