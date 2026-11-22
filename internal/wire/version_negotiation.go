package wire

import "encoding/binary"

// AppendVersionNegotiation encodes a whole unprotected packet. The caller
// supplies arbitrary unused bits and already-swapped connection IDs.
func AppendVersionNegotiation(dst []byte, unused byte, dcid, scid []byte, versions []Version) ([]byte, error) {
	if len(dcid) > 255 || len(scid) > 255 || len(versions) == 0 || len(versions) > (65527-7-len(dcid)-len(scid))/4 {
		return nil, ErrHeader
	}
	for _, v := range versions {
		if v == 0 {
			return nil, ErrVersion
		}
	}
	dst = append(dst, unused|0x80, 0, 0, 0, 0, byte(len(dcid)))
	dst = append(dst, dcid...)
	dst = append(dst, byte(len(scid)))
	dst = append(dst, scid...)
	for _, v := range versions {
		dst = binary.BigEndian.AppendUint32(dst, uint32(v))
	}
	return dst, nil
}
