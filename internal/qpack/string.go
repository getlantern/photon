package qpack

import "io"

// AppendString encodes a QPACK string with a 1–7 bit length prefix. The next
// higher bit is H; flags must leave both it and the length bits clear.
// Huffman selection is explicit so the caller can apply compression policy.
func AppendString(dst []byte, flags byte, prefix uint8, s string, huffman bool) ([]byte, error) {
	if prefix < 1 || prefix > 7 || flags&byte((1<<(prefix+1))-1) != 0 {
		return dst, ErrPrefix
	}
	size := uint64(len(s))
	if huffman {
		size = HuffmanSize(s)
		flags |= 1 << prefix
	}
	out, err := AppendInteger(dst, flags, prefix, size)
	if err != nil {
		return dst, err
	}
	if huffman {
		return AppendHuffman(out, s), nil
	}
	return append(out, s...), nil
}

// ConsumeString consumes one complete string. Incomplete or invalid input
// consumes no bytes. maxDecoded bounds output bytes for both string forms.
func ConsumeString(src []byte, prefix uint8, maxDecoded int) (string, int, error) {
	if prefix < 1 || prefix > 7 {
		return "", 0, ErrPrefix
	}
	if maxDecoded < 0 {
		return "", 0, ErrStringLimit
	}
	length, n, err := ConsumeInteger(src, prefix)
	if err != nil {
		return "", 0, err
	}
	if length > uint64(len(src)-n) {
		return "", 0, io.ErrUnexpectedEOF
	}
	end := n + int(length)
	if src[0]&(1<<prefix) != 0 {
		s, err := DecodeHuffman(src[n:end], maxDecoded)
		if err != nil {
			return "", 0, err
		}
		return s, end, nil
	}
	if length > uint64(maxDecoded) {
		return "", 0, ErrStringLimit
	}
	return string(src[n:end]), end, nil
}
