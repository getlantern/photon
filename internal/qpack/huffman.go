package qpack

import "errors"

var (
	ErrHuffman     = errors.New("qpack: invalid Huffman string")
	ErrStringLimit = errors.New("qpack: decoded string exceeds limit")
)

// HuffmanSize reports the encoded size, including final EOS-prefix padding.
func HuffmanSize(s string) uint64 {
	var bits uint64
	for i := 0; i < len(s); i++ {
		bits += uint64(huffmanCodes[s[i]].bits)
	}
	return (bits + 7) / 8
}

// AppendHuffman appends RFC 7541 Appendix B codes and at most seven padding bits.
func AppendHuffman(dst []byte, s string) []byte {
	var pending uint64
	var count uint8
	for i := 0; i < len(s); i++ {
		c := huffmanCodes[s[i]]
		pending = pending<<c.bits | uint64(c.code)
		count += c.bits
		for count >= 8 {
			count -= 8
			dst = append(dst, byte(pending>>count))
		}
	}
	if count != 0 {
		dst = append(dst, byte(pending<<(8-count))|byte((1<<(8-count))-1))
	}
	return dst
}

type huffmanNode struct {
	child  [2]int
	symbol int
}

var huffmanTree = buildHuffmanTree()

func buildHuffmanTree() []huffmanNode {
	nodes := []huffmanNode{{symbol: -1}}
	for symbol, c := range huffmanCodes {
		node := 0
		for bit := int(c.bits) - 1; bit >= 0; bit-- {
			branch := (c.code >> bit) & 1
			next := nodes[node].child[branch]
			if next == 0 {
				next = len(nodes)
				nodes = append(nodes, huffmanNode{symbol: -1})
				nodes[node].child[branch] = next
			}
			node = next
		}
		nodes[node].symbol = symbol
	}
	return nodes
}

// DecodeHuffman decodes a complete encoded string with an explicit byte limit.
// EOS symbols and invalid padding are rejected. Errors expose no partial result.
func DecodeHuffman(src []byte, maxDecoded int) (string, error) {
	if maxDecoded < 0 {
		return "", ErrStringLimit
	}
	var out []byte
	node, tail := 0, 0
	ones := true
	for _, octet := range src {
		for bit := 7; bit >= 0; bit-- {
			branch := (octet >> bit) & 1
			tail++
			ones = ones && branch == 1
			node = huffmanTree[node].child[branch]
			if node == 0 {
				return "", ErrHuffman
			}
			symbol := huffmanTree[node].symbol
			if symbol < 0 {
				continue
			}
			if symbol == 256 {
				return "", ErrHuffman
			}
			if len(out) == maxDecoded {
				return "", ErrStringLimit
			}
			out = append(out, byte(symbol))
			node, tail, ones = 0, 0, true
		}
	}
	if tail > 7 || !ones {
		return "", ErrHuffman
	}
	return string(out), nil
}
