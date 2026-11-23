"""Generate the QPACK Huffman codebook from RFC 7541 plain text on stdin."""
import re
import sys

rows = re.findall(r'\(\s*(\d+)\)\s+([|01]+)\s+([0-9a-f]+)\s+\[\s*(\d+)\]', sys.stdin.read())
assert len(rows) == 257
print('// Code generated from RFC 7541 Appendix B; DO NOT EDIT.')
print('// Source: https://www.rfc-editor.org/rfc/rfc7541.txt')
print('// RFC data copyright IETF Trust and the document authors; see LICENSE-RFC.')
print('package qpack\n\nvar huffmanCodes = [...]struct { code uint32; bits uint8 }{')
for i, (symbol, binary, code, length) in enumerate(rows):
    bits = binary.replace('|', '')
    assert int(symbol) == i and len(bits) == int(length)
    assert int(bits, 2) == int(code, 16)
    print(f'{{0x{code}, {length}}}, // {i}')
print('}')
