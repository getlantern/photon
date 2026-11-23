#!/usr/bin/env python3
"""Generate QPACK data from https://www.rfc-editor.org/rfc/rfc9204.xml.
Usage: python3 generate_static.py RFC9204_XML > ../static_table.go
Run gofmt on the result. No implementation code is imported from another QUIC.
"""
import json
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()
table = next(t for t in root.iter('table') if t.get('pn') == 'table-4')
rows = []
for row in table.findall('./tbody/tr'):
    cells = [''.join(cell.itertext()).strip() for cell in row.findall('td')]
    assert len(cells) == 3
    assert int(cells[0]) == len(rows)
    rows.append(cells[1:])
assert len(rows) == 99
print('// Code generated from RFC 9204 Appendix A with testdata/generate_static.py; DO NOT EDIT.')
print('// Source: https://www.rfc-editor.org/rfc/rfc9204.xml (Table 4).')
print('// RFC data copyright IETF Trust and the document authors; see LICENSE-RFC.')
print('package qpack\n\nvar staticTable = [...]Field{')
for i, (name, value) in enumerate(rows):
    print('    {%s, %s}, // %d' % (json.dumps(name), json.dumps(value), i))
print('}')
