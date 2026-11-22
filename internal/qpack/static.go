package qpack

import "errors"

const StaticTableLen = 99

var ErrStaticIndex = errors.New("qpack: invalid static table index")

// Static returns a copy of the RFC 9204 Appendix A entry at the zero-based index.
func Static(index uint64) (Field, error) {
	if index >= uint64(len(staticTable)) {
		return Field{}, ErrStaticIndex
	}
	return staticTable[index], nil
}

// Reverse indexes are initialized once and never mutated after publication.
var staticExact, staticNames = buildStaticIndexes()

func buildStaticIndexes() (map[Field]uint64, map[string]uint64) {
	exact := make(map[Field]uint64, len(staticTable))
	names := make(map[string]uint64)
	for i, f := range staticTable {
		if _, ok := exact[f]; !ok {
			exact[f] = uint64(i)
		}
		if _, ok := names[f.Name]; !ok {
			names[f.Name] = uint64(i)
		}
	}
	return exact, names
}

// FindStatic prefers an exact name/value match, then the lowest index with the
// same name. Inputs are byte-exact; HTTP name validation/normalization belongs
// to the HTTP layer. Sensitive fields still require caller policy for indexing.
func FindStatic(f Field) (index uint64, exact, found bool) {
	if id, ok := staticExact[f]; ok {
		return id, true, true
	}
	id, ok := staticNames[f.Name]
	return id, false, ok
}
