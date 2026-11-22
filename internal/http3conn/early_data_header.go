package http3conn

import "github.com/getlantern/photon/internal/qpack"

// RFC 8470 section 5.1 defines a single bit. Preserve presence when forwarding,
// including invalid values or repeated fields, but never emit multiple instances.
// Structural validation must run first. Do not mutate caller-owned fields or
// weaken NeverIndex when combining instances.
func canonicalEarlyData(fields []qpack.DecodedField) []qpack.DecodedField {
	first, count, never := -1, 0, false
	for i, field := range fields {
		if field.Name != "early-data" {
			continue
		}
		if first < 0 {
			first = i
		}
		count++
		never = never || field.NeverIndex
	}
	if first < 0 || count == 1 && fields[first].Value == "1" {
		return fields
	}
	result := make([]qpack.DecodedField, 0, len(fields)-count+1)
	for i, field := range fields {
		if field.Name == "early-data" {
			if i != first {
				continue
			}
			field.Value, field.NeverIndex = "1", never
		}
		result = append(result, field)
	}
	return result
}
