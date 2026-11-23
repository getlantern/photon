package http3conn

import (
	"fmt"

	"github.com/getlantern/photon/internal/qpack"
)

// ValidateOutgoingTrailers requires affirmative permission from each field's
// definition (RFC 9110 section 6.5.1). permits must reflect definitions understood
// by the application, not the presence of names in a peer's Trailer header.
// It is required even for an empty section. This checks fields only; message
// status, CONNECT state and placement must also permit a trailer section.
// Unknown incoming trailers may be kept separately or discarded; they must not
// be merged into headers without an explicit field-specific merge definition.
func ValidateOutgoingTrailers(fields []qpack.DecodedField, permits func(string) bool) error {
	if permits == nil {
		return fmt.Errorf("http3: trailer field permission policy is required")
	}
	if err := ValidateFieldStructure(fields, TrailerFields, false); err != nil {
		return err
	}
	if err := validateTrailerRouting(fields); err != nil {
		return err
	}
	for _, field := range fields {
		if !permits(field.Name) {
			return &MessageError{"field definition does not permit trailers"}
		}
	}
	return nil
}

// Receiving these fields as trailers must never alter framing or routing.
// General field eligibility on sending is enforced by the definition policy.
func validateTrailerRouting(fields []qpack.DecodedField) error {
	for _, field := range fields {
		switch field.Name {
		case "content-length", "host", "trailer", "early-data":
			return &MessageError{"framing or routing field in trailers"}
		}
	}
	return nil
}
