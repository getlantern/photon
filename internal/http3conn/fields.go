package http3conn

import (
	"fmt"
	"strings"

	"github.com/getlantern/photon/internal/qpack"
)

const MessageErrorCode uint64 = 0x10e

// MessageError is an HTTP/3 STREAM error, not a connection error. The request
// owner resets the affected stream with MessageErrorCode. It must not pass this
// error to the critical-stream dispatcher as a connection protocol failure.
type MessageError struct{ Reason string }

func (e *MessageError) Error() string { return fmt.Sprintf("http3: malformed message: %s", e.Reason) }

type FieldContext uint8

const (
	RequestFields FieldContext = iota
	ResponseFields
	TrailerFields
)

// ValidateFieldStructure checks field octets, pseudo-header context/ordering and
// duplicates, and prohibited connection-specific fields without normalizing the
// input. It does not check required pseudo-headers, URI/method/status semantics,
// content length or trailer eligibility; a complete message validator must add
// those checks. allowProtocol admits :protocol only when its extension is enabled.
func ValidateFieldStructure(fields []qpack.DecodedField, kind FieldContext, allowProtocol bool) error {
	if kind > TrailerFields {
		return fmt.Errorf("http3: invalid field context")
	}
	regular := false
	var seen uint8
	for _, field := range fields {
		name, value := field.Name, field.Value
		if name == "" {
			return &MessageError{"empty field name"}
		}
		if !validFieldValue(value) {
			return &MessageError{"invalid field value octets"}
		}
		if name[0] == ':' {
			if regular || kind == TrailerFields {
				return &MessageError{"pseudo-header after regular field or in trailers"}
			}
			bit := uint8(0)
			if kind == RequestFields {
				switch name {
				case ":method":
					bit = 1
				case ":scheme":
					bit = 2
				case ":authority":
					bit = 4
				case ":path":
					bit = 8
				case ":protocol":
					if allowProtocol {
						bit = 16
					}
				}
			} else if name == ":status" {
				bit = 32
			}
			if bit == 0 {
				return &MessageError{"undefined pseudo-header for field context"}
			}
			if seen&bit != 0 {
				return &MessageError{"duplicate pseudo-header"}
			}
			seen |= bit
			continue
		}
		regular = true
		for i := 0; i < len(name); i++ {
			if !lowerToken(name[i]) {
				return &MessageError{"invalid or uppercase field name"}
			}
		}
		switch name {
		case "connection", "proxy-connection", "keep-alive", "transfer-encoding", "upgrade":
			return &MessageError{"connection-specific field"}
		case "early-data":
			if kind != RequestFields {
				return &MessageError{"early-data prohibited in responses and trailers"}
			}
		case "te":
			if kind != RequestFields || !strings.EqualFold(value, "trailers") {
				return &MessageError{"invalid TE field"}
			}
		}
	}
	return nil
}
func lowerToken(b byte) bool {
	if b >= 'a' && b <= 'z' || b >= '0' && b <= '9' {
		return true
	}
	switch b {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	}
	return false
}
func validFieldValue(s string) bool {
	if len(s) > 0 && (s[0] == ' ' || s[0] == '\t' || s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] == 0x7f || (s[i] < 0x20 && s[i] != '\t') {
			return false
		}
	}
	return true
}
