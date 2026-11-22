package http3conn

import (
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/getlantern/photon/internal/qpack"
)

// CapsuleProtocol parses RFC 9297's Boolean Item Structured Field. False,
// non-Boolean, repeated and malformed values are treated as absent. Unknown
// parameters are validated and ignored using RFC 9651's bare-item grammar.
// A true result alone does not authorize capsules: the selected extension and
// successful CONNECT response must establish their use.
func CapsuleProtocol(header http.Header) bool {
	var value string
	count := 0
	for name, values := range header {
		if strings.EqualFold(name, "Capsule-Protocol") {
			count += len(values)
			if count > 1 {
				return false
			}
			if len(values) == 1 {
				value = values[0]
			}
		}
	}
	return count == 1 && capsuleBoolean(value)
}

// capsuleBoolean only needs to recognize a true Item, but must validate all
// ignored parameters before accepting it. Parsing is linear and allocation-free.
func capsuleBoolean(value string) bool {
	value = strings.Trim(value, " ")
	if !strings.HasPrefix(value, "?1") {
		return false
	}
	value = value[2:]
	for len(value) > 0 && value[0] == ';' {
		value = strings.TrimLeft(value[1:], " ")
		if len(value) == 0 || !(value[0] >= 'a' && value[0] <= 'z' || value[0] == '*') {
			return false
		}
		i := 1
		for i < len(value) && sfKey(value[i]) {
			i++
		}
		value = value[i:]
		if len(value) > 0 && value[0] == '=' {
			n := sfBareLength(value[1:])
			if n == 0 {
				return false
			}
			value = value[1+n:]
		}
	}
	return strings.Trim(value, " ") == ""
}
func sfKey(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.' || c == '*'
}
func sfBareLength(s string) int {
	if len(s) == 0 {
		return 0
	}
	switch s[0] {
	case '?':
		if len(s) >= 2 && (s[1] == '0' || s[1] == '1') {
			return 2
		}
		return 0
	case '"':
		for i := 1; i < len(s); i++ {
			if s[i] == '"' {
				return i + 1
			}
			if s[i] < 0x20 || s[i] > 0x7e {
				return 0
			}
			if s[i] == '\\' {
				i++
				if i == len(s) || s[i] != '\\' && s[i] != '"' {
					return 0
				}
			}
		}
		return 0
	case ':':
		end := strings.IndexByte(s[1:], ':')
		if end < 0 {
			return 0
		}
		end++
		data := s[1:end]
		digits := 0
		for digits < len(data) && sfBase64(data[digits]) {
			digits++
		}
		padding := len(data) - digits
		for i := digits; i < len(data); i++ {
			if data[i] != '=' {
				return 0
			}
		}
		remainder := digits % 4
		if remainder == 1 || padding > 2 || remainder == 0 && padding != 0 || remainder != 0 && padding > 4-remainder {
			return 0
		}
		return end + 1
	case '@':
		n := sfNumberLength(s[1:], false)
		if n == 0 {
			return 0
		}
		return n + 1
	case '%':
		return sfDisplayLength(s)
	}
	if s[0] == '-' || s[0] >= '0' && s[0] <= '9' {
		return sfNumberLength(s, true)
	}
	if !(s[0] >= 'a' && s[0] <= 'z' || s[0] >= 'A' && s[0] <= 'Z' || s[0] == '*') {
		return 0
	}
	i := 1
	for i < len(s) && (lowerToken(s[i]) || s[i] >= 'A' && s[i] <= 'Z' || s[i] == ':' || s[i] == '/') {
		i++
	}
	return i
}
func sfBase64(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '+' || c == '/'
}
func sfNumberLength(s string, decimal bool) int {
	i := 0
	if len(s) > 0 && s[0] == '-' {
		i++
	}
	start := i
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	digits := i - start
	if digits == 0 || digits > 15 {
		return 0
	}
	if i < len(s) && s[i] == '.' {
		if !decimal || digits > 12 {
			return 0
		}
		i++
		start = i
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		if i-start < 1 || i-start > 3 {
			return 0
		}
	}
	return i
}
func sfHex(c byte) (byte, bool) {
	if c >= '0' && c <= '9' {
		return c - '0', true
	}
	if c >= 'a' && c <= 'f' {
		return c - 'a' + 10, true
	}
	return 0, false
}
func sfDisplayLength(s string) int {
	if len(s) < 2 || s[1] != '"' {
		return 0
	}
	var runeBytes [utf8.UTFMax]byte
	used := 0
	for i := 2; i < len(s); i++ {
		c := s[i]
		if c == '"' {
			if used == 0 {
				return i + 1
			}
			return 0
		}
		if c < 0x20 || c > 0x7e {
			return 0
		}
		if c == '%' {
			if len(s)-i < 3 {
				return 0
			}
			hi, ok1 := sfHex(s[i+1])
			lo, ok2 := sfHex(s[i+2])
			if !ok1 || !ok2 {
				return 0
			}
			c = hi<<4 | lo
			i += 2
		}
		if used == 0 && c < utf8.RuneSelf {
			continue
		}
		runeBytes[used] = c
		used++
		if utf8.FullRune(runeBytes[:used]) {
			_, n := utf8.DecodeRune(runeBytes[:used])
			if n == 1 {
				return 0
			}
			used = 0
		}
	}
	return 0
}

// validateCapsuleFields is used only when an endpoint explicitly selects a
// capsule extension. Rejection responses may carry normal HTTP content, but
// cannot include Capsule-Protocol. Successful capsule streams cannot carry
// content metadata or use the excluded 204/205/206 response statuses.
func validateCapsuleFields(fields []qpack.DecodedField, control ControlData, config MessageConfig) error {
	successful := config.Kind == RequestFields || control.Status >= 200 && control.Status < 300
	if !successful {
		for _, field := range fields {
			if field.Name == "capsule-protocol" {
				return &MessageError{"capsule-protocol on unsuccessful response"}
			}
		}
		return nil
	}
	if config.Kind == RequestFields {
		if control.Method != "CONNECT" || control.Protocol == "" {
			return &MessageError{"capsules require Extended CONNECT"}
		}
	} else if config.RequestMethod != "CONNECT" || config.RequestProtocol == "" {
		return &MessageError{"capsules require Extended CONNECT"}
	}
	if config.Kind == ResponseFields && (control.Status == 204 || control.Status == 205 || control.Status == 206) {
		return &MessageError{"response status prohibited for capsules"}
	}
	for _, field := range fields {
		switch field.Name {
		case "content-length", "content-type", "transfer-encoding":
			return &MessageError{"content metadata prohibited for capsules"}
		}
	}
	return nil
}
