package http3conn

import (
	"net"
	"strconv"
	"strings"

	"github.com/getlantern/photon/internal/qpack"
)

// ControlData is the validated control data of one field section. Authority
// falls back to Host when :authority is absent. Strings borrow the input fields.
type ControlData struct {
	Method, Scheme, Authority, Path, Protocol string
	Status                                    int
}

// ValidateControlData adds required pseudo-header and basic value checks to
// ValidateFieldStructure (RFC 9114 sections 4.3-4.5). extendedConnect must reflect
// negotiated permission, not merely local implementation support (RFC 9220).
// This does not validate full URI grammar, scheme-specific authority rules for
// non-HTTP schemes, WebSocket handshake fields, content length, or message order.
// Trailer sections receive structural checks only.
func ValidateControlData(fields []qpack.DecodedField, kind FieldContext, extendedConnect bool) (ControlData, error) {
	var out ControlData
	if err := ValidateFieldStructure(fields, kind, extendedConnect); err != nil {
		return out, err
	}
	bad := func(reason string) (ControlData, error) { return ControlData{}, &MessageError{reason} }
	if kind == TrailerFields {
		return out, nil
	}
	var scheme, path, authority, protocol, hostSeen bool
	var host, status string
	for _, f := range fields {
		switch f.Name {
		case ":method":
			out.Method = f.Value
		case ":scheme":
			out.Scheme, scheme = f.Value, true
		case ":path":
			out.Path, path = f.Value, true
		case ":authority":
			out.Authority, authority = f.Value, true
		case ":protocol":
			out.Protocol, protocol = f.Value, true
		case ":status":
			status = f.Value
		case "host":
			if kind == RequestFields {
				if hostSeen {
					return bad("duplicate Host")
				}
				host, hostSeen = f.Value, true
			}
		}
	}
	if kind == ResponseFields {
		if len(status) != 3 || status[0] < '1' || status[0] > '5' || status[1] < '0' || status[1] > '9' || status[2] < '0' || status[2] > '9' || status == "101" {
			return bad("missing or invalid :status")
		}
		out.Status = int(status[0]-'0')*100 + int(status[1]-'0')*10 + int(status[2]-'0')
		return out, nil
	}
	if !validToken(out.Method) {
		return bad("missing or invalid :method")
	}
	if authority && out.Authority == "" || hostSeen && host == "" {
		return bad("empty authority or Host")
	}
	if authority && hostSeen && out.Authority != host {
		return bad("authority and Host differ")
	}
	if !authority {
		out.Authority = host
	}
	if protocol && (out.Method != "CONNECT" || !validToken(out.Protocol)) {
		return bad("invalid extended CONNECT protocol or method")
	}
	if out.Method == "CONNECT" && !protocol {
		if scheme || path || !authority {
			return bad("ordinary CONNECT requires authority and omits scheme/path")
		}
		host, port, err := net.SplitHostPort(out.Authority)
		if err != nil || host == "" || strings.ContainsAny(host, "@/?# \t") || port == "" {
			return bad("CONNECT requires host and port")
		}
		for i := range port {
			if port[i] < '0' || port[i] > '9' {
				return bad("invalid CONNECT port")
			}
		}
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil || n == 0 {
			return bad("invalid CONNECT port")
		}
		return out, nil
	}
	if !scheme || !validScheme(out.Scheme) || !path {
		return bad("missing or invalid scheme/path")
	}
	httpScheme := strings.EqualFold(out.Scheme, "http") || strings.EqualFold(out.Scheme, "https")
	if (httpScheme || protocol) && out.Authority == "" {
		return bad("missing authority")
	}
	if httpScheme && strings.Contains(out.Authority, "@") {
		return bad("userinfo in HTTP authority")
	}
	if httpScheme && out.Path == "" {
		return bad("empty HTTP path")
	}
	if out.Path == "*" {
		if out.Method != "OPTIONS" {
			return bad("asterisk path requires OPTIONS")
		}
	} else if out.Path != "" && (out.Path[0] != '/' || strings.ContainsAny(out.Path, "# \t")) {
		return bad("invalid request path")
	}
	return out, nil
}

func validToken(s string) bool {
	if s == "" {
		return false
	}
	for i := range s {
		if !lowerToken(s[i]) && !(s[i] >= 'A' && s[i] <= 'Z') {
			return false
		}
	}
	return true
}
func validScheme(s string) bool {
	alpha := func(b byte) bool { return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' }
	if s == "" || !alpha(s[0]) {
		return false
	}
	for i := 1; i < len(s); i++ {
		b := s[i]
		if !alpha(b) && !(b >= '0' && b <= '9') && b != '+' && b != '-' && b != '.' {
			return false
		}
	}
	return true
}
