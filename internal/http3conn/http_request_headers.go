package http3conn

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/getlantern/photon/internal/qpack"
)

// HTTPRequestHeaders constructs validated outbound HTTPS request fields without
// reading Body or changing request. Request.Host and ContentLength are authoritative;
// corresponding Header entries are ignored. Zero length with a non-nil Body means
// unknown length, as in net/http. Trailer names come from Request.Trailer; their
// values are read later, after Body EOF. Ordinary CONNECT is supported; extended
// CONNECT uses context metadata and requires a separate session negotiation gate.
// Opaque URLs and Unicode authorities require separate endpoint support.
// Body closure, trailer permission, dialing, compression and retries belong to
// the client owner. This function does not synthesize User-Agent or Accept-Encoding.
func HTTPRequestHeaders(request *http.Request) ([]qpack.DecodedField, error) {
	if request == nil || request.URL == nil || request.RequestURI != "" || request.URL.Opaque != "" || !strings.EqualFold(request.URL.Scheme, "https") || request.ContentLength < -1 {
		return nil, fmt.Errorf("http3: invalid outbound HTTPS request")
	}
	protocol := ExtendedConnectProtocol(request)
	if protocol != "" && (request.Method != "CONNECT" || !validToken(protocol)) {
		return nil, fmt.Errorf("http3: invalid Extended CONNECT metadata")
	}
	method := request.Method
	if method == "" {
		method = "GET"
	}
	authority := request.Host
	if authority == "" {
		authority = request.URL.Host
	}
	if err := outboundAuthority(authority); err != nil {
		return nil, err
	}
	if len(request.TransferEncoding) > 1 || len(request.TransferEncoding) == 1 && request.TransferEncoding[0] != "chunked" {
		return nil, fmt.Errorf("http3: unsupported request transfer encoding")
	}
	fields := []qpack.DecodedField{{Field: qpack.Field{Name: ":method", Value: method}}, {Field: qpack.Field{Name: ":authority", Value: authority}}}
	if method != "CONNECT" || protocol != "" {
		path := request.URL.EscapedPath()
		if path == "" {
			path = "/"
		}
		if request.URL.ForceQuery || request.URL.RawQuery != "" {
			path += "?" + request.URL.RawQuery
		}
		if strings.Contains(path, "#") {
			return nil, &MessageError{"fragment in request target"}
		}
		fields = append(fields, qpack.DecodedField{Field: qpack.Field{Name: ":scheme", Value: "https"}}, qpack.DecodedField{Field: qpack.Field{Name: ":path", Value: path}})
	}
	if protocol != "" {
		fields = append(fields, qpack.DecodedField{Field: qpack.Field{Name: ":protocol", Value: protocol}})
	}
	noBody := request.Body == nil || request.Body == http.NoBody
	if noBody && request.ContentLength > 0 {
		return nil, fmt.Errorf("http3: positive content length without body")
	}
	if method != "CONNECT" {
		length := request.ContentLength
		if noBody {
			length = 0
		}
		if length > 0 || noBody && (method == "POST" || method == "PUT" || method == "PATCH") {
			fields = append(fields, qpack.DecodedField{Field: qpack.Field{Name: "content-length", Value: strconv.FormatInt(length, 10)}})
		}
	}
	keys := make([]string, 0, len(request.Header))
	hasAuthorization := false
	for key := range request.Header {
		keys = append(keys, key)
		if strings.EqualFold(key, "Authorization") {
			hasAuthorization = true
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		name := strings.ToLower(key)
		switch name {
		case "host", "content-length", "trailer":
			continue
		}
		for _, value := range request.Header[key] {
			fields = append(fields, qpack.DecodedField{Field: qpack.Field{Name: name, Value: value}, NeverIndex: name == "authorization" || name == "proxy-authorization" || name == "cookie"})
		}
	}
	if request.URL.User != nil && !hasAuthorization {
		password, _ := request.URL.User.Password()
		value := "Basic " + base64.StdEncoding.EncodeToString([]byte(request.URL.User.Username()+":"+password))
		fields = append(fields, qpack.DecodedField{Field: qpack.Field{Name: "authorization", Value: value}, NeverIndex: true})
	}
	names := make([]string, 0, len(request.Trailer))
	seen := make(map[string]bool)
	for key := range request.Trailer {
		name := strings.ToLower(key)
		if !seen[name] {
			names = append(names, name)
			seen[name] = true
		}
	}
	sort.Strings(names)
	if len(names) > 0 {
		declaration := strings.Join(names, ", ")
		if _, err := announcedTrailers([]string{declaration}); err != nil {
			return nil, err
		}
		// Validate the exact map keys as well: a key containing a comma must not
		// be reinterpreted as two declared trailer names.
		for _, name := range names {
			if !validToken(name) {
				return nil, &MessageError{"invalid request trailer name"}
			}
		}
		fields = append(fields, qpack.DecodedField{Field: qpack.Field{Name: "trailer", Value: declaration}})
	}
	control, err := ValidateControlData(fields, RequestFields, protocol != "")
	if err != nil {
		return nil, err
	}
	if _, err := NewBodyLength(fields, control, RequestFields, ""); err != nil {
		return nil, err
	}
	return canonicalEarlyData(fields), nil
}
func outboundAuthority(authority string) error {
	if authority == "" {
		return &MessageError{"empty request authority"}
	}
	for i := 0; i < len(authority); i++ {
		if authority[i] <= 0x20 || authority[i] >= 0x7f {
			return &MessageError{"request authority requires ASCII without whitespace"}
		}
	}
	parsed, err := url.Parse("//" + authority)
	if err != nil || parsed.Host != authority || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return &MessageError{"invalid request authority"}
	}
	return nil
}
