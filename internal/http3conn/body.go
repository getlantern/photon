package http3conn

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/getlantern/photon/internal/qpack"
)

// ParseContentLength returns a canonical decimal string, or "" when absent.
// Equal repeated/list values are accepted (RFC 9110 section 8.6). Callers that
// forward fields must replace all such occurrences with this single value.
// Keeping decimal text avoids imposing an integer limit on HEAD/304 metadata.
// Field structure must already have been validated; input strings are borrowed.
func ParseContentLength(fields []qpack.DecodedField) (string, error) {
	var length string
	for _, f := range fields {
		if f.Name != "content-length" {
			continue
		}
		for value := range strings.SplitSeq(f.Value, ",") {
			value = strings.Trim(value, " \t")
			if value == "" {
				return "", &MessageError{"empty content-length"}
			}
			for i := 0; i < len(value); i++ {
				if value[i] < '0' || value[i] > '9' {
					return "", &MessageError{"invalid content-length"}
				}
			}
			value = strings.TrimLeft(value, "0")
			if value == "" {
				value = "0"
			}
			if length != "" && length != value {
				return "", &MessageError{"conflicting content-length values"}
			}
			length = value
		}
	}
	return length, nil
}

// BodyLength validates received DATA payload octets for one message. The caller
// supplies already validated control data and fields, and calls Finish only at
// the end of that message. It does not enforce frame order or trailer semantics.
// Methods require serialization. Errors are sticky; zero value is not usable.
// Empty DATA is accepted here; frame eligibility belongs to the stream machine.
type BodyLength struct {
	length                                   string
	received                                 uint64
	noContent, tunnel, initialized, finished bool
	err                                      error
}

// NewBodyLength configures receive-side semantics. requestMethod is required for
// responses. Successful CONNECT ignores Content-Length as required by RFC 9110
// section 9.3.6; its DATA (and CONNECT request DATA) is tunnel data, not content.
// Senders must separately prohibit Content-Length on successful CONNECT responses.
func NewBodyLength(fields []qpack.DecodedField, control ControlData, kind FieldContext, requestMethod string) (*BodyLength, error) {
	if kind != RequestFields && kind != ResponseFields {
		return nil, fmt.Errorf("http3: body length requires request or response headers")
	}
	if kind == ResponseFields && (control.Status < 100 || control.Status > 599 || !validToken(requestMethod)) {
		return nil, fmt.Errorf("http3: body length requires validated response status and request method")
	}
	if kind == RequestFields && !validToken(control.Method) {
		return nil, fmt.Errorf("http3: body length requires validated request method")
	}
	b := &BodyLength{initialized: true}
	b.tunnel = kind == RequestFields && control.Method == "CONNECT" || kind == ResponseFields && requestMethod == "CONNECT" && control.Status >= 200 && control.Status < 300
	if kind == ResponseFields && b.tunnel {
		return b, nil
	}
	length, err := ParseContentLength(fields)
	if err != nil {
		return nil, err
	}
	b.length = length
	if kind == ResponseFields {
		if (control.Status < 200 || control.Status == 204) && length != "" {
			return nil, &MessageError{"content-length prohibited for response status"}
		}
		b.noContent = requestMethod == "HEAD" || control.Status < 200 || control.Status == 204 || control.Status == 304 || control.Status == 205
		if control.Status == 205 && length != "" && length != "0" {
			return nil, &MessageError{"nonzero content-length on 205 response"}
		}
	}
	return b, nil
}

// Add counts actual DATA payload bytes, not an unfulfilled advertised frame size.
// It rejects overrun before the caller delivers those bytes to the application.
func (b *BodyLength) Add(n uint64) error {
	if b.err != nil {
		return b.err
	}
	if !b.initialized || b.finished {
		return fmt.Errorf("http3: body length is not active")
	}
	if n > ^uint64(0)-b.received {
		return b.fail("DATA length overflow")
	}
	total := b.received + n
	if b.noContent && total != 0 {
		return b.fail("content in a bodyless response")
	}
	if !b.noContent && !b.tunnel && b.length != "" {
		decimal := strconv.FormatUint(total, 10)
		if len(decimal) > len(b.length) || len(decimal) == len(b.length) && decimal > b.length {
			return b.fail("DATA exceeds content-length")
		}
	}
	b.received = total
	return nil
}
func (b *BodyLength) Finish() error {
	if b.err != nil {
		return b.err
	}
	if !b.initialized {
		return fmt.Errorf("http3: body length is not initialized")
	}
	if b.finished {
		return nil
	}
	if !b.noContent && !b.tunnel && b.length != "" && strconv.FormatUint(b.received, 10) != b.length {
		return b.fail("DATA shorter than content-length")
	}
	b.finished = true
	return nil
}
func (b *BodyLength) fail(reason string) error { b.err = &MessageError{reason}; return b.err }
