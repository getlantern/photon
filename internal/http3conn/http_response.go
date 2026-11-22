package http3conn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

// HTTPResponseConfig supplies endpoint policy and bounds interim responses.
// Body.Close owns cleanup once this call starts consuming frames, including on
// failure before final headers. Information receives an independent header map.
// TLS state, decompression, redirects, retries and tracing remain endpoint work.
type HTTPResponseConfig struct {
	Body             BodyConfig
	MaxInformational int
	Information      func(status int, headers http.Header) error
}

// ReadHTTPResponse consumes informational responses and returns final headers plus
// a streaming Body. The reader must be new and configured for request.Method.
// The caller must close a returned Body. Trailer values are populated while Body
// is read and must not be accessed concurrently with reads. No fields are merged
// from trailers into Header. Content-Length values too large for int64 stay intact
// in Header and use -1 in ContentLength; MessageReader still validates exact length.
func ReadHTTPResponse(ctx context.Context, request *http.Request, reader *MessageReader, config HTTPResponseConfig) (response *http.Response, err error) {
	if ctx == nil || request == nil || reader == nil || config.Body.Close == nil || config.MaxInformational < 0 || reader.message.config.Kind != ResponseFields || reader.message.config.RequestMethod != request.Method || reader.message.body != nil || reader.err != nil {
		return nil, fmt.Errorf("http3: invalid HTTP response adapter configuration")
	}
	transferred := false
	defer func() {
		if !transferred {
			config.Body.Close(false)
		}
	}()
	interim := 0
	for {
		event, e := reader.Next(ctx)
		// A read deadline interrupted the skipping of a frame in an earlier call.
		if errors.Is(e, h3.ErrPayloadPending) {
			if _, e := io.Copy(io.Discard, reader); e != nil {
				return nil, e
			}
			continue
		}
		if e != nil {
			return nil, e
		}
		switch event.Header.Type {
		case h3.Headers:
			headers, e := responseHeaderMap(event.Fields, request.Method == "CONNECT" && event.Control.Status >= 200 && event.Control.Status < 300)
			if e != nil {
				return nil, e
			}
			if event.Section == InformationalSection {
				interim++
				if interim > config.MaxInformational {
					return nil, &h3.Error{Code: h3.ExcessiveLoad, Reason: "informational response limit"}
				}
				if config.Information != nil {
					if e := config.Information(event.Control.Status, headers); e != nil {
						return nil, e
					}
				}
				continue
			}
			if event.Section != InitialSection {
				return nil, &h3.Error{Code: h3.FrameUnexpected, Reason: "response starts with trailers"}
			}
			trailers, e := announcedTrailers(headers.Values("Trailer"))
			if e != nil {
				return nil, e
			}
			headers.Del("Trailer")
			length := int64(-1)
			if text := headers.Get("Content-Length"); text != "" {
				if parsed, e := strconv.ParseInt(text, 10, 64); e == nil {
					length = parsed
				}
			}
			if reader.message.body.noContent && headers.Get("Content-Length") == "" {
				length = 0
			}
			response = &http.Response{StatusCode: event.Control.Status, Status: strconv.Itoa(event.Control.Status) + " " + http.StatusText(event.Control.Status), Proto: "HTTP/3.0", ProtoMajor: 3, ProtoMinor: 0, Header: headers, Trailer: trailers, ContentLength: length, Request: request}
			bodyConfig := config.Body
			bodyConfig.Trailers = func(fields []qpack.DecodedField) error {
				if config.Body.Trailers != nil {
					if e := config.Body.Trailers(fields); e != nil {
						return e
					}
				}
				for _, field := range fields {
					response.Trailer.Add(field.Name, field.Value)
				}
				return nil
			}
			response.Body, e = NewMessageBody(ctx, reader, bodyConfig)
			if e != nil {
				return nil, e
			}
			transferred = true
			return response, nil
		case h3.PushPromise:
			if config.Body.Push == nil {
				return nil, &h3.Error{Code: h3.IDError, Reason: "push promise without permission"}
			}
			if e := config.Body.Push(ctx, event, reader); e != nil {
				return nil, e
			}
		default:
			if _, e := io.Copy(io.Discard, reader); e != nil {
				return nil, e
			}
		}
	}
}
func responseHeaderMap(fields []qpack.DecodedField, ignoreLength bool) (http.Header, error) {
	if !ignoreLength {
		var err error
		fields, err = outgoingContentLength(fields)
		if err != nil {
			return nil, err
		}
	}
	result := make(http.Header)
	for _, field := range fields {
		if strings.HasPrefix(field.Name, ":") || ignoreLength && field.Name == "content-length" {
			continue
		}
		result.Add(field.Name, field.Value)
	}
	return result, nil
}
func announcedTrailers(values []string) (http.Header, error) {
	result := make(http.Header)
	for _, value := range values {
		for name := range strings.SplitSeq(value, ",") {
			name = strings.ToLower(strings.TrimSpace(name))
			fields := []qpack.DecodedField{{Field: qpack.Field{Name: name}}}
			if err := ValidateFieldStructure(fields, TrailerFields, false); err != nil {
				return nil, err
			}
			if err := validateTrailerRouting(fields); err != nil {
				return nil, err
			}
			result[http.CanonicalHeaderKey(name)] = nil
		}
	}
	return result, nil
}
